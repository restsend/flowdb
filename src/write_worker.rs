use crate::block_meta_index::BlockMetaIndex;
use crate::error::{FlowError, Result};
use crate::manifest::{Manifest, ManifestEntry, SstInfo};
use crate::memtable::MemTables;
use crate::record::{Config, InternalRecord, SyncMode};
use crate::sstable::SstWriter;
use crate::stats::StatsCounters;
use crate::storage::StorageBackend;
use crate::wal::Wal;
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

// ── WriteWorker (the actual WAL + memtable + flush logic) ────────────

pub(crate) struct WriteWorker {
    config: Config,
    pub(crate) wal: Wal,
    pub(crate) memtables: Arc<MemTables>,
    manifest: Arc<parking_lot::Mutex<Manifest>>,
    index: Arc<parking_lot::RwLock<BlockMetaIndex>>,
    stats: Arc<StatsCounters>,
    storage: Arc<dyn StorageBackend>,
}

impl WriteWorker {
    pub fn new(
        config: Config,
        wal: Wal,
        memtables: Arc<MemTables>,
        manifest: Arc<parking_lot::Mutex<Manifest>>,
        index: Arc<parking_lot::RwLock<BlockMetaIndex>>,
        stats: Arc<StatsCounters>,
        storage: Arc<dyn StorageBackend>,
    ) -> Self {
        Self {
            config,
            wal,
            memtables,
            manifest,
            index,
            stats,
            storage,
        }
    }

    #[allow(dead_code)]
    pub fn flush_wal(&mut self) -> Result<()> {
        self.wal.flush()
    }

    /// fsync the WAL and its parent directory so all acknowledged writes
    /// are durable on disk.  Called during `Engine::close` / `shutdown`.
    pub fn sync_wal(&mut self) -> Result<()> {
        self.wal.sync_all()
    }

    /// Core write: WAL + memtable insert (no fsync). Used by both
    /// `process_batch_encoded` (single) and `process_batch_group` (batched).
    fn process_batch_inner(
        &mut self,
        records: Vec<InternalRecord>,
        wal_buf: &[u8],
        mem_bytes: u64,
        num_records: u64,
        batch_max_seq: u64,
    ) -> Result<()> {
        self.wal.write_encoded(wal_buf, batch_max_seq)?;

        {
            let mut active = self.memtables.active_for_batch();
            for rec in records {
                active.insert(rec);
            }
        }

        self.stats.add_wal_bytes(mem_bytes);
        self.stats.records_written(num_records, mem_bytes);
        Ok(())
    }

    fn update_stats(&mut self) {
        let (rec_count, byte_count) = self.memtables.active_stats();
        self.stats.set_memtable(rec_count, byte_count);
        self.stats.set_frozen_count(self.memtables.frozen_count());
    }

    /// Process multiple submissions together with a single WAL fsync.
    pub fn process_batch_group(&mut self, submissions: &[Arc<Submission>]) -> Result<()> {
        while self.memtables.frozen_backpressure() {
            self.do_flush()?;
        }

        // WAL write for each batch (buffered I/O, no fsync yet).
        for sub in submissions {
            self.process_batch_inner(
                sub.records.clone(),
                &sub.wal_buf,
                sub.mem_bytes,
                sub.num_records,
                sub.batch_max_seq,
            )?;
        }

        // Single fsync if ANY batch requires synchronous durability.
        if submissions.iter().any(|s| s.sync_mode == SyncMode::Always) {
            self.wal.sync_all()?;
        }

        self.update_stats();

        if self.memtables.should_flush() {
            self.do_flush()?;
        }

        Ok(())
    }

    pub fn do_flush(&mut self) -> Result<()> {
        let _did_freeze = self.memtables.freeze();

        // Atomically move the oldest frozen memtable into the *flushing*
        // list (still queryable) and collect its sorted records.
        let all_records = match self.memtables.pop_frozen_to_flushing() {
            Some(r) => r,
            None => return Ok(()),
        };

        let start = std::time::Instant::now();
        let now_us = now_micros();
        let mut all_records = all_records;
        all_records.retain(|r| r.expire_at > now_us);

        if all_records.is_empty() {
            self.memtables.complete_flush();
            self.stats.set_frozen_count(self.memtables.frozen_count());
            return Ok(());
        }

        let sst_id;
        {
            let mf = self.manifest.lock();
            sst_id = mf.next_sst_id();
        }

        let (data, sst_bytes, block_infos, bloom) = SstWriter::write_to_buf(
            &all_records,
            self.config.block_size,
            self.config.bloom_bits_per_key,
        )?;

        self.storage.write_sst(sst_id, &data)?;

        let min_ts = all_records.iter().map(|r| r.ts).min().unwrap_or(0);
        let max_ts = all_records.iter().map(|r| r.ts).max().unwrap_or(0);
        let min_expire = all_records.iter().map(|r| r.expire_at).min().unwrap_or(0);
        let max_expire = all_records.iter().map(|r| r.expire_at).max().unwrap_or(0);
        let last_seq = all_records.iter().map(|r| r.seq).max().unwrap_or(0);

        let raw_bytes: u64 = all_records.iter().map(|r| r.estimated_size() as u64).sum();
        if raw_bytes > 0 {
            self.stats
                .set_compression_ratio(sst_bytes as f64 / raw_bytes as f64);
        }

        let sst_info = SstInfo {
            id: sst_id,
            records: all_records.len() as u64,
            bytes: sst_bytes,
            min_ts,
            max_ts,
            min_expire,
            max_expire,
            bloom: Some(bloom.clone()),
        };

        {
            let mut mf = self.manifest.lock();
            mf.append(&ManifestEntry::Flush {
                seq: last_seq,
                sst: sst_info,
                blocks: block_infos.clone(),
            })?;
            mf.append(&ManifestEntry::Checkpoint {
                last_flushed_seq: last_seq,
            })?;
        }

        {
            let mut idx = self.index.write();
            idx.add_sst(sst_id, &block_infos);
            idx.set_bloom(sst_id, bloom);
        }

        // The SST is now registered in the index — safe to remove the
        // memtable from the flushing list so it can finally be dropped.
        self.memtables.complete_flush();

        {
            if let Err(e) = self.wal.truncate_before(last_seq) {
                tracing::warn!("WAL truncate_before({}) failed: {}", last_seq, e);
            }
            if let Err(e) = self.wal.sync_all() {
                tracing::warn!("WAL sync_all after flush failed: {}", e);
            }
        }

        let elapsed = start.elapsed();
        self.stats.flush_done();
        self.stats.record_flush_latency(elapsed.as_micros() as u64);

        let mf = self.manifest.lock();
        let sst_count = mf.state().sstables.len();
        let total_sst_bytes: u64 = mf.state().sstables.values().map(|s| s.bytes).sum();
        self.stats.set_sstable(sst_count, total_sst_bytes);

        self.stats.set_index_stats(
            self.index.read().total_entries(),
            self.index.read().bucket_count(),
        );

        let (rec_count, byte_count) = self.memtables.active_stats();
        self.stats.set_memtable(rec_count, byte_count);
        self.stats.set_frozen_count(self.memtables.frozen_count());

        Ok(())
    }
}

// ── Submission (a single batch queued for group commit) ──────────────

pub(crate) struct Submission {
    pub records: Vec<InternalRecord>,
    pub wal_buf: Vec<u8>,
    pub mem_bytes: u64,
    pub num_records: u64,
    pub batch_max_seq: u64,
    pub sync_mode: SyncMode,
    pub completion: Arc<Completion>,
}

pub(crate) struct Completion {
    done: Mutex<bool>,
    error_msg: Mutex<Option<String>>,
    cv: Condvar,
}

impl Completion {
    pub fn new() -> Self {
        Self {
            done: Mutex::new(false),
            error_msg: Mutex::new(None),
            cv: Condvar::new(),
        }
    }
}

// ── WritePipeline — group commit orchestrator ────────────────────────

/// Wraps a [`WriteWorker`] with a group-commit queue.  Multiple
/// concurrent callers enqueue their batches; the first caller to arrive
/// drains the queue, processes all pending batches under a single
/// lock acquisition, and performs one WAL fsync for the group.
pub(crate) struct WritePipeline {
    /// The underlying write worker.
    pub worker: Mutex<WriteWorker>,
    /// Pending submissions not yet drained by the committer.
    queue: Mutex<Vec<Arc<Submission>>>,
    /// Set while a thread is draining the queue.
    committing: AtomicBool,
}

impl WritePipeline {
    pub fn new(worker: WriteWorker) -> Self {
        Self {
            worker: Mutex::new(worker),
            queue: Mutex::new(Vec::new()),
            committing: AtomicBool::new(false),
        }
    }

    /// Enqueue a write and wait for it to be committed.  If no other
    /// thread is currently committing, this thread becomes the commit
    /// leader and drains the entire queue.
    pub fn submit(&self, sub: Submission) -> Result<()> {
        let sub = Arc::new(sub);
        let completion = sub.completion.clone();

        // 1. Enqueue under the queue lock.
        {
            let mut q = self.queue.lock();
            q.push(sub);
        }
        // 2. Attempt to become the commit leader.
        if self
            .committing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            // We are the leader — drain and process.
            let _ = self.drain_and_process();
            // Release so the next leader can take over.
            self.committing.store(false, Ordering::Release);
            // If the leader's own batch was part of the group, the result
            // was already signalled; the wait below is a no-op.
        }
        // 3. Wait for our completion to be signalled.
        Self::wait_for_completion(&completion)
    }

    fn drain_and_process(&self) -> Result<()> {
        loop {
            let pending: Vec<Arc<Submission>> = {
                let mut q = self.queue.lock();
                if q.is_empty() {
                    break;
                }
                std::mem::take(&mut *q)
            };

            let result = {
                let mut w = self.worker.lock();
                w.process_batch_group(&pending)
            };

            let err_msg = result.as_ref().err().map(|e| e.to_string());
            for sub in &pending {
                let mut done = sub.completion.done.lock();
                *sub.completion.error_msg.lock() = err_msg.clone();
                *done = true;
                sub.completion.cv.notify_all();
            }
        }
        Ok(())
    }

    fn wait_for_completion(c: &Completion) -> Result<()> {
        let mut done = c.done.lock();
        while !*done {
            c.cv.wait(&mut done);
        }
        match c.error_msg.lock().take() {
            Some(msg) => Err(FlowError::Other(msg)),
            None => Ok(()),
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}
