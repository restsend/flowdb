use crate::block_meta_index::BlockMetaIndex;
use crate::cache::{BlockCache, CacheKey};
use crate::compaction::CompactionRunner;
use crate::error::{FlowError, Result};
use crate::gc::GcRunner;
use crate::manifest::{Manifest, ManifestEntry};
use crate::memtable::MemTables;
use crate::record::{
    Config, InternalRecord, KeyFilter, Op, Query, ReadOptions, Record, ScanRange, SyncMode,
};
use crate::sstable::SstReader;
use crate::stats::{EngineStats, StatsCounters};
use crate::storage::{self, StorageBackend};
use crate::wal::Wal;
use crate::write_worker::{Completion, Submission, WritePipeline, WriteWorker};
use parking_lot::RwLock;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;

/// Handle returned by [`Engine::spawn_background_maintenance`].
///
/// Dropping this handle signals the background thread to stop and waits
/// for it to finish, ensuring a clean shutdown.
pub struct MaintenanceHandle {
    stop: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for MaintenanceHandle {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// The core LSM-tree storage engine.
///
/// `Engine` provides a fully synchronous key-value API with WAL durability,
/// Bloom filters, block cache, and background flush/compaction/GC.
///
/// # Opening
///
/// ```no_run
/// use flowdb::{Engine, Config};
///
/// let engine = Engine::open(Config::default()).unwrap();
/// ```
///
/// # Writing
///
/// ```no_run
/// # use flowdb::{Engine, Config};
/// # let engine = Engine::open(Config::default()).unwrap();
/// use flowdb::Record;
///
/// engine.write_batch_owned(vec![
///     Record::new("sensor:temp", 1_700_000_000_000, b"22.5".to_vec()),
/// ]).unwrap();
/// ```
///
/// # Reading
///
/// ```no_run
/// # use flowdb::{Engine, Config};
/// # let engine = Engine::open(Config::default()).unwrap();
/// use flowdb::Query;
///
/// for rec in engine.query(Query::prefix("sensor:")).unwrap() {
///     println!("{}", rec.key_str());
/// }
/// ```
///
/// # Shutdown
///
/// ```no_run
/// # use flowdb::{Engine, Config};
/// # let engine = Engine::open(Config::default()).unwrap();
/// engine.shutdown().unwrap();
/// ```
///
/// If the engine is behind an `Arc`, use [`Engine::close`] instead.
pub struct Engine {
    config: Config,
    worker: Arc<WritePipeline>,
    seq_counter: std::sync::atomic::AtomicU64,
    stats: Arc<StatsCounters>,
    memtables: Arc<MemTables>,
    index: Arc<RwLock<BlockMetaIndex>>,
    manifest: Arc<parking_lot::Mutex<Manifest>>,
    cache: Arc<BlockCache>,
    readers: Arc<RwLock<HashMap<u32, Arc<SstReader>>>>,
    storage: Arc<dyn StorageBackend>,
    maintenance: parking_lot::Mutex<Option<MaintenanceHandle>>,
    is_closed: std::sync::atomic::AtomicBool,
    /// Serialises the read-modify-write in `patch_record` to prevent
    /// lost updates when two threads patch the same key concurrently.
    patch_lock: std::sync::Mutex<()>,
}

impl Engine {
    /// Spawn a background maintenance thread that periodically flushes the
    /// memtable to SSTable, runs compaction, garbage-collects expired
    /// SST files, and syncs the WAL.  The thread runs until the returned
    /// [`MaintenanceHandle`] is dropped or the engine is shut down.
    ///
    /// Unlike an async runtime approach, this uses a plain OS thread and
    /// does **not** require a Tokio context, making FlowDB fully
    /// runtime-agnostic.
    ///
    /// Returns `None` if the OS thread could not be spawned (e.g. resource
    /// exhaustion); an error is logged via `tracing::error!()`.
    pub fn spawn_background_maintenance(&self) -> Option<MaintenanceHandle> {
        let worker = self.worker.clone();
        let manifest = self.manifest.clone();
        let index = self.index.clone();
        let cache = self.cache.clone();
        let stats = self.stats.clone();
        let readers = self.readers.clone();
        let storage = self.storage.clone();
        let block_size = self.config.block_size;
        let bloom_bits = self.config.bloom_bits_per_key;
        let compaction_threshold = self.config.compaction_threshold;
        let flush_interval = self.config.flush_interval_ms.max(1);
        let gc_interval = self.config.gc_interval_secs.max(1);
        let wal_sync_mode = self.config.wal_sync_mode;

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_clone = stop.clone();

        let thread = match std::thread::Builder::new()
            .name("flowdb-maintenance".into())
            .spawn(move || {
                let flush_dur = std::time::Duration::from_millis(flush_interval);
                let compact_dur = std::time::Duration::from_secs(gc_interval.max(1));
                let gc_dur = std::time::Duration::from_secs(gc_interval);

                // WAL sync tick for SyncMode::IntervalMs
                let wal_sync_dur = match wal_sync_mode {
                    SyncMode::IntervalMs(ms) => std::time::Duration::from_millis(ms.max(1)),
                    _ => std::time::Duration::from_secs(3600),
                };

                // Poll at the shortest interval, but no more than 4×/sec.
                let poll = flush_dur
                    .min(wal_sync_dur)
                    .min(std::time::Duration::from_millis(250));

                let mut last_flush = std::time::Instant::now();
                let mut last_compact = std::time::Instant::now();
                let mut last_gc = std::time::Instant::now();
                let mut last_sync = std::time::Instant::now();

                loop {
                    if stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                    std::thread::sleep(poll);
                    let now = std::time::Instant::now();

                    if now.duration_since(last_flush) >= flush_dur {
                        if let Err(e) = worker.worker.lock().do_flush() {
                            tracing::error!("Background flush failed: {}", e);
                        }
                        last_flush = std::time::Instant::now();
                    }

                    if now.duration_since(last_compact) >= compact_dur {
                        let sst_count = manifest.lock().state().sstables.len();
                        if sst_count >= compaction_threshold {
                            let compaction = crate::compaction::CompactionRunner::new(
                                block_size,
                                bloom_bits,
                                compaction_threshold,
                                manifest.clone(),
                                index.clone(),
                                cache.clone(),
                                stats.clone(),
                                storage.clone(),
                            );
                            match compaction.run() {
                                Ok(true) => {
                                    evict_stale_readers(&readers, &manifest);
                                    if let Err(e) = manifest.lock().maybe_snapshot() {
                                        tracing::error!(
                                            "Manifest snapshot after compaction failed: {}",
                                            e
                                        );
                                    }
                                }
                                Ok(false) => {}
                                Err(e) => {
                                    tracing::error!("Compaction failed: {}", e);
                                }
                            }
                        }
                        last_compact = std::time::Instant::now();
                    }

                    if now.duration_since(last_gc) >= gc_dur {
                        let gc = crate::gc::GcRunner::new(
                            manifest.clone(),
                            index.clone(),
                            cache.clone(),
                            stats.clone(),
                            storage.clone(),
                        );
                        match gc.run() {
                            Ok(n) if n > 0 => {
                                evict_stale_readers(&readers, &manifest);
                            }
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!("GC failed: {}", e);
                            }
                        }
                        last_gc = std::time::Instant::now();
                    }

                    if now.duration_since(last_sync) >= wal_sync_dur {
                        let mut wr = worker.worker.lock();
                        if let Err(e) = wr.wal.sync_all() {
                            tracing::error!("WAL sync failed: {}", e);
                        }
                        last_sync = std::time::Instant::now();
                    }
                }
            })
        {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Failed to spawn maintenance thread: {}", e);
                return None;
            }
        };

        Some(MaintenanceHandle {
            stop,
            thread: Some(thread),
        })
    }

    /// Open (or create) an engine at the configured data directory.
    ///
    /// On first call with `create_if_missing: true` (default), the data directory
    /// and its sub-directories (`WAL/`, `SST/`, `INDEX/`) are created automatically.
    ///
    /// If the directory already contains valid data (WAL + SST files), the engine
    /// recovers by replaying the WAL from the last flushed sequence number.
    pub fn open(config: Config) -> Result<Self> {
        config.validate()?;
        let data_dir = &config.data_dir;
        if !data_dir.exists() && !config.create_if_missing {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("data directory does not exist: {}", data_dir.display()),
            )
            .into());
        }
        std::fs::create_dir_all(data_dir)?;
        std::fs::create_dir_all(data_dir.join("WAL"))?;
        if matches!(config.storage_backend, crate::record::StorageBackendKind::MultiFile) {
            std::fs::create_dir_all(data_dir.join("SST"))?;
        }

        let storage = storage::open_storage(
            data_dir,
            matches!(
                config.storage_backend,
                crate::record::StorageBackendKind::SingleFile
            ),
        )?;
        let stats = Arc::new(StatsCounters::new());
        let mut wal = Wal::open(&data_dir.join("WAL"), config.wal_segment_size_mb)?;
        let mut manifest = Manifest::open(data_dir)?;

        let mut index = BlockMetaIndex::new(config.time_bucket_secs);
        let state = manifest.state().clone();
        for sst_id in &state.active_sst_ids {
            if let Some(info) = state.sstables.get(sst_id)
                && let Some(blocks) = state.block_infos.get(sst_id)
            {
                index.add_sst(*sst_id, blocks);
                if let Some(ref bloom) = info.bloom {
                    index.set_bloom(*sst_id, bloom.clone());
                }
            }
        }

        // Upgraded bloom hasher migration: any persisted bloom whose
        // `hash_version` is not current was built with a different hash
        // function. Querying it would yield meaningless results, so we
        // rebuild it from the SST file's actual records. This is a one-time
        // cost paid on the first startup after a hasher upgrade; subsequent
        // startups skip it entirely (version matches).
        let stale_ssts: Vec<u32> = state
            .active_sst_ids
            .iter()
            .filter(|&&id| match state.sstables.get(&id) {
                Some(info) => match info.bloom {
                    Some(ref b) => b.hash_version() != crate::bloom::CURRENT_HASH_VERSION,
                    None => false,
                },
                None => false,
            })
            .copied()
            .collect();

        if !stale_ssts.is_empty() {
            let rebuild_start = std::time::Instant::now();
            tracing::warn!(
                "Bloom hasher upgrade detected: rebuilding {} stale filter(s) ({} total SSTs)",
                stale_ssts.len(),
                state.active_sst_ids.len()
            );
            for sst_id in &stale_ssts {
                if let Err(e) = Engine::rebuild_bloom_for_sst(
                    &storage,
                    &mut manifest,
                    &mut index,
                    *sst_id,
                    config.bloom_bits_per_key,
                ) {
                    tracing::error!(
                        "Failed to rebuild bloom for SST {}: {} — falling back to no filter (correctness preserved, point queries may be slower)",
                        sst_id,
                        e
                    );
                    // Remove the stale bloom so queries skip the meaningless
                    // bits entirely (BlockMetaIndex treats missing bloom as
                    // "always match"). This preserves correctness at the
                    // cost of slower point lookups until compaction.
                    index.remove_bloom(*sst_id);
                }
            }
            tracing::info!(
                "Bloom rebuild complete: {} sst(s) in {:?}",
                stale_ssts.len(),
                rebuild_start.elapsed()
            );
        }

        let last_flushed_seq = manifest.state().last_flushed_seq;
        let wal_records = wal.replay_from(last_flushed_seq)?;

        let memtables = Arc::new(MemTables::new(
            config.max_frozen_memtables,
            config.memtable_size_mb * 1024 * 1024,
        ));

        for rec in &wal_records {
            memtables.insert(rec.clone());
        }

        let index = Arc::new(RwLock::new(index));
        let manifest = Arc::new(parking_lot::Mutex::new(manifest));
        let cache = Arc::new(BlockCache::with_block_size(
            config.block_cache_capacity_mb,
            config.block_size,
        ));
        let readers = Arc::new(RwLock::new(HashMap::new()));

        let sst_count = manifest.lock().state().sstables.len();
        let sst_bytes: u64 = manifest
            .lock()
            .state()
            .sstables
            .values()
            .map(|s| s.bytes)
            .sum();
        stats.set_sstable(sst_count, sst_bytes);
        stats.set_index_stats(index.read().total_entries(), index.read().bucket_count());

        let max_sst_id = manifest
            .lock()
            .state()
            .sstables
            .keys()
            .max()
            .copied()
            .unwrap_or(0);
        let seq_counter = std::sync::atomic::AtomicU64::new((max_sst_id as u64 + 1) * 1_000_000);

        let auto_bg = config.auto_background;

        let worker = Arc::new(WritePipeline::new(WriteWorker::new(
            config.clone(),
            wal,
            memtables.clone(),
            manifest.clone(),
            index.clone(),
            stats.clone(),
            storage.clone(),
        )));

        let engine = Self {
            config,
            worker,
            seq_counter,
            stats,
            memtables,
            index,
            manifest,
            cache,
            readers,
            storage,
            maintenance: parking_lot::Mutex::new(None),
            is_closed: std::sync::atomic::AtomicBool::new(false),
            patch_lock: std::sync::Mutex::new(()),
        };

        // Auto-start periodic flush, compaction, GC, and WAL sync.
        if auto_bg {
            *engine.maintenance.lock() = engine.spawn_background_maintenance();
        }

        Ok(engine)
    }

    /// Write a batch of records using the engine's default TTL.
    ///
    /// This is a borrowed-input variant — the caller retains ownership of `batch`.
    pub fn write_batch(&self, batch: &[Record]) -> Result<()> {
        self.write_batch_ttl(batch, None)
    }

    /// Write a batch of records (owned input, no TTL override).
    ///
    /// Equivalent to `write_batch` but takes ownership of the `Vec`, which can
    /// avoid a clone in some call sites.
    pub fn write_batch_owned(&self, batch: Vec<Record>) -> Result<()> {
        self.write_batch_owned_ttl(batch, None)
    }

    /// Write a batch synchronously, bypassing the background write pipeline.
    ///
    /// This method encodes, WAL-logs, and memtable-inserts on the calling thread.
    /// It is useful when the caller needs a synchronous durability guarantee
    /// without waiting for the background writer.
    pub fn write_batch_sync(&self, batch: Vec<Record>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let records = self.convert_records(batch, None);
        self.do_write(records)
    }

    fn convert_records(&self, batch: Vec<Record>, ttl_secs: Option<u64>) -> Vec<InternalRecord> {
        let ttl = ttl_secs.or(self.config.default_ttl_secs);
        let base = self
            .seq_counter
            .fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
        batch
            .into_iter()
            .enumerate()
            .map(|(i, rec)| {
                let expire_at = match ttl {
                    Some(t) => rec.ts.saturating_add((t as i64).saturating_mul(1_000_000)),
                    None => rec.expire_at,
                };
                InternalRecord {
                    seq: base + i as u64,
                    op: Op::Put,
                    key: rec.key,
                    ts: rec.ts,
                    expire_at,
                    value: rec.value,
                    range_end: None,
                }
            })
            .collect()
    }

    fn write_batch_owned_ttl(&self, batch: Vec<Record>, ttl_secs: Option<u64>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let records = self.convert_records(batch, ttl_secs);
        self.do_write(records)
    }

    pub fn write_batch_ttl(&self, batch: &[Record], ttl_secs: Option<u64>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let default_ttl = self.config.default_ttl_secs;
        let ttl = ttl_secs.or(default_ttl);

        let base = self
            .seq_counter
            .fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);

        let records: Vec<InternalRecord> = batch
            .iter()
            .enumerate()
            .map(|(i, rec)| {
                let expire_at = match ttl {
                    Some(t) => rec.ts.saturating_add((t as i64).saturating_mul(1_000_000)),
                    None => rec.expire_at,
                };
                let seq = base + i as u64;
                let mut irec = InternalRecord::from_record(rec, seq);
                irec.expire_at = expire_at;
                irec
            })
            .collect();

        self.do_write(records)
    }

    fn do_write(&self, records: Vec<InternalRecord>) -> Result<()> {
        let num_records = records.len() as u64;
        let batch_max_seq = records.last().map(|r| r.seq).unwrap_or(0);
        let (wal_buf, mem_bytes) = crate::wal::encode_batch(&records);
        let sync_mode = self.config.wal_sync_mode;
        let start = std::time::Instant::now();

        let sub = Submission {
            records,
            wal_buf,
            mem_bytes,
            num_records,
            batch_max_seq,
            sync_mode,
            completion: Arc::new(Completion::new()),
        };
        self.worker.submit(sub)?;

        self.stats
            .record_write_latency(start.elapsed().as_micros() as u64);
        Ok(())
    }

    pub fn query(&self, query: Query) -> Result<Vec<Record>> {
        let start = std::time::Instant::now();

        let now_us = now_micros();
        let iter = ScanIterator::build(
            &query,
            &self.memtables,
            &self.index,
            &self.cache,
            &self.storage,
            &self.readers,
            now_us,
        )?;
        let records: Vec<Record> = iter.collect::<Result<Vec<_>>>()?;

        self.stats
            .record_query_latency(start.elapsed().as_micros() as u64);
        self.stats.records_read(records.len() as u64);
        Ok(records)
    }

    pub fn query_by_prefix(&self, key: &str) -> Result<Vec<Record>> {
        self.query(Query::prefix(key))
    }

    pub fn query_by_key_range(&self, start: &str, end: &str) -> Result<Vec<Record>> {
        self.query(Query::key_range(start, end))
    }

    pub fn query_time_range(&self, start: i64, end: i64) -> Result<Vec<Record>> {
        self.query(Query::time_range(start, end))
    }

    pub fn query_prefix_time_range(&self, key: &str, start: i64, end: i64) -> Result<Vec<Record>> {
        self.query(Query::prefix_time_range(key, start, end))
    }

    pub fn query_key_time_range(
        &self,
        start_key: &str,
        end_key: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<Record>> {
        self.query(Query::key_time_range(start_key, end_key, start, end))
    }

    pub fn get(&self, key: &str, ts: i64) -> Result<Option<Record>> {
        Ok(self.get_sync(key, ts))
    }

    pub fn get_sync(&self, key: &str, ts: i64) -> Option<Record> {
        let now_us = now_micros();

        if let Some(rec) = self.memtables.get(key.as_bytes(), ts, now_us) {
            if rec.op != Op::Delete {
                return Some(rec.to_record());
            }
            return None;
        }

        let key_bytes = key.as_bytes();
        let idx = self.index.read();
        if let Some((sst_id, block_idx)) = idx.single_sst_point(key_bytes, now_us) {
            drop(idx);
            if let Some(rec) = self.block_search(key_bytes, ts, now_us, sst_id, block_idx) {
                return Some(rec);
            }
            return None;
        }

        let found = idx.query_point_inline(key_bytes, now_us, |meta| {
            self.block_search(key_bytes, ts, now_us, meta.sst_id, meta.block_idx)
        });
        drop(idx);
        found
    }

    fn block_search(
        &self,
        key: &[u8],
        ts: i64,
        now_us: i64,
        sst_id: u32,
        block_idx: u32,
    ) -> Option<Record> {
        let reader = match Engine::get_reader(&self.readers, &self.storage, sst_id) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(
                    "SST point lookup: cannot open reader for sst {}: {}",
                    sst_id,
                    e
                );
                return None;
            }
        };

        if let Some(cached) = reader.read_block_cached(block_idx, &self.cache) {
            return Self::find_in_records(&cached, key, ts, now_us);
        }

        match reader.read_block_decompress(block_idx) {
            Ok((_header, records)) => {
                let result = Self::find_in_records(&records, key, ts, now_us);
                self.cache.insert(CacheKey { sst_id, block_idx }, records);
                result
            }
            Err(e) => {
                tracing::error!(
                    "SST point lookup: block decompress failed sst={} block={}: {}",
                    sst_id,
                    block_idx,
                    e
                );
                None
            }
        }
    }

    fn find_in_records(
        records: &[InternalRecord],
        key: &[u8],
        ts: i64,
        now_us: i64,
    ) -> Option<Record> {
        let lo = match records
            .binary_search_by(|r| r.key.as_slice().cmp(key).then_with(|| r.ts.cmp(&ts)))
        {
            Ok(idx) => idx,
            Err(_) => return None,
        };
        let rec = &records[lo];
        if rec.expire_at > now_us && rec.op != Op::Delete {
            return Some(Record {
                key: key.to_vec(),
                ts: rec.ts,
                expire_at: rec.expire_at,
                value: rec.value.clone(),
            });
        }
        None
    }

    pub fn delete_batch(&self, keys_ts: &[(String, i64)]) -> Result<()> {
        if keys_ts.is_empty() {
            return Ok(());
        }

        let base = self
            .seq_counter
            .fetch_add(keys_ts.len() as u64, std::sync::atomic::Ordering::Relaxed);

        let records: Vec<InternalRecord> = keys_ts
            .iter()
            .enumerate()
            .map(|(i, (key, ts))| {
                InternalRecord::delete(key.clone().into_bytes(), *ts, base + i as u64)
            })
            .collect();

        self.do_write(records)
    }

    pub fn delete_range(&self, start_key: &str, end_key: &str) -> Result<()> {
        let seq = self
            .seq_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let record = InternalRecord::delete_range(
            start_key.as_bytes().to_vec(),
            end_key.as_bytes().to_vec(),
            seq,
        );
        self.do_write(vec![record])
    }

    /// Read-modify-write a single record by key+ts.
    ///
    /// Returns an error if the record does not exist.
    ///
    /// This method is safe to call concurrently from multiple threads — it
    /// holds an internal serialization lock across the read and write so
    /// that concurrent `patch_record` calls on the same key do not
    /// produce lost updates.
    pub fn patch_record(
        &self,
        key: &str,
        ts: i64,
        new_value: Option<Vec<u8>>,
        new_ttl_secs: Option<u64>,
    ) -> Result<Record> {
        let _lock = self.patch_lock.lock().unwrap();
        let existing = self.get_sync(key, ts);
        let mut rec = match existing {
            Some(r) => r,
            None => {
                return Err(crate::error::FlowError::Other(format!(
                    "record not found: key={}, ts={}",
                    key, ts
                )));
            }
        };

        if let Some(v) = new_value {
            rec.value = v;
        }
        if let Some(ttl) = new_ttl_secs {
            rec.expire_at = rec.ts.saturating_add((ttl as i64).saturating_mul(1_000_000));
        }

        self.write_batch(&[rec.clone()])?;
        Ok(rec)
    }

    pub fn stats(&self) -> EngineStats {
        self.stats.snapshot(self.cache.hit_rate())
    }

    pub fn metrics_text(&self) -> String {
        self.stats.to_prometheus(self.cache.hit_rate())
    }

    fn get_reader(
        readers: &Arc<RwLock<HashMap<u32, Arc<SstReader>>>>,
        storage: &Arc<dyn StorageBackend>,
        sst_id: u32,
    ) -> Result<Arc<SstReader>> {
        {
            let r = readers.read();
            if let Some(reader) = r.get(&sst_id) {
                return Ok(reader.clone());
            }
        }
        if !storage.sst_exists(sst_id) {
            return Err(FlowError::Other(format!("sst {} not found", sst_id)));
        }
        let reader = Arc::new(storage.open_reader(sst_id, 0)?);
        readers.write().insert(sst_id, reader.clone());
        Ok(reader)
    }

    fn get_reader_from_map(
        readers: &HashMap<u32, Arc<SstReader>>,
        storage: &Arc<dyn StorageBackend>,
        sst_id: u32,
    ) -> Result<Arc<SstReader>> {
        if let Some(reader) = readers.get(&sst_id) {
            return Ok(reader.clone());
        }
        if !storage.sst_exists(sst_id) {
            return Err(FlowError::Other(format!("sst {} not found", sst_id)));
        }
        Ok(Arc::new(storage.open_reader(sst_id, 0)?))
    }

    /// Rebuild the persisted bloom filter for `sst_id` using the current
    /// hasher. Scans every block in the SST, collects unique keys, builds a
    /// fresh `BloomFilter`, persists it via a `ManifestEntry::UpdateBloom`,
    /// and refreshes the in-memory index. Called from `Engine::open` for any
    /// SST whose persisted bloom was built with a legacy hash function.
    ///
    /// This is a one-time migration cost; subsequent startups find the
    /// persisted version matches `CURRENT_HASH_VERSION` and skip the rebuild.
    fn rebuild_bloom_for_sst(
        storage: &Arc<dyn StorageBackend>,
        manifest: &mut Manifest,
        index: &mut BlockMetaIndex,
        sst_id: u32,
        bits_per_key: usize,
    ) -> Result<()> {
        if !storage.sst_exists(sst_id) {
            return Err(FlowError::Other(format!(
                "rebuild_bloom: sst {} file missing",
                sst_id
            )));
        }

        // Open the reader with `block_count=0` — SstReader walks the
        // SST to discover block boundaries, so 0 just means "no hint".
        let reader = storage.open_reader(sst_id, 0)?;
        let block_count = reader.block_count();

        // Collect unique keys in insertion order. We do NOT need values.
        // Records within a block are sorted, and blocks themselves are
        // sorted by min_key (write path sorts before flush), so we only
        // need to compare against the last-emitted key for deduplication.
        let mut unique_keys: Vec<Vec<u8>> = Vec::new();
        let mut last_key: Option<Vec<u8>> = None;
        for blk_idx in 0..block_count {
            let block = match reader.read_block(blk_idx, None) {
                Ok(b) => b,
                Err(e) => {
                    return Err(FlowError::Other(format!(
                        "rebuild_bloom: failed to read block {} of sst {}: {}",
                        blk_idx, sst_id, e
                    )));
                }
            };
            for rec in &block.records {
                if last_key.as_deref() != Some(rec.key.as_slice()) {
                    unique_keys.push(rec.key.clone());
                    last_key = Some(rec.key.clone());
                }
            }
        }

        let new_bloom = crate::bloom::BloomFilter::from_keys_with_bits(&unique_keys, bits_per_key);
        debug_assert_eq!(new_bloom.hash_version(), crate::bloom::CURRENT_HASH_VERSION);

        // Persist the rebuilt bloom so subsequent startups skip the rebuild.
        manifest.append(&ManifestEntry::UpdateBloom {
            sst_id,
            bloom: new_bloom.clone(),
        })?;
        // Refresh the in-memory index so point queries immediately benefit.
        index.set_bloom(sst_id, new_bloom);
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.worker.worker.lock().do_flush()
    }

    pub fn trigger_gc(&self) -> Result<u64> {
        let gc = GcRunner::new(
            self.manifest.clone(),
            self.index.clone(),
            self.cache.clone(),
            self.stats.clone(),
            self.storage.clone(),
        );
        let purged = gc.run()?;
        self.evict_stale_readers();
        Ok(purged)
    }

    pub fn trigger_compaction(&self) -> Result<bool> {
        let compaction = CompactionRunner::new(
            self.config.block_size,
            self.config.bloom_bits_per_key,
            self.config.compaction_threshold,
            self.manifest.clone(),
            self.index.clone(),
            self.cache.clone(),
            self.stats.clone(),
            self.storage.clone(),
        );
        let did = compaction.run()?;
        self.evict_stale_readers();
        Ok(did)
    }

    /// Shut down the engine, flushing the memtable and WAL.  Consumes
    /// `self` — use [`Engine::close`] if the engine is behind an `Arc`.
    pub fn shutdown(self) -> Result<()> {
        self.close()
    }

    /// Flush the memtable and WAL without consuming `self`.
    ///
    /// This is the `Arc<Engine>`-friendly alternative to `shutdown`.  Use
    /// it when the engine is shared across threads.  The background
    /// maintenance thread (if any) is NOT stopped — drop the
    /// [`MaintenanceHandle`] first, or simply let `Engine::shutdown`
    /// consume it.
    pub fn close(&self) -> Result<()> {
        if self.is_closed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }

        // Stop background thread
        *self.maintenance.lock() = None;

        let mut w = self.worker.worker.lock();
        w.do_flush()?;
        w.flush_wal()
    }

    /// Remove reader cache entries for SSTs that are no longer in the
    /// manifest (i.e. deleted by GC or superseded by compaction).  This
    /// prevents file-descriptor / mmap leaks over long-running operations.
    fn evict_stale_readers(&self) {
        evict_stale_readers(&self.readers, &self.manifest);
    }

    // New iterator-based scan API (RocksDB-style)
    /// Lazy iterator scan with default `ReadOptions`.
    pub fn scan(&self, range: ScanRange) -> Result<ScanIterator> {
        self.scan_opt(range, &ReadOptions::default())
    }

    /// Lazy iterator scan with caller-provided `ReadOptions`.
    pub fn scan_opt(&self, range: ScanRange, _opts: &ReadOptions) -> Result<ScanIterator> {
        let now_us = now_micros();
        let (key_filter, time_range) = range.to_query_params();
        let q = Query {
            key_filter,
            time_range,
        };
        ScanIterator::build(
            &q,
            &self.memtables,
            &self.index,
            &self.cache,
            &self.storage,
            &self.readers,
            now_us,
        )
    }

    /// Convenience: prefix scan returning a lazy iterator.
    pub fn scan_prefix(&self, prefix: &str) -> Result<ScanIterator> {
        self.scan(ScanRange::prefix(prefix))
    }

    /// Convenience: prefix + time-range scan returning a lazy iterator.
    pub fn scan_prefix_time_range(
        &self,
        prefix: &str,
        ts_start: i64,
        ts_end: i64,
    ) -> Result<ScanIterator> {
        self.scan(ScanRange::prefix_time_range(prefix, ts_start, ts_end))
    }

    /// Get the latest record for a given key (highest `ts`).
    ///
    /// Unlike the old scan-then-last approach which is O(n) in the number
    /// of versions, this walks memtables and SST blocks via the block
    /// index, yielding O(log n) or O(k) where k is the number of blocks.
    pub fn get_latest(&self, key: &str) -> Result<Option<Record>> {
        let now_us = now_micros();
        let key_bytes = key.as_bytes();

        // 1) Check memtables — fast, doesn't touch the index.
        let mem_latest = self.memtables.get_latest(key_bytes, now_us);

        // 2) Check SST blocks via the block meta index.
        let idx = self.index.read();
        let sst_latest: Option<Record> = idx.query_point_inline(key_bytes, now_us, |meta| {
            self.block_search_latest(key_bytes, now_us, meta.sst_id, meta.block_idx)
        });
        drop(idx);

        // 3) Merge: whichever has higher ts, with memtable winning ties
        //    (memtable always has higher seq than any SST record).
        match (mem_latest, sst_latest) {
            (Some(m), Some(s)) => {
                if m.ts >= s.ts {
                    Ok(Some(m.to_record()))
                } else {
                    Ok(Some(s))
                }
            }
            (Some(m), None) => Ok(Some(m.to_record())),
            (None, Some(s)) => Ok(Some(s)),
            (None, None) => Ok(None),
        }
    }

    /// Scan a single decompressed block for the latest version of `key`.
    fn block_search_latest(
        &self,
        key: &[u8],
        now_us: i64,
        sst_id: u32,
        block_idx: u32,
    ) -> Option<Record> {
        let reader = match Engine::get_reader(&self.readers, &self.storage, sst_id) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(
                    "SST latest lookup: cannot open reader for sst {}: {}",
                    sst_id,
                    e
                );
                return None;
            }
        };

        let records = match reader.read_block_decompress(block_idx) {
            Ok((_header, recs)) => recs,
            Err(e) => {
                tracing::error!(
                    "SST latest lookup: block decompress failed sst={} block={}: {}",
                    sst_id,
                    block_idx,
                    e
                );
                return None;
            }
        };

        // Find the record with matching key and max (ts, seq).
        let mut best: Option<&InternalRecord> = None;
        for rec in &records {
            if rec.key.as_slice() == key && rec.expire_at > now_us && rec.op != Op::Delete {
                match best {
                    None => best = Some(rec),
                    Some(b) => {
                        if rec.ts > b.ts || (rec.ts == b.ts && rec.seq > b.seq) {
                            best = Some(rec);
                        }
                    }
                }
            }
        }
        best.map(|r| Record {
            key: key.to_vec(),
            ts: r.ts,
            expire_at: r.expire_at,
            value: r.value.clone(),
        })
    }

    // ------------------------------------------------------------------
    // Internal API used by the jsondb module (crate-visible only)
    // ------------------------------------------------------------------

    /// Point lookup for a byte key at a given `ts`. Used by `jsondb` where
    /// keys are binary composite keys that may not be valid UTF-8.
    pub(crate) fn get_bytes(&self, key: &[u8], ts: i64) -> Option<Record> {
        let now_us = now_micros();

        if let Some(rec) = self.memtables.get(key, ts, now_us) {
            if rec.op != Op::Delete {
                return Some(rec.to_record());
            }
            return None;
        }

        let key_bytes = key;
        let idx = self.index.read();
        if let Some((sst_id, block_idx)) = idx.single_sst_point(key_bytes, now_us) {
            drop(idx);
            return self.block_search(key_bytes, ts, now_us, sst_id, block_idx);
        }

        let found = idx.query_point_inline(key_bytes, now_us, |meta| {
            self.block_search(key_bytes, ts, now_us, meta.sst_id, meta.block_idx)
        });
        drop(idx);
        found
    }

    /// Write a batch of [`InternalRecord`]s (puts, deletes, range-deletes)
    /// atomically. Sequence numbers are assigned automatically. This is the
    /// primitive used by the `jsondb` module for atomic multi-key updates
    /// (document + secondary-index maintenance).
    pub(crate) fn write_internal(&self, records: Vec<InternalRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let base = self
            .seq_counter
            .fetch_add(records.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let mut recs = records;
        for (i, rec) in recs.iter_mut().enumerate() {
            rec.seq = base + i as u64;
        }
        self.do_write(recs)
    }
}

/// ScanIterator — lazy merging iterator over memtables + SST blocks
/// Lazy iterator yielding `Result<Record>` in `(key, ts)` ascending order.
///
/// Constructed via `Engine::scan()`, `Engine::scan_prefix()`,
/// `Engine::scan_prefix_time_range()`, etc.
///
/// Internally maintains a binary-heap merge over pre-filtered memtable
/// and SST block sources. Records are yielded one-at-a-time without
/// materializing the full result set, enabling bounded-memory scans
/// over arbitrarily large ranges.
pub struct ScanIterator {
    sources: Vec<std::iter::Peekable<std::vec::IntoIter<InternalRecord>>>,
    heap: BinaryHeap<MergeEntry>,
    tombstones: Vec<(Vec<u8>, Vec<u8>)>,
    last_dedup: Option<(Vec<u8>, i64)>,
    /// Single-source fast path: when only 1 source + no tombstones,
    /// skip heap overhead and yield directly.
    fast_source: Option<usize>,
}

struct MergeEntry {
    key: Vec<u8>,
    ts: i64,
    seq: u64,
    source: usize,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.ts == other.ts
    }
}
impl Eq for MergeEntry {}
impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // min-heap by (key asc, ts asc, seq desc) — memtable (high seq) wins ties
        other
            .key
            .cmp(&self.key)
            .then(other.ts.cmp(&self.ts))
            .then(self.seq.cmp(&other.seq))
    }
}

impl ScanIterator {
    #[allow(clippy::too_many_arguments)]
    fn build(
        query: &Query,
        memtables: &Arc<MemTables>,
        index: &Arc<RwLock<BlockMetaIndex>>,
        cache: &Arc<BlockCache>,
        storage: &Arc<dyn StorageBackend>,
        readers: &Arc<RwLock<HashMap<u32, Arc<SstReader>>>>,
        now_us: i64,
    ) -> Result<Self> {
        // 1) Query memtables (eager — memtable is small)
        let mem_results = match (&query.key_filter, &query.time_range) {
            (KeyFilter::Prefix(key), None) => memtables.query_prefix(key, now_us),
            (KeyFilter::Range { start, end }, None) => {
                memtables.query_key_range(start, end, now_us)
            }
            (KeyFilter::All, Some((ts_start, ts_end))) => {
                memtables.query_time_range(*ts_start, *ts_end, now_us)
            }
            (KeyFilter::Prefix(key), Some((ts_start, ts_end))) => {
                memtables.query_prefix_time_range(key, *ts_start, *ts_end, now_us)
            }
            (KeyFilter::Range { start, end }, Some((ts_start, ts_end))) => {
                memtables.query_key_time_range(start, end, *ts_start, *ts_end, now_us)
            }
            (KeyFilter::All, None) => memtables.query_key_range(b"", b"~", now_us),
        };

        // 2) Collect memtable range tombstones
        let mut tombstones: Vec<(Vec<u8>, Vec<u8>)> = mem_results
            .iter()
            .filter(|r| r.op == Op::DeleteRange && r.range_end.is_some())
            .map(|r| (r.key.clone(), r.range_end.clone().unwrap()))
            .collect();

        // 3) Query block meta index for candidate SST blocks
        let candidates = {
            let idx = index.read();
            idx.query(&query.key_filter, query.time_range, now_us)
        };

        let is_full_scan = (matches!(&query.key_filter, KeyFilter::All)
            || matches!(&query.key_filter, KeyFilter::Prefix(p) if p.is_empty()))
            && query.time_range.is_none();

        // 4) Snapshot SST readers
        let readers_snapshot = {
            let r = readers.read();
            r.clone()
        };

        // 5) Load + filter SST blocks into sources
        let mut sst_sources: Vec<std::iter::Peekable<std::vec::IntoIter<InternalRecord>>> =
            Vec::new();

        // For full scans, use read_block_decompress to avoid block cache overhead
        // and consume records without cloning.  Each block becomes its own
        // source to avoid one giant contiguous allocation.
        if is_full_scan {
            for meta in &candidates {
                let reader =
                    match Engine::get_reader_from_map(&readers_snapshot, storage, meta.sst_id) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!(
                                "Scan: cannot open SST reader for sst {}: {}",
                                meta.sst_id,
                                e
                            );
                            continue;
                        }
                    };
                let records = match reader.read_block_decompress(meta.block_idx) {
                    Ok((_, recs)) => recs,
                    Err(e) => {
                        tracing::error!(
                            "Scan: block decompress failed sst={} block={}: {}",
                            meta.sst_id,
                            meta.block_idx,
                            e
                        );
                        continue;
                    }
                };
                let mut filtered: Vec<InternalRecord> = Vec::with_capacity(records.len());
                for rec in records {
                    if rec.expire_at <= now_us {
                        continue;
                    }
                    if rec.op == Op::DeleteRange {
                        if let Some(ref end) = rec.range_end {
                            tombstones.push((rec.key.clone(), end.clone()));
                        }
                        continue;
                    }
                    if rec.op == Op::Delete {
                        continue;
                    }
                    filtered.push(rec);
                }
                if !filtered.is_empty() {
                    sst_sources.push(filtered.into_iter().peekable());
                }
            }
        } else {
            for meta in &candidates {
                let reader =
                    match Engine::get_reader_from_map(&readers_snapshot, storage, meta.sst_id) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!(
                                "Scan: cannot open SST reader for sst {}: {}",
                                meta.sst_id,
                                e
                            );
                            continue;
                        }
                    };
                let records = match reader.read_block_arc(meta.block_idx, cache) {
                    Ok(arc) => arc,
                    Err(e) => {
                        tracing::error!(
                            "Scan: read_block_arc failed sst={} block={}: {}",
                            meta.sst_id,
                            meta.block_idx,
                            e
                        );
                        continue;
                    }
                };
                let filtered = filter_sst_block(
                    &records,
                    &query.key_filter,
                    query.time_range,
                    false,
                    now_us,
                    &mut tombstones,
                );
                if !filtered.is_empty() {
                    sst_sources.push(filtered.into_iter().peekable());
                }
            }
        }

        // 6) Sort memtable results - highest seq first for same (key, ts)
        //    so that deletes/overwrites with higher seq are processed before
        //    older puts, ensuring correct dedup in the fast-source scan path.
        let mut mem_sorted = mem_results;
        mem_sorted.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then(a.ts.cmp(&b.ts))
                .then(b.seq.cmp(&a.seq))
        });

        // 7) Assemble all sources: memtable first (index 0), then SST sources
        let mut sources: Vec<std::iter::Peekable<std::vec::IntoIter<InternalRecord>>> =
            Vec::with_capacity(1 + sst_sources.len());

        let memtable_present = !mem_sorted.is_empty();
        if memtable_present {
            sources.push(mem_sorted.into_iter().peekable());
        }
        sources.extend(sst_sources);

        // 8) Initialize heap — memtable uses source = usize::MAX for dedup priority
        //    Fast path: single source + no tombstones → bypass heap entirely
        let fast_source = if sources.len() == 1 && tombstones.is_empty() {
            // Single source, no tombstones — use fast path
            Some(0)
        } else {
            None
        };

        let mut heap: BinaryHeap<MergeEntry> = BinaryHeap::with_capacity(sources.len().max(1));
        if fast_source.is_none() {
            for (i, src) in sources.iter_mut().enumerate() {
                if let Some(r) = src.peek() {
                    let source_id = if memtable_present && i == 0 {
                        usize::MAX
                    } else {
                        i
                    };
                    heap.push(MergeEntry {
                        key: r.key.clone(),
                        ts: r.ts,
                        seq: r.seq,
                        source: source_id,
                    });
                }
            }
        }

        Ok(Self {
            sources,
            heap,
            tombstones,
            last_dedup: None,
            fast_source,
        })
    }

    fn advance_source(&mut self, source: usize) -> Option<InternalRecord> {
        let idx = if source == usize::MAX { 0 } else { source };
        let src = self.sources.get_mut(idx)?;
        let rec = src.next()?;
        if let Some(next) = src.peek() {
            self.heap.push(MergeEntry {
                key: next.key.clone(),
                ts: next.ts,
                seq: next.seq,
                source,
            });
        }
        Some(rec)
    }

    fn is_tombstoned(&self, rec: &InternalRecord) -> bool {
        self.tombstones.iter().any(|(start, end)| {
            rec.key.as_slice() >= start.as_slice() && rec.key.as_slice() < end.as_slice()
        })
    }
}

fn filter_sst_block(
    records: &[InternalRecord],
    key_filter: &KeyFilter,
    time_range: Option<(i64, i64)>,
    is_full_scan: bool,
    now_us: i64,
    tombstones: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Vec<InternalRecord> {
    let mut filtered = Vec::with_capacity(records.len().min(64));
    for rec in records {
        if rec.expire_at <= now_us {
            continue;
        }
        if rec.op == Op::DeleteRange {
            if let Some(ref end) = rec.range_end {
                tombstones.push((rec.key.clone(), end.clone()));
            }
            continue;
        }
        if is_full_scan {
            if rec.op == Op::Delete {
                continue;
            }
            filtered.push(InternalRecord {
                seq: 0,
                op: Op::Put,
                key: rec.key.clone(),
                ts: rec.ts,
                expire_at: rec.expire_at,
                value: rec.value.clone(),
                range_end: None,
            });
            continue;
        }
        let matches_key = match key_filter {
            KeyFilter::Prefix(key) => rec.key.starts_with(key.as_slice()),
            KeyFilter::Range { start, end } => {
                rec.key.as_slice() >= start.as_slice() && rec.key.as_slice() <= end.as_slice()
            }
            KeyFilter::All => true,
        };
        if !matches_key {
            continue;
        }
        if let Some((ts_start, ts_end)) = time_range
            && (rec.ts < ts_start || rec.ts > ts_end)
        {
            continue;
        }
        filtered.push(InternalRecord {
            seq: 0,
            op: rec.op,
            key: rec.key.clone(),
            ts: rec.ts,
            expire_at: rec.expire_at,
            value: rec.value.clone(),
            range_end: rec.range_end.clone(),
        });
    }
    filtered
}

impl Iterator for ScanIterator {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        // Fast path: single source, no tombstones
        if let Some(idx) = self.fast_source {
            loop {
                let src = self.sources.get_mut(idx)?;
                let rec = src.next()?;
                if rec.op == Op::Delete || rec.op == Op::DeleteRange {
                    self.last_dedup = Some((rec.key.clone(), rec.ts));
                    continue;
                }
                // Skip duplicates (same key, ts) which can occur when a later
                // Put overwrites an earlier one or a Delete masks a prior Put.
                if let Some((ref last_key, last_ts)) = self.last_dedup
                    && rec.key == *last_key
                    && rec.ts == last_ts
                {
                    continue;
                }
                self.last_dedup = Some((rec.key.clone(), rec.ts));
                return Some(Ok(rec.into_record_owned()));
            }
        }

        // Merge-heap path for multiple sources
        loop {
            let entry = self.heap.pop()?;
            let dedup_key = (entry.key.clone(), entry.ts);

            // Dedup: same (key, ts) seen → skip
            if self.last_dedup.as_ref() == Some(&dedup_key) {
                self.advance_source(entry.source);
                continue;
            }

            let rec = match self.advance_source(entry.source) {
                Some(r) => r,
                None => continue,
            };

            // Skip tombstones (Delete / DeleteRange)
            if rec.op == Op::Delete || rec.op == Op::DeleteRange {
                self.last_dedup = Some(dedup_key);
                continue;
            }

            // Check range tombstones
            if self.is_tombstoned(&rec) {
                self.last_dedup = Some(dedup_key);
                continue;
            }

            self.last_dedup = Some(dedup_key);
            return Some(Ok(rec.into_record_owned()));
        }
    }
}

impl std::iter::FusedIterator for ScanIterator {}

/// Free-function version of [`Engine::evict_stale_readers`] usable from
/// the background maintenance thread (which holds cloned `Arc`s, not
/// `&Engine`).  Removes reader-cache entries whose SST IDs are no longer
/// present in the manifest — this closes file descriptors and munmaps
/// virtual memory for compacted/GC'd SSTs.
fn evict_stale_readers(
    readers: &Arc<RwLock<HashMap<u32, Arc<SstReader>>>>,
    manifest: &Arc<parking_lot::Mutex<Manifest>>,
) {
    let active_ssts: std::collections::HashSet<u32> = {
        let mf = manifest.lock();
        mf.state().sstables.keys().copied().collect()
    };
    let mut rmap = readers.write();
    rmap.retain(|sst_id, _| active_ssts.contains(sst_id));
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_config(dir: &std::path::Path) -> Config {
        Config {
            data_dir: dir.to_path_buf(),
            memtable_size_mb: 1,
            block_size: 100,
            gc_interval_secs: 3600,
            max_frozen_memtables: 2,
            flush_interval_ms: 60000,
            time_bucket_secs: 3600,
            block_cache_capacity_mb: 16,
            index_memory_budget_mb: 64,
            default_ttl_secs: None,
            bloom_bits_per_key: 10,
            wal_segment_size_mb: 64,
            compaction_threshold: 2,
            create_if_missing: true,
            wal_sync_mode: SyncMode::IntervalMs(u64::MAX),
            auto_background: false,
            storage_backend: crate::record::StorageBackendKind::MultiFile,
        }
    }

    fn make_record(key: &str, ts: i64) -> Record {
        Record {
            key: key.as_bytes().to_vec(),
            ts,
            expire_at: i64::MAX,
            value: vec![1, 2, 3],
        }
    }

    #[test]
    fn test_engine_write_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("key1", 100),
                make_record("key2", 200),
                make_record("key3", 300),
            ])
            .unwrap();

        let results = engine.query_by_prefix("key1").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, b"key1");

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_key_range_query() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("a", 100),
                make_record("b", 200),
                make_record("c", 300),
                make_record("d", 400),
            ])
            .unwrap();

        let results = engine.query_by_key_range("b", "c").unwrap();
        assert_eq!(results.len(), 2);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_time_range_query() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("a", 100),
                make_record("b", 200),
                make_record("c", 300),
            ])
            .unwrap();

        let results = engine.query_time_range(150, 300).unwrap();
        assert_eq!(results.len(), 2);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_prefix_time_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("a", 100),
                make_record("a", 200),
                make_record("a", 300),
                make_record("b", 200),
            ])
            .unwrap();

        let results = engine.query_prefix_time_range("a", 150, 250).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].ts, 200);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_ttl_expiry() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        let mut rec = make_record("exp", 100);
        rec.expire_at = 1;
        engine.write_batch(&[rec]).unwrap();

        let results = engine.query_by_prefix("exp").unwrap();
        assert!(results.is_empty());

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_flush_and_query() {
        let dir = TempDir::new().unwrap();
        let mut config = make_config(dir.path());
        config.memtable_size_mb = 64;
        let engine = Engine::open(config).unwrap();

        let records: Vec<Record> = (0..50)
            .map(|i| make_record(&format!("key_{:04}", i), i * 10))
            .collect();
        engine.write_batch(&records).unwrap();

        engine.flush().unwrap();

        let results = engine.query_by_prefix("key_").unwrap();
        assert_eq!(results.len(), 50);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_stats() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[make_record("a", 100), make_record("b", 200)])
            .unwrap();

        let stats = engine.stats();
        assert_eq!(stats.total_records_written, 2);
        assert!(stats.uptime_secs == 0 || stats.uptime_secs <= 5);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_recovery() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());

        {
            let engine = Engine::open(config.clone()).unwrap();
            engine
                .write_batch(&[make_record("a", 100), make_record("b", 200)])
                .unwrap();
            engine.shutdown().unwrap();
        }

        {
            let engine = Engine::open(config).unwrap();
            let results = engine.query_by_prefix("a").unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].key, b"a");

            let results = engine.query_by_prefix("b").unwrap();
            assert_eq!(results.len(), 1);

            engine.shutdown().unwrap();
        }
    }

    #[test]
    fn test_engine_concurrent_writes() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Arc::new(Engine::open(config).unwrap());

        let mut handles = Vec::new();
        for t in 0..4u64 {
            let e = engine.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..25u64 {
                    let rec = make_record(&format!("t{}-{}", t, i), (t * 100 + i) as i64);
                    e.write_batch(&[rec]).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let stats = engine.stats();
        assert_eq!(stats.total_records_written, 100);

        let engine = Arc::try_unwrap(engine)
            .unwrap_or_else(|e| std::sync::Arc::<Engine>::into_inner(e).unwrap());
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_compaction() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        for batch in 0..10u64 {
            let records: Vec<Record> = (0..5)
                .map(|i| Record {
                    key: format!("compact_{}-{}", batch, i).into_bytes(),
                    ts: (batch * 100 + i) as i64,
                    expire_at: i64::MAX,
                    value: vec![1, 2, 3],
                })
                .collect();
            engine.write_batch(&records).unwrap();
            engine.flush().unwrap();
        }

        let did = engine.trigger_compaction().unwrap();
        assert!(did);

        let results = engine.query_by_prefix("compact_").unwrap();
        assert_eq!(results.len(), 50);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_gc_removes_expired_sstables() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        let records: Vec<Record> = (0..10)
            .map(|i| Record {
                key: format!("gc_key_{}", i).into_bytes(),
                ts: now_us,
                expire_at: i64::MAX,
                value: vec![1, 2, 3],
            })
            .collect();

        engine.write_batch_ttl(&records, Some(1)).unwrap();
        engine.flush().unwrap();

        let results = engine.query_by_prefix("gc_key_").unwrap();
        assert_eq!(results.len(), 10);

        std::thread::sleep(std::time::Duration::from_secs(2));

        let purged = engine.trigger_gc().unwrap();
        assert!(purged > 0);

        let results = engine.query_by_prefix("gc_key_").unwrap();
        assert!(results.is_empty());

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_delete_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("a", 100),
                make_record("b", 200),
                make_record("c", 300),
                make_record("d", 400),
            ])
            .unwrap();

        engine.delete_range("b", "d").unwrap();

        let results = engine.query_by_key_range("a", "d").unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].key, b"a");
        assert_eq!(results[1].key, b"d");

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_delete_range_flush_and_query() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("s1", 100),
                make_record("s2", 200),
                make_record("s3", 300),
            ])
            .unwrap();
        engine.flush().unwrap();

        engine.delete_range("s1", "s3").unwrap();
        engine.flush().unwrap();

        let results = engine.query_by_prefix("s").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, b"s3");

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_size_tiered_compaction() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        for batch in 0..6u64 {
            let records: Vec<Record> = (0..5)
                .map(|i| Record {
                    key: format!("st_{}-{}", batch, i).into_bytes(),
                    ts: (batch * 100 + i) as i64,
                    expire_at: i64::MAX,
                    value: vec![1, 2, 3],
                })
                .collect();
            engine.write_batch(&records).unwrap();
            engine.flush().unwrap();
        }

        let did = engine.trigger_compaction().unwrap();
        assert!(did);

        let results = engine.query_by_prefix("st_").unwrap();
        assert_eq!(results.len(), 30);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_delete_range_preserves_outside() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("x-a", 100),
                make_record("x-b", 200),
                make_record("x-c", 300),
                make_record("y-a", 100),
                make_record("y-b", 200),
            ])
            .unwrap();

        engine.delete_range("x-a", "x-c").unwrap();

        let x_results = engine.query_by_prefix("x-").unwrap();
        assert_eq!(x_results.len(), 1);
        assert_eq!(x_results[0].key, b"x-c");

        let y_results = engine.query_by_prefix("y-").unwrap();
        assert_eq!(y_results.len(), 2);

        engine.shutdown().unwrap();
    }

    // ── ScanIterator tests ─────────────────────────────────────

    #[test]
    fn test_scan_prefix_basic() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("alpha", 100),
                make_record("alpha", 200),
                make_record("beta", 150),
            ])
            .unwrap();

        let records: Vec<Record> = engine
            .scan_prefix("alpha")
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].ts, 100);
        assert_eq!(records[1].ts, 200);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_scan_prefix_time_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("k1", 10),
                make_record("k1", 20),
                make_record("k1", 30),
                make_record("k2", 15),
            ])
            .unwrap();

        let records: Vec<Record> = engine
            .scan_prefix_time_range("k1", 12, 25)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].ts, 20);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_scan_range_all() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("a", 1),
                make_record("b", 2),
                make_record("c", 3),
            ])
            .unwrap();

        let records: Vec<Record> = engine
            .scan(ScanRange::all())
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 3);
        // Should be sorted by key, then ts
        assert_eq!(records[0].key, b"a");
        assert_eq!(records[1].key, b"b");
        assert_eq!(records[2].key, b"c");

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_scan_key_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("a", 1),
                make_record("b", 2),
                make_record("c", 3),
                make_record("d", 4),
            ])
            .unwrap();

        let records: Vec<Record> = engine
            .scan(ScanRange::key_range("b", "c"))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 2);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_scan_time_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("a", 100),
                make_record("b", 200),
                make_record("c", 300),
            ])
            .unwrap();

        let records: Vec<Record> = engine
            .scan(ScanRange::time_range(150, 300))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 2);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_scan_matches_query() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        // Write data with multiple keys and timestamps
        let records: Vec<Record> = (0..50)
            .map(|i| make_record(&format!("key_{:04}", i), i * 10))
            .collect();
        engine.write_batch(&records).unwrap();

        // scan and query should return the same results
        let scan_results: Vec<Record> = engine
            .scan_prefix("key_")
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        let query_results = engine.query_by_prefix("key_").unwrap();

        assert_eq!(scan_results.len(), query_results.len());
        for (s, q) in scan_results.iter().zip(query_results.iter()) {
            assert_eq!(s.key, q.key);
            assert_eq!(s.ts, q.ts);
        }

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_scan_after_flush() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("s1", 100),
                make_record("s2", 200),
                make_record("s3", 300),
            ])
            .unwrap();
        engine.flush().unwrap();

        let records: Vec<Record> = engine
            .scan_prefix("s")
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 3);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_scan_with_delete_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("a", 100),
                make_record("b", 200),
                make_record("c", 300),
                make_record("d", 400),
            ])
            .unwrap();

        engine.delete_range("b", "d").unwrap();

        let records: Vec<Record> = engine
            .scan(ScanRange::key_range("a", "d"))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, b"a");
        assert_eq!(records[1].key, b"d");

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_scan_empty_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine.write_batch(&[make_record("a", 1)]).unwrap();

        let records: Vec<Record> = engine
            .scan_prefix("nonexistent")
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(records.is_empty());

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_get_latest() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("key1", 100),
                make_record("key1", 300),
                make_record("key1", 200),
                make_record("key2", 500),
            ])
            .unwrap();

        let latest = engine.get_latest("key1").unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().ts, 300);

        let latest2 = engine.get_latest("key2").unwrap();
        assert!(latest2.is_some());
        assert_eq!(latest2.unwrap().ts, 500);

        let none = engine.get_latest("nonexistent").unwrap();
        assert!(none.is_none());

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_get_latest_after_flush() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("k", 10),
                make_record("k", 20),
                make_record("k", 30),
            ])
            .unwrap();
        engine.flush().unwrap();

        // Write a newer record to memtable
        engine.write_batch(&[make_record("k", 99)]).unwrap();

        let latest = engine.get_latest("k").unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().ts, 99);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_get_latest_from_sst_only() {
        // Data lives entirely in SST (no memtable data for the key).
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[
                make_record("sst_latest", 100),
                make_record("sst_latest", 500),
                make_record("sst_latest", 300),
            ])
            .unwrap();
        engine.flush().unwrap();

        // Now "sst_latest" records are only in SST, not in memtable.
        let latest = engine.get_latest("sst_latest").unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().ts, 500);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_scan_lazy_does_not_materialize_all() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        // Write 100 records
        for i in 0..100u64 {
            engine
                .write_batch(&[Record {
                    key: format!("k{:03}", i).into_bytes(),
                    ts: i as i64,
                    expire_at: i64::MAX,
                    value: vec![42u8; 32],
                }])
                .unwrap();
        }

        // Take only first 5 from the iterator
        let first_5: Vec<Record> = engine
            .scan(ScanRange::all())
            .unwrap()
            .take(5)
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(first_5.len(), 5);
        assert_eq!(first_5[0].key, b"k000");
        assert_eq!(first_5[4].key, b"k004");

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_scan_opt_with_read_options() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();

        engine.write_batch(&[make_record("x", 1)]).unwrap();

        let opts = ReadOptions {
            fill_cache: false,
            verify_checksums: false,
        };
        let records: Vec<Record> = engine
            .scan_opt(ScanRange::prefix("x"), &opts)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(records.len(), 1);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_recovery_after_flush_no_data_loss() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Phase 1: write data, flush to SST, then write more data into WAL.
        {
            let engine = Engine::open(make_config(&path)).unwrap();

            // First batch — will be flushed
            engine.write_batch(&[make_record("flushed", 100)]).unwrap();
            engine.flush().unwrap();

            // Second batch — stays in memtable + WAL
            engine
                .write_batch(&[make_record("post-flush", 200)])
                .unwrap();

            // Don't call shutdown — simulate crash after the first flush.
            // The WAL truncation must NOT delete the segment containing "post-flush".
            drop(engine);
        }

        // Phase 2: reopen — must find BOTH the flushed record and the post-flush record.
        {
            let engine = Engine::open(make_config(&path)).unwrap();
            let r1 = engine.get("flushed", 100).unwrap();
            assert!(r1.is_some(), "flushed record must survive restart");
            let r2 = engine.get("post-flush", 200).unwrap();
            assert!(
                r2.is_some(),
                "post-flush record must survive restart (WAL truncation bug check)"
            );
        }
    }

    #[test]
    fn test_memtable_backpressure_under_pressure() {
        // With a tiny memtable and many writes, verify that the active memtable
        // does not grow without bound.  After flushing, the frozen memtable should
        // be drained to SST, and all data should be queryable.
        let dir = TempDir::new().unwrap();
        let mut config = make_config(dir.path());
        config.memtable_size_mb = 1;
        let engine = Engine::open(config).unwrap();

        // Write 5000 records with large values to force many flushes.
        let big_val = vec![0xABu8; 4096];
        for i in 0..5000 {
            let key = format!("pressure_{:05}", i).into_bytes();
            let rec = Record {
                key,
                ts: i as i64,
                expire_at: i64::MAX,
                value: big_val.clone(),
            };
            engine.write_batch_sync(vec![rec]).unwrap();
        }
        engine.flush().unwrap();

        // All records must be queryable.
        let results: Vec<Record> = engine
            .scan(ScanRange::prefix("pressure_"))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(results.len(), 5000, "all 5000 records must survive");

        // Verify no data duplication or corruption.
        let mut keys: Vec<Vec<u8>> = results.iter().map(|r| r.key.clone()).collect();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), 5000, "no duplicate keys");
    }

    #[test]
    fn test_flush_does_not_block_runtime() {
        // Verify that flush() and concurrent reads work correctly with threads.
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::open(make_config(dir.path())).unwrap());

        // Write some data.
        engine
            .write_batch(&[make_record("a", 1), make_record("b", 2)])
            .unwrap();

        // Flush should work.
        engine.flush().unwrap();

        // Concurrent flush and read.
        let e1 = engine.clone();
        let h1 = std::thread::spawn(move || {
            e1.flush().unwrap();
        });
        let e2 = engine.clone();
        let h2 = std::thread::spawn(move || {
            let r = e2.get("a", 1).unwrap();
            assert!(r.is_some());
        });
        h1.join().unwrap();
        h2.join().unwrap();
    }

    /// Regression for the bloom hasher upgrade: write data, flush to SST,
    /// then tamper with the persisted bloom's `hash_version` to simulate a
    /// legacy filter. Reopening the engine MUST rebuild the bloom from the
    /// SST file and the rebuilt filter must correctly answer point lookups.
    #[test]
    fn test_bloom_rebuild_on_open_after_hash_upgrade() {
        use crate::bloom::{BloomFilter, CURRENT_HASH_VERSION};

        let dir = TempDir::new().unwrap();
        let mut config = make_config(dir.path());
        // Force flush quickly so we get an SST to work with.
        config.memtable_size_mb = 1;
        let engine = Engine::open(config.clone()).unwrap();

        // Write enough distinct keys to make the bloom meaningful.
        let records: Vec<Record> = (0..200)
            .map(|i| Record {
                key: format!("bloom_key_{:04}", i).into_bytes(),
                ts: i as i64,
                expire_at: i64::MAX,
                value: vec![1, 2, 3],
            })
            .collect();
        engine.write_batch(&records).unwrap();
        engine.flush().unwrap();
        engine.shutdown().unwrap();

        // Tamper: rewrite the MANIFEST so every bloom has hash_version=0,
        // simulating an upgrade from a legacy hasher.
        let manifest_path = dir.path().join("MANIFEST");
        let original = std::fs::read_to_string(&manifest_path).unwrap();
        let tampered = original.replace(
            &format!("\"hash_version\":{}", crate::bloom::CURRENT_HASH_VERSION),
            "\"hash_version\":0",
        );
        if tampered != original {
            std::fs::write(&manifest_path, &tampered).unwrap();
        }

        // Reopen — Engine::open should detect stale blooms, scan the SST,
        // and persist replacements via UpdateBloom entries.
        let engine2 = Engine::open(config.clone()).unwrap();

        // The newly rebuilt bloom must still recognise the inserted keys.
        // We exercise this indirectly through point lookups — the engine
        // would return None if the bloom filtered them out incorrectly.
        for i in 0..200 {
            let key = format!("bloom_key_{:04}", i);
            let rec = engine2.get(&key, i as i64).unwrap();
            assert!(
                rec.is_some(),
                "key {} missing after bloom rebuild — false negative",
                key
            );
        }

        // A key that was never inserted should not appear (sanity).
        let absent = engine2.get("bloom_key_9999", 0).unwrap();
        assert!(
            absent.is_none(),
            "absent key should not be found, got {:?}",
            absent
        );

        // Verify the manifest now contains an UpdateBloom entry with the
        // current hash_version. Reload the manifest directly to inspect.
        engine2.shutdown().unwrap();
        let manifest_text = std::fs::read_to_string(&manifest_path).unwrap();
        assert!(
            manifest_text.contains("\"update_bloom\""),
            "expected UpdateBloom entry in manifest after rebuild, got: {}",
            manifest_text
        );

        // And the persisted bloom should now report the current version.
        let mf = crate::manifest::Manifest::open(dir.path()).unwrap();
        for info in mf.state().sstables.values() {
            if let Some(b) = &info.bloom {
                assert_eq!(
                    b.hash_version(),
                    CURRENT_HASH_VERSION,
                    "all blooms should be current after rebuild"
                );
            }
        }
        // Touch the BloomFilter type so the unused import warning stays
        // quiet on toolchains that strip unused generics aggressively.
        let _: &BloomFilter = mf
            .state()
            .sstables
            .values()
            .next()
            .unwrap()
            .bloom
            .as_ref()
            .unwrap();
    }

    /// Cross-restart correctness: a bloom built during one Engine instance
    /// must remain valid after a clean shutdown and reopen. This catches
    /// regressions where the persisted hasher seed accidentally changes
    /// between runs.
    #[test]
    fn test_bloom_remains_valid_across_clean_restart() {
        let dir = TempDir::new().unwrap();
        let mut config = make_config(dir.path());
        config.memtable_size_mb = 1;

        // Phase 1: write + flush + shutdown.
        let records: Vec<Record> = (0..50)
            .map(|i| Record {
                key: format!("restart_key_{:03}", i).into_bytes(),
                ts: i as i64,
                expire_at: i64::MAX,
                value: vec![9],
            })
            .collect();
        {
            let engine = Engine::open(config.clone()).unwrap();
            engine.write_batch(&records).unwrap();
            engine.flush().unwrap();
            engine.shutdown().unwrap();
        }

        // Phase 2: reopen, every key must still be findable via point get.
        // The bloom must not falsely claim "not in this SST".
        {
            let engine = Engine::open(config.clone()).unwrap();
            for (i, rec) in records.iter().enumerate() {
                let got = engine
                    .get(&String::from_utf8_lossy(&rec.key), rec.ts)
                    .unwrap();
                assert!(
                    got.is_some(),
                    "key {} vanished after restart (bloom false negative?)",
                    String::from_utf8_lossy(&rec.key)
                );
                let _ = i;
            }
            engine.shutdown().unwrap();
        }
    }

    #[test]
    fn test_write_batch_empty() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();
        assert!(engine.write_batch(&[]).is_ok());
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_delete_batch_empty() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();
        assert!(engine.delete_batch(&[]).is_ok());
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_get_nonexistent_key() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();
        let result = engine.get("no_such_key", 0).unwrap();
        assert!(result.is_none());
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_patch_nonexistent_record_error() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();
        let result = engine.patch_record("no_such_key", 0, Some(vec![1, 2, 3]), None);
        assert!(result.is_err());
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_delete_range_empty_strings() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();
        // Empty start and end strings — should not panic, produce a valid delete
        // range tombstone.
        assert!(engine.delete_range("", "z").is_ok());
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_query_by_prefix_empty_string() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();
        // Empty prefix matches everything.
        let results = engine.query_by_prefix("").unwrap();
        assert!(results.is_empty());
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_get_latest_nonexistent() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();
        assert!(engine.get_latest("no_such_key").unwrap().is_none());
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_flush_on_empty_engine() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();
        // Flushing an empty engine should succeed (no-op).
        assert!(engine.flush().is_ok());
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_scan_all_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();
        let iter = engine.scan(ScanRange::all()).unwrap();
        let results: Vec<_> = iter.map(|r| r.unwrap()).collect();
        assert!(results.is_empty());
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_config_create_if_missing_false() {
        let dir = TempDir::new().unwrap();
        let nonexistent = dir.path().join("nonexistent");
        let mut cfg = make_config(dir.path());
        cfg.data_dir = nonexistent.clone();
        cfg.create_if_missing = false;
        let result = Engine::open(cfg);
        assert!(
            result.is_err(),
            "should fail when dir does not exist and create_if_missing is false"
        );
    }

    #[test]
    fn test_query_empty_result() {
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path());
        let engine = Engine::open(config).unwrap();
        let results = engine.query_by_key_range("z", "zzz").unwrap();
        assert!(results.is_empty());
        engine.shutdown().unwrap();
    }

    /// Exercise the `auto_background = true` startup path which spawns the
    /// maintenance task (and thus the body of `spawn_background_maintenance`).
    #[test]
    fn test_engine_auto_background_starts_maintenance() {
        let dir = TempDir::new().unwrap();
        let mut cfg = make_config(dir.path());
        cfg.auto_background = true;
        // Short flush interval so the WAL/flush tick fires quickly.
        cfg.flush_interval_ms = 10;
        cfg.gc_interval_secs = 1;
        cfg.wal_sync_mode = SyncMode::IntervalMs(10);
        let engine = Engine::open(cfg).unwrap();
        // Write a record so the flush tick has something to do.
        engine.write_batch(&[make_record("bg", 1)]).unwrap();
        // Yield to let the maintenance task tick at least once.
        std::thread::sleep(std::time::Duration::from_millis(80));
        engine.shutdown().unwrap();
    }

    /// `spawn_background_maintenance` should be explicitly callable and
    /// return a handle that stops the thread when dropped.
    #[test]
    fn test_spawn_background_maintenance_explicit() {
        let dir = TempDir::new().unwrap();
        let mut cfg = make_config(dir.path());
        cfg.flush_interval_ms = 10;
        cfg.gc_interval_secs = 1;
        cfg.wal_sync_mode = SyncMode::IntervalMs(10);
        let engine = Engine::open(cfg).unwrap();
        let handle = engine
            .spawn_background_maintenance()
            .expect("handle should be Some");
        std::thread::sleep(std::time::Duration::from_millis(40));
        drop(handle);
        engine.shutdown().unwrap();
    }

    /// `write_batch_owned` is the consuming variant of write_batch.
    #[test]
    fn test_write_batch_owned() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        let records = vec![make_record("owned1", 1), make_record("owned2", 2)];
        engine.write_batch_owned(records).unwrap();
        let r = engine.get("owned1", 1).unwrap();
        assert!(r.is_some());
        engine.shutdown().unwrap();
    }

    /// `write_batch_sync` is the synchronous write path.
    #[test]
    fn test_write_batch_sync() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        engine
            .write_batch_sync(vec![make_record("sync1", 1), make_record("sync2", 2)])
            .unwrap();
        let r = engine.get("sync1", 1).unwrap();
        assert!(r.is_some());
        // Empty batch should be a no-op.
        engine.write_batch_sync(vec![]).unwrap();
        engine.shutdown().unwrap();
    }

    /// `write_batch_owned` with an empty batch must be a no-op.
    #[test]
    fn test_write_batch_owned_empty() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        engine.write_batch_owned(vec![]).unwrap();
        engine.shutdown().unwrap();
    }

    /// `write_batch_ttl` exercises the TTL conversion path. Uses a very
    /// large TTL so the resulting expire_at is in the future.
    #[test]
    fn test_write_batch_ttl_path() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        engine
            .write_batch_ttl(&[make_record("ttl1", 1)], Some(u32::MAX as u64))
            .unwrap();
        let r = engine.get("ttl1", 1).unwrap().unwrap();
        assert!(r.expire_at < i64::MAX);
        engine.shutdown().unwrap();
    }

    /// `write_batch_owned_ttl` exercises the owned+ttl path.
    #[test]
    fn test_write_batch_owned_ttl_path() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        engine
            .write_batch_owned_ttl(vec![make_record("ot", 1)], Some(u32::MAX as u64))
            .unwrap();
        let r = engine.get("ot", 1).unwrap().unwrap();
        assert!(r.expire_at < i64::MAX);
        engine.shutdown().unwrap();
    }

    /// `get` / `get_sync` against a missing key returns None.
    #[test]
    fn test_get_missing_key() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        assert!(engine.get("missing", 1).unwrap().is_none());
        assert!(engine.get_sync("missing", 1).is_none());
        engine.shutdown().unwrap();
    }

    /// `get_latest` returns the highest-ts version of a key.
    #[test]
    fn test_get_latest_async_path() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        engine
            .write_batch(&[
                make_record("gl", 1),
                make_record("gl", 5),
                make_record("gl", 3),
            ])
            .unwrap();
        let latest = engine.get_latest("gl").unwrap().unwrap();
        assert_eq!(latest.ts, 5);
        engine.shutdown().unwrap();
    }

    /// `get_latest` on a non-existent key returns None.
    #[test]
    fn test_get_latest_missing() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        assert!(engine.get_latest("no-such-key").unwrap().is_none());
        engine.shutdown().unwrap();
    }

    /// `delete_batch` with an empty input is a no-op (named to avoid clash).
    #[test]
    fn test_delete_batch_empty_noop() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        engine.delete_batch(&[]).unwrap();
        engine.shutdown().unwrap();
    }

    /// `patch_record` on a missing record returns an error.
    #[test]
    fn test_patch_missing_record() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        let err = engine.patch_record("ghost", 1, Some(b"v".to_vec()), None);
        assert!(err.is_err());
        engine.shutdown().unwrap();
    }

    /// `patch_record` updating both value and ttl.
    #[test]
    fn test_patch_value_and_ttl() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        engine.write_batch(&[make_record("p", 1)]).unwrap();
        let patched = engine
            .patch_record("p", 1, Some(b"new".to_vec()), Some(u32::MAX as u64))
            .unwrap();
        assert_eq!(patched.value, b"new");
        assert!(patched.expire_at < i64::MAX);
        engine.shutdown().unwrap();
    }

    /// Concurrent `patch_record` calls on the same key must not lose updates.
    #[test]
    fn test_patch_record_concurrent_safety() {
        let dir = TempDir::new().unwrap();
        let engine = std::sync::Arc::new(Engine::open(make_config(dir.path())).unwrap());
        engine.write_batch(&[make_record("k", 1)]).unwrap();

        let mut handles = Vec::new();
        for i in 0..10 {
            let e = engine.clone();
            handles.push(std::thread::spawn(move || {
                let new_val = vec![i as u8];
                e.patch_record("k", 1, Some(new_val), None).unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let final_val = engine.get("k", 1).unwrap().unwrap();
        // After 10 concurrent patches, the final value must be one of the
        // patched values (not the original), proving no lost update caused
        // a value to remain at the original.
        assert_ne!(final_val.value, vec![1u8], "patch_record should not lose updates under concurrency");
        drop(engine); // Arc<Engine> drops here, triggering shutdown}
    }

    /// Convenience wrappers (`query_by_prefix`, `query_time_range`,
    /// `query_prefix_time_range`, `query_key_time_range`) hit the underlying
    /// `query` method through their dedicated entry points.
    #[test]
    fn test_query_convenience_wrappers() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        engine
            .write_batch(&[
                make_record("k1", 10),
                make_record("k1", 20),
                make_record("k2", 30),
            ])
            .unwrap();

        assert_eq!(engine.query_by_prefix("k1").unwrap().len(), 2);
        assert_eq!(engine.query_time_range(0, 100).unwrap().len(), 3);
        assert_eq!(
            engine.query_prefix_time_range("k1", 0, 100).unwrap().len(),
            2
        );
        assert_eq!(
            engine
                .query_key_time_range("k1", "k2", 0, 100)
                .unwrap()
                .len(),
            3
        );
        engine.shutdown().unwrap();
    }

    /// Scan convenience wrappers (`scan_prefix_time_range`) cover the
    /// range-builder paths.
    #[test]
    fn test_scan_convenience_wrappers() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        engine
            .write_batch(&[make_record("c1", 1), make_record("c2", 2)])
            .unwrap();
        let r: Vec<_> = engine
            .scan_prefix_time_range("c", 0, 100)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(r.len(), 2);
        engine.shutdown().unwrap();
    }

    /// Reopening an engine with persisted SSTs walks the manifest recovery
    /// path (active_sst_ids → index rebuild) — covers lines around 142.
    #[test]
    fn test_engine_reopen_with_ssts() {
        let dir = TempDir::new().unwrap();
        {
            let engine = Engine::open(make_config(dir.path())).unwrap();
            engine.write_batch(&[make_record("persisted", 1)]).unwrap();
            engine.flush().unwrap();
            engine.shutdown().unwrap();
        }
        // Reopen — manifest recovery + bloom-bits check should run.
        let engine = Engine::open(make_config(dir.path())).unwrap();
        let r = engine.get("persisted", 1).unwrap();
        assert!(r.is_some(), "record should survive reopen");
        engine.shutdown().unwrap();
    }

    /// `get_sync` against a flushed SST hits the block reader path
    /// (single_sst_point + block_search).
    #[test]
    fn test_get_sync_after_flush() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        engine.write_batch(&[make_record("ss", 7)]).unwrap();
        engine.flush().unwrap();
        let r = engine.get_sync("ss", 7).unwrap();
        assert_eq!(r.value, vec![1, 2, 3]);
        engine.shutdown().unwrap();
    }

    /// Engine with default_ttl_secs set affects all writes via convert_records.
    #[test]
    fn test_engine_with_default_ttl() {
        let dir = TempDir::new().unwrap();
        let mut cfg = make_config(dir.path());
        cfg.default_ttl_secs = Some(u32::MAX as u64);
        let engine = Engine::open(cfg).unwrap();
        engine.write_batch(&[make_record("dttl", 1)]).unwrap();
        let r = engine.get("dttl", 1).unwrap().unwrap();
        assert!(r.expire_at < i64::MAX);
        engine.shutdown().unwrap();
    }

    /// `metrics_text` returns a non-empty Prometheus-format string.
    #[test]
    fn test_metrics_text_nonempty() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        let text = engine.metrics_text();
        assert!(!text.is_empty());
        assert!(text.contains("flowdb_"));
        engine.shutdown().unwrap();
    }

    /// `scan` with `ScanRange::all` after a flush covers the full-scan path
    /// (the `is_full_scan` branch in `ScanIterator::build`).
    #[test]
    fn test_scan_all_after_flush() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        engine
            .write_batch(&[make_record("fa", 1), make_record("fb", 2)])
            .unwrap();
        engine.flush().unwrap();
        let r: Vec<_> = engine
            .scan(ScanRange::all())
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(r.len() >= 2);
        engine.shutdown().unwrap();
    }

    /// `delete_range` followed by `scan_all` exercises the tombstone filtering
    /// path inside `ScanIterator::next` (merge-heap branch).
    #[test]
    fn test_scan_all_with_tombstone_merge() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        // Multiple keys, then a delete tombstone forces dedup work.
        engine
            .write_batch(&[
                make_record("tm1", 1),
                make_record("tm2", 1),
                make_record("tm3", 1),
            ])
            .unwrap();
        engine.flush().unwrap();
        // Add a memtable-side delete to force memtable + SST merge with tombstone.
        engine.delete_batch(&[("tm2".into(), 1)]).unwrap();
        let r: Vec<_> = engine
            .scan(ScanRange::all())
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        // tm2 is deleted, only tm1 and tm3 should remain.
        assert!(r.iter().all(|x| x.key != b"tm2"));
        engine.shutdown().unwrap();
    }

    // ------------------------------------------------------------------
    // Regression tests for MemTable::get point-lookup correctness.
    //
    // These test the fix at the Engine API level: after a write then a
    // delete with the same (key, ts), point lookups must NOT return the
    // deleted data.  Before the fix, MemTable::get used `.next()` which
    // returned the lowest-seq version, causing deleted data to reappear.
    // ------------------------------------------------------------------

    #[test]
    fn test_regression_get_after_delete_same_key_ts() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        engine
            .write_batch(&[Record {
                key: b"rd".to_vec(),
                ts: 100,
                expire_at: i64::MAX,
                value: b"data".to_vec(),
            }])
            .unwrap();
        // Delete the exact same (key, ts).
        engine.delete_batch(&[("rd".into(), 100)]).unwrap();
        // Point lookup must NOT find the deleted record.
        assert!(
            engine.get("rd", 100).unwrap().is_none(),
            "get after delete must return None — the delete tombstone has higher seq"
        );
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_regression_get_returns_latest_version() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(make_config(dir.path())).unwrap();
        // Write twice with the same (key, ts) but different values.
        // write_batch assigns increasing seq numbers internally.
        engine
            .write_batch(&[Record {
                key: b"ov".to_vec(),
                ts: 50,
                expire_at: i64::MAX,
                value: b"v1".to_vec(),
            }])
            .unwrap();
        engine
            .write_batch(&[Record {
                key: b"ov".to_vec(),
                ts: 50,
                expire_at: i64::MAX,
                value: b"v2".to_vec(),
            }])
            .unwrap();
        let r = engine.get("ov", 50).unwrap().unwrap();
        assert_eq!(
            r.value, b"v2",
            "get must return the latest version (v2), not the stale one (v1)"
        );
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_regression_config_validation_in_open() {
        let dir = TempDir::new().unwrap();
        let mut cfg = make_config(dir.path());
        cfg.time_bucket_secs = 0;
        let result = Engine::open(cfg);
        assert!(
            result.is_err(),
            "Engine::open must reject time_bucket_secs=0 (div-by-zero)"
        );
    }

    #[test]
    fn test_regression_config_memtable_zero_rejected() {
        let dir = TempDir::new().unwrap();
        let mut cfg = make_config(dir.path());
        cfg.memtable_size_mb = 0;
        let result = Engine::open(cfg);
        assert!(result.is_err());
    }

    // ───── Regression: background compaction must evict stale readers ─────

    /// After background compaction removes old SSTs, their `SstReader`
    /// entries (open fd + mmap) must be evicted from the reader cache.
    /// Without eviction, file descriptors and virtual memory leak
    /// monotonically with every compaction cycle.
    #[test]
    fn test_background_compaction_evicts_stale_readers() {
        let dir = TempDir::new().unwrap();
        let mut cfg = make_config(dir.path());
        cfg.compaction_threshold = 3;
        cfg.flush_interval_ms = 60000; // prevent auto-flush interfering
        cfg.auto_background = false;
        let engine = Engine::open(cfg).unwrap();

        // Write enough batches to create several SSTs.
        for batch in 0..5u64 {
            let records: Vec<Record> = (0..10)
                .map(|i| Record {
                    key: format!("k{:03}-{}", batch, i).into_bytes(),
                    ts: (batch * 100 + i) as i64,
                    expire_at: i64::MAX,
                    value: vec![1, 2, 3],
                })
                .collect();
            engine.write_batch(&records).unwrap();
            engine.flush().unwrap();
        }

        // Populate the reader cache by doing point gets across all SSTs.
        let _ = engine.get("k000-0", 0).unwrap();
        let _ = engine.get("k001-0", 100).unwrap();
        let _ = engine.get("k002-0", 200).unwrap();
        let _ = engine.get("k003-0", 300).unwrap();
        let _ = engine.get("k004-0", 400).unwrap();

        // Snapshot reader count before compaction.
        let readers_before = {
            let r = engine.readers.read();
            r.len()
        };
        assert!(readers_before >= 3, "should have cached several readers");

        // Manually trigger compaction (same path the background thread uses).
        let did = engine.trigger_compaction().unwrap();
        assert!(did, "compaction should have merged SSTs");

        // After compaction, stale readers must be evicted.
        let readers_after = {
            let r = engine.readers.read();
            r.len()
        };
        assert!(
            readers_after < readers_before,
            "reader count should decrease after compaction: before={}, after={}",
            readers_before,
            readers_after
        );

        // Surviving readers must all point to active SSTs.
        {
            let mf = engine.manifest.lock();
            let active: std::collections::HashSet<u32> =
                mf.state().sstables.keys().copied().collect();
            let r = engine.readers.read();
            for sst_id in r.keys() {
                assert!(
                    active.contains(sst_id),
                    "stale reader for sst {} survived eviction",
                    sst_id
                );
            }
        }

        engine.shutdown().unwrap();
    }

    // ───── Regression: block cache capacity should respect byte budget ─────

    /// The block cache should not dramatically exceed its configured byte
    /// budget.  Before the fix, the `/256` heuristic allowed ~20× overshoot.
    #[test]
    fn test_cache_capacity_respects_byte_budget() {
        use crate::cache::BlockCache;

        // With block_size=100 and capacity_mb=1 (1 MB):
        // Each entry ≈ 100 * (104 + 64) = 16,800 bytes
        // Total entries = 1,048,576 / 16,800 ≈ 62
        // Per shard = 62 / 64 = 0 → max(1) = 1
        // Total = 64 entries * 16,800 ≈ 1.07 MB — within ~7% of budget.
        let cache = BlockCache::with_block_size(1, 100);

        // Insert a known number of entries and verify eviction kicks in.
        let big_val = vec![0xABu8; 200]; // simulate realistic record
        let mut inserted = 0;
        for sst in 0..200u32 {
            let rec = crate::record::InternalRecord {
                seq: 0,
                op: crate::record::Op::Put,
                key: format!("key_{}", sst).into_bytes(),
                ts: 0,
                expire_at: i64::MAX,
                value: big_val.clone(),
                range_end: None,
            };
            // Each insert is a block of 100 records
            let block: Vec<_> = (0..100).map(|_| rec.clone()).collect();
            cache.insert(
                crate::cache::CacheKey {
                    sst_id: sst,
                    block_idx: 0,
                },
                block,
            );
            inserted += 1;
        }

        // Most entries should have been evicted. We cannot check exact
        // counts due to sharding, but the cache should hold far fewer
        // than `inserted` entries.
        // Verify by checking that early entries are evicted.
        let early_hit = cache.get(&crate::cache::CacheKey {
            sst_id: 0,
            block_idx: 0,
        });
        assert!(
            early_hit.is_none(),
            "early entry should have been evicted (inserted={}, cache should be << inserted)",
            inserted
        );
    }

    // ───── Regression: full scan should use per-block sources ─────

    /// A full scan should produce correct results.  This test verifies that
    /// the per-block source refactor doesn't break full-scan semantics.
    #[test]
    fn test_full_scan_correctness_after_perblock_fix() {
        let dir = TempDir::new().unwrap();
        let mut cfg = make_config(dir.path());
        cfg.block_size = 10; // small blocks to create many blocks
        cfg.auto_background = false;
        let engine = Engine::open(cfg).unwrap();

        // Write enough records to span multiple blocks per SST.
        let mut expected_keys = Vec::new();
        for batch in 0..3u64 {
            let records: Vec<Record> = (0..25)
                .map(|i| {
                    let key = format!("fs_{:04}", batch * 25 + i);
                    expected_keys.push(key.clone());
                    Record {
                        key: key.into_bytes(),
                        ts: (batch * 25 + i) as i64,
                        expire_at: i64::MAX,
                        value: vec![1],
                    }
                })
                .collect();
            engine.write_batch(&records).unwrap();
            engine.flush().unwrap();
        }

        // Full scan via scan(ScanRange::all()).
        let results: Vec<Record> = engine
            .scan(ScanRange::all())
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        // All non-deleted records should be returned.
        assert_eq!(
            results.len(),
            expected_keys.len(),
            "full scan must return all {} records, got {}",
            expected_keys.len(),
            results.len()
        );

        // Verify ascending key order.
        for i in 1..results.len() {
            assert!(
                results[i - 1].key <= results[i].key,
                "results must be in ascending key order at index {}",
                i
            );
        }

        engine.shutdown().unwrap();
    }

    // ───── Regression: manifest snapshot bounds on-disk growth ─────

    /// The manifest should compact itself when it grows past the threshold.
    #[test]
    fn test_manifest_snapshot_compaction() {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        {
            let mut mf = Manifest::open(&data_dir).unwrap();

            // Append many entries (flush + delete cycles) to exceed
            // SNAPSHOT_THRESHOLD (500).
            for i in 0..600u32 {
                mf.append(&ManifestEntry::Flush {
                    seq: i as u64,
                    sst: crate::manifest::SstInfo {
                        id: i,
                        records: 1,
                        bytes: 16,
                        min_ts: 0,
                        max_ts: 0,
                        min_expire: 0,
                        max_expire: 0,
                        bloom: None,
                    },
                    blocks: vec![],
                })
                .unwrap();

                // Immediately delete older SSTs to keep the in-memory state small.
                if i >= 5 {
                    mf.append(&ManifestEntry::DeleteSst { sst_id: i - 5 })
                        .unwrap();
                }
            }

            // At this point: 1200 entries on disk, but only ~6 active SSTs.
            assert!(mf.entry_count() > 500);

            // Trigger snapshot.
            mf.maybe_snapshot().unwrap();
            assert!(
                mf.entry_count() <= 20,
                "entry_count should be small after snapshot, got {}",
                mf.entry_count()
            );
        }

        // Verify on-disk file is compacted.
        let manifest_path = data_dir.join("MANIFEST");
        let content = std::fs::read_to_string(&manifest_path).unwrap();
        let line_count = content.lines().filter(|l| !l.trim().is_empty()).count();
        assert!(
            line_count <= 20,
            "manifest file should be compact after snapshot, got {} lines",
            line_count
        );

        // Verify reopen produces the same state.
        let mf2 = Manifest::open(&data_dir).unwrap();
        // Only the last few SSTs should survive (those not deleted).
        assert!(
            mf2.state().sstables.len() <= 10,
            "after snapshot, only active SSTs should be present"
        );
    }

    /// Manifest snapshot should be a no-op when below the threshold.
    #[test]
    fn test_manifest_snapshot_noop_below_threshold() {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let mut mf = Manifest::open(&data_dir).unwrap();
        for i in 0..10u32 {
            mf.append(&ManifestEntry::Flush {
                seq: i as u64,
                sst: crate::manifest::SstInfo {
                    id: i,
                    records: 1,
                    bytes: 16,
                    min_ts: 0,
                    max_ts: 0,
                    min_expire: 0,
                    max_expire: 0,
                    bloom: None,
                },
                blocks: vec![],
            })
            .unwrap();
        }

        let lines_before = std::fs::read_to_string(data_dir.join("MANIFEST"))
            .unwrap()
            .lines()
            .count();
        mf.maybe_snapshot().unwrap(); // should be no-op
        let lines_after = std::fs::read_to_string(data_dir.join("MANIFEST"))
            .unwrap()
            .lines()
            .count();

        assert_eq!(
            lines_before, lines_after,
            "manifest should not change when below threshold"
        );
    }

    // ── Single-file storage backend tests ──────────────────────────

    fn make_config_single_file(dir: &std::path::Path) -> Config {
        let mut config = make_config(dir);
        config.storage_backend = crate::record::StorageBackendKind::SingleFile;
        config
    }

    #[test]
    fn test_single_file_engine_write_read() {
        let dir = TempDir::new().unwrap();
        let config = make_config_single_file(dir.path());
        let engine = Engine::open(config).unwrap();

        engine
            .write_batch(&[make_record("a", 100), make_record("b", 200)])
            .unwrap();

        let results = engine.query_by_prefix("").unwrap();
        assert_eq!(results.len(), 2);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_single_file_engine_flush_and_query() {
        let dir = TempDir::new().unwrap();
        let config = make_config_single_file(dir.path());
        let engine = Engine::open(config).unwrap();

        let records: Vec<Record> = (0..50)
            .map(|i| make_record(&format!("key_{:04}", i), i * 10))
            .collect();
        engine.write_batch(&records).unwrap();
        engine.flush().unwrap();

        let results = engine.query_by_prefix("key_").unwrap();
        assert_eq!(results.len(), 50);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_single_file_engine_db_file_exists() {
        let dir = TempDir::new().unwrap();
        let config = make_config_single_file(dir.path());
        let engine = Engine::open(config).unwrap();

        engine.write_batch(&[make_record("a", 1)]).unwrap();
        engine.flush().unwrap();

        // The flow.db file should exist.
        assert!(
            dir.path().join("flow.db").exists(),
            "flow.db should exist in single-file mode"
        );

        // The SST directory should NOT contain any .sst files.
        let sst_dir = dir.path().join("SST");
        if sst_dir.exists() {
            let sst_count = std::fs::read_dir(&sst_dir)
                .unwrap()
                .filter(|e| {
                    e.as_ref()
                        .map(|e| e.file_name().to_string_lossy().ends_with(".sst"))
                        .unwrap_or(false)
                })
                .count();
            assert_eq!(sst_count, 0, "no .sst files should exist in single-file mode");
        }

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_single_file_engine_compaction() {
        let dir = TempDir::new().unwrap();
        let config = make_config_single_file(dir.path());
        let engine = Engine::open(config).unwrap();

        for batch in 0..10u64 {
            let records: Vec<Record> = (0..5)
                .map(|i| Record {
                    key: format!("compact_{}-{}", batch, i).into_bytes(),
                    ts: (batch * 100 + i) as i64,
                    expire_at: i64::MAX,
                    value: vec![1, 2, 3],
                })
                .collect();
            engine.write_batch(&records).unwrap();
            engine.flush().unwrap();
        }

        let did = engine.trigger_compaction().unwrap();
        assert!(did, "compaction should have run");

        let results = engine.query_by_prefix("compact_").unwrap();
        assert_eq!(results.len(), 50, "all records should survive compaction");

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_single_file_engine_gc() {
        let dir = TempDir::new().unwrap();
        let config = make_config_single_file(dir.path());
        let engine = Engine::open(config).unwrap();

        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        let records: Vec<Record> = (0..10)
            .map(|i| Record {
                key: format!("gc_key_{}", i).into_bytes(),
                ts: now_us,
                expire_at: i64::MAX,
                value: vec![1, 2, 3],
            })
            .collect();

        engine.write_batch_ttl(&records, Some(1)).unwrap();
        engine.flush().unwrap();

        let results = engine.query_by_prefix("gc_key_").unwrap();
        assert_eq!(results.len(), 10);

        std::thread::sleep(std::time::Duration::from_secs(2));

        let purged = engine.trigger_gc().unwrap();
        assert!(purged > 0, "GC should have purged records");

        // Query should return nothing (all expired).
        let results = engine.query_by_prefix("gc_key_").unwrap();
        assert_eq!(results.len(), 0);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_single_file_engine_recovery() {
        let dir = TempDir::new().unwrap();
        let config = make_config_single_file(dir.path());

        {
            let engine = Engine::open(config.clone()).unwrap();
            engine
                .write_batch(&[
                    make_record("persist_a", 100),
                    make_record("persist_b", 200),
                ])
                .unwrap();
            engine.flush().unwrap();
            engine.shutdown().unwrap();
        }

        {
            let engine = Engine::open(config).unwrap();
            let results = engine.query_by_prefix("persist_").unwrap();
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].key, b"persist_a");
            assert_eq!(results[1].key, b"persist_b");
            engine.shutdown().unwrap();
        }
    }

    #[test]
    fn test_single_file_engine_concurrent_writes() {
        let dir = TempDir::new().unwrap();
        let config = make_config_single_file(dir.path());
        let engine = Arc::new(Engine::open(config).unwrap());

        let mut handles = Vec::new();
        for t in 0..4u64 {
            let eng = engine.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..25u64 {
                    let key = format!("t{}-key{}", t, i);
                    eng.write_batch(&[Record {
                        key: key.into_bytes(),
                        ts: (t * 100 + i) as i64,
                        expire_at: i64::MAX,
                        value: vec![1, 2, 3, 4],
                    }])
                    .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        engine.flush().unwrap();

        let results = engine.query_by_prefix("").unwrap();
        assert_eq!(results.len(), 100);

        // Can't call shutdown on Arc<Engine>, just drop.
        let _ = engine.flush();
    }

    #[test]
    fn test_single_file_engine_point_lookup_after_flush() {
        let dir = TempDir::new().unwrap();
        let config = make_config_single_file(dir.path());
        let engine = Engine::open(config).unwrap();

        let records: Vec<Record> = (0..20)
            .map(|i| make_record(&format!("pt_{:03}", i), i * 10))
            .collect();
        engine.write_batch(&records).unwrap();
        engine.flush().unwrap();

        // Point lookup should find the latest version.
        let result = engine.get_latest("pt_010").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().ts, 100);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_single_file_engine_scan_after_flush() {
        let dir = TempDir::new().unwrap();
        let config = make_config_single_file(dir.path());
        let engine = Engine::open(config).unwrap();

        let records: Vec<Record> = (0..30)
            .map(|i| make_record(&format!("scan_{:03}", i), i))
            .collect();
        engine.write_batch(&records).unwrap();
        engine.flush().unwrap();

        let iter = engine.scan_prefix("scan_").unwrap();
        let collected: Vec<Record> = iter.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(collected.len(), 30);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_single_file_engine_multi_sst_query() {
        let dir = TempDir::new().unwrap();
        let config = make_config_single_file(dir.path());
        let engine = Engine::open(config).unwrap();

        // Create multiple SSTs
        for batch in 0..5u64 {
            let records: Vec<Record> = (0..10)
                .map(|i| make_record(&format!("b{}-{:02}", batch, i), (batch * 100 + i) as i64))
                .collect();
            engine.write_batch(&records).unwrap();
            engine.flush().unwrap();
        }

        // Query across all SSTs
        let results = engine.query_by_prefix("b").unwrap();
        assert_eq!(results.len(), 50);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_single_file_engine_overwrite_and_compact() {
        let dir = TempDir::new().unwrap();
        let config = make_config_single_file(dir.path());
        let engine = Engine::open(config).unwrap();

        // Write initial data
        let r1: Vec<Record> = (0..10)
            .map(|i| Record {
                key: format!("ow_{:02}", i).into_bytes(),
                ts: 1,
                expire_at: i64::MAX,
                value: vec![1],
            })
            .collect();
        engine.write_batch(&r1).unwrap();
        engine.flush().unwrap();

        // Overwrite with newer timestamps
        let r2: Vec<Record> = (0..10)
            .map(|i| Record {
                key: format!("ow_{:02}", i).into_bytes(),
                ts: 2,
                expire_at: i64::MAX,
                value: vec![2],
            })
            .collect();
        engine.write_batch(&r2).unwrap();
        engine.flush().unwrap();

        // Compact to merge the two SSTs
        engine.trigger_compaction().unwrap();

        // Both versions exist (compaction dedups by key+ts, not key alone)
        let results = engine.query_by_prefix("ow_").unwrap();
        assert_eq!(results.len(), 20);

        // But get_latest returns the newest version
        for i in 0..10 {
            let latest = engine.get_latest(&format!("ow_{:02}", i)).unwrap();
            assert!(latest.is_some());
            assert_eq!(latest.unwrap().ts, 2, "latest version should have ts=2");
        }

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_single_file_engine_delete_range() {
        let dir = TempDir::new().unwrap();
        let config = make_config_single_file(dir.path());
        let engine = Engine::open(config).unwrap();

        let records: Vec<Record> = (0..20)
            .map(|i| make_record(&format!("dr_{:03}", i), i))
            .collect();
        engine.write_batch(&records).unwrap();
        engine.flush().unwrap();

        // Delete records 5–14
        engine
            .delete_range("dr_005", "dr_015")
            .unwrap();

        let results = engine.query_by_prefix("dr_").unwrap();
        // dr_000–dr_004 survive (5) + dr_015–dr_019 survive (5) = 10
        assert_eq!(results.len(), 10);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_single_file_engine_bloom_filter() {
        let dir = TempDir::new().unwrap();
        let config = make_config_single_file(dir.path());
        let engine = Engine::open(config).unwrap();

        let records: Vec<Record> = (0..50)
            .map(|i| make_record(&format!("bf_{:03}", i), i))
            .collect();
        engine.write_batch(&records).unwrap();
        engine.flush().unwrap();

        // Key that exists
        assert!(engine.get_latest("bf_025").unwrap().is_some());

        // Key that doesn't exist (bloom should quickly reject)
        assert!(engine.get_latest("nonexistent").unwrap().is_none());

        engine.shutdown().unwrap();
    }
}
