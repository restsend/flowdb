use crate::block_meta_index::BlockMetaIndex;
use crate::cache::BlockCache;
use crate::error::Result;
use crate::manifest::{Manifest, ManifestEntry, SstInfo};
use crate::record::{InternalRecord, Op};
use crate::sstable::{SstReader, SstStreamWriter};
use crate::stats::StatsCounters;
use crate::storage::StorageBackend;
use std::sync::Arc;

pub(crate) struct CompactionRunner {
    block_size: usize,
    bloom_bits_per_key: usize,
    compaction_threshold: usize,
    manifest: Arc<parking_lot::Mutex<Manifest>>,
    index: Arc<parking_lot::RwLock<BlockMetaIndex>>,
    cache: Arc<BlockCache>,
    stats: Arc<StatsCounters>,
    storage: Arc<dyn StorageBackend>,
}

impl CompactionRunner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        block_size: usize,
        bloom_bits_per_key: usize,
        compaction_threshold: usize,
        manifest: Arc<parking_lot::Mutex<Manifest>>,
        index: Arc<parking_lot::RwLock<BlockMetaIndex>>,
        cache: Arc<BlockCache>,
        stats: Arc<StatsCounters>,
        storage: Arc<dyn StorageBackend>,
    ) -> Self {
        Self {
            block_size,
            bloom_bits_per_key,
            compaction_threshold,
            manifest,
            index,
            cache,
            stats,
            storage,
        }
    }

    fn should_compact(&self) -> bool {
        let mf = self.manifest.lock();
        mf.state().sstables.len() >= self.compaction_threshold
    }

    pub fn run(&self) -> Result<bool> {
        if !self.should_compact() {
            return Ok(false);
        }

        let sst_ids: Vec<u32>;
        {
            let mf = self.manifest.lock();
            let state = mf.state();
            let mut ids: Vec<u32> = state.active_sst_ids.clone();
            ids.sort();
            if ids.is_empty() {
                return Ok(false);
            }
            sst_ids = ids;
        }

        let candidates = self.pick_compaction_candidates(&sst_ids);
        if candidates.is_empty() {
            return Ok(false);
        }

        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;

        let mut heap: std::collections::BinaryHeap<HeapEntry> = std::collections::BinaryHeap::new();
        let mut iterators: Vec<SstBlockIterator> = Vec::new();

        for (idx, &sst_id) in candidates.iter().enumerate() {
            if !self.storage.sst_exists(sst_id) {
                continue;
            }
            let reader = self.storage.open_reader(sst_id, 0)?;
            if reader.block_count() == 0 {
                continue;
            }
            let iter = SstBlockIterator::new(reader, idx, now_us);
            iterators.push(iter);
        }

        if iterators.is_empty() {
            return Ok(false);
        }

        for (idx, iter) in iterators.iter_mut().enumerate() {
            if let Some(rec) = iter.next_record() {
                heap.push(HeapEntry {
                    record: rec,
                    source: idx,
                });
            }
        }

        // Pre-allocate bloom filter.  Over-estimate is harmless (lowers FPR).
        let estimated_records: usize = candidates
            .iter()
            .filter_map(|id| {
                let mf = self.manifest.lock();
                mf.state()
                    .sstables
                    .get(id)
                    .map(|info| info.records as usize)
            })
            .sum();

        let new_sst_id;
        {
            let mf = self.manifest.lock();
            new_sst_id = mf.next_sst_id();
        }

        let mut writer = SstStreamWriter::new(
            self.block_size,
            estimated_records,
            self.bloom_bits_per_key,
        )?;

        let mut last_dedup: Option<(Vec<u8>, i64)> = None;
        let mut records_written: u64 = 0;

        while let Some(entry) = heap.pop() {
            let dedup_key = (entry.record.key.clone(), entry.record.ts);
            if last_dedup.as_ref() != Some(&dedup_key) {
                if entry.record.op != Op::Delete && entry.record.op != Op::DeleteRange {
                    writer.write_record(&entry.record)?;
                    records_written += 1;
                }
                last_dedup = Some(dedup_key);
            }

            if let Some(rec) = iterators[entry.source].next_record() {
                heap.push(HeapEntry {
                    record: rec,
                    source: entry.source,
                });
            }
        }

        if records_written == 0 {
            for sst_id in &candidates {
                self.cache.invalidate_sst(*sst_id);
                {
                    let mut mf = self.manifest.lock();
                    mf.append(&ManifestEntry::GcDeleteSst { sst_id: *sst_id })?;
                }
                {
                    let mut idx = self.index.write();
                    idx.remove_sst(*sst_id);
                }
                if let Err(e) = self.storage.delete_sst(*sst_id) {
                    tracing::warn!("Compaction: failed to delete SST {}: {}", sst_id, e);
                }
            }
            self.refresh_stats();
            return Ok(true);
        }

        let (data, sst_bytes, block_infos, bloom) = writer.finish()?;
        self.storage.write_sst(new_sst_id, &data)?;

        let min_ts = block_infos.iter().map(|b| b.min_ts).min().unwrap_or(0);
        let max_ts = block_infos.iter().map(|b| b.max_ts).max().unwrap_or(0);
        let min_expire = block_infos.iter().map(|b| b.min_expire).min().unwrap_or(0);
        let max_expire = block_infos.iter().map(|b| b.max_expire).max().unwrap_or(0);

        let new_info = SstInfo {
            id: new_sst_id,
            records: records_written,
            bytes: sst_bytes,
            min_ts,
            max_ts,
            min_expire,
            max_expire,
            bloom: Some(bloom.clone()),
        };

        {
            let mut mf = self.manifest.lock();
            mf.append(&ManifestEntry::Compaction {
                removed: candidates.clone(),
                added: vec![new_info.clone()],
                blocks: vec![(new_sst_id, block_infos.clone())],
            })?;
        }

        {
            let mut idx = self.index.write();
            for sst_id in &candidates {
                idx.remove_sst(*sst_id);
            }
            idx.add_sst(new_sst_id, &block_infos);
            idx.set_bloom(new_sst_id, bloom);
        }

        for sst_id in &candidates {
            self.cache.invalidate_sst(*sst_id);
            if let Err(e) = self.storage.delete_sst(*sst_id) {
                tracing::warn!("Compaction: failed to delete SST {}: {}", sst_id, e);
            }
        }

        self.stats.compaction_done();
        self.refresh_stats();
        Ok(true)
    }

    fn pick_compaction_candidates(&self, all_ids: &[u32]) -> Vec<u32> {
        let mf = self.manifest.lock();
        let state = mf.state();

        let mut sized: Vec<(u32, u64)> = all_ids
            .iter()
            .filter_map(|id| state.sstables.get(id).map(|info| (*id, info.bytes)))
            .collect();

        sized.sort_by_key(|(_, bytes)| *bytes);

        if sized.len() < 2 {
            return sized.iter().map(|(id, _)| *id).collect();
        }

        let min_size = sized[0].1;
        let threshold = min_size.max(1) * 4;
        let candidates: Vec<u32> = sized
            .iter()
            .take_while(|(_, bytes)| *bytes <= threshold)
            .map(|(id, _)| *id)
            .collect();

        if candidates.len() >= 2 {
            candidates
        } else {
            sized.iter().take(2).map(|(id, _)| *id).collect()
        }
    }

    fn refresh_stats(&self) {
        let mf = self.manifest.lock();
        let sst_count = mf.state().sstables.len();
        let total_bytes: u64 = mf.state().sstables.values().map(|s| s.bytes).sum();
        self.stats.set_sstable(sst_count, total_bytes);
        drop(mf);

        let idx = self.index.read();
        self.stats
            .set_index_stats(idx.total_entries(), idx.bucket_count());
    }
}

struct SstBlockIterator {
    reader: SstReader,
    current_block: u32,
    block_records: Vec<InternalRecord>,
    record_pos: usize,
    now_us: i64,
}

impl SstBlockIterator {
    fn new(reader: SstReader, _source_idx: usize, now_us: i64) -> Self {
        Self {
            reader,
            current_block: 0,
            block_records: Vec::new(),
            record_pos: 0,
            now_us,
        }
    }

    fn next_record(&mut self) -> Option<InternalRecord> {
        loop {
            if self.record_pos < self.block_records.len() {
                let rec = self.block_records[self.record_pos].clone();
                self.record_pos += 1;
                return Some(rec);
            }

            if self.current_block >= self.reader.block_count() {
                return None;
            }

            match self.reader.read_block(self.current_block, None) {
                Ok(block) => {
                    self.block_records = block
                        .records
                        .into_iter()
                        .filter_map(|mut r| {
                            if r.expire_at > self.now_us {
                                r.seq = 0;
                                Some(r)
                            } else {
                                None
                            }
                        })
                        .collect();
                    self.block_records
                        .sort_by(|a, b| b.key.cmp(&a.key).then(b.ts.cmp(&a.ts)));
                    self.record_pos = 0;
                    self.current_block += 1;
                }
                Err(_) => {
                    self.current_block += 1;
                    continue;
                }
            }
        }
    }
}

#[derive(Eq, PartialEq)]
struct HeapEntry {
    record: InternalRecord,
    source: usize,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.record
            .key
            .cmp(&other.record.key)
            .then(self.record.ts.cmp(&other.record.ts))
            .then(other.record.seq.cmp(&self.record.seq))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::Manifest;
    use crate::storage::MultiFileStorage;
    use parking_lot::RwLock;
    use tempfile::TempDir;

    fn make_test_runner(dir: &std::path::Path, threshold: usize) -> CompactionRunner {
        let storage: Arc<dyn StorageBackend> = Arc::new(MultiFileStorage::new(dir));
        CompactionRunner::new(
            100,
            10,
            threshold,
            Arc::new(parking_lot::Mutex::new(Manifest::open(dir).unwrap())),
            Arc::new(RwLock::new(BlockMetaIndex::new(3600))),
            Arc::new(BlockCache::new(1)),
            Arc::new(StatsCounters::new()),
            storage,
        )
    }

    #[test]
    fn test_should_compact_false_when_empty() {
        let dir = TempDir::new().unwrap();
        let runner = make_test_runner(dir.path(), 2);
        assert!(!runner.should_compact());
    }

    #[test]
    fn test_should_compact_false_below_threshold() {
        let dir = TempDir::new().unwrap();
        let runner = make_test_runner(dir.path(), 5);
        assert!(!runner.should_compact());
    }

    #[test]
    fn test_heap_entry_ordering() {
        let a = InternalRecord {
            seq: 1,
            op: Op::Put,
            key: b"a".to_vec(),
            ts: 100,
            expire_at: i64::MAX,
            value: vec![],
            range_end: None,
        };
        let b = InternalRecord {
            seq: 2,
            op: Op::Put,
            key: b"b".to_vec(),
            ts: 200,
            expire_at: i64::MAX,
            value: vec![],
            range_end: None,
        };
        let e1 = HeapEntry {
            record: a.clone(),
            source: 0,
        };
        let e2 = HeapEntry {
            record: b.clone(),
            source: 0,
        };
        // (key, ts, seq desc) — a.key < b.key, so e1 < e2
        assert!(e1 < e2);

        // Same key same ts, higher seq should be "less" (min-heap pops it first)
        let a2 = InternalRecord {
            seq: 3,
            ..a.clone()
        };
        let e3 = HeapEntry {
            record: a2,
            source: 0,
        };
        assert!(e3 < e1, "higher seq should pop first for same key+ts");
    }

    #[test]
    fn test_run_returns_false_when_no_sst() {
        let dir = TempDir::new().unwrap();
        let runner = make_test_runner(dir.path(), 1);
        assert!(!runner.run().unwrap());
    }
}
