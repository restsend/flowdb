use flowdb::{Config, Engine, Record, StorageBackendKind};
use std::hint::black_box;
use std::path::Path;
use tempfile::TempDir;

fn make_config(dir: &Path, backend: StorageBackendKind) -> Config {
    Config {
        data_dir: dir.to_path_buf(),
        memtable_size_mb: 64,
        block_size: 8192,
        gc_interval_secs: 3600,
        max_frozen_memtables: 2,
        flush_interval_ms: 60000,
        time_bucket_secs: 3600,
        block_cache_capacity_mb: 128,
        index_memory_budget_mb: 256,
        default_ttl_secs: None,
        bloom_bits_per_key: 10,
        wal_segment_size_mb: 64,
        compaction_threshold: 2,
        compaction_interval_ms: 60_000,
        create_if_missing: true,
        wal_sync_mode: flowdb::SyncMode::IntervalMs(u64::MAX),
        auto_background: false,
        storage_backend: backend,
        compression: flowdb::record::CompressionAlgorithm::Lz4,
    }
}

fn main() {
    let n = 10_000;
    let records: Vec<Record> = (0..n)
        .map(|i| Record {
            key: format!("key_{:06}", i).into_bytes(),
            ts: i as i64 * 100,
            expire_at: i64::MAX,
            value: vec![1, 2, 3, 4],
        })
        .collect();

    for backend in [StorageBackendKind::MultiFile, StorageBackendKind::SingleFile] {
        let label = match backend {
            StorageBackendKind::MultiFile => "MultiFile",
            StorageBackendKind::SingleFile => "SingleFile",
        };
        let dir = TempDir::new().unwrap();
        let config = make_config(dir.path(), backend);
        let engine = Engine::open(config).unwrap();

        let start = std::time::Instant::now();
        for chunk in records.chunks(500) {
            engine.write_batch(chunk).unwrap();
        }
        let write_dur = start.elapsed();

        let start = std::time::Instant::now();
        engine.flush().unwrap();
        let flush_dur = start.elapsed();

        let start = std::time::Instant::now();
        for i in 0..1000 {
            let _ = black_box(engine.get_latest(&format!("key_{:06}", i % n)).unwrap());
        }
        let lookup_dur = start.elapsed();

        let start = std::time::Instant::now();
        let results = engine.query_by_prefix("key_").unwrap();
        let scan_dur = start.elapsed();

        println!("{}:", label);
        println!("  Write  {} records: {:?}", n, write_dur);
        println!("  Flush:              {:?}", flush_dur);
        println!("  Lookup 1000 keys:   {:?}", lookup_dur);
        println!("  Scan {} results:    {:?}", results.len(), scan_dur);
        println!();

        engine.shutdown().unwrap();
    }
}
