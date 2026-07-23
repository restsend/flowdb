//! FlowDB Engine — basic CRUD example.
//!
//! Demonstrates opening/closing the engine, writing records (batched
//! and single), point lookups, prefix/range scanning, iterators, and
//! flushing/compaction.

use std::time::{SystemTime, UNIX_EPOCH};

use flowdb::{Config, Engine, Query, Record, ScanRange};

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

fn main() {
    // Use a temp directory so repeated runs are clean.
    let dir = tempfile::TempDir::with_prefix("flowdb_basic_engine_").unwrap();

    // ── 1. Open ────────────────────────────────────────────────────
    let engine = Engine::open(Config {
        data_dir: dir.path().to_path_buf(),
        memtable_size_mb: 4,            // small memtable → fast flush
        flush_interval_ms: 500,
        auto_background: true,          // background flush / compact / GC
        compression: flowdb::record::CompressionAlgorithm::Lz4,
        ..Config::default()
    })
    .unwrap();

    // ── 2. Write records ──────────────────────────────────────────
    let ts = now_micros();
    let batch: Vec<Record> = (0..5)
        .map(|i| Record::new(
            format!("sensor:{}", i),
            ts + i as i64 * 1_000,
            format!("{{ \"temp\": {:.1} }}", 20.0 + i as f64 * 0.5).into_bytes(),
        ))
        .collect();

    // write_batch_owned takes ownership of the Vec
    engine.write_batch_owned(batch).unwrap();

    // Write a single record via a one-element batch
    engine
        .write_batch_owned(vec![Record::new(
            "sensor:special",
            ts + 10_000,
            b"{\"alert\": true}".to_vec(),
        )])
        .unwrap();

    // ── 3. Point lookup ───────────────────────────────────────────
    let found = engine.get("sensor:2", ts + 2_000).unwrap();
    match found {
        Some(r) => println!("get sensor:2 → {}", String::from_utf8_lossy(&r.value)),
        None => println!("get sensor:2 → not found"),
    }

    // ── 4. Prefix query ──────────────────────────────────────────
    let results = engine.query(Query::prefix("sensor:")).unwrap();
    println!("prefix scan 'sensor:' → {} records", results.len());
    for r in &results {
        println!(
            "  key={} ts={} val={}",
            r.key_str(),
            r.ts,
            String::from_utf8_lossy(&r.value)
        );
    }

    // ── 5. Time-range query ──────────────────────────────────────
    let mid = ts + 2_500;
    let results = engine
        .query(Query::prefix_time_range("sensor:", ts, mid))
        .unwrap();
    println!(
        "time range [{}, {}] → {} records",
        ts,
        mid,
        results.len()
    );

    // ── 6. Key-range query ───────────────────────────────────────
    let results = engine
        .query(Query::key_range("sensor:1", "sensor:3"))
        .unwrap();
    println!("key range [sensor:1, sensor:3] → {} records", results.len());

    // ── 7. Lazy iterator (ScanIterator) ──────────────────────────
    let mut iter = engine.scan(ScanRange::prefix("sensor:")).unwrap();
    let mut count = 0usize;
    while let Some(Ok(r)) = iter.next() {
        count += 1;
        let _ = r;
    }
    println!("lazy scan → {} records", count);

    // ── 8. Delete ────────────────────────────────────────────────
    engine
        .delete_batch(&[("sensor:special".into(), ts + 10_000)])
        .unwrap();
    let gone = engine.get("sensor:special", ts + 10_000).unwrap();
    println!("deleted sensor:special → {:?}", gone.is_none());

    // ── 9. Flush & Compact ───────────────────────────────────────
    engine.flush().unwrap();
    engine.trigger_compaction().unwrap();

    // ── 10. Stats ────────────────────────────────────────────────
    let stats = engine.stats();
    println!(
        "stats: {} sstables, {} records read, hits {:.1}%",
        stats.sstable_count,
        stats.total_records_read,
        stats.block_cache_hit_rate * 100.0
    );

    // ── 11. Shutdown ─────────────────────────────────────────────
    engine.shutdown().unwrap();
    println!("Done (data in {})", dir.path().display());
}
