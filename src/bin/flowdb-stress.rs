#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use flowdb::{Config, Engine, Query, Record, SyncMode};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn make_temp_dir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("flowdb-stress-{}-{}", std::process::id(), id));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup_temp_dir(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

fn stress_config(dir: &Path) -> Config {
    Config {
        data_dir: dir.to_path_buf(),
        memtable_size_mb: 4,
        block_size: 4096,
        gc_interval_secs: 999999,
        max_frozen_memtables: 4,
        flush_interval_ms: 60000,
        time_bucket_secs: 3600,
        block_cache_capacity_mb: 64,
        index_memory_budget_mb: 64,
        default_ttl_secs: None,
        bloom_bits_per_key: 10,
        wal_segment_size_mb: 64,
        compaction_threshold: 2,
        compaction_interval_ms: 60_000,
        create_if_missing: true,
        wal_sync_mode: SyncMode::Always,
        auto_background: false,
        storage_backend: flowdb::StorageBackendKind::MultiFile,
    }
}

fn format_duration(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1_000 {
        format!("{}µs", us)
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1_000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

fn format_bytes(b: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    if b < KB {
        format!("{}B", b)
    } else if b < MB {
        format!("{:.1}KB", b as f64 / KB as f64)
    } else {
        format!("{:.1}MB", b as f64 / MB as f64)
    }
}

fn print_header(title: &str) {
    println!();
    println!("════════════════════════════════════════════════════════");
    println!("  {}", title);
    println!("════════════════════════════════════════════════════════");
}

fn print_result(label: &str, count: u64, elapsed: Duration, extra: Option<&str>) {
    let throughput = count as f64 / elapsed.as_secs_f64();
    let avg = elapsed.as_nanos() as f64 / count as f64;
    let avg_str = if avg < 1_000.0 {
        format!("{:.0}ns", avg)
    } else if avg < 1_000_000.0 {
        format!("{:.1}µs", avg / 1_000.0)
    } else {
        format!("{:.2}ms", avg / 1_000_000.0)
    };
    let extra_str = extra.map(|s| format!("  {}", s)).unwrap_or_default();
    println!(
        "  {:<40} {:>8} ops  {:>12.0} ops/s  avg {:>10}{}",
        label, count, throughput, avg_str, extra_str,
    );
}

fn bench_sequential_writes(
    engine: &Engine,
    n: u64,
    batch_size: usize,
    value_len: usize,
) -> Duration {
    let start = Instant::now();
    let mut key_counter = 0u64;
    let total_batches = (n as usize).div_ceil(batch_size);
    for _ in 0..total_batches {
        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            if key_counter >= n {
                break;
            }
            batch.push(Record {
                key: format!("seq_{:08}", key_counter).into_bytes(),
                ts: key_counter as i64 * 100,
                expire_at: i64::MAX,
                value: vec![0xABu8; value_len],
            });
            key_counter += 1;
        }
        engine.write_batch(&batch).unwrap();
    }
    start.elapsed()
}

fn bench_concurrent_writes(
    engine: Arc<Engine>,
    total_records: u64,
    concurrency: usize,
    batch_size: usize,
    value_len: usize,
) -> Duration {
    let written = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = Vec::new();
    for worker_id in 0..concurrency {
        let engine = engine.clone();
        let written = written.clone();
        handles.push(std::thread::spawn(move || {
            loop {
                let batch_start = written.fetch_add(batch_size as u64, Ordering::Relaxed);
                if batch_start >= total_records {
                    written.fetch_sub(batch_size as u64, Ordering::Relaxed);
                    break;
                }
                let actual = (batch_size as u64).min(total_records - batch_start) as usize;
                let mut batch = Vec::with_capacity(actual);
                for j in 0..actual {
                    let idx = batch_start + j as u64;
                    batch.push(Record {
                        key: format!("cw{}_{}", worker_id, idx).into_bytes(),
                        ts: idx as i64 * 100,
                        expire_at: i64::MAX,
                        value: vec![0xCDu8; value_len],
                    });
                }
                engine.write_batch(&batch).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    start.elapsed()
}

fn bench_concurrent_reads(
    engine: Arc<Engine>,
    prefixes: &[String],
    concurrency: usize,
    queries_per_worker: usize,
) -> Duration {
    let start = Instant::now();
    let mut handles = Vec::new();
    for worker_id in 0..concurrency {
        let engine = engine.clone();
        let prefix = prefixes[worker_id % prefixes.len()].clone();
        handles.push(std::thread::spawn(move || {
            let mut total_records = 0usize;
            for _ in 0..queries_per_worker {
                let results = engine.query(Query::prefix(&prefix)).unwrap();
                total_records += results.len();
            }
            total_records
        }));
    }
    let mut total = 0usize;
    for h in handles {
        total += h.join().unwrap();
    }
    let elapsed = start.elapsed();
    let total_queries = concurrency * queries_per_worker;
    print_result(
        &format!("concurrent read ({} workers)", concurrency),
        total_queries as u64,
        elapsed,
        Some(&format!("returned {} records total", total)),
    );
    elapsed
}

fn bench_mixed_rw(
    engine: Arc<Engine>,
    total_ops: u64,
    write_ratio: f64,
    concurrency: usize,
    value_len: usize,
) -> Duration {
    let ops_per_worker = total_ops as usize / concurrency;
    let writes_per_worker = (ops_per_worker as f64 * write_ratio) as usize;

    let start = Instant::now();
    let mut handles = Vec::new();
    for worker_id in 0..concurrency {
        let engine = engine.clone();
        handles.push(std::thread::spawn(move || {
            let mut write_count = 0u64;
            let mut read_count = 0u64;
            for i in 0..ops_per_worker {
                if i < writes_per_worker {
                    let batch = vec![Record {
                        key: format!("mix{}_{}", worker_id, i).into_bytes(),
                        ts: i as i64 * 100,
                        expire_at: i64::MAX,
                        value: vec![0xEFu8; value_len],
                    }];
                    engine.write_batch(&batch).unwrap();
                    write_count += 1;
                } else {
                    let prefix = format!("mix{}_{}", worker_id, i % 10);
                    let _ = engine.query(Query::prefix(&prefix)).unwrap();
                    read_count += 1;
                }
            }
            (write_count, read_count)
        }));
    }
    let mut total_writes = 0u64;
    let mut total_reads = 0u64;
    for h in handles {
        let (w, r) = h.join().unwrap();
        total_writes += w;
        total_reads += r;
    }
    let elapsed = start.elapsed();
    print_result(
        &format!(
            "mixed r/w ({}w, {}% write)",
            concurrency,
            (write_ratio * 100.0) as usize
        ),
        total_writes + total_reads,
        elapsed,
        Some(&format!("w={} r={}", total_writes, total_reads)),
    );
    elapsed
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn main() {
    let dir = make_temp_dir();
    let config = stress_config(&dir);

    println!("╔════════════════════════════════════════════════════════╗");
    println!("║           FlowDB Throughput Stress Test               ║");
    println!("╚════════════════════════════════════════════════════════╝");
    println!();
    println!("  data_dir:    {}", dir.display());
    println!("  memtable:    {}MB", config.memtable_size_mb);
    println!("  block_size:  {}B", config.block_size);

    let engine = Arc::new(Engine::open(config).unwrap());

    let small_val = 64usize;
    let medium_val = 512usize;
    let large_val = 4096usize;

    // ── 1. Sequential writes ────────────────────────────────
    print_header("1. Sequential Writes (single writer)");

    let n = 50_000u64;
    let elapsed = bench_sequential_writes(&engine, n, 100, small_val);
    print_result(
        &format!("batch=100, val={}B", small_val),
        n,
        elapsed,
        Some(&format!("data={}", format_bytes(n * small_val as u64))),
    );

    let n = 20_000u64;
    let elapsed = bench_sequential_writes(&engine, n, 50, medium_val);
    print_result(
        &format!("batch=50, val={}B", medium_val),
        n,
        elapsed,
        Some(&format!("data={}", format_bytes(n * medium_val as u64))),
    );

    let n = 5_000u64;
    let elapsed = bench_sequential_writes(&engine, n, 10, large_val);
    print_result(
        &format!("batch=10, val={}B", large_val),
        n,
        elapsed,
        Some(&format!("data={}", format_bytes(n * large_val as u64))),
    );

    // ── 2. Concurrent writes ────────────────────────────────
    print_header("2. Concurrent Writes (multi-writer)");

    for &concurrency in &[1usize, 4, 8, 16] {
        let total = 50_000u64;
        let elapsed = bench_concurrent_writes(engine.clone(), total, concurrency, 50, small_val);
        print_result(
            &format!("{} workers, batch=50", concurrency),
            total,
            elapsed,
            None,
        );
    }

    // ── 3. Flush to SST ─────────────────────────────────────
    print_header("3. Flush to SST");
    let before = engine.stats();
    let flush_start = Instant::now();
    engine.flush().unwrap();
    let flush_elapsed = flush_start.elapsed();
    let after = engine.stats();
    println!(
        "  flush: {}  memtable {} -> {} records, sstables {} -> {}",
        format_duration(flush_elapsed),
        before.memtable_records,
        after.memtable_records,
        before.sstable_count,
        after.sstable_count,
    );

    // ── 4. Queries (from memtable + SST) ────────────────────
    print_header("4. Query Benchmarks");

    let n_q = 1_000usize;

    // narrow prefix — should hit few blocks
    let start = Instant::now();
    for _ in 0..n_q {
        let _ = engine.query(Query::prefix("seq_000000")).unwrap();
    }
    let elapsed = start.elapsed();
    print_result("prefix (narrow, ~1 record)", n_q as u64, elapsed, None);

    // wider prefix
    let start = Instant::now();
    for _ in 0..100 {
        let _ = engine.query(Query::prefix("seq_0000")).unwrap();
    }
    let elapsed = start.elapsed();
    print_result("prefix (wide, ~10K records)", 100, elapsed, None);

    // key_range
    let start = Instant::now();
    for _ in 0..n_q {
        let _ = engine
            .query(Query::key_range("seq_00000000", "seq_00000100"))
            .unwrap();
    }
    let elapsed = start.elapsed();
    print_result("key_range (100-key span)", n_q as u64, elapsed, None);

    // ── 5. Concurrent reads ─────────────────────────────────
    print_header("5. Concurrent Reads (prefix queries)");

    let prefixes: Vec<String> = (0..10).map(|i| format!("seq_{:04}", i * 500)).collect();
    for &concurrency in &[1usize, 4, 8] {
        bench_concurrent_reads(engine.clone(), &prefixes, concurrency, 100);
    }

    // ── 6. Mixed read/write ─────────────────────────────────
    print_header("6. Mixed Read/Write Workload");

    for &ratio in &[0.2f64, 0.5, 0.8] {
        bench_mixed_rw(engine.clone(), 10_000, ratio, 4, small_val);
    }

    // ── 7. Compaction ───────────────────────────────────────
    print_header("7. Compaction");
    let before = engine.stats();
    let compact_start = Instant::now();
    let did_compact = engine.trigger_compaction().unwrap();
    let compact_elapsed = compact_start.elapsed();
    let after = engine.stats();
    println!(
        "  compaction: {}  ran={}  sstables {} -> {}",
        format_duration(compact_elapsed),
        did_compact,
        before.sstable_count,
        after.sstable_count,
    );

    // ── 8. Post-compaction query ────────────────────────────
    print_header("8. Post-Compaction Queries");
    let start = Instant::now();
    for _ in 0..n_q {
        let _ = engine.query(Query::prefix("seq_000000")).unwrap();
    }
    let elapsed = start.elapsed();
    print_result(
        "prefix (narrow, post-compaction)",
        n_q as u64,
        elapsed,
        None,
    );

    // ── 9. Summary ──────────────────────────────────────────
    print_header("9. Engine Statistics");
    let stats = engine.stats();
    println!(
        "  block_cache_hit_rate:  {:.1}%",
        stats.block_cache_hit_rate * 100.0,
    );
    println!(
        "  records written/read:  {} / {}",
        stats.total_records_written, stats.total_records_read,
    );
    println!(
        "  bytes written:         {}",
        format_bytes(stats.total_bytes_written),
    );
    println!(
        "  flushes / compactions: {} / {}",
        stats.total_flushes, stats.total_compaction_runs,
    );
    println!(
        "  write_latency  p50={:<10} p90={:<10} p99={}",
        format_duration(Duration::from_micros(stats.write_latency_p50_us)),
        format_duration(Duration::from_micros(stats.write_latency_p90_us)),
        format_duration(Duration::from_micros(stats.write_latency_p99_us)),
    );
    println!(
        "  query_latency  p50={:<10} p90={:<10} p99={}",
        format_duration(Duration::from_micros(stats.query_latency_p50_us)),
        format_duration(Duration::from_micros(stats.query_latency_p90_us)),
        format_duration(Duration::from_micros(stats.query_latency_p99_us)),
    );

    match Arc::try_unwrap(engine) {
        Ok(e) => e.shutdown().unwrap(),
        Err(arc) => {
            arc.flush().unwrap();
        }
    }
    cleanup_temp_dir(&dir);
    println!();
    println!("Done.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_format_duration_micros() {
        assert_eq!(format_duration(Duration::from_micros(500)), "500µs");
    }

    #[test]
    fn test_format_duration_millis() {
        assert_eq!(format_duration(Duration::from_micros(1_500)), "1.5ms");
    }

    #[test]
    fn test_format_duration_secs() {
        assert_eq!(format_duration(Duration::from_micros(3_500_000)), "3.50s");
    }

    #[test]
    fn test_stress_config_values() {
        let dir = make_temp_dir();
        let cfg = stress_config(&dir);
        assert_eq!(cfg.memtable_size_mb, 4);
        assert_eq!(cfg.block_size, 4096);
        assert_eq!(cfg.max_frozen_memtables, 4);
        assert!(cfg.create_if_missing);
        assert_eq!(cfg.wal_sync_mode, SyncMode::Always);
        assert!(!cfg.auto_background);
        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_format_bytes_under_kb() {
        assert_eq!(format_bytes(0), "0B");
        assert_eq!(format_bytes(512), "512B");
        assert_eq!(format_bytes(1023), "1023B");
    }

    #[test]
    fn test_format_bytes_kb_range() {
        assert_eq!(format_bytes(1024), "1.0KB");
        assert_eq!(format_bytes(2048), "2.0KB");
        assert_eq!(format_bytes(2560), "2.5KB");
    }

    #[test]
    fn test_format_bytes_mb_range() {
        assert_eq!(format_bytes(1024 * 1024), "1.0MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1024.0MB");
    }

    #[test]
    fn test_print_header_writes_label() {
        // Smoke test — just make sure it does not panic.
        print_header("test header");
    }

    #[test]
    fn test_print_result_basic() {
        let d = Duration::from_micros(1_000);
        print_result("basic", 100, d, None);
        print_result("with-extra", 100, d, Some("extra info"));
    }

    #[test]
    fn test_print_result_avg_ns() {
        // 100 ns / op -> "<X>ns" formatting branch.
        let d = Duration::from_nanos(500);
        print_result("fast", 5, d, None);
    }

    #[test]
    fn test_print_result_avg_ms() {
        // 5 ms / op -> ms branch in avg formatting.
        let d = Duration::from_millis(50);
        print_result("slow", 10, d, None);
    }

    #[test]
    fn test_bench_sequential_writes_runs() {
        let dir = make_temp_dir();
        let cfg = stress_config(&dir);
        let engine = Engine::open(cfg).unwrap();
        let d = bench_sequential_writes(&engine, 5, 2, 8);
        assert!(d.as_nanos() > 0);
        engine.shutdown().unwrap();
        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_bench_concurrent_writes_runs() {
        let dir = make_temp_dir();
        let cfg = stress_config(&dir);
        let engine = Arc::new(Engine::open(cfg).unwrap());
        let d = bench_concurrent_writes(engine.clone(), 50, 2, 5, 8);
        assert!(d.as_nanos() > 0);
        let _ = engine.flush();
        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_bench_concurrent_reads_runs() {
        let dir = make_temp_dir();
        let cfg = stress_config(&dir);
        let engine = Arc::new(Engine::open(cfg).unwrap());
        engine
            .write_batch(&[Record {
                key: b"prefix_a".to_vec(),
                ts: 1,
                expire_at: i64::MAX,
                value: vec![1],
            }])
            .unwrap();
        let prefixes = vec!["prefix_a".to_string()];
        let d = bench_concurrent_reads(engine.clone(), &prefixes, 2, 3);
        assert!(d.as_nanos() > 0);
        let _ = engine.flush();
        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_bench_mixed_rw_runs() {
        let dir = make_temp_dir();
        let cfg = stress_config(&dir);
        let engine = Arc::new(Engine::open(cfg).unwrap());
        let d = bench_mixed_rw(engine.clone(), 40, 0.5, 2, 16);
        assert!(d.as_nanos() > 0);
        let _ = engine.flush();
        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_bench_mixed_rw_all_reads() {
        // write_ratio = 0.0 → all reads (exercises the else branch entirely).
        let dir = make_temp_dir();
        let cfg = stress_config(&dir);
        let engine = Arc::new(Engine::open(cfg).unwrap());
        let d = bench_mixed_rw(engine.clone(), 20, 0.0, 2, 8);
        assert!(d.as_nanos() > 0);
        let _ = engine.flush();
        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_bench_sequential_writes_smaller_than_batch() {
        // n < batch_size → exercises the inner break path.
        let dir = make_temp_dir();
        let cfg = stress_config(&dir);
        let engine = Engine::open(cfg).unwrap();
        let d = bench_sequential_writes(&engine, 3, 100, 4);
        assert!(d.as_nanos() > 0);
        engine.shutdown().unwrap();
        cleanup_temp_dir(&dir);
    }

    #[test]
    fn test_bench_concurrent_writes_over_subscription() {
        // concurrency * batch_size > total_records → exercises the
        // fetch_sub rollback when batch_start exceeds total.
        let dir = make_temp_dir();
        let cfg = stress_config(&dir);
        let engine = Arc::new(Engine::open(cfg).unwrap());
        let d = bench_concurrent_writes(engine.clone(), 5, 8, 50, 8);
        assert!(d.as_nanos() > 0);
        let _ = engine.flush();
        cleanup_temp_dir(&dir);
    }
}
