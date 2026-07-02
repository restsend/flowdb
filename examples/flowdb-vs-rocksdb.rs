use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flowdb::{Config, Engine, Query, Record};

const N: u64 = 100_000;
const BATCH: usize = 100;
const VAL_LEN: usize = 128;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("bench-{}-{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

fn fmt_dur(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1_000 {
        format!("{}µs", us)
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1_000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

fn fmt_bytes(b: u64) -> String {
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

fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                total += dir_size(&p);
            } else if let Ok(m) = p.metadata() {
                total += m.len();
            }
        }
    }
    total
}

struct BenchResult {
    label: String,
    ops: u64,
    throughput: f64,
    elapsed: Duration,
    extra: Option<String>,
}

fn print_cmp(flowdb: &BenchResult, rocksdb: &BenchResult) {
    let ratio = flowdb.throughput / rocksdb.throughput;
    let winner = if ratio > 1.05 {
        format!("FlowDB {:.2}x faster", ratio)
    } else if ratio < 0.95 {
        format!("RocksDB {:.2}x faster", 1.0 / ratio)
    } else {
        "~same".to_string()
    };
    println!(
        "  {:<30} {:>12.0} ops/s  {:>12.0} ops/s  {}",
        flowdb.label, flowdb.throughput, rocksdb.throughput, winner,
    );
}

fn print_single(r: &BenchResult) {
    let extra = r.extra.as_deref().unwrap_or("");
    println!(
        "  {:<30} {:>10} ops  {:>12.0} ops/s  {}  {}",
        r.label,
        r.ops,
        r.throughput,
        fmt_dur(r.elapsed),
        extra,
    );
}

fn flowdb_config(dir: &Path) -> Config {
    Config {
        data_dir: dir.to_path_buf(),
        memtable_size_mb: 64,
        block_size: 8192,
        gc_interval_secs: 999999,
        max_frozen_memtables: 4,
        flush_interval_ms: 60000,
        time_bucket_secs: 3600,
        block_cache_capacity_mb: 128,
        index_memory_budget_mb: 256,
        default_ttl_secs: None,
        bloom_bits_per_key: 10,
        wal_segment_size_mb: 64,
        compaction_threshold: 1,
        create_if_missing: true,
        wal_sync_mode: flowdb::SyncMode::IntervalMs(u64::MAX),
        auto_background: false,
        storage_backend: flowdb::StorageBackendKind::MultiFile,
    }
}

fn rocksdb_opts(dir: &Path) -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.set_max_write_buffer_number(4);
    opts.set_target_file_size_base(64 * 1024 * 1024);
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    opts.set_use_fsync(false);
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_block_size(8192);
    block_opts.set_bloom_filter(10.0, true);
    opts.set_block_based_table_factory(&block_opts);
    let _ = dir;
    opts
}

fn make_key(i: u64) -> String {
    format!("key_{:08}", i)
}

fn make_value() -> Vec<u8> {
    vec![0xABu8; VAL_LEN]
}

// ── FlowDB benchmarks ──────────────────────────────────

fn main() {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║        FlowDB vs RocksDB — Comparative Benchmark         ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
    println!("  records: {}  batch: {}  value: {}B", N, BATCH, VAL_LEN);

    // ── FlowDB ────────────────────────────────────────────
    let fdir = temp_dir("flowdb");
    let fcfg = flowdb_config(&fdir);
    let fengine = Arc::new(Engine::open(fcfg).unwrap());

    println!();
    println!("══════════ FlowDB ══════════");

    let f_seq_write = bench_flowdb_seq_write(&fengine, N, BATCH);
    print_single(&f_seq_write);

    let f_conc_write = bench_flowdb_conc_write(fengine.clone(), N, 8, BATCH);
    print_single(&f_conc_write);

    fengine.flush().unwrap();
    fengine.trigger_compaction().unwrap();
    let sst_count = fengine.stats().sstable_count;
    println!("  flushed → {} sstables", sst_count);

    let f_point = bench_flowdb_point(&fengine, 10_000);
    print_single(&f_point);

    let f_prefix = bench_flowdb_prefix(&fengine, 1_000);
    print_single(&f_prefix);

    let f_full_scan = bench_flowdb_full_scan(&fengine);
    print_single(&f_full_scan);

    let f_stats = fengine.stats();
    let fsize = dir_size(&fdir);

    println!(
        "  cache hit rate:  {:.1}%",
        f_stats.block_cache_hit_rate * 100.0
    );
    println!("  disk usage:      {}", fmt_bytes(fsize));
    println!(
        "  query latency:   p50={} p90={} p99={}",
        fmt_dur(Duration::from_micros(f_stats.query_latency_p50_us)),
        fmt_dur(Duration::from_micros(f_stats.query_latency_p90_us)),
        fmt_dur(Duration::from_micros(f_stats.query_latency_p99_us)),
    );

    // ── RocksDB ───────────────────────────────────────────
    let rdir = temp_dir("rocksdb");
    let ropts = rocksdb_opts(&rdir);
    let rdb = Arc::new(rocksdb::DB::open(&ropts, &rdir).unwrap());

    println!();
    println!("══════════ RocksDB ══════════");

    let r_seq_write = bench_rocksdb_seq_write(&rdb, N, BATCH);
    print_single(&r_seq_write);

    let r_conc_write = bench_rocksdb_conc_write(rdb.clone(), N, 8, BATCH);
    print_single(&r_conc_write);

    rocksdb::DB::compact_range(&rdb, None::<&[u8]>, None::<&[u8]>);

    let r_point = bench_rocksdb_point(&rdb, 10_000);
    print_single(&r_point);

    let r_prefix = bench_rocksdb_prefix(&rdb, 1_000);
    print_single(&r_prefix);

    let r_full_scan = bench_rocksdb_full_scan(&rdb);
    print_single(&r_full_scan);

    let rsize = dir_size(&rdir);
    println!("  disk usage:      {}", fmt_bytes(rsize));

    // ── Comparison ────────────────────────────────────────
    println!();
    println!("══════════ Comparison ══════════");
    println!(
        "  {:<30} {:>14}  {:>14}  Verdict",
        "Category", "FlowDB", "RocksDB"
    );
    println!("  {}", "-".repeat(85));

    print_cmp(
        &BenchResult {
            label: "Sequential Write".into(),
            ops: f_seq_write.ops,
            throughput: f_seq_write.throughput,
            elapsed: f_seq_write.elapsed,
            extra: None,
        },
        &BenchResult {
            label: String::new(),
            ops: r_seq_write.ops,
            throughput: r_seq_write.throughput,
            elapsed: r_seq_write.elapsed,
            extra: None,
        },
    );

    print_cmp(
        &BenchResult {
            label: "Concurrent Write (8 threads)".into(),
            ops: f_conc_write.ops,
            throughput: f_conc_write.throughput,
            elapsed: f_conc_write.elapsed,
            extra: None,
        },
        &BenchResult {
            label: String::new(),
            ops: r_conc_write.ops,
            throughput: r_conc_write.throughput,
            elapsed: r_conc_write.elapsed,
            extra: None,
        },
    );

    print_cmp(
        &BenchResult {
            label: "Point Query".into(),
            ops: f_point.ops,
            throughput: f_point.throughput,
            elapsed: f_point.elapsed,
            extra: None,
        },
        &BenchResult {
            label: String::new(),
            ops: r_point.ops,
            throughput: r_point.throughput,
            elapsed: r_point.elapsed,
            extra: None,
        },
    );

    print_cmp(
        &BenchResult {
            label: "Prefix Scan".into(),
            ops: f_prefix.ops,
            throughput: f_prefix.throughput,
            elapsed: f_prefix.elapsed,
            extra: None,
        },
        &BenchResult {
            label: String::new(),
            ops: r_prefix.ops,
            throughput: r_prefix.throughput,
            elapsed: r_prefix.elapsed,
            extra: None,
        },
    );

    print_cmp(
        &BenchResult {
            label: "Full Scan".into(),
            ops: f_full_scan.ops,
            throughput: f_full_scan.throughput,
            elapsed: f_full_scan.elapsed,
            extra: None,
        },
        &BenchResult {
            label: String::new(),
            ops: r_full_scan.ops,
            throughput: r_full_scan.throughput,
            elapsed: r_full_scan.elapsed,
            extra: None,
        },
    );

    {
        let ratio = fsize as f64 / rsize as f64;
        let verdict = if ratio < 0.95 {
            format!("FlowDB {:.1}% smaller", (1.0 - ratio) * 100.0)
        } else if ratio > 1.05 {
            format!("RocksDB {:.1}% smaller", (1.0 - 1.0 / ratio) * 100.0)
        } else {
            "~same".to_string()
        };
        println!(
            "  {:<30} {:>14}  {:>14}  {}",
            "Storage",
            fmt_bytes(fsize),
            fmt_bytes(rsize),
            verdict,
        );
    }

    // cleanup
    drop(rdb);
    match Arc::try_unwrap(fengine) {
        Ok(e) => e.shutdown().unwrap(),
        Err(a) => {
            a.flush().unwrap();
        }
    }
    cleanup(&fdir);
    cleanup(&rdir);
    println!();
    println!("Done.");
}

// ── FlowDB benchmark functions ──────────────────────────

fn bench_flowdb_seq_write(engine: &Engine, n: u64, batch: usize) -> BenchResult {
    let val = make_value();
    let start = Instant::now();
    let mut key_counter = 0u64;
    let total_batches = (n as usize).div_ceil(batch);
    for _ in 0..total_batches {
        let mut records = Vec::with_capacity(batch);
        for _ in 0..batch {
            if key_counter >= n {
                break;
            }
            records.push(Record {
                key: make_key(key_counter).into_bytes(),
                ts: key_counter as i64,
                expire_at: i64::MAX,
                value: val.clone(),
            });
            key_counter += 1;
        }
        engine.write_batch_owned(records).unwrap();
    }
    let elapsed = start.elapsed();
    let throughput = n as f64 / elapsed.as_secs_f64();
    BenchResult {
        label: "Sequential Write".into(),
        ops: n,
        throughput,
        elapsed,
        extra: Some(fmt_bytes(n * VAL_LEN as u64)),
    }
}

fn bench_flowdb_conc_write(
    engine: Arc<Engine>,
    n: u64,
    threads: usize,
    batch: usize,
) -> BenchResult {
    use std::sync::atomic::{AtomicU64, Ordering};
    let counter = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut handles = Vec::new();
    for tid in 0..threads {
        let engine = engine.clone();
        let counter = counter.clone();
        let val = make_value();
        handles.push(std::thread::spawn(move || {
            let mut local_written = 0u64;
            loop {
                let batch_start = counter.fetch_add(batch as u64, Ordering::Relaxed);
                if batch_start >= n {
                    counter.fetch_sub(batch as u64, Ordering::Relaxed);
                    break;
                }
                let actual = (batch as u64).min(n - batch_start) as usize;
                let mut records = Vec::with_capacity(actual);
                for j in 0..actual {
                    let idx = batch_start + j as u64;
                    records.push(Record {
                        key: format!("cw{}_{}", tid, idx).into_bytes(),
                        ts: idx as i64,
                        expire_at: i64::MAX,
                        value: val.clone(),
                    });
                }
                engine.write_batch_sync(records).unwrap();
                local_written += actual as u64;
            }
            local_written
        }));
    }
    let mut total = 0u64;
    for h in handles {
        total += h.join().unwrap();
    }
    let elapsed = start.elapsed();
    BenchResult {
        label: format!("Concurrent Write ({} threads)", threads),
        ops: total,
        throughput: total as f64 / elapsed.as_secs_f64(),
        elapsed,
        extra: None,
    }
}

fn bench_flowdb_point(engine: &Engine, rounds: usize) -> BenchResult {
    let start = Instant::now();
    for i in 0..rounds {
        let key = make_key((i as u64 * 10) % N);
        let ts = ((i as u64 * 10) % N) as i64;
        let _ = engine.get(&key, ts).unwrap();
    }
    let elapsed = start.elapsed();
    BenchResult {
        label: "Point Query".into(),
        ops: rounds as u64,
        throughput: rounds as f64 / elapsed.as_secs_f64(),
        elapsed,
        extra: None,
    }
}

fn bench_flowdb_prefix(engine: &Engine, rounds: usize) -> BenchResult {
    let start = Instant::now();
    let mut total_records = 0usize;
    for i in 0..rounds {
        let prefix = format!("key_{:04}", i % 100);
        let results = engine.query(Query::prefix(&prefix)).unwrap();
        total_records += results.len();
    }
    let elapsed = start.elapsed();
    BenchResult {
        label: "Prefix Scan".into(),
        ops: rounds as u64,
        throughput: rounds as f64 / elapsed.as_secs_f64(),
        elapsed,
        extra: Some(format!("~{} recs/query", total_records / rounds)),
    }
}

fn bench_flowdb_full_scan(engine: &Engine) -> BenchResult {
    let start = Instant::now();
    let results = engine.query(Query::prefix("")).unwrap();
    let elapsed = start.elapsed();
    BenchResult {
        label: "Full Scan".into(),
        ops: 1,
        throughput: 1.0 / elapsed.as_secs_f64(),
        elapsed,
        extra: Some(format!("{} records", results.len())),
    }
}

// ── RocksDB benchmark functions ─────────────────────────

fn make_cf_key(key: &str, ts: i64) -> Vec<u8> {
    let mut buf = key.as_bytes().to_vec();
    buf.extend_from_slice(&ts.to_be_bytes());
    buf
}

fn bench_rocksdb_seq_write(db: &Arc<rocksdb::DB>, n: u64, batch: usize) -> BenchResult {
    let val = make_value();
    let start = Instant::now();
    let mut key_counter = 0u64;
    let total_batches = (n as usize).div_ceil(batch);
    for _ in 0..total_batches {
        let mut wb = rocksdb::WriteBatch::default();
        for _ in 0..batch {
            if key_counter >= n {
                break;
            }
            let cf_key = make_cf_key(&make_key(key_counter), key_counter as i64);
            wb.put(&cf_key, &val);
            key_counter += 1;
        }
        db.write(wb).unwrap();
    }
    let elapsed = start.elapsed();
    BenchResult {
        label: "Sequential Write".into(),
        ops: n,
        throughput: n as f64 / elapsed.as_secs_f64(),
        elapsed,
        extra: Some(fmt_bytes(n * VAL_LEN as u64)),
    }
}

fn bench_rocksdb_conc_write(
    db: Arc<rocksdb::DB>,
    n: u64,
    threads: usize,
    batch: usize,
) -> BenchResult {
    use std::sync::atomic::{AtomicU64, Ordering};
    let counter = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut handles = Vec::new();
    for tid in 0..threads {
        let db = db.clone();
        let counter = counter.clone();
        let val = make_value();
        handles.push(std::thread::spawn(move || {
            let mut local_written = 0u64;
            loop {
                let batch_start = counter.fetch_add(batch as u64, Ordering::Relaxed);
                if batch_start >= n {
                    counter.fetch_sub(batch as u64, Ordering::Relaxed);
                    break;
                }
                let actual = (batch as u64).min(n - batch_start) as usize;
                let mut wb = rocksdb::WriteBatch::default();
                for j in 0..actual {
                    let idx = batch_start + j as u64;
                    let key = format!("cw{}_{}", tid, idx);
                    let cf_key = make_cf_key(&key, idx as i64);
                    wb.put(&cf_key, &val);
                }
                db.write(wb).unwrap();
                local_written += actual as u64;
            }
            local_written
        }));
    }
    let mut total = 0u64;
    for h in handles {
        total += h.join().unwrap();
    }
    let elapsed = start.elapsed();
    BenchResult {
        label: format!("Concurrent Write ({} threads)", threads),
        ops: total,
        throughput: total as f64 / elapsed.as_secs_f64(),
        elapsed,
        extra: None,
    }
}

fn bench_rocksdb_point(db: &Arc<rocksdb::DB>, rounds: usize) -> BenchResult {
    let start = Instant::now();
    for i in 0..rounds {
        let key = make_key((i as u64 * 10) % N);
        let ts = ((i as u64 * 10) % N) as i64;
        let cf_key = make_cf_key(&key, ts);
        let _ = db.get(&cf_key).unwrap();
    }
    let elapsed = start.elapsed();
    BenchResult {
        label: "Point Query".into(),
        ops: rounds as u64,
        throughput: rounds as f64 / elapsed.as_secs_f64(),
        elapsed,
        extra: None,
    }
}

fn bench_rocksdb_prefix(db: &Arc<rocksdb::DB>, rounds: usize) -> BenchResult {
    let start = Instant::now();
    let mut total_records = 0usize;
    for i in 0..rounds {
        let prefix = format!("key_{:04}", i % 100);
        let lower = prefix.as_bytes().to_vec();
        let mut upper = prefix.as_bytes().to_vec();
        let last = upper.last_mut().unwrap();
        *last = last.wrapping_add(1);
        let mut iter = db.raw_iterator();
        iter.seek(&lower);
        let mut count = 0usize;
        while iter.valid() {
            if let Some(k) = iter.key() {
                if k.starts_with(&lower) {
                    count += 1;
                    iter.next();
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        total_records += count;
    }
    let elapsed = start.elapsed();
    BenchResult {
        label: "Prefix Scan".into(),
        ops: rounds as u64,
        throughput: rounds as f64 / elapsed.as_secs_f64(),
        elapsed,
        extra: Some(format!("~{} recs/query", total_records / rounds)),
    }
}

fn bench_rocksdb_full_scan(db: &Arc<rocksdb::DB>) -> BenchResult {
    let start = Instant::now();
    let mut count = 0usize;
    let mut iter = db.raw_iterator();
    iter.seek_to_first();
    while iter.valid() {
        count += 1;
        iter.next();
    }
    let elapsed = start.elapsed();
    BenchResult {
        label: "Full Scan".into(),
        ops: 1,
        throughput: 1.0 / elapsed.as_secs_f64(),
        elapsed,
        extra: Some(format!("{} records", count)),
    }
}
