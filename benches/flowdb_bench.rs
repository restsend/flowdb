use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use flowdb::{Config, Engine, Query, Record, StorageBackendKind};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::io::Write;
use std::path::Path;
use tempfile::TempDir;

#[derive(Debug, Clone)]
struct BenchRecord {
    key: String,
    ts: i64,
    expire_at: i64,
    value: Vec<u8>,
}

fn make_bench_record(key: &str, ts: i64) -> BenchRecord {
    BenchRecord {
        key: key.to_string(),
        ts,
        expire_at: i64::MAX,
        value: vec![1, 2, 3, 4],
    }
}

fn make_bench_records(n: usize) -> Vec<BenchRecord> {
    (0..n)
        .map(|i| make_bench_record(&format!("key_{:06}", i), i as i64 * 100))
        .collect()
}

fn make_config(dir: &Path) -> Config {
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
        storage_backend: StorageBackendKind::MultiFile,
        compression: flowdb::record::CompressionAlgorithm::Lz4,
    }
}

fn crc32_simple(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811C9DC5;
    for &b in data {
        hash ^= b as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

fn wal_encode_record(
    key: &str,
    ts: i64,
    expire_at: i64,
    value: &[u8],
    seq: u64,
    buf: &mut Vec<u8>,
) {
    buf.extend_from_slice(&seq.to_be_bytes());
    let key_bytes = key.as_bytes();
    buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(key_bytes);
    buf.extend_from_slice(&ts.to_be_bytes());
    buf.extend_from_slice(&expire_at.to_be_bytes());
    buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
    buf.extend_from_slice(value);
    let payload_start = buf.len() - (8 + 2 + key_bytes.len() + 8 + 8 + 4 + value.len());
    let crc = crc32_simple(&buf[payload_start..]);
    buf.extend_from_slice(&crc.to_be_bytes());
}

fn wal_decode_record(data: &[u8]) -> Option<(BenchRecord, usize)> {
    let mut pos = 0;
    if data.len() < 8 + 2 {
        return None;
    }
    let _seq = u64::from_be_bytes(data[..8].try_into().unwrap());
    pos += 8;
    let key_len = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2;
    if pos + key_len > data.len() {
        return None;
    }
    let key = String::from_utf8_lossy(&data[pos..pos + key_len]).into_owned();
    pos += key_len;
    if pos + 16 > data.len() {
        return None;
    }
    let ts = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
    pos += 8;
    let expire_at = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
    pos += 8;
    if pos + 4 > data.len() {
        return None;
    }
    let val_len = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if pos + val_len + 4 > data.len() {
        return None;
    }
    let value = data[pos..pos + val_len].to_vec();
    pos += val_len;
    pos += 4;
    Some((
        BenchRecord {
            key,
            ts,
            expire_at,
            value,
        },
        pos,
    ))
}

struct MemTableBench {
    data: BTreeMap<(String, i64, u64), BenchRecord>,
}

impl MemTableBench {
    fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    fn insert(&mut self, rec: BenchRecord, seq: u64) {
        let key = (rec.key.clone(), rec.ts, seq);
        self.data.insert(key, rec);
    }

    fn query_prefix(&self, prefix: &str, now_us: i64) -> Vec<&BenchRecord> {
        let start = (prefix.to_string(), i64::MIN, u64::MIN);
        let end = (prefix.to_string(), i64::MAX, u64::MAX);
        self.data
            .range(start..=end)
            .filter(|(_, v)| v.expire_at >= now_us)
            .map(|(_, v)| v)
            .collect()
    }

    fn query_key_range(&self, start_key: &str, end_key: &str, now_us: i64) -> Vec<&BenchRecord> {
        let start = (start_key.to_string(), i64::MIN, u64::MIN);
        let end = (end_key.to_string(), i64::MAX, u64::MAX);
        self.data
            .range(start..=end)
            .filter(|(_, v)| v.expire_at >= now_us)
            .map(|(_, v)| v)
            .collect()
    }

    fn query_time_range(&self, ts_start: i64, ts_end: i64, now_us: i64) -> Vec<&BenchRecord> {
        self.data
            .iter()
            .filter(|((_, ts, _), v)| *ts >= ts_start && *ts <= ts_end && v.expire_at >= now_us)
            .map(|(_, v)| v)
            .collect()
    }
}

const BLOCK_MAGIC: u32 = 0x54534E42;
const HEADER_SIZE: usize = 48;

fn sst_encode_records(records: &[BenchRecord]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(records.len() * 64);
    for rec in records {
        let key_bytes = rec.key.as_bytes();
        buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(key_bytes);
        buf.extend_from_slice(&rec.ts.to_be_bytes());
        buf.extend_from_slice(&rec.expire_at.to_be_bytes());
        buf.extend_from_slice(&(rec.value.len() as u32).to_be_bytes());
        buf.extend_from_slice(&rec.value);
    }
    buf
}

fn sst_decode_records(data: &[u8], count: usize) -> Vec<BenchRecord> {
    let mut records = Vec::with_capacity(count);
    let mut pos = 0;
    for _ in 0..count {
        if pos + 2 > data.len() {
            break;
        }
        let key_len = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if pos + key_len > data.len() {
            break;
        }
        let key = String::from_utf8_lossy(&data[pos..pos + key_len]).into_owned();
        pos += key_len;
        if pos + 16 > data.len() {
            break;
        }
        let ts = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let expire_at = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        if pos + 4 > data.len() {
            break;
        }
        let val_len = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + val_len > data.len() {
            break;
        }
        let value = data[pos..pos + val_len].to_vec();
        pos += val_len;
        records.push(BenchRecord {
            key,
            ts,
            expire_at,
            value,
        });
    }
    records
}

#[derive(Debug, Clone)]
struct BenchBlockMeta {
    sst_id: u32,
    block_idx: u32,
    min_key: String,
    max_key: String,
    min_ts: i64,
    max_ts: i64,
    #[allow(dead_code)]
    min_expire: i64,
    max_expire: i64,
}

impl BenchBlockMeta {
    fn overlaps_key_prefix(&self, prefix: &str) -> bool {
        let prefix_end = increment_prefix(prefix);
        self.min_key.as_str() < prefix_end.as_str() && self.max_key.as_str() >= prefix
    }

    fn overlaps_key_range(&self, start: &str, end: &str) -> bool {
        self.min_key.as_str() <= end && self.max_key.as_str() >= start
    }

    fn overlaps_time(&self, ts_start: i64, ts_end: i64) -> bool {
        self.min_ts <= ts_end && self.max_ts >= ts_start
    }

    fn is_expired(&self, now_us: i64) -> bool {
        self.max_expire < now_us
    }
}

fn increment_prefix(s: &str) -> String {
    let mut bytes: Vec<u8> = s.bytes().collect();
    while let Some(last) = bytes.last_mut() {
        if *last < 255 {
            *last += 1;
            return String::from_utf8(bytes).unwrap_or_else(|_| s.to_string() + "\u{ffff}");
        }
        bytes.pop();
    }
    "\u{ffff}".to_string()
}

struct BenchBlockMetaIndex {
    by_key: BTreeMap<String, Vec<BenchBlockMeta>>,
    by_time: BTreeMap<i64, Vec<BenchBlockMeta>>,
    time_bucket_us: i64,
}

impl BenchBlockMetaIndex {
    fn new(time_bucket_secs: u64) -> Self {
        Self {
            by_key: BTreeMap::new(),
            by_time: BTreeMap::new(),
            time_bucket_us: time_bucket_secs as i64 * 1_000_000,
        }
    }

    fn add_block(&mut self, meta: BenchBlockMeta) {
        let key = meta.min_key.clone();
        self.by_key.entry(key).or_default().push(meta.clone());
        let bucket = meta.min_ts / self.time_bucket_us;
        self.by_time.entry(bucket).or_default().push(meta);
    }

    fn query_prefix(&self, prefix: &str, now_us: i64) -> Vec<&BenchBlockMeta> {
        let mut result = Vec::new();
        for metas in self.by_key.values() {
            for m in metas {
                if !m.is_expired(now_us) && m.overlaps_key_prefix(prefix) {
                    result.push(m);
                }
            }
        }
        result
    }

    fn query_key_range(&self, start: &str, end: &str, now_us: i64) -> Vec<&BenchBlockMeta> {
        let mut result = Vec::new();
        for metas in self.by_key.values() {
            for m in metas {
                if !m.is_expired(now_us) && m.overlaps_key_range(start, end) {
                    result.push(m);
                }
            }
        }
        result
    }

    fn query_time_range(&self, ts_start: i64, ts_end: i64, now_us: i64) -> Vec<&BenchBlockMeta> {
        let bucket_start = ts_start / self.time_bucket_us;
        let bucket_end = ts_end / self.time_bucket_us;
        let mut result = Vec::new();
        for (_, metas) in self.by_time.range(bucket_start..=bucket_end) {
            for m in metas {
                if !m.is_expired(now_us) && m.overlaps_time(ts_start, ts_end) {
                    result.push(m);
                }
            }
        }
        result
    }

    fn query_prefix_time_range(
        &self,
        prefix: &str,
        ts_start: i64,
        ts_end: i64,
        now_us: i64,
    ) -> Vec<&BenchBlockMeta> {
        let key_candidates = self.query_prefix(prefix, now_us);
        let bucket_start = ts_start / self.time_bucket_us;
        let bucket_end = ts_end / self.time_bucket_us;
        let mut time_set = std::collections::HashSet::new();
        for (_, metas) in self.by_time.range(bucket_start..=bucket_end) {
            for m in metas {
                if !m.is_expired(now_us) && m.overlaps_time(ts_start, ts_end) {
                    time_set.insert((m.sst_id, m.block_idx));
                }
            }
        }
        key_candidates
            .into_iter()
            .filter(|m| time_set.contains(&(m.sst_id, m.block_idx)))
            .collect()
    }

    fn query_key_time_range(
        &self,
        start: &str,
        end: &str,
        ts_start: i64,
        ts_end: i64,
        now_us: i64,
    ) -> Vec<&BenchBlockMeta> {
        let key_candidates = self.query_key_range(start, end, now_us);
        let bucket_start = ts_start / self.time_bucket_us;
        let bucket_end = ts_end / self.time_bucket_us;
        let mut time_set = std::collections::HashSet::new();
        for (_, metas) in self.by_time.range(bucket_start..=bucket_end) {
            for m in metas {
                if !m.is_expired(now_us) && m.overlaps_time(ts_start, ts_end) {
                    time_set.insert((m.sst_id, m.block_idx));
                }
            }
        }
        key_candidates
            .into_iter()
            .filter(|m| time_set.contains(&(m.sst_id, m.block_idx)))
            .collect()
    }
}

fn build_block_meta_index(n_sst: u32, blocks_per_sst: u32) -> BenchBlockMetaIndex {
    let mut idx = BenchBlockMetaIndex::new(3600);
    let mut key_counter = 0u32;
    for sst_id in 1..=n_sst {
        for block_idx in 0..blocks_per_sst {
            let min_key = format!("key_{:08}", key_counter);
            key_counter += 10;
            let max_key = format!("key_{:08}", key_counter - 1);
            let base_ts = (sst_id as i64) * 1_000_000_000 + (block_idx as i64) * 100_000;
            idx.add_block(BenchBlockMeta {
                sst_id,
                block_idx,
                min_key,
                max_key,
                min_ts: base_ts,
                max_ts: base_ts + 99_000,
                min_expire: i64::MAX,
                max_expire: i64::MAX,
            });
        }
    }
    idx
}

fn bench_wal_write_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");

    for size in [100usize, 1000, 5000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("encode_batch", size), &size, |b, &size| {
            let records = make_bench_records(size);
            b.iter(|| {
                let mut buf = Vec::with_capacity(size * 64);
                for (i, rec) in records.iter().enumerate() {
                    wal_encode_record(
                        black_box(&rec.key),
                        black_box(rec.ts),
                        black_box(rec.expire_at),
                        black_box(&rec.value),
                        black_box(i as u64),
                        &mut buf,
                    );
                }
                black_box(&buf);
            });
        });

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("decode_batch", size), &size, |b, &size| {
            let records = make_bench_records(size);
            let mut buf = Vec::with_capacity(size * 64);
            for (i, rec) in records.iter().enumerate() {
                wal_encode_record(
                    &rec.key,
                    rec.ts,
                    rec.expire_at,
                    &rec.value,
                    i as u64,
                    &mut buf,
                );
            }
            b.iter(|| {
                let mut pos = 0;
                let mut count = 0;
                while pos < buf.len() {
                    if let Some((_, advance)) = wal_decode_record(black_box(&buf[pos..])) {
                        pos += advance;
                        count += 1;
                    } else {
                        break;
                    }
                }
                black_box(count);
                assert_eq!(count, size);
            });
        });
    }

    group.finish();
}

fn bench_memtable(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable");

    for size in [100usize, 1000, 5000] {
        let records = make_bench_records(size);

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("insert", size), &size, |b, &_size| {
            b.iter(|| {
                let mut mt = MemTableBench::new();
                for (i, rec) in records.iter().enumerate() {
                    mt.insert(rec.clone(), black_box(i as u64));
                }
                black_box(&mt);
            });
        });

        let mut mt = MemTableBench::new();
        for (i, rec) in records.iter().enumerate() {
            mt.insert(rec.clone(), i as u64);
        }

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("query_prefix", size),
            &size,
            |b, &_size| {
                b.iter(|| {
                    let results = mt.query_prefix(black_box("key_00"), i64::MAX);
                    black_box(results.len());
                });
            },
        );

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("query_key_range", size),
            &size,
            |b, &_size| {
                b.iter(|| {
                    let results = mt.query_key_range(
                        black_box("key_000100"),
                        black_box("key_000500"),
                        i64::MAX,
                    );
                    black_box(results.len());
                });
            },
        );

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("query_time_range", size),
            &size,
            |b, &_size| {
                b.iter(|| {
                    let results =
                        mt.query_time_range(black_box(10000), black_box(200000), i64::MAX);
                    black_box(results.len());
                });
            },
        );
    }

    group.finish();
}

fn bench_sstable(c: &mut Criterion) {
    let mut group = c.benchmark_group("sstable");

    for size in [100usize, 1000, 5000] {
        let records = make_bench_records(size);
        let block_size = 100;

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("write", size), &size, |b, &_size| {
            b.iter_batched(
                || TempDir::new().unwrap(),
                |dir| {
                    let path = dir.path().join("test.sst");
                    let mut file = std::fs::File::create(&path).unwrap();
                    let mut total_bytes: u64 = 0;
                    for chunk in records.chunks(block_size) {
                        let raw_data = sst_encode_records(chunk);
                        let data_len = raw_data.len() as u32;
                        let compressed = lz4_flex::block::compress(&raw_data);
                        let compressed_len = compressed.len() as u32;
                        let min_ts = chunk.iter().map(|r| r.ts).min().unwrap_or(0);
                        let max_ts = chunk.iter().map(|r| r.ts).max().unwrap_or(0);
                        let min_expire = chunk.iter().map(|r| r.expire_at).min().unwrap_or(0);
                        let max_expire = chunk.iter().map(|r| r.expire_at).max().unwrap_or(0);
                        let mut header_bytes = [0u8; HEADER_SIZE];
                        let mut pos = 0;
                        header_bytes[pos..pos + 4].copy_from_slice(&BLOCK_MAGIC.to_be_bytes());
                        pos += 4;
                        header_bytes[pos..pos + 4]
                            .copy_from_slice(&(chunk.len() as u32).to_be_bytes());
                        pos += 4;
                        header_bytes[pos..pos + 8].copy_from_slice(&min_ts.to_be_bytes());
                        pos += 8;
                        header_bytes[pos..pos + 8].copy_from_slice(&max_ts.to_be_bytes());
                        pos += 8;
                        header_bytes[pos..pos + 8].copy_from_slice(&min_expire.to_be_bytes());
                        pos += 8;
                        header_bytes[pos..pos + 8].copy_from_slice(&max_expire.to_be_bytes());
                        pos += 8;
                        header_bytes[pos..pos + 4].copy_from_slice(&data_len.to_be_bytes());
                        pos += 4;
                        header_bytes[pos..pos + 4].copy_from_slice(&compressed_len.to_be_bytes());
                        file.write_all(&header_bytes).unwrap();
                        file.write_all(&compressed).unwrap();
                        total_bytes += HEADER_SIZE as u64 + compressed.len() as u64;
                    }
                    file.flush().unwrap();
                    black_box(total_bytes);
                },
                BatchSize::SmallInput,
            );
        });

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.sst");
        let mut file = std::fs::File::create(&path).unwrap();
        let mut block_offsets: Vec<u64> = Vec::new();
        let mut offset: u64 = 0;
        for chunk in records.chunks(block_size) {
            block_offsets.push(offset);
            let raw_data = sst_encode_records(chunk);
            let data_len = raw_data.len() as u32;
            let compressed = lz4_flex::block::compress(&raw_data);
            let compressed_len = compressed.len() as u32;
            let min_ts = chunk.iter().map(|r| r.ts).min().unwrap_or(0);
            let max_ts = chunk.iter().map(|r| r.ts).max().unwrap_or(0);
            let min_expire = chunk.iter().map(|r| r.expire_at).min().unwrap_or(0);
            let max_expire = chunk.iter().map(|r| r.expire_at).max().unwrap_or(0);
            let mut header_bytes = [0u8; HEADER_SIZE];
            let mut pos = 0;
            header_bytes[pos..pos + 4].copy_from_slice(&BLOCK_MAGIC.to_be_bytes());
            pos += 4;
            header_bytes[pos..pos + 4].copy_from_slice(&(chunk.len() as u32).to_be_bytes());
            pos += 4;
            header_bytes[pos..pos + 8].copy_from_slice(&min_ts.to_be_bytes());
            pos += 8;
            header_bytes[pos..pos + 8].copy_from_slice(&max_ts.to_be_bytes());
            pos += 8;
            header_bytes[pos..pos + 8].copy_from_slice(&min_expire.to_be_bytes());
            pos += 8;
            header_bytes[pos..pos + 8].copy_from_slice(&max_expire.to_be_bytes());
            pos += 8;
            header_bytes[pos..pos + 4].copy_from_slice(&data_len.to_be_bytes());
            pos += 4;
            header_bytes[pos..pos + 4].copy_from_slice(&compressed_len.to_be_bytes());
            file.write_all(&header_bytes).unwrap();
            file.write_all(&compressed).unwrap();
            offset += HEADER_SIZE as u64 + compressed.len() as u64;
        }
        file.flush().unwrap();
        let file_data = std::fs::read(&path).unwrap();

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("read_all_blocks", size),
            &size,
            |b, &_size| {
                b.iter(|| {
                    let mut all_records = Vec::new();
                    for &blk_offset in &block_offsets {
                        let pos = blk_offset as usize;
                        let _num_records =
                            u32::from_be_bytes(file_data[pos + 4..pos + 8].try_into().unwrap());
                        let compressed_len =
                            u32::from_be_bytes(file_data[pos + 44..pos + 48].try_into().unwrap())
                                as usize;
                        let data_len =
                            u32::from_be_bytes(file_data[pos + 40..pos + 44].try_into().unwrap())
                                as usize;
                        let compressed_start = pos + HEADER_SIZE;
                        let compressed_end = compressed_start + compressed_len;
                        let raw = lz4_flex::block::decompress(
                            &file_data[compressed_start..compressed_end],
                            data_len,
                        )
                        .unwrap();
                        let decoded = sst_decode_records(&raw, data_len);
                        all_records.extend(decoded);
                    }
                    black_box(all_records.len());
                });
            },
        );
    }

    group.finish();
}

fn bench_block_meta_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_meta_index");

    for n_sst in [10u32, 50, 100] {
        let blocks_per_sst: u32 = 20;
        let total_blocks = n_sst * blocks_per_sst;

        group.throughput(Throughput::Elements(total_blocks as u64));
        group.bench_with_input(
            BenchmarkId::new("add_blocks", n_sst),
            &n_sst,
            |b, &n_sst| {
                b.iter(|| {
                    let mut idx = BenchBlockMetaIndex::new(3600);
                    let mut key_counter = 0u32;
                    for sst_id in 1..=n_sst {
                        for block_idx in 0..blocks_per_sst {
                            let min_key = format!("key_{:08}", key_counter);
                            key_counter += 10;
                            let max_key = format!("key_{:08}", key_counter - 1);
                            let base_ts =
                                (sst_id as i64) * 1_000_000_000 + (block_idx as i64) * 100_000;
                            idx.add_block(BenchBlockMeta {
                                sst_id,
                                block_idx,
                                min_key,
                                max_key,
                                min_ts: base_ts,
                                max_ts: base_ts + 99_000,
                                min_expire: i64::MAX,
                                max_expire: i64::MAX,
                            });
                        }
                    }
                    black_box(&idx);
                });
            },
        );

        let idx = build_block_meta_index(n_sst, blocks_per_sst);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("query_prefix", n_sst), &n_sst, |b, _| {
            b.iter(|| {
                let results = idx.query_prefix(black_box("key_0000"), 0);
                black_box(results.len());
            });
        });

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("query_key_range", n_sst),
            &n_sst,
            |b, _| {
                b.iter(|| {
                    let results = idx.query_key_range(
                        black_box("key_00001000"),
                        black_box("key_00002000"),
                        0,
                    );
                    black_box(results.len());
                });
            },
        );

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("query_time_range", n_sst),
            &n_sst,
            |b, _| {
                b.iter(|| {
                    let ts_start = 10_000_000_000i64;
                    let ts_end = 20_000_000_000i64;
                    let results = idx.query_time_range(black_box(ts_start), black_box(ts_end), 0);
                    black_box(results.len());
                });
            },
        );

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("query_prefix_time_range", n_sst),
            &n_sst,
            |b, _| {
                b.iter(|| {
                    let ts_start = 10_000_000_000i64;
                    let ts_end = 50_000_000_000i64;
                    let results =
                        idx.query_prefix_time_range(black_box("key_"), ts_start, ts_end, 0);
                    black_box(results.len());
                });
            },
        );

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("query_key_time_range", n_sst),
            &n_sst,
            |b, _| {
                b.iter(|| {
                    let ts_start = 10_000_000_000i64;
                    let ts_end = 50_000_000_000i64;
                    let results = idx.query_key_time_range(
                        black_box("key_00001000"),
                        black_box("key_00005000"),
                        ts_start,
                        ts_end,
                        0,
                    );
                    black_box(results.len());
                });
            },
        );
    }

    group.finish();
}

fn bench_engine(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine");

    for batch_size in [10usize, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("write_batch", batch_size),
            &batch_size,
            |b, &batch_size| {
                b.iter_batched(
                    || {
                        let dir = TempDir::new().unwrap();
                        let config = make_config(dir.path());
                        let engine = Engine::open(config).unwrap();
                        let records: Vec<Record> = (0..batch_size)
                            .map(|i| Record {
                                key: format!("bench_key_{:06}", i).into_bytes(),
                                ts: i as i64 * 100,
                                expire_at: i64::MAX,
                                value: vec![42u8; 32],
                            })
                            .collect();
                        (dir, engine, records)
                    },
                    |(dir, engine, records): (TempDir, Engine, Vec<Record>)| {
                        engine.write_batch(&records).unwrap();
                        engine.shutdown().unwrap();
                        drop(dir);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    let records_for_query: Vec<Record> = (0..1000)
        .map(|i| Record {
            key: format!("query_key_{:06}", i).into_bytes(),
            ts: i as i64 * 100,
            expire_at: i64::MAX,
            value: vec![42u8; 32],
        })
        .collect();

    let dir = TempDir::new().unwrap();
    let config = make_config(dir.path());
    let engine = Engine::open(config).unwrap();
    engine.write_batch(&records_for_query).unwrap();

    group.throughput(Throughput::Elements(1));
    group.bench_function("query_prefix", |b| {
        b.iter(|| {
            let results = engine.query(Query::prefix("query_key_000")).unwrap();
            black_box(results.len());
        });
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("query_key_range", |b| {
        b.iter(|| {
            let results = engine
                .query(Query::key_range("query_key_000100", "query_key_000500"))
                .unwrap();
            black_box(results.len());
        });
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("query_time_range", |b| {
        b.iter(|| {
            let results = engine.query(Query::time_range(10000, 50000)).unwrap();
            black_box(results.len());
        });
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("query_prefix_time_range", |b| {
        b.iter(|| {
            let results = engine
                .query(Query::prefix_time_range("query_key_", 10000, 50000))
                .unwrap();
            black_box(results.len());
        });
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("query_key_time_range", |b| {
        b.iter(|| {
            let results = engine
                .query(Query::key_time_range(
                    "query_key_000100",
                    "query_key_000500",
                    10000,
                    50000,
                ))
                .unwrap();
            black_box(results.len());
        });
    });

    engine.shutdown().unwrap();

    group.finish();
}

fn bench_crc32(c: &mut Criterion) {
    let mut group = c.benchmark_group("crc32");

    for size in [64usize, 256, 1024, 4096] {
        let data = vec![0xABu8; size];

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::new("crc32_iso_hdlc", size),
            &size,
            |b, &_size| {
                b.iter(|| {
                    let crc = crc32_simple(black_box(&data));
                    black_box(crc);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_wal_write_read,
    bench_memtable,
    bench_sstable,
    bench_block_meta_index,
    bench_engine,
    bench_crc32,
);
criterion_main!(benches);
