use serde::{Deserialize, Serialize};
use std::ops::Bound;
use std::path::PathBuf;

/// Serde adapter for `Record.key` (`Vec<u8>`).
///
/// Serialises as a UTF-8 **string** for HTTP / JSON API compatibility (lossy
/// replacement bytes `U+FFFD` for non-UTF-8 keys). On deserialise, accepts
/// either a JSON string (converted to its UTF-8 bytes) or a JSON array of
/// bytes (preserved as-is). This keeps old clients that send `"key": "abc"`
/// working while letting future binary-key clients send raw bytes.
pub(crate) mod key_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(key: &[u8], s: S) -> Result<S::Ok, S::Error> {
        let cow = String::from_utf8_lossy(key);
        s.serialize_str(&cow)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum KeyRepr {
            Str(String),
            Bytes(Vec<u8>),
        }
        match KeyRepr::deserialize(d)? {
            KeyRepr::Str(s) => Ok(s.into_bytes()),
            KeyRepr::Bytes(b) => Ok(b),
        }
    }
}

/// A single key-value record in the LSM engine.
///
/// Each record has a binary `key`, a microsecond-precision `ts` timestamp,
/// an `expire_at` time (in microseconds since epoch), and a binary `value`.
/// Records with `expire_at <= now` are skipped by queries and eventually
/// garbage-collected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// Binary key. Use [`Record::key_str`] for lossy UTF-8 display.
    #[serde(with = "key_serde")]
    pub key: Vec<u8>,
    /// Microsecond timestamp (e.g. `SystemTime::now()` as microseconds since Unix epoch).
    pub ts: i64,
    /// Expiration time in microseconds since Unix epoch.
    /// Use `i64::MAX` for records that should never expire.
    pub expire_at: i64,
    /// Binary payload. Any byte sequence is accepted.
    pub value: Vec<u8>,
}

impl Record {
    /// Convenience constructor mirroring the old `String`-keyed API.
    pub fn new(key: impl Into<Vec<u8>>, ts: i64, value: Vec<u8>) -> Self {
        Self {
            key: key.into(),
            ts,
            expire_at: i64::MAX,
            value,
        }
    }

    /// View the key as a UTF-8 string slice when callers know keys are text.
    /// Returns the raw bytes as a lossy string for non-UTF-8 keys.
    pub fn key_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.key)
    }
}

/// Operation type for a write-batch entry.
///
/// `Put` inserts or updates a record. `Delete` removes a specific `(key, ts)`
/// version. `DeleteRange` removes all records whose keys fall within
/// `[key, range_end)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Op {
    /// Insert or update a record.
    #[default]
    Put = 0,
    /// Delete a specific `(key, ts)` version.
    Delete = 1,
    /// Delete all records with keys in `[key, range_end)`.
    DeleteRange = 2,
}

impl Op {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Op::Delete,
            2 => Op::DeleteRange,
            _ => Op::Put,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InternalRecord {
    pub seq: u64,
    pub op: Op,
    pub key: Vec<u8>,
    pub ts: i64,
    pub expire_at: i64,
    pub value: Vec<u8>,
    pub range_end: Option<Vec<u8>>,
}

impl InternalRecord {
    pub fn from_record(rec: &Record, seq: u64) -> Self {
        Self {
            seq,
            op: Op::Put,
            key: rec.key.clone(),
            ts: rec.ts,
            expire_at: rec.expire_at,
            value: rec.value.clone(),
            range_end: None,
        }
    }

    pub fn delete(key: Vec<u8>, ts: i64, seq: u64) -> Self {
        Self {
            seq,
            op: Op::Delete,
            key,
            ts,
            expire_at: i64::MAX,
            value: vec![],
            range_end: None,
        }
    }

    pub fn delete_range(start_key: Vec<u8>, end_key: Vec<u8>, seq: u64) -> Self {
        Self {
            seq,
            op: Op::DeleteRange,
            key: start_key,
            ts: 0,
            expire_at: i64::MAX,
            value: vec![],
            range_end: Some(end_key),
        }
    }

    pub fn to_record(&self) -> Record {
        Record {
            key: self.key.clone(),
            ts: self.ts,
            expire_at: self.expire_at,
            value: self.value.clone(),
        }
    }

    pub fn into_record_owned(self) -> Record {
        Record {
            key: self.key,
            ts: self.ts,
            expire_at: self.expire_at,
            value: self.value,
        }
    }

    pub fn estimated_size(&self) -> usize {
        let base = 8 + 1 + 2 + self.key.len() + 8 + 8 + 4 + self.value.len();
        match &self.range_end {
            Some(re) => base + 2 + re.len(),
            None => base,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum SyncMode {
    /// fsync the WAL after every write batch. Maximum durability, lower
    /// throughput (each batch incurs a synchronous disk I/O).
    #[default]
    Always,
    /// fsync the WAL on a periodic tick (milliseconds). Balances durability
    /// with throughput by coalescing multiple batches into a single fsync.
    /// Requires the background maintenance task to be running.
    IntervalMs(u64),
}

/// Engine configuration.
///
/// Use `Config::default()` for sensible defaults, or construct inline:
///
/// Selects the on-disk storage backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackendKind {
    /// One `.sst` file per SST — the classic multi-file layout.
    MultiFile,
    /// All SSTs packed inside a single `.db` container file.
    SingleFile,
}

impl Default for StorageBackendKind {
    fn default() -> Self {
        Self::MultiFile
    }
}

/// ```no_run
/// use flowdb::Config;
///
/// let config = Config {
///     data_dir: "./my_data".into(),
///     memtable_size_mb: 16,
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Data directory path. Created automatically unless `create_if_missing` is false.
    pub data_dir: PathBuf,
    /// Default TTL (seconds) applied to all records that do not have an explicit `expire_at`.
    /// `None` means records live forever unless explicitly deleted.
    /// Maximum safe value is ~9.22 × 10¹² (≈ 292,000 years); larger values
    /// overflow `i64` microsecond conversion and are rejected by `validate()`.
    pub default_ttl_secs: Option<u64>,
    /// Garbage collection interval in seconds (default: 3600 = 1 hour).
    /// GC purges SST files whose records have all expired.
    pub gc_interval_secs: u64,
    /// Active memtable size threshold in MB (default: 64).
    /// When the active memtable exceeds this, it is frozen and a flush is triggered.
    pub memtable_size_mb: usize,
    /// Maximum number of frozen (immutable) memtables before write backpressure kicks in
    /// (default: 2). When this limit is reached, writers block until a flush completes.
    pub max_frozen_memtables: usize,
    /// SST block size in bytes (default: 8192). Each block is independently compressed.
    pub block_size: usize,
    /// Background flush interval in milliseconds (default: 1000).
    /// The background maintenance thread flushes the active memtable at this interval.
    pub flush_interval_ms: u64,
    /// Time bucket width in seconds (default: 3600). The block-level index groups
    /// SST records into time buckets for efficient time-range pruning.
    /// Maximum safe value is ~9.22 × 10¹² (≈ 292,000 years); larger values
    /// overflow `i64` when converted to microseconds and are rejected by `validate()`.
    pub time_bucket_secs: u64,
    /// Memory budget for the block meta index in MB (default: 256).
    pub index_memory_budget_mb: usize,
    /// Block cache capacity in MB (default: 128). Shared across all SST readers.
    pub block_cache_capacity_mb: usize,
    /// Bloom filter bits per key (default: 10). Higher values reduce false-positive
    /// rates at the cost of more memory. Set to 0 to disable bloom filters.
    pub bloom_bits_per_key: usize,
    /// Maximum WAL segment file size in MB (default: 64).
    /// When a WAL segment exceeds this size, a new segment is created.
    pub wal_segment_size_mb: u64,
    /// Number of SST files needed to trigger a compaction (default: 2).
    /// Higher values reduce write amplification but increase read amplification.
    pub compaction_threshold: usize,
    /// Background compaction interval in milliseconds (default: 60000 = 60 seconds).
    /// The background maintenance thread checks for compaction candidates at this
    /// interval, independently of `gc_interval_secs`. Lower values keep SST file
    /// counts low at the cost of more CPU/IO for merging.
    #[serde(default = "default_compaction_interval_ms")]
    pub compaction_interval_ms: u64,
    /// Auto-create the data directory if it does not exist (default: true).
    pub create_if_missing: bool,
    /// WAL sync mode (default: `SyncMode::Always`).
    /// - `Always`: fsync every batch (maximum durability).
    /// - `IntervalMs(n)`: fsync on a periodic tick (higher throughput, may lose recent writes).
    #[serde(default)]
    pub wal_sync_mode: SyncMode,
    /// Automatically spawn a background maintenance thread for flush, compaction, GC,
    /// and periodic WAL sync (default: true). Set to false in tests that want full
    /// manual control over maintenance operations.
    #[serde(default = "default_background")]
    pub auto_background: bool,
    /// On-disk storage backend (default: `MultiFile`).
    /// - `MultiFile`: one `.sst` file per SST under `{data_dir}/SST/`.
    /// - `SingleFile`: all SSTs packed inside a single `{data_dir}/flow.db` file.
    #[serde(default)]
    pub storage_backend: StorageBackendKind,
}

fn default_background() -> bool {
    true
}

fn default_compaction_interval_ms() -> u64 {
    60_000
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            default_ttl_secs: None,
            gc_interval_secs: 3600,
            memtable_size_mb: 64,
            max_frozen_memtables: 2,
            block_size: 8192,
            flush_interval_ms: 1000,
            time_bucket_secs: 3600,
            index_memory_budget_mb: 256,
            block_cache_capacity_mb: 128,
            bloom_bits_per_key: 10,
            wal_segment_size_mb: 64,
            compaction_threshold: 2,
            compaction_interval_ms: 60_000,
            create_if_missing: true,
            wal_sync_mode: SyncMode::Always,
            auto_background: true,
            storage_backend: StorageBackendKind::MultiFile,
        }
    }
}

impl Config {
    /// Validate all configuration fields and return an error if any value
    /// is out of range or would cause runtime panics (e.g. division by
    /// zero in the time-bucket index).  Called by `Engine::open`.
    pub fn validate(&self) -> crate::error::Result<()> {
        use crate::error::FlowError;

        if self.memtable_size_mb == 0 {
            return Err(FlowError::Config(
                "memtable_size_mb must be >= 1 (0 causes a flush storm)".into(),
            ));
        }
        if self.max_frozen_memtables == 0 {
            return Err(FlowError::Config(
                "max_frozen_memtables must be >= 1 (0 means unbounded growth)".into(),
            ));
        }
        if self.block_size == 0 {
            return Err(FlowError::Config("block_size must be >= 1".into()));
        }
        if self.time_bucket_secs == 0 {
            return Err(FlowError::Config(
                "time_bucket_secs must be >= 1 (0 causes division-by-zero panic)".into(),
            ));
        }
        if self.bloom_bits_per_key == 0 {
            return Err(FlowError::Config(
                "bloom_bits_per_key must be >= 1 (0 produces a useless filter)".into(),
            ));
        }
        if self.wal_segment_size_mb == 0 {
            return Err(FlowError::Config(
                "wal_segment_size_mb must be >= 1 (0 causes segment rollover on every write)"
                    .into(),
            ));
        }
        if self.compaction_threshold == 0 {
            return Err(FlowError::Config(
                "compaction_threshold must be >= 1 (0 disables compaction entirely)".into(),
            ));
        }
        if self.compaction_interval_ms == 0 {
            return Err(FlowError::Config(
                "compaction_interval_ms must be >= 1 (0 causes tight-loop spinning)".into(),
            ));
        }
        if self.block_cache_capacity_mb == 0 {
            return Err(FlowError::Config(
                "block_cache_capacity_mb must be >= 1".into(),
            ));
        }
        if let Some(ttl) = self.default_ttl_secs
            && ttl == 0
        {
            return Err(FlowError::Config(
                "default_ttl_secs must be > 0 if set (0 = instant expiry)".into(),
            ));
        }
        if let Some(ttl) = self.default_ttl_secs
            && ttl > i64::MAX as u64 / 1_000_000
        {
            return Err(FlowError::Config(
                "default_ttl_secs is too large (would overflow i64 when converted to microseconds)"
                    .into(),
            ));
        }
        let max_bucket = i64::MAX as u64 / 1_000_000;
        if self.time_bucket_secs > max_bucket {
            return Err(FlowError::Config(
                "time_bucket_secs is too large (product with 1_000_000 would overflow i64)"
                    .into(),
            ));
        }
        Ok(())
    }
}

/// Key matching strategy used by [`Query`] and internally by the scan pipeline.
#[derive(Debug, Clone)]
pub enum KeyFilter {
    /// Match all keys starting with the given prefix.
    Prefix(Vec<u8>),
    /// Match all keys within `[start, end]` (inclusive on both ends).
    Range { start: Vec<u8>, end: Vec<u8> },
    /// Match every key (full scan).
    All,
}

/// A query over the LSM engine, specifying key filtering and optional time range.
///
/// Construct one of the convenience methods:
/// - [`Query::prefix`]
/// - [`Query::key_range`]
/// - [`Query::time_range`]
/// - [`Query::prefix_time_range`]
/// - [`Query::key_time_range`]
#[derive(Debug, Clone)]
pub struct Query {
    /// How to match keys.
    pub key_filter: KeyFilter,
    /// Optional inclusive time range `(start_micros, end_micros)`.
    pub time_range: Option<(i64, i64)>,
}

impl Query {
    pub fn prefix(key: impl Into<String>) -> Self {
        Self {
            key_filter: KeyFilter::Prefix(key.into().into_bytes()),
            time_range: None,
        }
    }

    pub fn key_range(start: impl Into<String>, end: impl Into<String>) -> Self {
        Self {
            key_filter: KeyFilter::Range {
                start: start.into().into_bytes(),
                end: end.into().into_bytes(),
            },
            time_range: None,
        }
    }

    pub fn time_range(start: i64, end: i64) -> Self {
        Self {
            key_filter: KeyFilter::All,
            time_range: Some((start, end)),
        }
    }

    pub fn prefix_time_range(key: impl Into<String>, start: i64, end: i64) -> Self {
        Self {
            key_filter: KeyFilter::Prefix(key.into().into_bytes()),
            time_range: Some((start, end)),
        }
    }

    pub fn key_time_range(
        start_key: impl Into<String>,
        end_key: impl Into<String>,
        start: i64,
        end: i64,
    ) -> Self {
        Self {
            key_filter: KeyFilter::Range {
                start: start_key.into().into_bytes(),
                end: end_key.into().into_bytes(),
            },
            time_range: Some((start, end)),
        }
    }
}

/// Read options for scan / get operations (RocksDB-style).
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// Whether to populate the block cache on reads (default: true).
    pub fill_cache: bool,
    /// Whether to verify checksums on SST reads (default: true).
    pub verify_checksums: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            fill_cache: true,
            verify_checksums: true,
        }
    }
}

/// Scan range specifying key + time bounds for `Engine::scan`.
#[derive(Debug, Clone)]
pub struct ScanRange {
    pub key_start: Bound<Vec<u8>>,
    pub key_end: Bound<Vec<u8>>,
    pub ts_start: Bound<i64>,
    pub ts_end: Bound<i64>,
}

impl ScanRange {
    /// Prefix scan: `[prefix, next_prefix)` with unbounded time.
    pub fn prefix(p: impl AsRef<str>) -> Self {
        let bytes = p.as_ref().as_bytes().to_vec();
        let end = increment_prefix_bytes(&bytes);
        Self {
            key_start: Bound::Included(bytes.clone()),
            key_end: Bound::Excluded(end),
            ts_start: Bound::Unbounded,
            ts_end: Bound::Unbounded,
        }
    }

    /// Time range scan with unbounded key.
    pub fn time_range(start: i64, end: i64) -> Self {
        Self {
            key_start: Bound::Unbounded,
            key_end: Bound::Unbounded,
            ts_start: Bound::Included(start),
            ts_end: Bound::Included(end),
        }
    }

    /// Prefix + time range scan.
    pub fn prefix_time_range(p: impl AsRef<str>, ts_start: i64, ts_end: i64) -> Self {
        let bytes = p.as_ref().as_bytes().to_vec();
        let end = increment_prefix_bytes(&bytes);
        Self {
            key_start: Bound::Included(bytes.clone()),
            key_end: Bound::Excluded(end),
            ts_start: Bound::Included(ts_start),
            ts_end: Bound::Included(ts_end),
        }
    }

    /// Key range scan: `[start, end]` with unbounded time.
    pub fn key_range(start: impl AsRef<str>, end: impl AsRef<str>) -> Self {
        Self {
            key_start: Bound::Included(start.as_ref().as_bytes().to_vec()),
            key_end: Bound::Included(end.as_ref().as_bytes().to_vec()),
            ts_start: Bound::Unbounded,
            ts_end: Bound::Unbounded,
        }
    }

    /// Key range + time range scan.
    pub fn key_time_range(
        start: impl AsRef<str>,
        end: impl AsRef<str>,
        ts_start: i64,
        ts_end: i64,
    ) -> Self {
        Self {
            key_start: Bound::Included(start.as_ref().as_bytes().to_vec()),
            key_end: Bound::Included(end.as_ref().as_bytes().to_vec()),
            ts_start: Bound::Included(ts_start),
            ts_end: Bound::Included(ts_end),
        }
    }

    /// Full scan (all keys, all times).
    pub fn all() -> Self {
        Self {
            key_start: Bound::Unbounded,
            key_end: Bound::Unbounded,
            ts_start: Bound::Unbounded,
            ts_end: Bound::Unbounded,
        }
    }

    /// Convert to the internal `KeyFilter` + optional `(ts_start, ts_end)`.
    pub(crate) fn to_query_params(&self) -> (KeyFilter, Option<(i64, i64)>) {
        let kf = match (&self.key_start, &self.key_end) {
            (Bound::Included(s), Bound::Excluded(e)) if is_prefix_range(s, e) => {
                KeyFilter::Prefix(s.clone())
            }
            (Bound::Included(s), Bound::Included(e)) => KeyFilter::Range {
                start: s.clone(),
                end: e.clone(),
            },
            (Bound::Included(s), Bound::Excluded(e)) => KeyFilter::Range {
                start: s.clone(),
                end: e.clone(),
            },
            _ => KeyFilter::All,
        };
        let tr = match (&self.ts_start, &self.ts_end) {
            (Bound::Included(s), Bound::Included(e)) => Some((*s, *e)),
            _ => None,
        };
        (kf, tr)
    }
}

pub(crate) fn increment_prefix_bytes(key: &[u8]) -> Vec<u8> {
    let mut bytes = key.to_vec();
    while let Some(last) = bytes.last_mut() {
        if *last < 255 {
            *last += 1;
            return bytes;
        }
        bytes.pop();
    }
    let mut sentinel = key.to_vec();
    sentinel.push(0);
    sentinel
}

fn is_prefix_range(start: &[u8], end: &[u8]) -> bool {
    end == increment_prefix_bytes(start)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_internal_record_estimated_size() {
        let rec = Record {
            key: b"hello".to_vec(),
            ts: 100,
            expire_at: i64::MAX,
            value: vec![1, 2, 3, 4, 5],
        };
        let irec = InternalRecord::from_record(&rec, 1);
        let expected = 8 + 1 + 2 + 5 + 8 + 8 + 4 + 5;
        assert_eq!(irec.estimated_size(), expected);
    }

    #[test]
    fn test_internal_record_estimated_size_empty() {
        let rec = Record {
            key: Vec::new(),
            ts: 0,
            expire_at: 0,
            value: Vec::new(),
        };
        let irec = InternalRecord::from_record(&rec, 0);
        let expected = (8 + 1 + 2) + 8 + 8 + 4;
        assert_eq!(irec.estimated_size(), expected);
    }

    #[test]
    fn test_query_key_time_range_constructor() {
        let q = Query::key_time_range("a", "z", 100, 200);
        match &q.key_filter {
            KeyFilter::Range { start, end } => {
                assert_eq!(start, b"a".as_slice());
                assert_eq!(end, b"z".as_slice());
            }
            _ => panic!("expected range"),
        }
        assert_eq!(q.time_range, Some((100, 200)));
    }

    #[test]
    fn test_query_prefix_time_range_constructor() {
        let q = Query::prefix_time_range("key", 100, 200);
        match &q.key_filter {
            KeyFilter::Prefix(k) => assert_eq!(k, b"key".as_slice()),
            _ => panic!("expected prefix"),
        }
        assert_eq!(q.time_range, Some((100, 200)));
    }

    #[test]
    fn test_query_time_range_constructor() {
        let q = Query::time_range(100, 200);
        assert!(matches!(q.key_filter, KeyFilter::All));
        assert_eq!(q.time_range, Some((100, 200)));
    }

    #[test]
    fn test_config_default_values() {
        let config = Config::default();
        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert_eq!(config.default_ttl_secs, None);
        assert_eq!(config.gc_interval_secs, 3600);
        assert_eq!(config.memtable_size_mb, 64);
        assert_eq!(config.max_frozen_memtables, 2);
        assert_eq!(config.block_size, 8192);
        assert_eq!(config.flush_interval_ms, 1000);
        assert_eq!(config.time_bucket_secs, 3600);
        assert_eq!(config.index_memory_budget_mb, 256);
        assert_eq!(config.block_cache_capacity_mb, 128);
        assert_eq!(config.bloom_bits_per_key, 10);
        assert_eq!(config.wal_segment_size_mb, 64);
        assert_eq!(config.compaction_threshold, 2);
        assert_eq!(config.compaction_interval_ms, 60_000);
        assert!(config.create_if_missing);
    }

    #[test]
    fn test_internal_record_from_to_record_roundtrip() {
        let rec = Record {
            key: b"test".to_vec(),
            ts: 42,
            expire_at: 100,
            value: vec![7, 8, 9],
        };
        let irec = InternalRecord::from_record(&rec, 99);
        assert_eq!(irec.seq, 99);
        assert_eq!(irec.key, b"test".as_slice());
        assert_eq!(irec.ts, 42);
        assert_eq!(irec.expire_at, 100);
        assert_eq!(irec.value, vec![7, 8, 9]);

        let back = irec.to_record();
        assert_eq!(back.key, rec.key);
        assert_eq!(back.ts, rec.ts);
        assert_eq!(back.expire_at, rec.expire_at);
        assert_eq!(back.value, rec.value);
    }

    #[test]
    fn test_read_options_default() {
        let opts = ReadOptions::default();
        assert!(opts.fill_cache);
        assert!(opts.verify_checksums);
    }

    #[test]
    fn test_scan_range_prefix() {
        let r = ScanRange::prefix("foo");
        assert!(matches!(r.key_start, Bound::Included(_)));
        assert!(matches!(r.key_end, Bound::Excluded(_)));
        assert!(matches!(r.ts_start, Bound::Unbounded));
        let (kf, tr) = r.to_query_params();
        assert!(matches!(kf, KeyFilter::Prefix(_)));
        assert!(tr.is_none());
    }

    #[test]
    fn test_scan_range_time_range() {
        let r = ScanRange::time_range(10, 20);
        assert!(matches!(r.key_start, Bound::Unbounded));
        let (kf, tr) = r.to_query_params();
        assert!(matches!(kf, KeyFilter::All));
        assert_eq!(tr, Some((10, 20)));
    }

    #[test]
    fn test_scan_range_prefix_time_range() {
        let r = ScanRange::prefix_time_range("bar", 1, 100);
        let (kf, tr) = r.to_query_params();
        assert!(matches!(kf, KeyFilter::Prefix(_)));
        assert_eq!(tr, Some((1, 100)));
    }

    #[test]
    fn test_scan_range_key_range() {
        let r = ScanRange::key_range("a", "z");
        let (kf, tr) = r.to_query_params();
        match kf {
            KeyFilter::Range { start, end } => {
                assert_eq!(start, b"a");
                assert_eq!(end, b"z");
            }
            _ => panic!("expected range"),
        }
        assert!(tr.is_none());
    }

    #[test]
    fn test_scan_range_all() {
        let r = ScanRange::all();
        let (kf, tr) = r.to_query_params();
        assert!(matches!(kf, KeyFilter::All));
        assert!(tr.is_none());
    }

    #[test]
    fn test_increment_prefix_bytes() {
        assert_eq!(increment_prefix_bytes(b"abc"), b"abd");
        assert_eq!(increment_prefix_bytes(b"a\xff"), b"b");
        // \xff\xff → all bytes overflow, sentinel [0xff, 0xff, 0] appended
        assert_eq!(increment_prefix_bytes(b"\xff\xff"), vec![0xff, 0xff, 0]);
    }

    #[test]
    fn test_sync_mode_default() {
        assert_eq!(SyncMode::default(), SyncMode::Always);
    }

    #[test]
    fn test_sync_mode_serde() {
        assert_eq!(
            serde_json::to_string(&SyncMode::Always).unwrap(),
            r#""always""#
        );
        let m: SyncMode = serde_json::from_str(r#""always""#).unwrap();
        assert_eq!(m, SyncMode::Always);

        let m: SyncMode = serde_json::from_str(r#"{"intervalms":1}"#).unwrap();
        assert_eq!(m, SyncMode::IntervalMs(1));
    }

    #[test]
    fn test_config_serde_defaults() {
        let json = r#"{"data_dir":"./data","memtable_size_mb":64,"block_size":8192,"flush_interval_ms":1000,"time_bucket_secs":3600,"index_memory_budget_mb":256,"block_cache_capacity_mb":128,"bloom_bits_per_key":10,"wal_segment_size_mb":64,"compaction_threshold":2,"create_if_missing":true,"gc_interval_secs":3600,"max_frozen_memtables":2}"#;
        let c: Config = serde_json::from_str(json).unwrap();
        assert_eq!(c.wal_sync_mode, SyncMode::Always);
        assert!(c.auto_background);
        assert_eq!(c.compaction_interval_ms, 60_000);
    }

    #[test]
    fn test_scan_range_key_time_range() {
        let r = ScanRange::key_time_range("a", "z", 100, 200);
        let (kf, tr) = r.to_query_params();
        match kf {
            KeyFilter::Range { start, end } => {
                assert_eq!(start, b"a");
                assert_eq!(end, b"z");
            }
            _ => panic!("expected range"),
        }
        assert_eq!(tr, Some((100, 200)));
    }

    // ------------------------------------------------------------------
    // Config::validate regression tests
    // ------------------------------------------------------------------

    #[test]
    fn test_config_validate_ok() {
        let cfg = Config {
            data_dir: std::env::temp_dir().join("flowdb-validate-ok"),
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_config_validate_time_bucket_zero() {
        let cfg = Config {
            time_bucket_secs: 0,
            ..Default::default()
        };
        assert!(
            cfg.validate().is_err(),
            "time_bucket_secs=0 must be rejected (div-by-zero)"
        );
    }

    #[test]
    fn test_config_validate_memtable_zero() {
        let cfg = Config {
            memtable_size_mb: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validate_max_frozen_zero() {
        let cfg = Config {
            max_frozen_memtables: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validate_block_size_zero() {
        let cfg = Config {
            block_size: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validate_bloom_zero() {
        let cfg = Config {
            bloom_bits_per_key: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validate_wal_segment_zero() {
        let cfg = Config {
            wal_segment_size_mb: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validate_compaction_threshold_zero() {
        let cfg = Config {
            compaction_threshold: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validate_compaction_interval_zero() {
        let cfg = Config {
            compaction_interval_ms: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validate_cache_zero() {
        let cfg = Config {
            block_cache_capacity_mb: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validate_ttl_zero() {
        let cfg = Config {
            default_ttl_secs: Some(0),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validate_ttl_overflow() {
        // i64::MAX / 1_000_000 ≈ 9.22e12; anything above that overflows
        // when converted to microseconds.
        let cfg = Config {
            default_ttl_secs: Some((i64::MAX as u64 / 1_000_000) + 1),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validate_time_bucket_overflow() {
        // time_bucket_secs * 1_000_000 must fit in i64.
        let max_bucket = i64::MAX as u64 / 1_000_000;
        let cfg = Config {
            time_bucket_secs: max_bucket + 1,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }
}
