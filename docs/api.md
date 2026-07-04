# FlowDB API Reference

## Engine (LSM Storage Engine)

```rust
use flowdb::{Engine, Config, Record, Query, ScanRange, ReadOptions, SyncMode};
```

### Opening & Closing

```rust
pub fn Engine::open(config: Config) -> Result<Self>
pub fn Engine::shutdown(self) -> Result<()>       // consumes self, flushes + stops bg maintenance
pub fn Engine::close(&self) -> Result<()>          // flushes + syncs WAL, keeps engine alive
```

### Writing

```rust
pub fn Engine::write_batch(&self, batch: &[Record]) -> Result<()>
pub fn Engine::write_batch_owned(&self, batch: Vec<Record>) -> Result<()>
pub fn Engine::write_batch_sync(&self, batch: Vec<Record>) -> Result<()>
pub fn Engine::write_batch_ttl(&self, batch: &[Record], ttl_secs: Option<u64>) -> Result<()>
```

### Reading

```rust
pub fn Engine::get(&self, key: &str, ts: i64) -> Result<Option<Record>>
pub fn Engine::get_sync(&self, key: &str, ts: i64) -> Option<Record>
pub fn Engine::get_latest(&self, key: &str) -> Result<Option<Record>>

pub fn Engine::query(&self, query: Query) -> Result<Vec<Record>>
pub fn Engine::query_by_prefix(&self, key: &str) -> Result<Vec<Record>>
pub fn Engine::query_by_key_range(&self, start: &str, end: &str) -> Result<Vec<Record>>
pub fn Engine::query_time_range(&self, start: i64, end: i64) -> Result<Vec<Record>>
pub fn Engine::query_prefix_time_range(&self, key: &str, start: i64, end: i64) -> Result<Vec<Record>>
pub fn Engine::query_key_time_range(&self, start_key: &str, end_key: &str, start: i64, end: i64) -> Result<Vec<Record>>
```

### Lazy Scan (Iterator)

```rust
pub fn Engine::scan(&self, range: ScanRange) -> Result<ScanIterator>
pub fn Engine::scan_opt(&self, range: ScanRange, opts: &ReadOptions) -> Result<ScanIterator>
pub fn Engine::scan_prefix(&self, prefix: &str) -> Result<ScanIterator>
pub fn Engine::scan_prefix_time_range(&self, prefix: &str, ts_start: i64, ts_end: i64) -> Result<ScanIterator>
```

`ScanIterator` implements `Iterator<Item = Result<Record>>` and `FusedIterator`.

### Deleting

```rust
pub fn Engine::delete_batch(&self, keys_ts: &[(String, i64)]) -> Result<()>
pub fn Engine::delete_range(&self, start_key: &str, end_key: &str) -> Result<()>
pub fn Engine::patch_record(&self, key: &str, ts: i64, new_value: Option<Vec<u8>>, new_ttl_secs: Option<u64>) -> Result<Record>
```

> `patch_record` is safe to call concurrently — it holds an internal
> serialization lock across the read and write, preventing lost updates.

### Maintenance

```rust
pub fn Engine::flush(&self) -> Result<()>
pub fn Engine::trigger_compaction(&self) -> Result<bool>
pub fn Engine::trigger_gc(&self) -> Result<u64>
pub fn Engine::spawn_background_maintenance(&self) -> Option<MaintenanceHandle>
```

### Statistics

```rust
pub fn Engine::stats(&self) -> EngineStats
pub fn Engine::metrics_text(&self) -> String    // Prometheus-format text
```

---

## Query

```rust
pub fn Query::prefix(key: impl Into<String>) -> Self
pub fn Query::key_range(start: impl Into<String>, end: impl Into<String>) -> Self
pub fn Query::time_range(start: i64, end: i64) -> Self
pub fn Query::prefix_time_range(key: impl Into<String>, start: i64, end: i64) -> Self
pub fn Query::key_time_range(start_key: impl Into<String>, end_key: impl Into<String>, start: i64, end: i64) -> Self
```

---

## ScanRange

```rust
pub fn ScanRange::prefix(p: impl AsRef<str>) -> Self
pub fn ScanRange::time_range(start: i64, end: i64) -> Self
pub fn ScanRange::prefix_time_range(p: impl AsRef<str>, ts_start: i64, ts_end: i64) -> Self
pub fn ScanRange::key_range(start: impl AsRef<str>, end: impl AsRef<str>) -> Self
pub fn ScanRange::key_time_range(start: impl AsRef<str>, end: impl AsRef<str>, ts_start: i64, ts_end: i64) -> Self
pub fn ScanRange::all() -> Self
```

---

## Record

```rust
pub struct Record {
    pub key: Vec<u8>,
    pub ts: i64,
    pub expire_at: i64,
    pub value: Vec<u8>,
}

impl Record {
    pub fn new(key: impl Into<Vec<u8>>, ts: i64, value: Vec<u8>) -> Self
    pub fn key_str(&self) -> Cow<'_, str>
}
```

---

## Config

```rust
pub struct Config {
    pub data_dir: PathBuf,
    pub default_ttl_secs: Option<u64>,     // default TTL for all records (max ~9.22e12)
    pub gc_interval_secs: u64,              // default: 3600 (1 hour)
    pub memtable_size_mb: usize,            // default: 64
    pub max_frozen_memtables: usize,        // default: 2
    pub block_size: usize,                  // default: 8192
    pub flush_interval_ms: u64,             // default: 1000
    pub time_bucket_secs: u64,              // default: 3600 (max ~9.22e12)
    pub index_memory_budget_mb: usize,      // default: 256
    pub block_cache_capacity_mb: usize,     // default: 128
    pub bloom_bits_per_key: usize,          // default: 10
    pub wal_segment_size_mb: u64,           // default: 64
    pub compaction_threshold: usize,        // default: 2
    pub compaction_interval_ms: u64,        // default: 60000 (60s)
    pub create_if_missing: bool,            // default: true
    pub wal_sync_mode: SyncMode,            // default: Always
    pub auto_background: bool,              // default: true
}
```

### SyncMode

```rust
pub enum SyncMode {
    Always,                  // fsync every batch (safe, slower)
    IntervalMs(u64),         // fsync on periodic tick (fast, may lose recent writes)
}
```

### ReadOptions

```rust
pub struct ReadOptions {
    pub fill_cache: bool,           // default: true
    pub verify_checksums: bool,     // default: true
}
```

---

## EngineStats

```rust
pub struct EngineStats {
    pub total_records_written: u64,
    pub total_bytes_written: u64,
    pub total_records_read: u64,
    pub total_records_expired: u64,
    pub total_flushes: u64,
    pub total_gc_runs: u64,
    pub total_compaction_runs: u64,
    pub records_purged_by_gc: u64,
    pub memtable_records: usize,
    pub memtable_bytes: usize,
    pub frozen_memtable_count: usize,
    pub sstable_count: usize,
    pub sstable_bytes: u64,
    pub wal_bytes: u64,
    pub block_meta_index_entries: usize,
    pub time_index_buckets: usize,
    pub block_cache_hit_rate: f64,
    pub compression_ratio: f64,
    pub write_latency_p50_us: u64,
    pub write_latency_p90_us: u64,
    pub write_latency_p99_us: u64,
    pub query_latency_p50_us: u64,
    pub query_latency_p90_us: u64,
    pub query_latency_p99_us: u64,
    pub flush_latency_p50_us: u64,
    pub flush_latency_p90_us: u64,
    pub flush_latency_p99_us: u64,
    pub uptime_secs: u64,
    // ... plus UDP/HTTP counters for optional network transport
}
```

---

## JsonDB (JSON Document Layer)

```rust
use flowdb::jsondb::{JsonDB, Transaction, TransactionMode, QueryBuilder, SortDir, StoreSchema, IndexSchema};
```

### Opening

```rust
pub fn JsonDB::open(config: Config) -> Result<Self>
pub fn JsonDB::from_engine(engine: Engine) -> Result<Self>
pub fn JsonDB::shutdown(self) -> Result<()>
pub fn JsonDB::close(&self) -> Result<()>
pub fn JsonDB::engine(&self) -> &Engine
```

### Schema Management

```rust
pub fn JsonDB::create_object_store(&self, name: &str, key_path: &str) -> Result<()>
pub fn JsonDB::delete_object_store(&self, name: &str) -> Result<()>
pub fn JsonDB::create_index(&self, store: &str, name: &str, key_paths: &[&str], unique: bool) -> Result<()>
pub fn JsonDB::create_index_on(&self, store: &str, name: &str, key_path: &str, unique: bool) -> Result<()>
pub fn JsonDB::delete_index(&self, store: &str, name: &str) -> Result<()>
pub fn JsonDB::store_names(&self) -> Vec<String>
pub fn JsonDB::get_store(&self, name: &str) -> Option<StoreDef>
```

### CRUD

```rust
pub fn JsonDB::put(&self, store: &str, doc: Value) -> Result<Value>
pub fn JsonDB::get(&self, store: &str, key: &Value) -> Result<Option<Value>>
pub fn JsonDB::delete(&self, store: &str, key: &Value) -> Result<()>
pub fn JsonDB::put_auto(&self, store: &str, doc: Value) -> Result<Value>
pub fn JsonDB::count(&self, store: &str) -> Result<usize>
pub fn JsonDB::scan(&self, store: &str) -> Result<Vec<Value>>
```

> **Security:** String primary keys must not contain null bytes (`\x00`).
> They are rejected at write time (`put`, `put_auto`) to prevent
> composite-key separator injection.

### Index Queries

```rust
pub fn JsonDB::get_by_index(&self, store: &str, index: &str, value: &Value) -> Result<Vec<Value>>
pub fn JsonDB::range_by_index(&self, store: &str, index: &str, start: &Value, end: &Value) -> Result<Vec<Value>>
```

### QueryBuilder

```rust
pub fn JsonDB::query<'a>(&'a self, store: &'a str) -> QueryBuilder<'a>

impl<'a> QueryBuilder<'a> {
    pub fn new(db: &'a JsonDB, store: &'a str) -> Self
    pub fn where_eq(self, field: &str, value: Value) -> Self
    pub fn where_range(self, field: &str, start: Value, end: Value) -> Self
    pub fn where_in(self, field: &str, values: Vec<Value>) -> Self
    pub fn order_by(self, field: &str, dir: SortDir) -> Self
    pub fn limit(self, n: usize) -> Self
    pub fn offset(self, n: usize) -> Self
    pub fn collect(self) -> Result<Vec<Value>>
    pub fn collect_doc<T: DeserializeOwned>(self) -> Result<Vec<T>>
}
```

### Transactions

```rust
pub fn JsonDB::transaction<'db>(&'db self, stores: &[&str], mode: TransactionMode) -> Result<Transaction<'db>>

pub enum TransactionMode { ReadOnly, ReadWrite }

impl Transaction {
    pub fn put(&mut self, store: &str, doc: Value) -> Result<Value>
    pub fn get(&self, store: &str, key: &Value) -> Result<Option<Value>>
    pub fn delete(&mut self, store: &str, key: &Value) -> Result<()>
    pub fn count(&self, store: &str) -> Result<usize>
    pub fn scan(&self, store: &str) -> Result<Vec<Value>>
    pub fn get_by_index(&self, store: &str, index: &str, value: &Value) -> Result<Vec<Value>>
    pub fn range_by_index(&self, store: &str, index: &str, start: &Value, end: &Value) -> Result<Vec<Value>>
    pub fn put_auto(&mut self, store: &str, doc: Value) -> Result<Value>
    pub fn commit(self) -> Result<()>
    pub fn abort(self)
}
```

### Serde (Typed) API

```rust
pub fn JsonDB::put_doc<T: Serialize>(&self, store: &str, doc: &T) -> Result<Value>
pub fn JsonDB::get_doc<T: DeserializeOwned>(&self, store: &str, key: impl KeyArg) -> Result<Option<T>>
pub fn JsonDB::delete_doc(&self, store: &str, key: impl KeyArg) -> Result<()>
```

`KeyArg` is implemented for: `&str`, `String`, `i64`, `i32`, `u64`, `u32`, `Value`, `&Value`.

---

## StoreDef & IndexDef

```rust
pub struct StoreDef {
    pub name: String,
    pub key_path: String,
    pub auto_increment: bool,
    pub indexes: Vec<IndexDef>,
    pub next_auto_id: u64,
}

pub struct IndexDef {
    pub name: String,
    pub key_paths: Vec<String>,
    pub unique: bool,
    pub multi_entry: bool,
}
```

---

## SortDir

```rust
pub enum SortDir {
    Asc,
    Desc,
}
```

---

## ObjectStore Derive Macro

```rust
pub use flowdb::ObjectStore;

// Or via the jsondb module:
pub trait flowdb::jsondb::ObjectStore {
    fn store_def() -> StoreSchema;
}
```

The `#[derive(ObjectStore)]` macro generates `StoreDef` from struct annotations:

```rust
use flowdb::ObjectStore;
use flowdb::jsondb::StoreSchema;

#[derive(ObjectStore)]
#[store(name = "users", key_path = "id")]     // name defaults to struct name
struct User {
    id: String,
    #[index(name = "by_email", unique)]
    email: String,
    #[index(name = "by_age")]
    age: u32,
}
```

This generates an `ObjectStore` impl equivalent to:

```rust
impl ObjectStore for User {
    fn store_def() -> StoreSchema {
        StoreSchema::new("users", "id")
            .with_index("by_email", &["email"], true)
            .with_index("by_age", &["age"], false)
    }
}
```

Apply via `JsonDB::apply_schema::<T>()`:

```rust
db.apply_schema::<User>().unwrap();   // one call sets up store + all indexes
```

### Container attributes (`#[store(...)]`)

| Attribute | Description |
|-----------|-------------|
| `key_path = "..."` | **Required.** Primary key field path |
| `name = "..."` | Store name (defaults to struct name) |
| `auto_increment` | Enable auto-increment primary keys |
| **Security** | String primary keys must not contain null bytes (`\x00`) — they are rejected at write time to prevent composite-key separator injection.

### Field attributes (`#[index(...)]`)

| Attribute | Description |
|-----------|-------------|
| `unique` | Create a unique index |
| `name = "..."` | Custom index name (defaults to field name) |

---

## StoreDef Builder

```rust
impl StoreSchema {
    pub fn new(name: &str, key_path: &str) -> Self;
    pub fn with_index(self, name: &str, key_paths: &[&str], unique: bool) -> Self;
    pub fn with_auto_increment(self) -> Self;
}
```

Example:

```rust
let def = StoreSchema::new("users", "id")
    .with_index("by_email", &["email"], true)
    .with_index("by_city_age", &["city", "age"], false);

db.apply_store(&def).unwrap();
```

## JsonDB — Schema Apply Methods

```rust
pub fn JsonDB::apply_store(&self, def: &StoreDef) -> Result<()>
pub fn JsonDB::apply_schemas(&self, stores: &[StoreDef]) -> Result<()>
pub fn JsonDB::apply_schema<T: ObjectStore>(&self) -> Result<()>
```

`apply_store` is idempotent — it diffs against the persisted schema and:
- Creates missing stores
- Creates missing indexes (with backfill for existing documents)
- Removes extra indexes
- Errors on conflicting index definitions
```
