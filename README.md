# FlowDB

A high-performance embedded storage engine written in Rust, powered by an LSM-tree architecture with WAL, SSTables, and Bloom filters. Now includes **JsonDB** — a built-in IndexedDB-compatible JSON document database layer with ACID transactions and secondary indexes.

**Fully synchronous API** — no async runtime required. FlowDB uses plain OS threads for background maintenance, making it runtime-agnostic. Use it from Tokio, async-std, smol, or plain synchronous code without any wrappers.

[![Crates.io](https://img.shields.io/crates/v/flowdb)](https://crates.io/crates/flowdb)
[![npm](https://img.shields.io/npm/v/@restsend/flowdb)](https://www.npmjs.com/package/@restsend/flowdb)
[![Docs](https://img.shields.io/badge/docs-api-blue)](docs/api.md)
[![Tutorials](https://img.shields.io/badge/learn-tutorials-green)](docs/tutorials/index.md)

---

## Quick Start

```toml
[dependencies]
flowdb = "0.6"
serde_json = "1"  # optional, for JsonDB
```

### LSM Engine

```rust
use flowdb::{Engine, Config, Record, Query};

let engine = Engine::open(Config::default())?;
engine.write_batch(&[Record::new("sensor:temp", 1_700_000_000_000, b"22.5".to_vec())])?;
for result in engine.query(Query::prefix("sensor:"))? {
    println!("{}", result.key_str());
}
engine.shutdown()?;
```

### JsonDB Document Store

```rust
use flowdb::jsondb::{JsonDB, StoreSchema};
use serde_json::json;

let db = JsonDB::open(Default::default())?;
db.apply_store(&StoreSchema::new("users", "id")
    .with_index("by_email", &["email"], true)
)?;
db.put("users", json!({"id": "u1", "email": "a@b.com"}))?;
let doc = db.get("users", &json!("u1"))?;
```

Or with the derive macro:

```rust
use flowdb::ObjectStore;

#[derive(ObjectStore)]
#[store(name = "users", key_path = "id")]
struct User {
    id: String,
    #[index(unique)]
    email: String,
}

db.apply_schema::<User>()?;
```

## Node.js (npm)

```bash
npm install @restsend/flowdb
```

```js
const { FlowDB, KeyRange } = require('@restsend/flowdb')

const db = FlowDB.open({ dataDir: './data' })
await db.createObjectStore('users', 'id')
await db.createIndex('users', 'byEmail', 'email', { unique: true })

// CRUD
await db.put('users', { id: 'u1', email: 'a@b.com' })
const doc = await db.get('users', 'u1')
await db.add('users', { id: 'u2', email: 'b@b.com' }) // insert-only

// Query with KeyRange + getAll
const kr = KeyRange.bound('u1', 'u5')
const results = await db.getAll('users', kr, 10)

// Cursor (callback style)
await db.openCursor('users', null, 'next', (item) => {
  if (item.done) return
  console.log(item.key, item.value)
})

// Cursor (async iterator)
for await (const item of db.cursor('users', null, 'next')) {
  console.log(item.key, item.value)
}

// Transaction with read-your-writes
const tx = db.transaction(['users'], 'readwrite')
tx.put('users', { id: 'u3', email: 'c@c.com' })
const read = await tx.get('users', 'u3') // sees pending write!
await tx.commit()
await db.close()
```

Zero Rust toolchain required. Full IndexedDB-compatible API with ACID transactions.

**Tutorial → [05-nodejs](docs/tutorials/en/05-nodejs.md)**

---

**Full API reference → [docs/api.md](docs/api.md)**  

**Tutorials → [docs/tutorials/](docs/tutorials/index.md)**

### English

| Tutorial | Description |
|----------|-------------|
| [01-basic-engine](docs/tutorials/en/01-basic-engine.md) | Engine CRUD: write, read, query, delete, flush, compact |
| [02-basic-jsondb](docs/tutorials/en/02-basic-jsondb.md) | JsonDB: stores, indexes, QueryBuilder, transactions, serde |
| [03-supabase-pattern](docs/tutorials/en/03-supabase-pattern.md) | Auth, sessions, RLS, compound-index queries (Supabase-like) |
| [04-supabase-server](docs/tutorials/en/04-supabase-server.md) | Axum web server: REST API + HTML/JS UI with auth and todo CRUD |

### 中文

| 教程 | 说明 |
|------|------|
| [01-basic-engine](docs/tutorials/zh/01-basic-engine.md) | Engine CRUD：写入、读取、查询、删除、刷盘、合并 |
| [02-basic-jsondb](docs/tutorials/zh/02-basic-jsondb.md) | JsonDB：对象存储、索引、QueryBuilder、事务、serde |
| [03-supabase-pattern](docs/tutorials/zh/03-supabase-pattern.md) | 嵌入式认证：用户、会话、行级安全、复合索引查询 |
| [04-supabase-server](docs/tutorials/zh/04-supabase-server.md) | Axum 服务器：REST API + HTML/JS 界面，含认证和待办 CRUD |

### Run the Examples

```bash
cargo run --example basic_engine
cargo run --example basic_jsondb
cargo run --example supabase_example
cargo run --example supabase-server   # then open http://localhost:3000
```

---

## Benchmark Results (v0.4.0)

### LSM Engine vs RocksDB (100K records, 128B values, batch=100, release, Apple M-series)

| Category | FlowDB | RocksDB | Result |
|---|---|---|---|
| Sequential Write | 4.5M ops/s | 3.1M ops/s | **FlowDB 1.42x faster** |
| Concurrent Write (8 threads) | 9.4M ops/s | 4.7M ops/s | **FlowDB 2.02x faster** |
| Point Query | 6.0M ops/s | 549K ops/s | **FlowDB 10.95x faster** |
| Prefix Scan (~200 recs) | 72K ops/s | 11K ops/s | **FlowDB 6.39x faster** |
| Full Scan (200K recs) | 65 ops/s | 40 ops/s | **FlowDB 1.63x faster** |
| Storage | 2.0MB | 1.8MB | ~same |

### JsonDB Document Layer (release, Apple M-series, SyncMode::Always)

| Category | Throughput |
|---|---|
| Sequential write (single doc) | ~121 docs/s |
| Batch write (100 docs/batch) | ~7,057 docs/s |
| Point read by primary key | ~244,741 ops/s |
| Index lookups (equality) | ~9,402 queries/s |
| Auto-increment | ~53 ops/s |

> Note: Write throughput is bottlenecked by WAL fsync (`SyncMode::Always`).
> For higher throughput, use batch writes (transaction) or `SyncMode::IntervalMs`.

---

## Features

### LSM Engine
- LSM-tree storage with WAL (write-ahead log) for crash recovery
- **Fully synchronous API** — no `async`, no Tokio dependency, runtime-agnostic
- **Background maintenance on OS threads** — flush, compaction, GC via `std::thread`
- Per-record WAL checksums for corruption detection on replay
- Config validation — invalid configs rejected at startup
- Frozen memtable backpressure — writes stall when flush can't keep up
- Lazy scan iterator (RocksDB-style `ScanIterator`) for bounded-memory range scans
- Bloom filters for fast point query negative checks
- lz4 compression for all SST blocks (flush + compaction)
- Buffered WAL writes (256KB buffer) for reduced syscall overhead
- WAL pre-encoding outside the write lock for better concurrency
- Time-bucketed block index with binary search
- **LRU block cache** (64 shards) with true LRU eviction
- Vec-based active memtable for O(1) writes
- **Size-tiered compaction** with streaming heap merge (low memory footprint)
- Range tombstones for efficient bulk key-range deletion
- Garbage collection (TTL expiry) and point deletes
- Graceful shutdown — flushes WAL + memtables before exit
- Engine stats — structured counters + Prometheus-format metrics

### JsonDB (IndexedDB-compatible layer)
- **ACID transactions** with atomic batch commit, MVCC snapshot isolation, and OCC conflict detection
- **Secondary indexes** with automatic maintenance on CRUD
- **Unique constraint** enforcement on indexed fields
- Auto-increment primary keys
- Read-your-writes consistency within transactions
- **Snapshot isolation** via MVCC sequence numbers — repeatable reads within a transaction
- **OCC conflict detection** — concurrent modifications to read keys are detected at commit time
- **Cross-crash batch atomicity** — WAL batch-commit markers ensure all-or-nothing recovery
- Schema persistence across restarts (automatic recovery)
- **Full IndexedDB-compatible API** — `add`, `clear`, `getAll`, `getAllKeys`, `getKey`, `count(query?)`, `openCursor`
- **KeyRange** factory — `KeyRange.only()`, `.bound()`, `.lowerBound()`, `.upperBound()`
- **Cursor** with 4 directions — `next`, `prev`, `nextunique`, `prevunique`
- **Async iterator** — `for await (const item of db.cursor(store, range, dir))`
- IndexedDB-like schema API — `create_object_store`, `create_index`, `put`, `get`, `delete`, `transaction`
- Index queries — point lookup (`get_by_index`) and range queries (`range_by_index`)
- Multi-field indexes via dotted key paths (e.g. `"address.city"`)
- Multi-entry indexes (`multiEntry: true`) for array-valued fields
- Auto-increment primary keys (`put_auto`)
- Primary key null-byte (`\x00`) validation — prevents composite-key separator injection

---

## Configuration

```rust
use flowdb::Config;

let config = Config {
    data_dir: "./data".into(),
    memtable_size_mb: 64,
    wal_sync_mode: flowdb::SyncMode::Always,
    ..Default::default()
};
```

| Parameter | Default | Description |
|---|---|---|
| `data_dir` | `"./data"` | Data directory path |
| `wal_sync_mode` | `Always` | WAL fsync: `Always` (safe) or `IntervalMs(n)` (fast) |
| `memtable_size_mb` | `64` | Active memtable flush threshold |
| `block_cache_capacity_mb` | `128` | Block cache capacity (MB) |
| `block_size` | `8192` | SST block size (bytes) |
| `bloom_bits_per_key` | `10` | Bloom filter bits per key |
| `compaction_threshold` | `2` | SST file count to trigger compaction |
| `compaction_interval_ms` | `60000` | Background compaction interval (ms), independent of `gc_interval_secs` |
| `flush_interval_ms` | `1000` | Background flush interval (ms) |
| `gc_interval_secs` | `3600` | Garbage collection interval (s) |
| `time_bucket_secs` | `3600` | Time bucket width for block index (max ~9.22e12) |
| `default_ttl_secs` | `None` | Default TTL for all records (max ~9.22e12, set per-batch via `write_batch_ttl`) |
| `max_frozen_memtables` | `2` | Max frozen memtables before write backpressure |
| `index_memory_budget_mb` | `256` | Block meta index memory budget |
| `wal_segment_size_mb` | `64` | WAL segment file size |
| `create_if_missing` | `true` | Auto-create data directory |
| `auto_background` | `true` | Auto-start background maintenance thread |

**Full parameter docs → [docs/api.md](docs/api.md#config)**

---

## Architecture

```
LSM Engine:
  Write Path:  Client → encode → WriteWorker → WAL (fsync) + MemTable → (flush) SST
  Read Path:   Client → MemTable → Block Index → Bloom → SST (LRU cached)

JsonDB Layer (built-in):
  Document:   D\x00{store}\x00{primary_key} → serialized JSON
  Index:      I\x00{store}\x00{index}\x00{encoded_value}\x00{primary_key} → primary_key
  Schema:     S\x00{store} → serialized StoreDef
```

## License

MIT.
