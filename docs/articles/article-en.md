# FlowDB JsonDB: Designing a High-Performance Embedded JSON Document Database in Rust

> **Keywords**: Embedded Database, JSON Document, Rust, LSM-Tree, IndexedDB, ACID Transactions, Secondary Indexes

## 1. Introduction

The Rust ecosystem has long lacked a good embedded document database. SQLite is powerful but requires C bindings and complex build configuration; sled provides KV storage but no document model; IndexedDB is a browser standard but unavailable in native applications.

FlowDB JsonDB solves this problem—a **native Rust implementation of an IndexedDB-compatible JSON document database** with ACID transactions, secondary indexes, zero IPC overhead, and zero async runtime dependencies.

## 2. Architecture Overview

![FlowDB Architecture](architecture.svg)

### 2.1 Layered Design

FlowDB's core is an **LSM-Tree engine** (Log-Structured Merge-Tree), with the **JsonDB layer** built on top. The entire system follows a clean layered architecture:

**LSM Engine Layer** (storage):
- Write path: Client → encode_batch → WriteWorker (Mutex) → WAL (fsync) + MemTable → (flush) SST
- Read path: Client → MemTable → Block Index → Bloom Filter → SST (LRU cached)
- Background: flush, compaction, GC, WAL sync

**JsonDB Document Layer** (business logic):
- Document storage: `D\x00{store}\x00{primary_key}` → JSON bytes
- Secondary indexes: `I\x00{store}\x00{index}\x00{encoded_value}\x00{pk}` → pk bytes
- Schema metadata: `S\x00{store}` → serialized StoreDef

### 2.2 Why LSM-Tree?

| Feature | LSM-Tree | B-Tree |
|---------|----------|--------|
| Write throughput | **High** (sequential append) | Medium (random writes) |
| Point read | Medium (check multiple layers) | **High** (O(log n)) |
| Range scan | **High** (sequential read) | High |
| Compression ratio | **High** (Zstd on SST) | Low (page fragmentation) |
| Write amplification | High (needs compaction) | Low |

For document database workloads (write-heavy, range queries), LSM-Tree is the natural choice.

## 3. JsonDB Document Model

### 3.1 Composite Key Encoding

![Key Encoding Diagram](key-encoding.svg)

**All JsonDB data shares a single FlowDB keyspace**, distinguished by a 1-byte prefix:

| Prefix | Type | Format |
|--------|------|--------|
| `0x01` | Document | `D\x00{store}\x00{pk}` |
| `0x02` | Index entry | `I\x00{store}\x00{index}\x00{val}\x00{pk}` |
| `0x03` | Schema | `S\x00{store}` |
| `0x04` | Counter | `C\x00{store}` |

The elegance of this design:
- **Prefix scans** naturally separate data by type
- **Lexicographic ordering** makes range queries efficient at the byte level
- **Composite indexes** naturally support multi-field prefix queries

### 3.2 Sortable Value Encoding

Index value encoding is the key to JsonDB's query performance. We use a **type-tag prefix** scheme:

```
null    →  [0x01]
false   →  [0x02]
true    →  [0x03]
i64     →  [0x04] + 8-byte big-endian with sign bit flipped
u64     →  [0x05] + 8-byte big-endian
f64     →  [0x06] + adjusted IEEE 754
string  →  [0x07] + UTF-8 bytes
```

This encoding guarantees **correct cross-type ordering** (null < bool < number < string) and **preserves ordering within each type**. For example:

```
index key: I\x00users\x00by_age\x00[4]0x80{8 bytes for 0}\x00pk1
index key: I\x00users\x00by_age\x00[4]0x7F{8 bytes for -1}\x00pk2
```
→ Byte comparison: `0x80 > 0x7F` → age `0 > -1` ✓

## 4. ACID Transaction Implementation

![Transaction Flow Diagram](transaction.svg)

### 4.1 OCC + MVCC Isolation Model

JsonDB transactions use **Optimistic Concurrency Control (OCC)** with **MVCC snapshot isolation**:

```
BEGIN:
  Record snapshot_seq = engine.last_seq()  // committed_seq atomic

READ:
  Write buffer first → engine fallback (get_bytes_seq)
  Engine filters out records with seq > snapshot_seq
  Track (key, value) in OCC read-set

WRITE:
  Local buffer only (HashMap), NOT sent to engine

COMMIT:
  1. Acquire write_lock (serializes vs direct put/delete)
  2. OCC validation: re-read each read-set key, abort if changed
  3. Unique constraint validation (engine + write buffer)
  4. Build batch: delete old indexes + write doc + write new indexes + counter
  5. engine.write_internal() — atomic write to WAL + MemTable
  6. Mark committed = true (only after success)
```

### 4.2 Isolation Comparison with IndexedDB

| Feature | IndexedDB | FlowDB JsonDB |
|---------|-----------|---------------|
| Transaction modes | ReadOnly / ReadWrite | ReadOnly / ReadWrite |
| Isolation level | Snapshot Isolation | Snapshot Isolation (OCC) |
| Conflict detection | Pessimistic (per-store lock) | Optimistic (OCC + write_lock) |
| Atomicity | per-request | per-batch (write_internal + WAL commit marker) |
| Auto-rollback | Timeout / abort | Drop Transaction (discard buffer) |

### 4.3 Crash Safety

```rust
// Every write goes through WAL
engine.write_internal(&[
    // Counter increment + document write + index entries — all in one batch
    counter_record,
    doc_record,
    index_entry_1,
    index_entry_2,
])?;
// Either ALL persisted or NONE persisted
// SyncMode::Always: fsync after every batch
// WAL batch-commit marker ensures cross-crash atomicity
```

WAL uses **FxHash checksums** per record and **batch-commit markers** for cross-crash atomicity. On crash recovery:
1. Read all WAL segments
2. Decode records, tracking pending batches
3. When a BatchCommit marker is found, promote pending records to committed
4. If no markers exist (old format), all records are committed (backward-compatible)
5. Records after the last marker (uncommitted) are discarded
6. Filter out records with `seq <= last_flushed_seq`
7. Re-insert to MemTable

## 5. Secondary Indexes & Composite Queries

### 5.1 Index Maintenance

Every document write automatically maintains related indexes:

```
PUT {id: "u1", email: "new@b.com", city: "NYC", age: 30}:
  1. Read old doc → get old email="old@b.com", old city/age
  2. Delete old index entries:
     DELETE idx_key(by_email, "old@b.com", "u1")
     DELETE idx_key(by_city_age, "NYC", 30, "u1")
  3. Write new document
  4. Write new index entries:
     PUT idx_key(by_email, "new@b.com", "u1")
     PUT idx_key(by_city_age, "NYC", 30, "u1")
  → All in a single atomic batch
```

### 5.2 QueryBuilder

For multi-field queries, QueryBuilder automatically selects the best index:

```rust
// Automatically picks the by_city_age composite index
let docs = db.query("users")
    .where_eq("city", json!("NYC"))
    .where_range("age", json!(25), json!(35))
    .order_by("age", SortDir::Asc)
    .limit(10)
    .collect()?;

// Execution plan:
// 1. by_city_age matches first 2 filter fields → highest score
// 2. Build scan range: I\x00users\x00by_city_age\x00[7]NYC\x00[4]25 → [7]NYC\x00[4]35
// 3. Scan index → point-get docs → predicate filter → early-termination at limit
// 4. order_by matches index first field → skip in-memory sort
```

## 6. Performance Benchmarks

![Benchmark Diagram](benchmark.svg)

### LSM Engine vs RocksDB

| Category | FlowDB | RocksDB | Advantage |
|----------|--------|---------|-----------|
| Sequential Write | 4.5M ops/s | 3.1M ops/s | **1.42x** |
| Concurrent Write (8 thr) | 9.4M ops/s | 4.7M ops/s | **2.02x** |
| Point Query | 6.0M ops/s | 549K ops/s | **10.95x** |
| Prefix Scan (~200 recs) | 72K ops/s | 11K ops/s | **6.39x** |

### JsonDB Document Layer

| Operation | Throughput |
|-----------|------------|
| Sequential write (single) | ~121 docs/s |
| Batch write (100/batch) | ~7,057 docs/s |
| Point read | ~156,075 ops/s |
| Index lookup | ~7,463 queries/s |
| Auto-increment | ~53 ops/s |

> Write throughput is bottlenecked by WAL fsync (SyncMode::Always). Use batch commits or SyncMode::IntervalMs for higher throughput.

## 7. Comparison with Alternatives

| Feature | FlowDB JsonDB | SQLite (rusqlite) | sled | serde_json + KV store |
|---------|--------------|-------------------|------|----------------------|
| **Language** | Pure Rust | C + FFI | Rust | Rust |
| **Dependencies** | Zero C | libsqlite3 | Zero C | Zero C |
| **Transactions** | ✅ OCC + MVCC | ✅ 2PC | ✅ MVCC | ❌ |
| **Secondary indexes** | ✅ Auto-maintained | ✅ CREATE INDEX | ❌ | ❌ |
| **Composite indexes** | ✅ | ✅ | ❌ | ❌ |
| **QueryBuilder** | ✅ | Need raw SQL | ❌ | ❌ |
| **Generic struct API** | ✅ put_doc/get_doc | ❌ | ✅ | ❌ |
| **Crash recovery** | ✅ WAL + checksum | ✅ WAL + journal | ✅ | ❌ |
| **Build time** | ~3s | ~30s (bindgen) | ~5s | ~2s |
| **Package size** | ~680KB | ~1.5MB | ~500KB | ~200KB |
| **Async** | Zero | Zero | Zero | N/A |

## 8. Use Cases & Limitations

### Best Fit

- **Desktop applications** (Rust-side IndexedDB replacement for Tauri/Electron)
- **Mobile apps** (iOS/Android via FFI, replace SQLite)
- **IoT devices** (Raspberry Pi, edge gateways — zero C dependencies)
- **Game save data** (JSON documents naturally match game data models)
- **Configuration systems** (JSON config database with indexed queries)

### Not Suitable

- Big data analytics (>10M docs, needs columnar storage)
- Complex JOIN queries (no SQL engine)
- High write throughput (>10K writes/s, fsync bottleneck)
- Cross-process / network access (embedded-only design)

## 9. Conclusion

FlowDB JsonDB is **the first IndexedDB-compatible high-performance embedded JSON document database in the Rust ecosystem**. It achieves high write throughput and predictable read latency through its LSM-Tree engine, implements efficient secondary indexes and composite queries through carefully designed key encoding, and provides ACID transaction guarantees through OCC + MVCC.

**Not just another Rust database—a complete document data management solution designed for modern applications.**

---

*GitHub: [github.com/restsend/flowdb](https://github.com/restsend/flowdb)*  
*crates.io: [crates.io/crates/flowdb](https://crates.io/crates/flowdb)*
