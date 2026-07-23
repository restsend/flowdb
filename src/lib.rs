//! FlowDB — an embedded LSM-tree storage engine with a built-in JSON document database.
//!
//! # Overview
//!
//! FlowDB is a high-performance embedded storage engine written in Rust, powered by an
//! LSM-tree architecture with WAL, SSTables, and Bloom filters. It includes **JsonDB**,
//! an IndexedDB-compatible JSON document layer with ACID transactions and secondary indexes.
//!
//! **Fully synchronous API** — no async runtime required. FlowDB uses plain OS threads
//! for background maintenance, making it runtime-agnostic.
//!
//! # LSM Engine
//!
//! The core engine (`Engine`) provides a key-value store where each record has a binary key,
//! microsecond timestamp, expiry time, and binary value. It supports point lookups, prefix
//! scans, range queries, time-range queries, lazy iterators, and batched writes.
//!
//! ```no_run
//! use flowdb::{Engine, Config, Record, Query};
//!
//! let engine = Engine::open(Config::default()).unwrap();
//! engine.write_batch(&[Record::new("key", 1_700_000_000_000, b"value".to_vec())]).unwrap();
//! for result in engine.query(Query::prefix("key")).unwrap() {
//!     println!("{}", result.key_str());
//! }
//! engine.shutdown().unwrap();
//! ```
//!
//! # JsonDB Document Layer
//!
//! JsonDB is a document database built on top of the LSM engine, providing an IndexedDB-like
//! API with object stores, secondary indexes, ACID transactions, and serde integration.
//!
//! ```no_run
//! use flowdb::jsondb::{JsonDB, StoreSchema};
//! use serde_json::json;
//!
//! let db = JsonDB::open(Default::default()).unwrap();
//! db.apply_store(&StoreSchema::new("users", "id")
//!     .with_index("by_email", &["email"], true)
//! ).unwrap();
//! db.put("users", json!({"id": "u1", "email": "a@b.com"})).unwrap();
//! let doc = db.get("users", &json!("u1")).unwrap();
//! ```
//!
//! # When to use FlowDB
//!
//! - **Embedded databases** — no server process, no external dependencies
//! - **Edge Functions / Serverless** — embed directly in Wasm or VM functions
//! - **IoT / Time-series** — efficient time-bucketed index and TTL expiry
//! - **Offline-first apps** — local-first with eventual sync to remote
//! - **Testing** — fully isolated backend without Docker or network I/O

pub mod cache;
pub mod engine;
pub mod error;
pub mod jsondb;
pub mod record;
pub mod stats;

mod block_meta_index;
mod bloom;
mod compaction;
mod gc;
mod manifest;
mod memtable;
mod sstable;
mod storage;
mod wal;
mod write_worker;

pub use engine::{Engine, MaintenanceHandle, ScanIterator};
pub use error::{FlowError, Result};
pub use flowdb_derive::ObjectStore;
pub use record::{
    CompressionAlgorithm, Config, KeyFilter, Op, Query, ReadOptions, Record, ScanRange,
    StorageBackendKind, SyncMode,
};
pub use stats::EngineStats;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default_values() {
        let config = Config::default();
        assert_eq!(config.data_dir, std::path::PathBuf::from("./data"));
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
        assert!(config.create_if_missing);
    }
}
