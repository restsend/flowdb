# AGENTS.md – FlowDB

## Build & Test Commands

- Build (debug): `cargo build`
- Build all targets: `cargo build --tests`
- Run all tests: `cargo test`
- Run only library tests: `cargo test --lib`
- Run the fuzz suite: `cargo test --test fuzz_tests`
- Run benches: `cargo bench`

## Lock Ordering

To avoid deadlocks, all background tasks must acquire locks in this order:

```
cache shard locks → manifest → index → worker (WritePipeline)
```

- **Flush path**: manifest → index (releases between)
- **GC path**: index → manifest (separate scopes)
- **Compaction path**: manifest → index (separate scopes)
- **Never** hold two locks from this set simultaneously unless the order above
  is strictly followed.

## Architecture (v0.3.0+)

FlowDB is a **pure embedded storage engine** with a **fully synchronous API**.
There is no async runtime dependency — no Tokio, no async-std, no smol.
Background maintenance (flush, compaction, GC, WAL sync) runs on a plain
`std::thread::spawn` thread managed by [`MaintenanceHandle`].

All public `Engine` methods are plain `fn` — no `async`, no `.await`.

## Coverage

This project enforces **≥ 90% line/region/function coverage** across the entire crate
(including the CLI binary).

Run coverage locally:

```bash
cargo llvm-cov                       # text summary
cargo llvm-cov --html --open         # interactive HTML report
cargo llvm-cov --summary-only        # just the totals
```

How we hit 90%+ across the whole crate:

1. The `fn main()` entry point in `src/bin/flowdb-stress.rs` is tagged with
   `#[cfg_attr(coverage_nightly, coverage(off))]` because it is pure process
   bootstrap and is exercised via integration tests, not unit tests. The file
   enables the unstable `coverage_attribute` feature only under
   `cfg(coverage_nightly)`, which `cargo-llvm-cov` sets automatically on
   nightly toolchains.

2. All logic in the binary (CLI parsing, all `bench_*` / `format_*` /
   `print_*` helpers in the stress tool) is factored into free functions with
   unit tests in the same file's `mod tests`.

3. `Cargo.toml` declares the `lints.rust.unexpected_cfgs` table so the
   `cfg(coverage)` / `cfg(coverage_nightly)` cfgs don't trigger warnings.

4. Engine background maintenance (`spawn_background_maintenance`,
   `auto_background = true`) is covered by
   `engine::tests::test_engine_auto_background_starts_maintenance` and
   `engine::tests::test_spawn_background_maintenance_explicit`.

If you add new code, please add corresponding tests and run
`cargo llvm-cov --summary-only` before committing — the TOTAL line coverage
must stay at or above 95%.

## Project Layout

```
src/
  lib.rs              – public API surface (Config, Engine, Record, Query, ...)
  engine.rs           – Engine + ScanIterator + MaintenanceHandle (the core)
  memtable.rs         – in-memory write buffer (Vec-based active, BTreeMap frozen)
  wal.rs              – write-ahead log (FxHash checksums, buffered writes)
  sstable.rs          – on-disk sorted-string table reader/writer
  block_meta_index.rs – fine-grained block-level index
  bloom.rs            – bloom filter for SST point queries
  cache.rs            – block cache (LRU, 64 shards)
  compaction.rs       – size-tiered compaction (streaming heap merge)
  gc.rs               – expired-SST garbage collection
  manifest.rs         – append-only manifest log (JSON)
  record.rs           – Record / InternalRecord / Query / Config / ScanRange types
  write_worker.rs     – single-writer worker driving WAL + memtable
  stats.rs            – engine stats + Prometheus exporter
  error.rs            – FlowError / Result
  jsondb/             – IndexedDB-compatible JSON document layer
    mod.rs            – JsonDB, Transaction, full API
    encoding.rs       – key/value encoding, JSON field extraction
    schema.rs         – StoreDef / IndexDef / schema persistence
  bin/
    flowdb-stress.rs  – `flowdb-stress` benchmarking binary
tests/
  fuzz_tests.rs       – property/fuzz tests with arbitrary (6 suites)
benches/
  flowdb_bench.rs     – criterion micro-benchmarks
examples/
  flowdb-vs-rocksdb.rs – comparative benchmark vs RocksDB

bindings/node/         – napi-rs native Node.js addon
  src/lib.rs           – #[napi] Rust bindings (FlowDb, JsTransaction)
  Cargo.toml           – napi crate (workspace member)
  build.rs             – napi-build setup
  index.js             – Platform-specific .node loader
  package.json         – npm package config
  ts/                  – TypeScript wrapper source
    index.ts, jsondb.ts, transaction.ts, types.ts
```

## napi-rs Node.js Bindings

`bindings/node/` is a Cargo workspace member that builds a native Node.js addon.

### Build (release)

```bash
cd bindings/node && napi build --platform --release
```

### Test

```bash
cd bindings/node && node -e "const m=require('.'); m.FlowDb.open({dataDir:'/tmp/t',createIfMissing:true}).close()"
```

### Cross-platform publishing (local, no GitHub CI)

All 5 targets are built locally — Mac mini handles the two darwin targets,
a Linux x86_64 machine handles linux-gnu ×2 and windows-msvc via SSH.

```bash
# 1. Mac mini: build darwin-arm64 + darwin-x64
cd bindings/node && ./scripts/build-all.sh

# 2. Linux SSH: build linux-x64 + linux-arm64 + windows-msvc
LINUX_SSH=user@linux-host ./scripts/remote-build.sh

# 3. Generate platform subpackages
node scripts/publish-platforms.js

# 4. Publish main package + 5 platform subpackages
npm publish && for p in npm/*/; do (cd "$p" && npm publish); done
```

The TypeScript wrapper in `ts/` is compiled with `tsc` to `dist/`. The `index.js` loader handles platform detection.

## Test Conventions

- Inline `#[cfg(test)] mod tests` per module for fine-grained unit tests.
- All tests are plain `#[test] fn` — no `#[tokio::test]`, no `async`.
- Concurrency tests use `std::thread::spawn` (not `tokio::spawn`).
- Tests use `tempfile::TempDir` for filesystem isolation. The lone exception
  is `src/bin/flowdb-stress.rs`'s `make_temp_dir()` helper, which uses an
  atomic counter to give each parallel test its own directory.
- Tests must clean up after themselves (`engine.shutdown().unwrap()`
  for engines owned by value; `engine.flush()` for engines behind an
  `Arc` that cannot be moved out).
