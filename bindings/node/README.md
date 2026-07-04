# @restsend/flowdb

> IndexedDB-compatible embedded JSON document store for Node.js — powered by FlowDB (Rust)

[![npm version](https://img.shields.io/npm/v/@restsend/flowdb.svg)](https://www.npmjs.com/package/@restsend/flowdb)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

FlowDB is a high-performance embedded storage engine written in Rust. This
package provides native Node.js bindings via [napi-rs], exposing an
IndexedDB-compatible JSON document API with transactions, indexes, and
range queries.

## Features

- **IndexedDB-compatible API** — `createObjectStore`, `createIndex`,
  `put`, `get`, `delete`, `scan`, transactions
- **Secondary indexes** — point lookups + range scans on any JSON field
- **ACID transactions** — multi-store, `readonly` / `readwrite` modes
- **Auto-key generation** — `putAuto` assigns sequential `_id` values
- **TTL support** — per-database default TTL for automatic expiry
- **LSM-tree storage** — WAL + memtable + SSTables with size-tiered
  compaction
- **No native runtime dependency** — pure Rust engine, no Tokio, no
  async-std

## Installation

```bash
npm install @restsend/flowdb
```

Pre-built binaries are provided for:

| Platform            | Architecture |
|---------------------|-------------|
| macOS (darwin)      | arm64, x64  |
| Linux (gnu)         | arm64, x64  |
| Windows (msvc)      | x64         |

## Quick Start

```js
const { FlowDB } = require('@restsend/flowdb')

// Open a database
const db = FlowDB.open({
  dataDir: './my-data',
  createIfMissing: true,
})

// Create an object store (like an IndexedDB object store)
await db.createObjectStore('users', 'email')

// Insert documents
await db.put('users', { email: 'alice@example.com', name: 'Alice', age: 30 })
await db.put('users', { email: 'bob@example.com', name: 'Bob', age: 25 })

// Point lookup by key
const alice = await db.get('users', 'alice@example.com')
console.log(alice) // { email: 'alice@example.com', name: 'Alice', age: 30 }

// Scan all documents in a store
const all = await db.scan('users')
console.log(all.length) // 2

// Create a secondary index
await db.createIndex('users', 'by_age', 'age')

// Query by index
const young = await db.getByIndex('users', 'by_age', 25)
console.log(young) // [{ email: 'bob@example.com', name: 'Bob', age: 25 }]

// Range query by index
const range = await db.rangeByIndex('users', 'by_age', 20, 30)

// Delete
await db.delete('users', 'bob@example.com')

// Close
await db.close()
```

## Transactions

```js
const tx = db.transaction(['users', 'orders'], 'readwrite')

tx.put('users', { email: 'carol@example.com', name: 'Carol' })
tx.put('orders', { id: 42, user: 'carol@example.com', total: 99.9 })

await tx.commit()   // atomically persist both writes
// or
tx.abort()          // discard all pending writes
```

## Auto-Key Generation

```js
await db.createObjectStore('events', '_id')

const id1 = await db.putAuto('events', { type: 'click', ts: Date.now() })
const id2 = await db.putAuto('events', { type: 'view', ts: Date.now() })

console.log(id1, id2) // 1, 2 (sequential)
```

## Configuration

```ts
interface OpenConfig {
  dataDir: string              // Required — data directory path
  createIfMissing?: boolean    // Default: true
  defaultTtlSecs?: number      // Default TTL in seconds (undefined = forever)
  memtableSizeMb?: number      // Default: 64
  blockCacheCapacityMb?: number // Default: 128
  bloomBitsPerKey?: number     // Default: 10
  compactionIntervalMs?: number // Default: 60000 (60s) — compaction cadence
}
```

### Tuning compaction

The `compactionIntervalMs` option controls how often the background thread
merges small SST files. The default of 60 000 ms (60 s) keeps the SST file
count low. Increase it to reduce I/O, or decrease it for more aggressive
merging.

## API Reference

### `FlowDB.open(config): FlowDB`

Open or create a database. Returns a `FlowDB` instance.

### `FlowDB` instance methods

| Method | Returns | Description |
|--------|---------|-------------|
| `put(store, value)` | `Promise<void>` | Insert/update a document |
| `get(store, key)` | `Promise<unknown>` | Point lookup by store key |
| `delete(store, key)` | `Promise<void>` | Delete by key |
| `putAuto(store, value)` | `Promise<number>` | Insert with auto-generated `_id` |
| `scan(store)` | `Promise<unknown[]>` | List all documents in store |
| `count(store)` | `Promise<number>` | Document count in store |
| `storeNames()` | `string[]` | List all store names |
| `createObjectStore(name, keyPath)` | `Promise<void>` | Create a store |
| `deleteObjectStore(name)` | `Promise<void>` | Delete a store |
| `createIndex(store, name, keyPath, unique?)` | `Promise<void>` | Create secondary index |
| `deleteIndex(store, name)` | `Promise<void>` | Delete secondary index |
| `getByIndex(store, index, value)` | `Promise<unknown[]>` | Point lookup via index |
| `rangeByIndex(store, index, start, end)` | `Promise<unknown[]>` | Range scan via index |
| `transaction(stores, mode)` | `Transaction` | Start a transaction |
| `close()` | `Promise<void>` | Close the database |

### `Transaction` methods

| Method | Returns | Description |
|--------|---------|-------------|
| `put(store, value)` | `void` | Queue an insert/update |
| `delete(store, key)` | `void` | Queue a delete |
| `commit()` | `Promise<void>` | Atomically commit all queued ops |
| `abort()` | `void` | Discard all queued ops |

## License

MIT © [restsend](https://github.com/restsend)

[napi-rs]: https://napi.rs
