# @restsend/flowdb

> IndexedDB-compatible embedded JSON document store for Node.js — powered by FlowDB (Rust)

[![npm version](https://img.shields.io/npm/v/@restsend/flowdb.svg)](https://www.npmjs.com/package/@restsend/flowdb)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

FlowDB is a high-performance embedded storage engine written in Rust. This
package provides native Node.js bindings via [napi-rs], exposing a **full
IndexedDB-compatible** JSON document API with ACID transactions, secondary
indexes, cursors, and key-range queries.

## Features

- **Full IndexedDB-compatible API** — `add`, `clear`, `getAll`, `getAllKeys`,
  `getKey`, `count(query?)`, `openCursor`
- **Cursors** — 4 directions: `next`, `prev`, `nextunique`, `prevunique`
- **Async iterator** — `for await (const item of db.cursor(...))`
- **KeyRange** — `KeyRange.only()`, `.bound()`, `.lowerBound()`, `.upperBound()`
- **Secondary indexes** — point lookups + range scans, composite + multi-entry
- **ACID transactions** — multi-store, read-your-writes, `readonly` / `readwrite`
- **Auto-key generation** — `putAuto` with `createObjectStore(name, keyPath, true)`
- **Get metadata** — `getWithMeta` / `scanWithMeta` returns `{value, _tsMs, _expireAtMs}`
- **TTL support** — per-database default TTL for automatic expiry
- **LSM-tree storage** — WAL + memtable + SSTables with size-tiered compaction
- **No native dependency** — pure Rust engine, no Tokio, no async-std

## Installation

```bash
npm install @restsend/flowdb
```

Pre-built binaries for:

| Platform            | Architecture |
|---------------------|-------------|
| macOS (darwin)      | arm64, x64  |
| Linux (gnu)         | arm64, x64  |
| Windows (msvc)      | x64         |

## Quick Start

```js
const { FlowDB, KeyRange } = require('@restsend/flowdb')

// Open a database
const db = FlowDB.open({ dataDir: './my-data', createIfMissing: true })

// Create stores
await db.createObjectStore('users', 'email')
await db.createObjectStore('events', '_id', true) // auto-increment

// CRUD
await db.put('users', { email: 'alice@example.com', name: 'Alice', age: 30 })
const alice = await db.get('users', 'alice@example.com')

// Insert-only (fails if key exists)
await db.add('users', { email: 'bob@example.com', name: 'Bob' }).catch(e => {})

// Query with KeyRange + limit
const kr = KeyRange.bound(20, 30)
const results = await db.getAll('users', kr, 100)
const keys = await db.getAllKeys('users', kr, 100)
const total = await db.count('users', kr)

// Secondary index
await db.createIndex('users', 'by_age', 'age')
const young = await db.getByIndex('users', 'by_age', 25)
const range = await db.rangeByIndex('users', 'by_age', 20, 30)

// Multi-entry index (array-valued fields)
await db.createIndex('items', 'by_tag', 'tags', { multiEntry: true })
await db.put('items', { id: 'i1', tags: ['red', 'blue'] })
const blueItems = await db.getByIndex('items', 'by_tag', 'blue')

// Cursor (callback style)
await db.openCursor('users', KeyRange.bound('a', 'z'), 'next', (item) => {
  if (item.done) return
  console.log(item.key, item.value)
})

// Cursor (async iterator)
for await (const item of db.cursor('users', null, 'prev')) {
  console.log(item.key, item.value)
}

// Index cursor
for await (const item of db.cursorByIndex('users', 'by_age', null, 'next')) {
  console.log(item.key, item.primaryKey, item.value)
}

// Clear store
await db.clear('events')

// Close
await db.close()
```

## Transactions

```js
const tx = db.transaction(['users', 'orders'], 'readwrite')

tx.put('users', { email: 'carol@example.com', name: 'Carol' })
tx.put('orders', { id: 42, user: 'carol@example.com', total: 99.9 })

// Read-your-writes within transaction
const carol = await tx.get('users', 'carol@example.com')
const count = await tx.count('users')
const all = await tx.scan('users')

await tx.commit()   // atomically persist
// or
tx.abort()          // discard
```

## KeyRange

```js
const { KeyRange } = require('@restsend/flowdb')

KeyRange.only('alice@x.com')                    // match exactly one key
KeyRange.bound('a', 'z')                        // [a, z] closed
KeyRange.bound('a', 'z', true, false)           // (a, z] half-open
KeyRange.lowerBound(18)                          // [18, +inf)
KeyRange.lowerBound(18, true)                    // (18, +inf)
KeyRange.upperBound(65)                          // (-inf, 65]
KeyRange.upperBound(65, true)                    // (-inf, 65)
```

## Configuration

```ts
interface OpenConfig {
  dataDir: string              // Required
  createIfMissing?: boolean    // Default: true
  defaultTtlSecs?: number      // Default TTL in seconds (undefined = forever)
  memtableSizeMb?: number      // Default: 64
  blockCacheCapacityMb?: number // Default: 128
  bloomBitsPerKey?: number     // Default: 10
  compactionIntervalMs?: number // Default: 60000 (60s)
}
```

## API Reference

### `FlowDB`

| Method | Returns | Description |
|--------|---------|-------------|
| `put(store, value)` | `Promise<void>` | Insert/update |
| `add(store, value)` | `Promise<unknown>` | Insert-only, fails if key exists |
| `get(store, key)` | `Promise<unknown>` | Point lookup |
| `getWithMeta(store, key)` | `Promise<unknown>` | Returns doc with `_tsMs`, `_expireAtMs` |
| `getKey(store, key)` | `Promise<unknown>` | Key existence check |
| `delete(store, key)` | `Promise<void>` | Delete by key |
| `putAuto(store, value)` | `Promise<number>` | Auto-increment insert |
| `scan(store)` | `Promise<unknown[]>` | All documents |
| `scanWithMeta(store)` | `Promise<unknown[]>` | All docs with metadata |
| `getAll(store, query?, count?)` | `Promise<unknown[]>` | Filtered by KeyRange + limit |
| `getAllKeys(store, query?, count?)` | `Promise<unknown[]>` | Keys only |
| `clear(store)` | `Promise<void>` | Remove all documents |
| `count(store, query?)` | `Promise<number>` | Count with optional KeyRange |
| `storeNames()` | `string[]` | List store names |
| `createObjectStore(name, keyPath, autoIncrement?)` | `Promise<void>` | Create store |
| `deleteObjectStore(name)` | `Promise<void>` | Delete store |
| `createIndex(store, name, keyPath, options?)` | `Promise<void>` | Create index (options: `{unique?, multiEntry?}`) |
| `deleteIndex(store, name)` | `Promise<void>` | Delete index |
| `getByIndex(store, index, value)` | `Promise<unknown[]>` | Index point lookup |
| `rangeByIndex(store, index, start, end)` | `Promise<unknown[]>` | Index range scan |
| `openCursor(store, query, direction, callback)` | `Promise<void>` | Callback-style cursor (directions: `next`, `prev`, `nextunique`, `prevunique`) |
| `openCursorByIndex(store, index, query, direction, callback)` | `Promise<void>` | Index cursor (callback) |
| `close()` | `Promise<void>` | Close database |

### Cursor (async iterator)

| Method | Returns | Description |
|--------|---------|-------------|
| `cursor(store, query?, direction?)` | `AsyncIterable<CursorItem>` | `for await (const item of db.cursor(...))` |
| `cursorByIndex(store, index, query?, direction?)` | `AsyncIterable<IndexCursorItem>` | Index async iterator |

### `Transaction`

| Method | Returns | Description |
|--------|---------|-------------|
| `put(store, value)` | `void` | Queue insert/update |
| `putAuto(store, value)` | `void` | Queue auto-increment |
| `delete(store, key)` | `void` | Queue delete |
| `get(store, key)` | `Promise<unknown>` | Read (sees pending writes) |
| `count(store)` | `Promise<number>` | Count (sees pending writes) |
| `scan(store)` | `Promise<unknown[]>` | All docs (sees pending writes) |
| `getByIndex(store, index, value)` | `Promise<unknown[]>` | Index lookup (sees pending writes) |
| `rangeByIndex(store, index, start, end)` | `Promise<unknown[]>` | Index range (sees pending writes) |
| `commit()` | `Promise<void>` | Atomically apply all queued ops |
| `abort()` | `void` | Discard all queued ops |

### Types

| Export | Description |
|--------|-------------|
| `KeyRange` | Factory: `.only()`, `.bound()`, `.lowerBound()`, `.upperBound()` |
| `KeyRange` (interface) | `{ lower?, upper?, lowerOpen?, upperOpen? }` |
| `CursorDirection` | `'next' \| 'prev' \| 'nextunique' \| 'prevunique'` |
| `CursorItem` | `{ key, value, done }` |
| `IndexCursorItem` | `{ key, primaryKey, value, done }` |

## License

MIT © [restsend](https://github.com/restsend)

[napi-rs]: https://napi.rs
