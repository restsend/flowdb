# FlowDB Node.js Tutorial — IndexedDB-compatible Database for JavaScript

[« Back to Tutorials](../index.md)

---

### Objective

Use FlowDB from Node.js with `npm install @restsend/flowdb` — zero Rust toolchain required.
FlowDB provides an IndexedDB-like document database with ACID transactions,
secondary indexes, and unique constraints.

### Prerequisites

```bash
npm install @restsend/flowdb
```

### Step-by-Step

#### 1. Open a Database

```js
const { FlowDB } = require('@restsend/flowdb')

const db = FlowDB.open({ dataDir: './mydata' })
```

This creates (or reopens) a database at `./mydata`. All data persists across restarts.

#### 2. Create Object Stores and Indexes

Object stores are like tables. Each has a `keyPath` — the field used as the
primary key.

```js
await db.createObjectStore('users', 'id')
await db.createObjectStore('posts', 'id')

// Secondary index on a single field
await db.createIndex('users', 'byEmail', 'email', true)   // unique=true

// Composite index on multiple fields
await db.createIndex('users', 'byNameAge', ['name', 'age'])
```

#### 3. Insert / Update Documents

```js
await db.put('users', { id: 'u1', name: 'Alice', email: 'a@b.com', age: 30 })
await db.put('users', { id: 'u2', name: 'Bob',   email: 'b@b.com', age: 25 })
await db.put('posts', { id: 'p1', title: 'Hello', authorId: 'u1' })
```

`put` upserts — if a document with the same key exists, it is replaced.

#### 4. Read by Primary Key

```js
const doc = await db.get('users', 'u1')
console.log(doc.name) // 'Alice'

const missing = await db.get('users', 'nonexistent')
console.log(missing) // null
```

#### 5. Delete

```js
await db.delete('posts', 'p1')
const gone = await db.get('posts', 'p1')
console.log(gone) // null
```

#### 6. Count and Scan

```js
const count = await db.count('users') // 2
const all = await db.scan('users')    // [{ id: 'u1', ... }, { id: 'u2', ... }]
```

#### 7. Query by Index

```js
// Equality lookup on unique index
const byEmail = await db.getByIndex('users', 'byEmail', 'a@b.com')
// → [{ id: 'u1', name: 'Alice', email: 'a@b.com', age: 30 }]

// Range query: age in [25, 35)
const ranged = await db.rangeByIndex('users', 'byAge', 25, 35)
// → [Bob (25), Alice (30)]
```

Range queries are **exclusive** of the end value: `[start, end)`.

#### 8. Atomic Transactions

```js
const tx = db.transaction(['users', 'posts'], 'readwrite')

tx.put('users', { id: 'u3', name: 'Charlie', email: 'c@b.com' })
tx.put('posts', { id: 'p3', title: 'Tx post', authorId: 'u3' })

await tx.commit()
// Both writes succeed or neither does — atomic.

// Or discard:
tx2.abort()
```

All writes in a transaction are applied atomically in a single batch commit.
Read-your-writes consistency is supported within the same transaction.

#### 9. Close

```js
await db.close()
```

Always close the database when done to flush pending writes and release resources.

### Full Example

```js
const { FlowDB } = require('@restsend/flowdb')

async function main() {
  const db = FlowDB.open({ dataDir: './demo' })

  await db.createObjectStore('users', 'id')
  await db.createIndex('users', 'byEmail', ['email'], true)

  await db.put('users', { id: '1', name: 'Alice', email: 'alice@x.com' })
  const doc = await db.get('users', '1')
  console.log('Got:', doc)

  const tx = db.transaction(['users'], 'readwrite')
  tx.put('users', { id: '2', name: 'Bob', email: 'bob@x.com' })
  await tx.commit()

  console.log('Users:', await db.count('users'))
  await db.close()
}

main().catch(console.error)
```

### Building an HTTP Server

Since FlowDB is a plain Node.js native addon, you can wrap it in any
HTTP framework:

```js
const express = require('express')
const { FlowDB } = require('@restsend/flowdb')

const db = FlowDB.open({ dataDir: './data' })
const app = express()
app.use(express.json())

app.post('/api/:store', async (req, res) => {
  await db.put(req.params.store, req.body)
  res.json({ ok: true })
})

app.get('/api/:store/:key', async (req, res) => {
  const doc = await db.get(req.params.store, req.params.key)
  res.json(doc ?? { error: 'not found' })
})

app.listen(3000)
```

A complete BaaS server (auth, JWT, WebSocket realtime) can be built in pure
JavaScript on top of `flowdb` — no Rust required.
