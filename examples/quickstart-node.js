// FlowDB Node.js Quickstart
// Run: node examples/quickstart-node.js
//
// Prerequisites:
//   cd bindings/node && npm install && npx napi build --platform --release
//
// Or if flowdb is installed from npm:
//   npm install @restsend/flowdb

const { FlowDB, KeyRange } = require('../bindings/node')

async function main() {
  const db = FlowDB.open({ dataDir: '/tmp/flowdb-quickstart' })

  // Create stores
  await db.createObjectStore('users', 'id')
  await db.createObjectStore('events', '_id', true) // auto-increment

  // Create indexes (with options object for unique + multiEntry)
  await db.createIndex('users', 'byEmail', 'email', { unique: true })
  await db.createIndex('users', 'byAge', 'age', false)
  await db.createIndex('items', 'byTag', 'tags', { multiEntry: true })

  // ── CRUD ────────────────────────────────────────────────────
  await db.put('users', { id: 'u1', name: 'Alice', email: 'a@b.com', age: 30 })
  await db.put('users', { id: 'u2', name: 'Bob', email: 'b@b.com', age: 25 })

  // add() — insert-only (fails if key exists)
  await db.add('users', { id: 'u3', name: 'Charlie', email: 'c@b.com', age: 35 })
  const dupErr = await db.add('users', { id: 'u3', name: 'Duplicate' }).catch(e => e.message)
  console.log('add duplicate:', dupErr.includes('exists'))

  // Point reads
  const doc = await db.get('users', 'u1')
  console.log('get:', JSON.stringify(doc))
  const keyExists = await db.getKey('users', 'u1')
  console.log('getKey:', JSON.stringify(keyExists))

  // ── Bulk reads with KeyRange ────────────────────────────────
  const kr = KeyRange.bound('Alice', 'Charlie')
  const results = await db.getAll('users', kr)
  console.log('getAll by name range:', results.length)
  const keys = await db.getAllKeys('users', kr)
  console.log('getAllKeys:', JSON.stringify(keys))
  const total = await db.count('users', kr)
  console.log('count(range):', total)

  // ── Index queries ───────────────────────────────────────────
  const byEmail = await db.getByIndex('users', 'byEmail', 'a@b.com')
  console.log('byEmail:', JSON.stringify(byEmail))
  const byRange = await db.rangeByIndex('users', 'byAge', 20, 30)
  console.log('rangeByIndex age [20,30):', byRange.length)

  // ── Auto-increment ──────────────────────────────────────────
  const id1 = await db.putAuto('events', { type: 'click', ts: Date.now() })
  const id2 = await db.putAuto('events', { type: 'view', ts: Date.now() })
  console.log('putAuto:', id1, id2)

  // ── Cursor (callback) ───────────────────────────────────────
  const items = []
  await db.openCursor('users', null, 'next', (item) => {
    if (item.done) return
    items.push(item.key)
  })
  console.log('cursor callback:', JSON.stringify(items))

  // ── Cursor (async iterator) ─────────────────────────────────
  const names = []
  for await (const item of db.cursor('users', null, 'prev')) {
    names.push(item.value.name)
  }
  console.log('cursor reverse:', JSON.stringify(names))

  // ── Transaction with read-your-writes ───────────────────────
  const tx = db.transaction(['users'], 'readwrite')
  tx.put('users', { id: 'u4', name: 'Diana', email: 'd@b.com', age: 28 })
  const txRead = await tx.get('users', 'u4') // sees pending write
  console.log('tx read-your-writes:', txRead.name)
  tx.putAuto('events', { type: 'tx-auto' })
  await tx.commit()

  // ── Clear ───────────────────────────────────────────────────
  await db.clear('events')
  console.log('clear events:', await db.count('events'))

  await db.close()
  console.log('Done.')
}

main().catch(console.error)
