// FlowDB Node.js Quickstart
// Run: node examples/quickstart-node.js
//
// Prerequisites:
//   cd bindings/node && npm install && npx napi build --platform --release
//
// Or if flowdb is installed from npm:
//   npm install @restsend/flowdb

const { FlowDB } = require('../bindings/node')

async function main() {
  const db = FlowDB.open({ dataDir: '/tmp/flowdb-quickstart' })

  // Create object store (like IndexedDB)
  await db.createObjectStore('users', 'id')
  await db.createIndex('users', 'byEmail', ['email'], true)

  // CRUD
  await db.put('users', { id: 'u1', name: 'Alice', email: 'a@b.com', age: 30 })
  await db.put('users', { id: 'u2', name: 'Bob', email: 'b@b.com', age: 25 })

  const doc = await db.get('users', 'u1')
  console.log('get:', JSON.stringify(doc))

  // Index query
  const byEmail = await db.getByIndex('users', 'byEmail', 'a@b.com')
  console.log('byEmail:', JSON.stringify(byEmail))

  // Transaction
  const tx = db.transaction(['users'], 'readwrite')
  tx.put('users', { id: 'u3', name: 'Charlie', email: 'c@b.com' })
  await tx.commit()
  console.log('count:', await db.count('users'))

  await db.close()
  console.log('Done.')
}

main().catch(console.error)
