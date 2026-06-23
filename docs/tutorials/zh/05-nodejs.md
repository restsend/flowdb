# FlowDB Node.js 教程 — 面向 JavaScript 的 IndexedDB 兼容数据库

[« 返回教程列表](../index.md)

---

### 目标

通过 `npm install @restsend/flowdb` 在 Node.js 中使用 FlowDB —— 无需 Rust 工具链。
FlowDB 提供与 IndexedDB 兼容的文档数据库，支持 ACID 事务、二级索引和唯一约束。

### 安装

```bash
npm install @restsend/flowdb
```

### 步骤

#### 1. 打开数据库

```js
const { FlowDB } = require('@restsend/flowdb')

const db = FlowDB.open({ dataDir: './mydata' })
```

创建（或重新打开）一个存储在 `./mydata` 目录的数据库。所有数据在重启后持久保留。

#### 2. 创建对象存储和索引

对象存储类似表，每个存储有一个 `keyPath` 字段作为主键。

```js
await db.createObjectStore('users', 'id')
await db.createObjectStore('posts', 'id')

// 单字段二级索引
await db.createIndex('users', 'byEmail', 'email', true)   // unique=true

// 复合索引（多字段）
await db.createIndex('users', 'byNameAge', ['name', 'age'])
```

#### 3. 插入 / 更新文档

```js
await db.put('users', { id: 'u1', name: 'Alice', email: 'a@b.com', age: 30 })
await db.put('users', { id: 'u2', name: 'Bob',   email: 'b@b.com', age: 25 })
```

`put` 是 upsert —— 如果主键已存在则替换。

#### 4. 按主键读取

```js
const doc = await db.get('users', 'u1')
console.log(doc.name) // 'Alice'

const missing = await db.get('users', 'nonexistent')
console.log(missing) // null
```

#### 5. 删除

```js
await db.delete('posts', 'p1')
```

#### 6. 计数和扫描

```js
const count = await db.count('users') // 2
const all = await db.scan('users')    // 所有文档的数组
```

#### 7. 索引查询

```js
// 唯一索引等值查询
const byEmail = await db.getByIndex('users', 'byEmail', 'a@b.com')

// 范围查询：age in [25, 35)
const ranged = await db.rangeByIndex('users', 'byAge', 25, 35)
```

范围查询是**前闭后开**区间 `[start, end)`。

#### 8. 原子事务

```js
const tx = db.transaction(['users', 'posts'], 'readwrite')
tx.put('users', { id: 'u3', name: 'Charlie' })
tx.put('posts', { id: 'p3', title: '事务测试' })
await tx.commit()  // 原子提交：要么全部成功，要么全部失败

// 或放弃：
tx2.abort()
```

#### 9. 关闭

```js
await db.close()
```

### 完整示例

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

### 构建 HTTP 服务器

FlowDB 是原生 Node.js 插件，可用任意 HTTP 框架包装：

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

完整的 BaaS 服务器（认证、JWT、WebSocket 实时推送）可以纯 JavaScript
基于 `flowdb` 构建 —— 无需编写 Rust。
