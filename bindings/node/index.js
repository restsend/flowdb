// FlowDB — Native Node.js addon
// Platform detection + wrapper classes

const path = require('path')

// ── Native module loader ────────────────────────────────────────

function loadNative() {
  const { platform, arch } = process
  const SUFFIXES = {
    darwin: { arm64: 'darwin-arm64', x64: 'darwin-x64' },
    linux: { arm64: 'linux-arm64-gnu', x64: 'linux-x64-gnu' },
    win32: { x64: 'win32-x64-msvc' },
  }
  const suffix = SUFFIXES[platform]?.[arch]
  if (!suffix) {
    throw new Error(
      `FlowDB: unsupported platform ${platform}-${arch}. ` +
      'Supported: darwin-arm64, darwin-x64, linux-x64-gnu, linux-arm64-gnu, win32-x64-msvc'
    )
  }

  // 1. Try platform-specific package (published separately)
  try {
    return require(`@restsend/flowdb-${suffix}`)
  } catch (_) {
    // not installed — fall through
  }

  // 2. Try local .node file (for development / direct install)
  try {
    return require(path.join(__dirname, `flowdb-node.${suffix}.node`))
  } catch (e) {
    throw new Error(
      `FlowDB: failed to load native module for ${platform}-${arch}. ` +
      'Run `napi build --platform --release` first. ' + e.message
    )
  }
}

const native = loadNative()

// ── Transaction ──────────────────────────────────────────────────

class Transaction {
  constructor(nativeTx) {
    this._tx = nativeTx
  }

  put(store, value) {
    this._tx.put(store, value)
  }

  putAuto(store, value) {
    this._tx.putAuto(store, value)
  }

  delete(store, key) {
    this._tx.delete(store, key)
  }

  async get(store, key) {
    return this._tx.get(store, key)
  }

  async count(store) {
    return this._tx.count(store)
  }

  async scan(store) {
    return this._tx.scan(store)
  }

  async getByIndex(store, index, value) {
    return this._tx.getByIndex(store, index, value)
  }

  async rangeByIndex(store, index, start, end) {
    return this._tx.rangeByIndex(store, index, start, end)
  }

  async commit() {
    await this._tx.commit()
  }

  abort() {
    this._tx.abort()
  }
}

// ── FlowDB ──────────────────────────────────────────────────────

class FlowDB {
  constructor(nativeDb) {
    this._db = nativeDb
  }

  static open(config) {
    const cfg = {
      dataDir: config.dataDir,
      createIfMissing: config.createIfMissing !== false,
    }
    // Only set optional fields if provided (undefined → napi skips the field)
    if (config.defaultTtlSecs != null) cfg.defaultTtlSecs = config.defaultTtlSecs
    if (config.memtableSizeMb != null) cfg.memtableSizeMb = config.memtableSizeMb
    if (config.blockCacheCapacityMb != null) cfg.blockCacheCapacityMb = config.blockCacheCapacityMb
    if (config.bloomBitsPerKey != null) cfg.bloomBitsPerKey = config.bloomBitsPerKey
    if (config.compactionIntervalMs != null) cfg.compactionIntervalMs = config.compactionIntervalMs
    const db = native.FlowDb.open(cfg)
    return new FlowDB(db)
  }

  async put(store, value) { await this._db.put(store, value) }

  async add(store, value) { return this._db.add(store, value) }

  async get(store, key) { return this._db.get(store, key) }

  async getWithMeta(store, key) { return this._db.getWithMeta(store, key) }

  async getKey(store, key) { return this._db.getKey(store, key) }

  async delete(store, key) { return this._db.delete(store, key) }

  async putAuto(store, value) { return this._db.putAuto(store, value) }

  async scan(store) { return this._db.scan(store) }

  async scanWithMeta(store) { return this._db.scanWithMeta(store) }

  async getAll(store, query, count) {
    return this._db.getAll(store, query || null, count != null ? count : null)
  }

  async getAllKeys(store, query, count) {
    return this._db.getAllKeys(store, query || null, count != null ? count : null)
  }

  async clear(store) { await this._db.clear(store) }

  async count(store, query) {
    if (query != null) return this._db.countQuery(store, query)
    return this._db.count(store)
  }

  async createObjectStore(name, keyPath, autoIncrement) {
    await this._db.createObjectStore(name, keyPath, autoIncrement || false)
  }

  async deleteObjectStore(name) {
    await this._db.deleteObjectStore(name)
  }

  async createIndex(store, name, keyPath, options) {
    const paths = Array.isArray(keyPath) ? keyPath : [keyPath]
    const unique = typeof options === 'boolean' ? options : (options && options.unique) || false
    const multiEntry = (options && options.multiEntry) || false
    await this._db.createIndex(store, name, paths, unique, multiEntry)
  }

  async deleteIndex(store, name) {
    await this._db.deleteIndex(store, name)
  }

  async getByIndex(store, index, value) {
    return this._db.getByIndex(store, index, value)
  }

  async rangeByIndex(store, index, start, end) {
    return this._db.rangeByIndex(store, index, start, end)
  }

  storeNames() { return this._db.storeNames() }

  // ── Cursor (callback-style) ──────────────────────────────────
  // db.openCursor(store, keyRange, direction, (item) => {
  //   if (item.done) return
  //   console.log(item.key, item.value)
  // })
  openCursor(store, query, direction, callback) {
    return this._db.openCursor(store, query || null, direction || 'next', (item) => {
      callback(item)
    })
  }

  openCursorByIndex(store, index, query, direction, callback) {
    return this._db.openCursorByIndex(store, index, query || null, direction || 'next', (item) => {
      callback(item)
    })
  }

  // ── Cursor (async iterator) ──────────────────────────────────
  // for await (const item of db.cursor('users')) { ... }
  cursor(store, query, direction) {
    const nativeDb = this._db
    const queue = []
    let resolveWait = null
    let done = false

    nativeDb.openCursor(store, query || null, direction || 'next', (item) => {
      if (item.done) {
        done = true
      }
      if (resolveWait) {
        const r = resolveWait
        resolveWait = null
        r(done ? { done: true } : { value: item, done: false })
      } else {
        queue.push(item)
      }
    })

    return {
      [Symbol.asyncIterator]() {
        return {
          async next() {
            if (queue.length > 0) {
              const item = queue.shift()
              return { value: item, done: false }
            }
            if (done) return { done: true }
            return new Promise((resolve) => { resolveWait = resolve })
          }
        }
      }
    }
  }

  cursorByIndex(store, index, query, direction) {
    const nativeDb = this._db
    const queue = []
    let resolveWait = null
    let done = false

    nativeDb.openCursorByIndex(store, index, query || null, direction || 'next', (item) => {
      if (item.done) {
        done = true
      }
      if (resolveWait) {
        const r = resolveWait
        resolveWait = null
        r(done ? { done: true } : { value: item, done: false })
      } else {
        queue.push(item)
      }
    })

    return {
      [Symbol.asyncIterator]() {
        return {
          async next() {
            if (queue.length > 0) {
              const item = queue.shift()
              return { value: item, done: false }
            }
            if (done) return { done: true }
            return new Promise((resolve) => { resolveWait = resolve })
          }
        }
      }
    }
  }

  async close() { await this._db.close() }

  transaction(stores, mode) {
    const tx = this._db.transaction(stores, mode)
    return new Transaction(tx)
  }
}

// ── KeyRange factory ─────────────────────────────────────────────

const KeyRange = {
  only(key) {
    return { lower: key, upper: key, lowerOpen: false, upperOpen: false }
  },
  bound(lower, upper, lowerOpen, upperOpen) {
    return { lower, upper, lowerOpen: lowerOpen || false, upperOpen: upperOpen || false }
  },
  lowerBound(key, open) {
    return { lower: key, lowerOpen: open || false }
  },
  upperBound(key, open) {
    return { upper: key, upperOpen: open || false }
  },
}

module.exports = { FlowDB, Transaction, KeyRange }
