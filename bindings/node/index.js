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

  delete(store, key) {
    this._tx.delete(store, key)
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

  async get(store, key) { return this._db.get(store, key) }

  async delete(store, key) { await this._db.delete(store, key) }

  async putAuto(store, value) { return this._db.putAuto(store, value) }

  async scan(store) { return this._db.scan(store) }

  async createObjectStore(name, keyPath) {
    await this._db.createObjectStore(name, keyPath)
  }

  async deleteObjectStore(name) {
    await this._db.deleteObjectStore(name)
  }

  async createIndex(store, name, keyPath, unique) {
    const paths = Array.isArray(keyPath) ? keyPath : [keyPath]
    await this._db.createIndex(store, name, paths, !!unique)
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

  async count(store) { return this._db.count(store) }

  storeNames() { return this._db.storeNames() }

  async close() { await this._db.close() }

  transaction(stores, mode) {
    const tx = this._db.transaction(stores, mode)
    return new Transaction(tx)
  }
}

module.exports = { FlowDB, Transaction }
