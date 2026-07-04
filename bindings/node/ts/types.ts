export interface OpenConfig {
  dataDir: string
  createIfMissing?: boolean
  defaultTtlSecs?: number
  memtableSizeMb?: number
  blockCacheCapacityMb?: number
  bloomBitsPerKey?: number
  compactionIntervalMs?: number
}
