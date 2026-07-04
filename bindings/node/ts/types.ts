export interface OpenConfig {
  dataDir: string
  createIfMissing?: boolean
  defaultTtlSecs?: number
  memtableSizeMb?: number
  blockCacheCapacityMb?: number
  bloomBitsPerKey?: number
  compactionIntervalMs?: number
}

export interface KeyRange {
  lower?: unknown
  upper?: unknown
  lowerOpen?: boolean
  upperOpen?: boolean
}

export declare const KeyRange: {
  only(key: unknown): KeyRange
  bound(lower: unknown, upper: unknown, lowerOpen?: boolean, upperOpen?: boolean): KeyRange
  lowerBound(key: unknown, open?: boolean): KeyRange
  upperBound(key: unknown, open?: boolean): KeyRange
}

export type CursorDirection = 'next' | 'prev' | 'nextunique' | 'prevunique'

export interface CursorItem {
  key: unknown
  value: unknown
  done: boolean
}

export interface IndexCursorItem {
  key: unknown
  primaryKey: unknown
  value: unknown
  done: boolean
}
