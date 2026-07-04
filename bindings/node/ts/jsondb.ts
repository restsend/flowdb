import { Transaction } from './transaction'
import { CursorDirection, CursorItem, IndexCursorItem, KeyRange, OpenConfig } from './types'

export declare class FlowDB {
  static open(config: OpenConfig): FlowDB

  put(store: string, value: unknown): Promise<void>
  add(store: string, value: unknown): Promise<unknown>
  get(store: string, key: unknown): Promise<unknown>
  getWithMeta(store: string, key: unknown): Promise<unknown>
  getKey(store: string, key: unknown): Promise<unknown>
  delete(store: string, key: unknown): Promise<void>
  putAuto(store: string, value: unknown): Promise<unknown>
  scan(store: string): Promise<unknown[]>
  scanWithMeta(store: string): Promise<unknown[]>
  getAll(store: string, query?: KeyRange, count?: number): Promise<unknown[]>
  getAllKeys(store: string, query?: KeyRange, count?: number): Promise<unknown[]>
  clear(store: string): Promise<void>
  count(store: string, query?: KeyRange): Promise<number>

  createObjectStore(name: string, keyPath: string, autoIncrement?: boolean): Promise<void>
  deleteObjectStore(name: string): Promise<void>

  createIndex(
    store: string, name: string, keyPath: string | string[],
    options?: boolean | { unique?: boolean; multiEntry?: boolean }
  ): Promise<void>
  deleteIndex(store: string, name: string): Promise<void>

  getByIndex(store: string, index: string, value: unknown): Promise<unknown[]>
  rangeByIndex(store: string, index: string, start: unknown, end: unknown): Promise<unknown[]>

  storeNames(): string[]

  openCursor(
    store: string, query: KeyRange | null, direction: CursorDirection,
    callback: (item: CursorItem) => void
  ): Promise<void>
  openCursorByIndex(
    store: string, index: string, query: KeyRange | null, direction: CursorDirection,
    callback: (item: IndexCursorItem) => void
  ): Promise<void>

  cursor(store: string, query?: KeyRange, direction?: CursorDirection): AsyncIterable<CursorItem>
  cursorByIndex(
    store: string, index: string, query?: KeyRange, direction?: CursorDirection
  ): AsyncIterable<IndexCursorItem>

  close(): Promise<void>

  transaction(stores: string[], mode: 'readonly' | 'readwrite'): Transaction
}
