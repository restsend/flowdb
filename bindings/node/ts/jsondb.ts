import { Transaction } from './transaction'
import { OpenConfig } from './types'

export declare class FlowDB {
  static open(config: OpenConfig): FlowDB

  put(store: string, value: unknown): Promise<void>
  get(store: string, key: unknown): Promise<unknown>
  getWithMeta(store: string, key: unknown): Promise<unknown>
  delete(store: string, key: unknown): Promise<void>
  putAuto(store: string, value: unknown): Promise<unknown>
  scan(store: string): Promise<unknown[]>
  scanWithMeta(store: string): Promise<unknown[]>

  createObjectStore(name: string, keyPath: string, autoIncrement?: boolean): Promise<void>
  deleteObjectStore(name: string): Promise<void>
  createIndex(store: string, name: string, keyPath: string | string[], unique?: boolean): Promise<void>
  deleteIndex(store: string, name: string): Promise<void>

  getByIndex(store: string, index: string, value: unknown): Promise<unknown[]>
  rangeByIndex(store: string, index: string, start: unknown, end: unknown): Promise<unknown[]>

  count(store: string): Promise<number>
  storeNames(): string[]

  close(): Promise<void>

  transaction(stores: string[], mode: 'readonly' | 'readwrite'): Transaction
}
