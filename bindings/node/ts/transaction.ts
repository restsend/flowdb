export declare class Transaction {
  put(store: string, value: unknown): void
  putAuto(store: string, value: unknown): void
  delete(store: string, key: unknown): void
  get(store: string, key: unknown): Promise<unknown>
  count(store: string): Promise<number>
  scan(store: string): Promise<unknown[]>
  getByIndex(store: string, index: string, value: unknown): Promise<unknown[]>
  rangeByIndex(store: string, index: string, start: unknown, end: unknown): Promise<unknown[]>
  commit(): Promise<void>
  abort(): void
}
