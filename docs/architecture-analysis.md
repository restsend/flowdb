# FlowDB 架构解析：一个 Rust 时序存储引擎为什么能比RocksDB快好几倍

FlowDB是用Rust编写的高性能时序数据库, 代码在
https://github.com/restsend/flowdb

如果只看 README，FlowDB 给人的第一印象是“一个性能很高的 Rust LSM 引擎”。但真正有价值的问题不是“它快不快”，而是“它为什么快”。

从源码看，FlowDB 并不是靠某个单点黑科技取胜，而是把写路径、查询路径、扫描路径和后台整理路径都压到非常短、非常直接。它没有试图做一个“什么都支持”的通用 KV 内核，而是围绕时间序列场景，做了一组很克制但极有效的工程选择：

- 写入先 WAL、后 MemTable，但尽量把编码和内存分配放在锁外。
- MemTable 不追求全程有序，而是活跃态只做最快的追加，冻结时再转成有序结构。
- 查询不去扫整个 LSM 树，而是先靠块级索引和 Bloom Filter 迅速缩小候选集合。
- 范围扫描不一次性物化结果，而是用惰性迭代器逐条归并返回。
- Flush 和 Compaction 用两套压缩策略，分别优化吞吐和空间。

这篇文章基于 FlowDB 当前源码实现，系统拆解它的架构、数据路径，以及它性能出众的根本原因。

## 一、先看结果：FlowDB 的性能表现到了什么水平

README 中给出了和 RocksDB 的对比基准。默认条件是：

- 100K records
- value 大小 128B
- batch size 100
- release 构建
- Apple M 系列芯片

README 给出的结果是：

| 指标 | FlowDB | RocksDB | 结论 |
|---|---:|---:|---|
| 顺序写 | 5.7M ops/s | 3.0M ops/s | FlowDB 1.92x |
| 8 线程并发写 | 6.7M ops/s | 4.1M ops/s | FlowDB 1.63x |
| 点查 | 6.6M ops/s | 524K ops/s | FlowDB 12.7x |
| 前缀扫描 | 71K ops/s | 10.7K ops/s | FlowDB 6.6x |
| 全扫描 | 65 ops/s | 39 ops/s | FlowDB 1.67x |
| 存储占用 | 1.9MB | 1.8MB | 基本相当 |

这组数据最值得关注的不是写入，而是点查和前缀扫描。写入快，很多 LSM 引擎都能做到；但点查快了一个数量级，通常意味着索引组织、过滤策略和读路径实现确实做对了。

## 二、FlowDB 的整体架构

从模块划分看，FlowDB 的核心组成很清晰：

- `engine.rs`：统一入口，负责写入、查询、扫描、flush、compaction、gc。
- `wal.rs`：预写日志，负责崩溃恢复。
- `memtable.rs`：内存表，分 active 和 frozen 两层。
- `sstable.rs`：磁盘上的 SST 文件读写。
- `block_meta_index.rs`：块级元数据索引，负责快速缩小读放大范围。
- `bloom.rs`：Bloom Filter，用于快速判断 key 是否不在某个 SST 中。
- `cache.rs`：块缓存。
- `compaction.rs`：压缩合并。
- `gc.rs`：TTL 驱动的过期数据清理。
- `manifest.rs`：持久化元信息，记录当前活跃 SST 集合及块信息。
- `write_worker.rs`：真正执行写路径和 flush。

可以把它理解为一个典型但很“瘦身”的 LSM 架构：

![FlowDB 架构图](svg/flowdb-architecture.svg)

核心思路可以概括成一句话：

**写时尽量只做顺序追加，读时尽量只看最少的数据块。**

## 三、写路径：为什么 FlowDB 的写入吞吐这么高

### 1. 锁外预编码，缩短临界区

写入最终会走到 `Engine::do_write`。这个函数有一个很关键的细节：在进入 `WriteWorker` 的互斥锁之前，已经先完成了 WAL buffer 编码。

也就是说，真正进入临界区之后，锁内只做三件事：

- 把编码后的 buffer 追加到 WAL
- 把记录插入 active memtable
- 必要时触发 flush

这件事看起来普通，但对并发写吞吐影响很大。因为一批记录的序列化、容量扩容、字节拼装这些 CPU 工作都被放到了锁外，互斥锁只保护“状态变更”本身。这样一来，写锁持有时间就非常短。

### 2. WAL 是纯顺序追加，而且有 256KB 缓冲

`wal.rs` 里用的是 `BufWriter`，缓冲区大小 256KB。它不是每条记录都落盘，而是先进入用户态缓冲，减少 syscall 频率。

WAL 记录格式也很克制：

- `seq`
- `op`
- `key_len + key`
- `ts`
- `expire_at`
- `range_end`
- `value`

没有额外的 checksum、没有复杂页结构、没有事务层附加元数据。这使得 WAL 的编码、写入、回放都非常直接。

对于时间序列这类高吞吐场景，顺序追加几乎总是最便宜的写法。FlowDB 把这一点贯彻得很彻底。

### 3. MemTable 的活跃态不是有序结构，而是“日志态”

FlowDB 的 MemTable 设计非常有意思。它不是一开始就用 BTree 或 SkipList，而是分成两种状态：

- 活跃态：`Vec<InternalRecord> + AHashMap<(hash(key), ts), usize>`
- 冻结态：`BTreeMap<(Vec<u8>, i64, u64), InternalRecord>`

这背后的思想是：

**写入阶段最重要的是 append 快，而不是随时有序。**

活跃态下：

- `Vec::push` 是顺序写，cache 友好。
- `AHashMap` 提供点查索引。
- 插入时不做平衡树维护，不做跳表多层链接更新。

等 memtable 满了再 freeze，统一把日志态转换成有序的 `BTreeMap`，把排序成本推迟到 flush 前。这个策略非常适合“写多读多，但写路径必须极短”的场景。

### 4. 批量分配序列号，无锁推进

`Engine` 用 `AtomicU64` 做全局 `seq_counter`，每次 `fetch_add(batch.len())` 为一批记录分配连续序列号。这里用的是 `Relaxed` 序语，因为只要求全局单调唯一，不需要更强的跨线程同步语义。

这类设计虽然看起来小，但它避免了中心化 ID 分配锁，在高并发写入时非常重要。

### 5. 写路径的一个关键判断：先快，再稳

FlowDB 的写路径并没有把 flush 彻底异步化。active memtable 达到阈值后，它会 freeze，然后由 `WriteWorker` 把 frozen memtable 刷成 SST。如果 frozen memtable 队列过深，还会通过 `max_frozen_memtables` 形成背压。

这意味着它没有为了“表面吞吐”无限吞数据，而是让内存上限和后台速度之间维持一个稳定平衡。对于存储引擎来说，这比一味堆积更合理。

## 四、读路径：FlowDB 为什么点查和前缀查询特别快

如果说写入快主要靠“少做事”，那读路径快主要靠“少看数据”。

FlowDB 的查询顺序是：

1. 先查 memtables
2. 再查块级索引 `BlockMetaIndex`
3. 再用 Bloom Filter 过滤 SST
4. 再定位 block
5. 最后才真正解压和扫描 block 内记录

这个顺序非常关键，因为它把磁盘读和解压缩推迟到了最后一步。

### 1. MemTable 先命中，避免不必要的磁盘访问

`Engine::get_sync` 先查 active 和 frozen memtable。如果命中的是最新记录，整个查询就结束了。对于刚写入不久的数据，这个路径非常短。

尤其是在时间序列场景下，热点数据天然集中在最新时间窗口，这种“先看内存”的收益会非常大。

### 2. 块级索引不是按记录建，而是按 block 建

`block_meta_index.rs` 维护的是块元数据，不是全量记录索引。每个 block 只记录这些信息：

- `min_key`
- `max_key`
- `min_ts`
- `max_ts`
- `max_expire`
- 所属 `sst_id` 和 `block_idx`

索引分成两套：

- `by_key`：按 key 范围筛候选 block
- `by_time`：按时间桶筛候选 block

查询时先用 key 条件拿候选，再和时间桶条件做交集。这样可以把真正需要打开的 block 数量压到很低。

这其实是 FlowDB 很重要的“时间序列特化”之一。因为时间过滤在它这里不是附加条件，而是一级索引条件。

### 3. Bloom Filter 过滤的是“某个 key 是否可能存在于某个 SST”

FlowDB 的 Bloom Filter 粒度是每个 SST 一份，构建时基于唯一 key 集合生成。查询时如果 Bloom Filter 明确返回“不可能存在”，整个 SST 直接跳过。

这一点对点查性能影响巨大。

很多通用 KV 引擎面对时间序列数据时，会把主键组织成复合键，比如 `(key, ts)`。这样 Bloom Filter 过滤的是某个完整复合键，结果就是同一个逻辑 key 的多个时间点会把过滤粒度打散。FlowDB 这里反过来，直接围绕时序场景做了按 key 粒度的过滤，所以点查效率会高很多。

### 4. SSTReader 用 mmap，减少 read syscall

`SstReader` 打开 SST 后直接 mmap 整个文件，并在初始化时扫一遍 header，构建每个 block 的 offset 表。之后读 block 就不需要再做普通 `read` 系统调用，而是直接从映射内存切片读取。

这不会消除解压成本，但会显著减少系统调用和内核态切换。

对于高频点查来说，这种收益非常直接。

### 5. Block Cache 用分片 LRU，减少锁争用

FlowDB 的 block cache 不是单一大锁，而是 64 个 shard，每个 shard 内部是一个 `LruCache`。缓存键是 `(sst_id, block_idx)`。

分片意味着：

- 不同 block 更可能落到不同 shard
- 多线程读时，缓存竞争被摊薄
- 热块可以重复命中，避免重复解压

而且缓存里放的是 `Arc<Vec<InternalRecord>>`，同一个 block 可被多个查询共享，不需要重复分配一份结果。

### 6. 点查不只是“查到了”，而是“只解压最少的 block”

真正决定点查性能的，不是二分查找本身，而是查询前面的过滤链条是否足够强。FlowDB 的链条是：

- memtable 命中则结束
- 否则看块级索引是否有覆盖范围
- 再用 Bloom Filter 排除整个 SST
- 再按 block 范围做二分定位
- 最后在 block 内二分或顺序定位

这套链路把 IO、解压和对象构造都推迟到了最后。因此 FlowDB 的点查性能才会和通用引擎拉开数量级差距。

## 五、范围扫描：FlowDB 为何扫描也能做到高效

FlowDB 的扫描不是简单地把查询结果都收集到 `Vec<Record>` 再返回，而是提供了 `ScanIterator`。

这个设计的价值在于：

- 不需要一次性把大范围结果全部物化到内存
- 可以按需 `take(n)`
- 可以边归并边输出

### 1. 扫描是多源归并，而不是全量排序

扫描会同时面对多个来源：

- active memtable
- 多个 frozen memtable
- 多个 SST block

FlowDB 的做法是把这些来源都转成有序 source，再用 `BinaryHeap` 做 k-way merge。这样复杂度更可控，且不需要先把所有记录拼一起再排序。

### 2. 有单源快路径

如果扫描时发现只有一个 source，并且没有 range tombstone 干扰，`ScanIterator` 会进入 fast path，直接顺序返回记录，连堆归并的成本都省掉。

这是一个很典型的工程优化：在很多真实场景里，“理论上的多路合并”实际只剩一路。为这个场景单独开一条快路径，收益往往很高。

### 3. 全扫描明确绕过 block cache

FlowDB 还刻意把“全表扫描”和“范围查询”区分对待。全表扫描时，它会直接解压 block，但不写入 block cache。

原因非常现实：

**顺序扫过的大量冷数据不应该把热点缓存冲掉。**

这一点说明 FlowDB 并不是机械地“所有读都走缓存”，而是知道什么场景该保护缓存命中率。

## 六、落盘与后台整理：Flush、Compaction、GC 是怎么做的

### 1. Flush：快优先，所以用 LZ4

冻结 memtable 落成 SST 时，FlowDB 选择 LZ4 压缩，而不是一开始就用 Zstd。原因很简单：

- flush 紧邻写路径
- flush 变慢会直接反压写入

因此这里首先要保吞吐，而不是极致压缩率。

### 2. Compaction：省空间优先，所以用 Zstd

到了 compaction，FlowDB 改用 Zstd。因为 compaction 是后台行为，延迟敏感度没那么高，这时候更值得换更好的压缩比，减少后续读取和存储成本。

这就是 README 里提到的双压缩策略：

- flush 用 LZ4
- compaction 用 Zstd

这并不是“为了好看”的 feature，而是非常实用的路径分治。

### 3. Compaction 不是暴力全量重写，而是 size-tiered 合并

`compaction.rs` 的候选选择策略是按 SST 大小排序，优先挑选大小相近的一批 SST 做合并。这个思路本质上是 size-tiered compaction：

- 避免小表和超大表频繁混合重写
- 减少写放大
- 在结构简单和性能之间取得平衡

而在真正合并时，FlowDB 用 `SstBlockIterator + BinaryHeap` 做流式归并。也就是说，它不会把所有 SST 记录一次性读进内存，而是按 block 懒加载，边读边归并。

这使 compaction 的内存占用与 SST 总大小弱相关，而更接近于“block 大小 × 归并路数”。

### 4. GC：TTL 过期后按 SST 粒度整表清理

GC 的思路也很务实。它不是逐条扫描删除过期记录，而是看某个 SST 的 `max_expire` 是否已经整体早于当前时间：

- 如果整个 SST 都过期，直接删除文件
- 更新 index
- 更新 manifest
- 清理 cache

这比记录级回收成本低太多了。对于时间序列这种“整段数据整体过期”的场景，这种粗粒度回收是非常高效的。

## 七、为什么说 FlowDB 的性能优势不是偶然，而是架构决定的

从源码看，FlowDB 的高性能主要来自以下几类机制叠加。

### 1. 它把通用系统的复杂性拿掉了

FlowDB 没有试图做成一个面向所有场景的底座。它没有复杂事务层、没有多列族体系、没有过于庞杂的 compaction 策略矩阵、没有为极端兼容性增加的大量抽象成本。

少一个功能，不只是少几百行代码，而是少一层判断、少一层状态同步、少一个热路径分支。

### 2. 它围绕时间序列数据组织了索引和过滤结构

时间序列有三个天然特征：

- 写多，且大多按时间追加
- 热点集中在新数据
- 读经常带 key 范围和时间范围

FlowDB 的几乎每个关键数据结构都在迎合这三个特征：

- 活跃 memtable 以追加为中心
- 索引按 key 和 time bucket 双维组织
- Bloom Filter 按 key 过滤
- GC 以过期窗口为单位清理

因此它不是“碰巧在时序场景快”，而是从模型层就按时序场景设计了。

### 3. 它的热路径非常短

高性能系统一个很朴素的规律是：真正快的实现，热路径代码通常都不长。

FlowDB 的写入热路径基本可以概括成：

1. 批量分配 seq
2. 锁外编码 WAL buffer
3. 锁内顺序追加 WAL
4. 插入 active memtable

查询热路径也可以概括成：

1. 查 memtable
2. 查 block meta index
3. Bloom Filter 排除 SST
4. 读 block
5. 返回结果

没有太多旁路，没有太多“框架式中转”，这是性能的底层原因。

### 4. Rust 在这里真正发挥了“零开销抽象”的价值

FlowDB 不是简单“用 Rust 重写一遍 LSM”，而是比较充分地利用了 Rust 的优势：

- 所有权语义让 `write_batch_owned` 这类路径天然零拷贝。
- `Arc` 让 reader 和 cached block 共享变得便宜且安全。
- `AtomicU64` 让批量 seq 分配无需额外同步层。
- `parking_lot` 比标准锁更适合这种高频短临界区。
- `ahash` 让 hash lookup 更快。
- `memmap2` 提供高效的文件映射访问。

这些单点优化单独看都不惊人，但全部叠加在热路径上，结果就会非常明显。

## 八、FlowDB 的几个关键取舍，也决定了它的边界

一篇架构分析如果只讲优点，价值不高。FlowDB 很快，但它的快也来自明确的取舍。

### 1. 它更像“面向时序场景的专用引擎”，不是全能 KV 内核

如果你需要的是复杂事务、多租户隔离、极其成熟的跨平台运维生态，那么 FlowDB 当前显然不是要替代那类系统。

它做的是更聚焦的事情：

- 单机
- LSM
- 时序/日志类数据
- 高吞吐写入
- 高效点查与扫描

### 2. 活跃 memtable 的哈希索引是针对点查优化的

它对追加写和点查非常友好，但如果有人想把 active memtable 本身当成一个复杂有序索引去做大量范围查询，那它不会像全程有序结构那样自然。

FlowDB 的应对方式是：活跃态负责快写，冻结后再进入有序世界。这个选择是明确站在写入性能一侧的。

### 3. Size-tiered compaction 简单高效，但不一定适合所有负载

相较于更复杂的 leveled compaction，size-tiered 的实现更轻，但在某些读放大更敏感、数据规模更大、层次更复杂的场景下，可能还有继续进化空间。

不过从当前项目目标看，这个选择是合理的：先把架构做轻，把热点路径做短。

## 九、结论：FlowDB 快，不是因为“Rust”，而是因为“Rust + 正确的架构约束”

只说“FlowDB 很快，因为它是 Rust 写的”，这句话并不成立。Rust 不是性能魔法，真正起决定作用的是下面这几件事同时成立：

1. 写路径足够短，编码放锁外，落日志顺序追加。
2. MemTable 的结构选型优先服务于追加写和点查。
3. 查询路径在真正读盘前做了足够多轮过滤。
4. 扫描是惰性归并，不做无意义的大结果集物化。
5. Flush、Compaction、GC 都围绕时序场景做了很现实的取舍。
6. Rust 把这些设计以较低的实现成本、安全地落成了代码。

如果一定要用一句话总结 FlowDB 的架构哲学，那就是：

**它没有试图做“最通用的存储引擎”，而是努力把“时序 LSM 的关键路径”做到了足够短、足够直接、足够少拐弯。**

这也是它能在点查、前缀扫描和写吞吐上明显跑赢 RocksDB 对照配置的根本原因。

对于想研究 Rust 存储引擎实现的人来说，FlowDB 很有参考价值。它展示了一条非常清晰的路线：

- 先围绕场景做减法
- 再围绕热路径做数据结构设计
- 最后再用语言和库把每一层开销压下去

这套方法论，比单纯讨论“某个库快不快”更值得借鉴。
