use crate::bloom::BloomFilter;
use crate::cache::{BlockCache, CacheKey};
use crate::error::{FlowError, Result};
use crate::manifest::BlockInfo;
use crate::record::{InternalRecord, Op};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

const BLOCK_MAGIC_LZ4: u32 = 0x54534E42;
pub(crate) const HEADER_SIZE: usize = 48;

pub(crate) struct SstBlock {
    pub records: Vec<InternalRecord>,
}

#[derive(Debug, Clone)]
pub(crate) struct BlockHeader {
    pub num_records: u32,
    pub min_ts: i64,
    pub max_ts: i64,
    pub min_expire: i64,
    pub max_expire: i64,
    pub data_len: u32,
    pub compressed_len: u32,
}

impl BlockHeader {
    /// Serialises the header into a fixed-size big-endian byte array (48 bytes).
    /// Layout: magic(4) | num_records(4) | min_ts(8) | max_ts(8) |
    ///         min_expire(8) | max_expire(8) | data_len(4) | compressed_len(4)
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let magic = BLOCK_MAGIC_LZ4;
        let mut buf = [0u8; HEADER_SIZE];
        let mut pos = 0;
        buf[pos..pos + 4].copy_from_slice(&magic.to_be_bytes());
        pos += 4;
        buf[pos..pos + 4].copy_from_slice(&self.num_records.to_be_bytes());
        pos += 4;
        buf[pos..pos + 8].copy_from_slice(&self.min_ts.to_be_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.max_ts.to_be_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.min_expire.to_be_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.max_expire.to_be_bytes());
        pos += 8;
        buf[pos..pos + 4].copy_from_slice(&self.data_len.to_be_bytes());
        pos += 4;
        buf[pos..pos + 4].copy_from_slice(&self.compressed_len.to_be_bytes());
        buf
    }

    /// Parses a `BlockHeader` from a big-endian 48-byte slice.
    /// Each field is read via `data[i..j].try_into().unwrap()` — the length
    /// check against `HEADER_SIZE` guarantees the conversion never panics.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(FlowError::Corruption {
                file: "sst".into(),
                msg: "block header too short".into(),
            });
        }
        let magic = u32::from_be_bytes(data[..4].try_into().unwrap());
        if magic != BLOCK_MAGIC_LZ4 {
            return Err(FlowError::InvalidMagic {
                expected: BLOCK_MAGIC_LZ4,
                actual: magic,
            });
        }
        Ok(Self {
            num_records: u32::from_be_bytes(data[4..8].try_into().unwrap()),
            min_ts: i64::from_be_bytes(data[8..16].try_into().unwrap()),
            max_ts: i64::from_be_bytes(data[16..24].try_into().unwrap()),
            min_expire: i64::from_be_bytes(data[24..32].try_into().unwrap()),
            max_expire: i64::from_be_bytes(data[32..40].try_into().unwrap()),
            data_len: u32::from_be_bytes(data[40..44].try_into().unwrap()),
            compressed_len: u32::from_be_bytes(data[44..48].try_into().unwrap()),
        })
    }
}

fn decompress_block(data: &[u8], header: &BlockHeader) -> Result<Vec<u8>> {
    lz4_flex::block::decompress(data, header.data_len as usize)
        .map_err(|e| FlowError::Other(format!("lz4 decompress: {}", e)))
}

/// Encodes records into a compact binary buffer (big-endian, no compression).
/// Per-record layout: key_len(2) | key | ts(8) | expire_at(8) | op(1) |
///                    range_end_len(2) | range_end | val_len(4) | value
fn encode_records(records: &[InternalRecord]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(records.len() * 64);
    for rec in records {
        buf.extend_from_slice(&(rec.key.len() as u16).to_be_bytes());
        buf.extend_from_slice(&rec.key);
        buf.extend_from_slice(&rec.ts.to_be_bytes());
        buf.extend_from_slice(&rec.expire_at.to_be_bytes());
        buf.push(rec.op.to_u8());
        match &rec.range_end {
            Some(re) => {
                buf.extend_from_slice(&(re.len() as u16).to_be_bytes());
                buf.extend_from_slice(re);
            }
            None => {
                buf.extend_from_slice(&0u16.to_be_bytes());
            }
        }
        buf.extend_from_slice(&(rec.value.len() as u32).to_be_bytes());
        buf.extend_from_slice(&rec.value);
    }
    buf
}

/// Decodes `count` records from a big-endian byte slice.
/// Integer fields are read via `data[pos..pos+N].try_into().unwrap()` after
/// an explicit bounds check, so the unwrap is guaranteed safe.
fn decode_records(data: &[u8], count: u32) -> Result<Vec<InternalRecord>> {
    let mut records = Vec::with_capacity(count as usize);
    let mut pos = 0;
    for _ in 0..count {
        if pos + 2 > data.len() {
            break;
        }
        let key_len = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if pos + key_len > data.len() {
            break;
        }
        let key = data[pos..pos + key_len].to_vec();
        pos += key_len;

        if pos + 17 > data.len() {
            break;
        }
        let ts = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let expire_at = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let op = Op::from_u8(data[pos]);
        pos += 1;

        if pos + 2 > data.len() {
            break;
        }
        let re_len = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let range_end = if re_len > 0 {
            if pos + re_len > data.len() {
                break;
            }
            let s = data[pos..pos + re_len].to_vec();
            pos += re_len;
            Some(s)
        } else {
            None
        };

        if pos + 4 > data.len() {
            break;
        }
        let val_len = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + val_len > data.len() {
            break;
        }
        let value = data[pos..pos + val_len].to_vec();
        pos += val_len;

        records.push(InternalRecord {
            seq: 0,
            op,
            key,
            ts,
            expire_at,
            value,
            range_end,
        });
    }
    Ok(records)
}

pub(crate) struct SstWriter;

impl SstWriter {
    /// Encode an SST into an in-memory buffer.
    ///
    /// Returns `(data, total_bytes, block_infos, bloom)`.
    /// The caller is responsible for persisting `data` via a
    /// [`crate::storage::StorageBackend`].
    #[allow(clippy::too_many_arguments)]
    pub fn write_to_buf(
        records: &[InternalRecord],
        block_size: usize,
        bloom_bits_per_key: usize,
    ) -> Result<(Vec<u8>, u64, Vec<BlockInfo>, BloomFilter)> {
        let mut buf = Vec::with_capacity(records.len() * 64);
        let mut block_infos = Vec::new();
        let mut total_bytes: u64 = 0;

        let mut unique_keys: Vec<Vec<u8>> = Vec::new();
        let mut last_key: Option<Vec<u8>> = None;
        for rec in records {
            if last_key.as_deref() != Some(&rec.key) {
                unique_keys.push(rec.key.clone());
                last_key = Some(rec.key.clone());
            }
        }
        let bloom = BloomFilter::from_keys_with_bits(&unique_keys, bloom_bits_per_key);

        for chunk in records.chunks(block_size.max(1)) {
            let raw_data = encode_records(chunk);
            let data_len = raw_data.len() as u32;
            let compressed = lz4_flex::block::compress(&raw_data);
            let compressed_len = compressed.len() as u32;

            let min_ts = chunk.iter().map(|r| r.ts).min().unwrap_or(0);
            let max_ts = chunk.iter().map(|r| r.ts).max().unwrap_or(0);
            let min_expire = chunk.iter().map(|r| r.expire_at).min().unwrap_or(0);
            let max_expire = chunk.iter().map(|r| r.expire_at).max().unwrap_or(0);

            let first_key = chunk
                .iter()
                .map(|r| r.key.as_slice())
                .min()
                .map(|k| k.to_vec())
                .unwrap_or_default();
            let last_key = chunk
                .iter()
                .map(|r| r.key.as_slice())
                .max()
                .map(|k| k.to_vec())
                .unwrap_or_default();

            let header = BlockHeader {
                num_records: chunk.len() as u32,
                min_ts,
                max_ts,
                min_expire,
                max_expire,
                data_len,
                compressed_len,
            };

            let header_bytes = header.to_bytes();
            buf.extend_from_slice(&header_bytes);
            buf.extend_from_slice(&compressed);
            total_bytes += HEADER_SIZE as u64 + compressed.len() as u64;

            block_infos.push(BlockInfo {
                block_idx: block_infos.len() as u32,
                min_key: first_key,
                max_key: last_key,
                min_ts,
                max_ts,
                min_expire,
                max_expire,
            });
        }

        Ok((buf, total_bytes, block_infos, bloom))
    }

    /// Write an SST directly to a file path (legacy multi-file API).
    ///
    /// Creates the file, writes all blocks, flushes, and fsyncs.
    #[allow(dead_code)]
    #[allow(clippy::too_many_arguments)]
    pub fn write(
        path: &Path,
        records: &[InternalRecord],
        block_size: usize,
        bloom_bits_per_key: usize,
    ) -> Result<(u64, Vec<BlockInfo>, BloomFilter)> {
        let (data, total_bytes, block_infos, bloom) =
            Self::write_to_buf(records, block_size, bloom_bits_per_key)?;
        let mut file = std::fs::File::create(path)?;
        file.write_all(&data)?;
        file.flush()?;
        file.sync_all()?;
        Ok((total_bytes, block_infos, bloom))
    }
}

/// Streaming SST writer — writes records one-at-a-time without
/// materialising the full record set in memory.
///
/// Construction takes an *estimated* unique-key count for the bloom
/// filter pre-allocation. An over-estimate is harmless (lower FPR).
/// Records are buffered until a block is full, then compressed and
/// appended to the internal buffer. Call [`SstStreamWriter::finish`] to
/// flush the last block and obtain the serialized bytes + metadata.
pub(crate) struct SstStreamWriter {
    buf: Vec<u8>,
    block_size: usize,
    bloom: BloomFilter,
    current_block: Vec<InternalRecord>,
    block_infos: Vec<BlockInfo>,
    total_bytes: u64,
    finished: bool,
}

impl SstStreamWriter {
    pub fn new(
        block_size: usize,
        estimated_keys: usize,
        bloom_bits_per_key: usize,
    ) -> Result<Self> {
        let bloom = BloomFilter::with_bits_per_key(estimated_keys.max(1), bloom_bits_per_key);
        Ok(Self {
            buf: Vec::new(),
            block_size,
            bloom,
            current_block: Vec::with_capacity(block_size),
            block_infos: Vec::new(),
            total_bytes: 0,
            finished: false,
        })
    }

    /// Append a single record. Automatically flushes the current block
    /// when it reaches `block_size` records.
    pub fn write_record(&mut self, rec: &InternalRecord) -> Result<()> {
        self.bloom.insert(&rec.key);
        self.current_block.push(rec.clone());

        if self.current_block.len() >= self.block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    /// Flush the final block (if any) and return the serialized bytes
    /// plus accumulated metadata `(data, total_bytes, block_infos, bloom)`.
    pub fn finish(mut self) -> Result<(Vec<u8>, u64, Vec<BlockInfo>, BloomFilter)> {
        if !self.current_block.is_empty() {
            self.flush_block()?;
        }
        self.finished = true;
        Ok((
            std::mem::take(&mut self.buf),
            self.total_bytes,
            self.block_infos.clone(),
            self.bloom.clone(),
        ))
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        let raw_data = encode_records(&self.current_block);
        let data_len = raw_data.len() as u32;
        let compressed = lz4_flex::block::compress(&raw_data);
        let compressed_len = compressed.len() as u32;

        let min_ts = self.current_block.iter().map(|r| r.ts).min().unwrap_or(0);
        let max_ts = self.current_block.iter().map(|r| r.ts).max().unwrap_or(0);
        let min_expire = self
            .current_block
            .iter()
            .map(|r| r.expire_at)
            .min()
            .unwrap_or(0);
        let max_expire = self
            .current_block
            .iter()
            .map(|r| r.expire_at)
            .max()
            .unwrap_or(0);

        let first_key = self
            .current_block
            .iter()
            .map(|r| r.key.as_slice())
            .min()
            .map(|k| k.to_vec())
            .unwrap_or_default();
        let last_key = self
            .current_block
            .iter()
            .map(|r| r.key.as_slice())
            .max()
            .map(|k| k.to_vec())
            .unwrap_or_default();

        let header = BlockHeader {
            num_records: self.current_block.len() as u32,
            min_ts,
            max_ts,
            min_expire,
            max_expire,
            data_len,
            compressed_len,
        };

        let header_bytes = header.to_bytes();
        self.buf.extend_from_slice(&header_bytes);
        self.buf.extend_from_slice(&compressed);
        self.total_bytes += HEADER_SIZE as u64 + compressed.len() as u64;

        self.block_infos.push(BlockInfo {
            block_idx: self.block_infos.len() as u32,
            min_key: first_key,
            max_key: last_key,
            min_ts,
            max_ts,
            min_expire,
            max_expire,
        });

        self.current_block.clear();
        Ok(())
    }
}

impl Drop for SstStreamWriter {
    fn drop(&mut self) {
        if !self.finished && !self.current_block.is_empty() {
            if let Err(e) = self.flush_block() {
                tracing::error!("SstStreamWriter::drop: flush_block failed: {}", e);
            }
        }
    }
}

/// Backing store for an [`SstReader`].
enum SstBacking {
    /// Owns the file handle and mmap (one-SST-per-file mode).
    File {
        _file: std::fs::File,
        mmap: memmap2::Mmap,
    },
    /// Borrows a region of a shared mmap (single-file container mode).
    /// Multiple SstReaders can share the same `Arc<Mmap>` with different
    /// `base`/`len` ranges.
    Region {
        _mmap: Arc<memmap2::Mmap>,
        base: usize,
        len: usize,
    },
}

impl SstBacking {
    /// Returns the byte slice covering just this SST's data.
    fn data(&self) -> &[u8] {
        match self {
            SstBacking::File { mmap, .. } => &mmap[..],
            SstBacking::Region {
                _mmap, base, len, ..
            } => &_mmap[*base..*base + *len],
        }
    }
}

pub(crate) struct SstReader {
    backing: SstBacking,
    sst_id: u32,
    block_offsets: Vec<u64>,
}

impl SstReader {
    /// Open an SST from its own file path (multi-file mode).
    pub fn open(path: &Path, sst_id: u32, block_count: usize) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let total_size = file.metadata()?.len() as usize;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        Self::build(SstBacking::File { _file: file, mmap }, sst_id, block_count, total_size)
    }

    /// Open an SST from a shared mmap region (single-file mode).
    /// `base` is the byte offset within the mmap where this SST's data
    /// begins; `len` is the number of bytes in this SST's region.
    pub fn open_region(
        mmap: Arc<memmap2::Mmap>,
        base: usize,
        len: usize,
        sst_id: u32,
        block_count: usize,
    ) -> Result<Self> {
        Self::build(
            SstBacking::Region {
                _mmap: mmap,
                base,
                len,
            },
            sst_id,
            block_count,
            len,
        )
    }

    fn build(
        backing: SstBacking,
        sst_id: u32,
        block_count: usize,
        total_size: usize,
    ) -> Result<Self> {
        let data = backing.data();
        let mut offsets = Vec::with_capacity(block_count);
        let mut pos: usize = 0;
        while pos + HEADER_SIZE <= total_size {
            offsets.push(pos as u64);
            let header = BlockHeader::from_bytes(&data[pos..pos + HEADER_SIZE])?;
            pos += HEADER_SIZE + header.compressed_len as usize;
        }

        Ok(Self {
            backing,
            sst_id,
            block_offsets: offsets,
        })
    }

    pub fn read_block(&self, block_idx: u32, cache: Option<&BlockCache>) -> Result<SstBlock> {
        let cache_key = CacheKey {
            sst_id: self.sst_id,
            block_idx,
        };

        if let Some(cache) = cache
            && let Some(cached) = cache.get(&cache_key)
        {
            return Ok(SstBlock {
                records: (*cached).clone(),
            });
        }

        let raw_records = self.read_block_inner(block_idx)?;

        if let Some(cache) = cache {
            cache.insert(cache_key, raw_records.clone());
        }

        Ok(SstBlock {
            records: raw_records,
        })
    }

    pub fn read_block_arc(
        &self,
        block_idx: u32,
        cache: &BlockCache,
    ) -> Result<Arc<Vec<InternalRecord>>> {
        let cache_key = CacheKey {
            sst_id: self.sst_id,
            block_idx,
        };

        if let Some(cached) = cache.get(&cache_key) {
            return Ok(cached);
        }

        let raw_records = self.read_block_inner(block_idx)?;
        cache.insert(cache_key, raw_records.clone());
        Ok(Arc::new(raw_records))
    }

    fn read_block_inner(&self, block_idx: u32) -> Result<Vec<InternalRecord>> {
        let offset =
            self.block_offsets
                .get(block_idx as usize)
                .ok_or(FlowError::BlockNotFound {
                    sst_id: self.sst_id,
                    block_idx,
                })?;

        let data = self.backing.data();
        let pos = *offset as usize;
        if pos + HEADER_SIZE > data.len() {
            return Err(FlowError::Corruption {
                file: format!("sst_{}", self.sst_id),
                msg: format!("block {} out of bounds", block_idx),
            });
        }

        let header = BlockHeader::from_bytes(&data[pos..pos + HEADER_SIZE])?;
        let compressed_start = pos + HEADER_SIZE;
        let compressed_end = compressed_start + header.compressed_len as usize;
        if compressed_end > data.len() {
            return Err(FlowError::Corruption {
                file: format!("sst_{}", self.sst_id),
                msg: format!("block {} compressed data truncated", block_idx),
            });
        }

        let raw = decompress_block(&data[compressed_start..compressed_end], &header)?;
        decode_records(&raw, header.num_records)
    }

    pub fn read_block_cached(
        &self,
        block_idx: u32,
        cache: &BlockCache,
    ) -> Option<Arc<Vec<InternalRecord>>> {
        let cache_key = CacheKey {
            sst_id: self.sst_id,
            block_idx,
        };
        cache.get(&cache_key)
    }

    pub fn read_block_decompress(
        &self,
        block_idx: u32,
    ) -> Result<(BlockHeader, Vec<InternalRecord>)> {
        let offset =
            self.block_offsets
                .get(block_idx as usize)
                .ok_or(FlowError::BlockNotFound {
                    sst_id: self.sst_id,
                    block_idx,
                })?;

        let data = self.backing.data();
        let pos = *offset as usize;
        if pos + HEADER_SIZE > data.len() {
            return Err(FlowError::Corruption {
                file: format!("sst_{}", self.sst_id),
                msg: format!("block {} out of bounds", block_idx),
            });
        }

        let header = BlockHeader::from_bytes(&data[pos..pos + HEADER_SIZE])?;
        let compressed_start = pos + HEADER_SIZE;
        let compressed_end = compressed_start + header.compressed_len as usize;
        if compressed_end > data.len() {
            return Err(FlowError::Corruption {
                file: format!("sst_{}", self.sst_id),
                msg: format!("block {} compressed data truncated", block_idx),
            });
        }

        let raw = decompress_block(&data[compressed_start..compressed_end], &header)?;
        let records = decode_records(&raw, header.num_records)?;
        Ok((header, records))
    }

    pub fn block_count(&self) -> u32 {
        self.block_offsets.len() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;
    use tempfile::TempDir;

    fn make_records(n: usize) -> Vec<InternalRecord> {
        (0..n)
            .map(|i| {
                InternalRecord::from_record(
                    &Record {
                        key: format!("key_{:04}", i).into_bytes(),
                        ts: (i * 100) as i64,
                        expire_at: i64::MAX,
                        value: vec![1, 2, 3, 4],
                    },
                    i as u64,
                )
            })
            .collect()
    }

    #[test]
    fn test_sst_write_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.sst");
        let records = make_records(100);

        let (bytes, block_infos, _) = SstWriter::write(&path, &records, 10, 10).unwrap();
        assert!(bytes > 0);
        assert_eq!(block_infos.len(), 10);

        let reader = SstReader::open(&path, 1, block_infos.len()).unwrap();
        assert_eq!(reader.block_count(), 10);

        let block = reader.read_block(0, None).unwrap();
        assert_eq!(block.records.len(), 10);
        assert_eq!(block.records[0].key.as_slice(), b"key_0000");
    }

    #[test]
    fn test_sst_all_blocks_readable() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.sst");
        let records = make_records(50);

        let (_, block_infos, _) = SstWriter::write(&path, &records, 10, 10).unwrap();
        let reader = SstReader::open(&path, 1, block_infos.len()).unwrap();

        let mut all_records = Vec::new();
        for i in 0..reader.block_count() {
            let block = reader.read_block(i, None).unwrap();
            all_records.extend(block.records);
        }

        assert_eq!(all_records.len(), 50);
        for (i, rec) in all_records.iter().enumerate() {
            assert_eq!(rec.key, format!("key_{:04}", i).into_bytes());
        }
    }

    #[test]
    fn test_sst_block_metadata() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.sst");
        let records = make_records(20);

        let (_, block_infos, _) = SstWriter::write(&path, &records, 10, 10).unwrap();
        assert_eq!(block_infos.len(), 2);

        assert_eq!(block_infos[0].min_key, b"key_0000");
        assert_eq!(block_infos[0].max_key, b"key_0009");
        assert_eq!(block_infos[0].min_ts, 0);
        assert_eq!(block_infos[0].max_ts, 900);

        assert_eq!(block_infos[1].min_key, b"key_0010");
        assert_eq!(block_infos[1].max_key, b"key_0019");
    }

    #[test]
    fn test_sst_compression() {
        let dir = TempDir::new().unwrap();
        let records: Vec<InternalRecord> = (0..100)
            .map(|i| {
                InternalRecord::from_record(
                    &Record {
                        key: b"same_key".to_vec(),
                        ts: i,
                        expire_at: i64::MAX,
                        value: vec![0u8; 100],
                    },
                    i as u64,
                )
            })
            .collect();

        let path = dir.path().join("compressed.sst");
        let (bytes, _, _) = SstWriter::write(&path, &records, 100, 10).unwrap();

        let raw_size: usize = records.iter().map(|r| r.estimated_size()).sum();
        assert!(bytes < raw_size as u64);
    }

    #[test]
    fn test_sst_stream_writer_empty() {
        let writer = SstStreamWriter::new(10, 0, 10).unwrap();
        let (data, bytes, blocks, _bloom) = writer.finish().unwrap();
        assert!(data.is_empty(), "empty writer produces zero bytes");
        assert_eq!(bytes, 0, "empty writer produces zero bytes");
        assert!(blocks.is_empty());
    }

    #[test]
    fn test_sst_stream_writer_single_block() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("single.sst");
        let records = make_records(5);
        let mut writer = SstStreamWriter::new(100, 5, 10).unwrap();
        for rec in &records {
            writer.write_record(rec).unwrap();
        }
        let (data, bytes, blocks, bloom) = writer.finish().unwrap();
        assert!(bytes > 0, "should have written data");
        assert_eq!(blocks.len(), 1, "all records → single block");
        assert_eq!(data.len() as u64, bytes);

        // Persist and verify the SST can be read back.
        std::fs::write(&path, &data).unwrap();
        let reader = SstReader::open(&path, 1, blocks.len()).unwrap();
        assert_eq!(reader.block_count(), 1);
        let block = reader.read_block(0, None).unwrap();
        assert_eq!(block.records.len(), 5);
        assert_eq!(block.records[0].key, b"key_0000");
        // The bloom must recognise the inserted keys.
        for rec in &records {
            assert!(
                bloom.may_contain(&rec.key),
                "bloom must contain {}",
                String::from_utf8_lossy(&rec.key)
            );
        }
    }

    #[test]
    fn test_sst_stream_writer_multi_block() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("multi.sst");
        let records = make_records(25);
        // block_size = 10 → 3 blocks (10 + 10 + 5)
        let mut writer = SstStreamWriter::new(10, 25, 10).unwrap();
        for rec in &records {
            writer.write_record(rec).unwrap();
        }
        let (data, bytes, blocks, bloom) = writer.finish().unwrap();
        assert!(bytes > 0);
        assert_eq!(blocks.len(), 3);

        // Verify full read-back.
        std::fs::write(&path, &data).unwrap();
        let reader = SstReader::open(&path, 1, blocks.len()).unwrap();
        let mut all_records = Vec::new();
        for i in 0..reader.block_count() {
            let block = reader.read_block(i, None).unwrap();
            all_records.extend(block.records);
        }
        assert_eq!(all_records.len(), 25);
        for (i, rec) in all_records.iter().enumerate() {
            assert_eq!(rec.key, format!("key_{:04}", i).into_bytes());
        }
        // Bloom sanity.
        for rec in &records {
            assert!(bloom.may_contain(&rec.key));
        }
    }

    #[test]
    fn test_sst_stream_writer_roundtrip_via_reader() {
        // Write with stream writer, read with SstReader → must match
        // the same records written via the batch SstWriter.
        let dir = TempDir::new().unwrap();
        let s_path = dir.path().join("stream.sst");
        let b_path = dir.path().join("batch.sst");

        let records = make_records(50);
        // Stream write
        let mut sw = SstStreamWriter::new(10, 50, 10).unwrap();
        for rec in &records {
            sw.write_record(rec).unwrap();
        }
        let (s_data, _, s_blocks, _) = sw.finish().unwrap();
        std::fs::write(&s_path, &s_data).unwrap();
        // Batch write
        let (_, b_blocks, _) = SstWriter::write(&b_path, &records, 10, 10).unwrap();

        assert_eq!(s_blocks.len(), b_blocks.len());

        // Both files must produce the same records.
        let s_reader = SstReader::open(&s_path, 1, s_blocks.len()).unwrap();
        let b_reader = SstReader::open(&b_path, 1, b_blocks.len()).unwrap();

        for idx in 0..s_reader.block_count() {
            let s_block = s_reader.read_block(idx, None).unwrap();
            let b_block = b_reader.read_block(idx, None).unwrap();
            assert_eq!(
                s_block.records.len(),
                b_block.records.len(),
                "block {} record count mismatch",
                idx
            );
            for (s_rec, b_rec) in s_block.records.iter().zip(b_block.records.iter()) {
                assert_eq!(s_rec.key, b_rec.key);
                assert_eq!(s_rec.ts, b_rec.ts);
                assert_eq!(s_rec.value, b_rec.value);
            }
        }
    }

    #[test]
    fn test_sst_reader_open_region() {
        // Verify SstReader::open_region works for a byte range within a larger buffer.
        let records = make_records(30);
        let (data, _, block_infos, _) = SstWriter::write_to_buf(&records, 10, 10).unwrap();

        // Simulate a container file: header padding + SST data + trailing junk
        let padding = 128usize;
        let trailing = 64usize;
        let mut container = Vec::with_capacity(padding + data.len() + trailing);
        container.extend_from_slice(&vec![0u8; padding]);
        container.extend_from_slice(&data);
        container.extend_from_slice(&vec![0xFFu8; trailing]);

        // Write container to a temp file and mmap it
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("container.db");
        std::fs::write(&path, &container).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };
        let mmap = Arc::new(mmap);

        // Open a reader pointing at just the SST region
        let reader =
            SstReader::open_region(mmap, padding, data.len(), 42, block_infos.len()).unwrap();
        assert_eq!(reader.block_count(), block_infos.len() as u32);

        // Read all blocks and verify
        let mut all = Vec::new();
        for i in 0..reader.block_count() {
            all.extend(reader.read_block(i, None).unwrap().records);
        }
        assert_eq!(all.len(), 30);
        assert_eq!(all[0].key, b"key_0000");
        assert_eq!(all[29].key, b"key_0029");
    }
}
