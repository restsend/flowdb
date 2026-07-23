use crate::error::{FlowError, Result};
use crate::sstable::SstReader;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Abstracts how SST bytes are persisted and retrieved.
///
/// Two implementations exist:
/// - [`MultiFileStorage`]: one `.sst` file per SST (the classic layout).
/// - [`SingleFileStorage`]: all SSTs appended inside a single `.db`
///   container file.
///
/// All methods are `&self`; implementations must be `Send + Sync`.
/// Durability (fsync) is the responsibility of each implementation.
pub(crate) trait StorageBackend: Send + Sync {
    /// Persist `data` as the SST identified by `sst_id`.
    ///
    /// The implementation must guarantee that a crash after a successful
    /// return leaves the SST fully durable on disk.
    fn write_sst(&self, sst_id: u32, data: &[u8]) -> Result<()>;

    /// Open a reader for the given SST.
    ///
    /// `block_count_hint` is a capacity hint for the internal block-offset
    /// vector (0 means unknown — the reader will scan the SST to discover
    /// block boundaries).
    fn open_reader(&self, sst_id: u32, block_count_hint: usize) -> Result<SstReader>;

    /// Delete an SST from storage.  Idempotent — deleting a non-existent
    /// SST returns `Ok(())`.
    fn delete_sst(&self, sst_id: u32) -> Result<()>;

    /// Returns `true` if the SST exists in storage.
    fn sst_exists(&self, sst_id: u32) -> bool;
}

// ── MultiFileStorage ─────────────────────────────────────────────────

/// Classic one-file-per-SST backend.
///
/// Layout: `{data_dir}/SST/{sst_id:09}.sst`
/// Writes are atomic: data is written to a `.sst.tmp` file, fsync'd,
/// then renamed to the final `.sst` path, followed by a directory fsync.
pub(crate) struct MultiFileStorage {
    sst_dir: PathBuf,
}

impl MultiFileStorage {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            sst_dir: data_dir.join("SST"),
        }
    }

    #[inline]
    fn sst_path(&self, sst_id: u32) -> PathBuf {
        self.sst_dir.join(format!("{:09}.sst", sst_id))
    }

    #[inline]
    fn tmp_path(&self, sst_id: u32) -> PathBuf {
        self.sst_dir.join(format!("{:09}.sst.tmp", sst_id))
    }
}

impl StorageBackend for MultiFileStorage {
    fn write_sst(&self, sst_id: u32, data: &[u8]) -> Result<()> {
        std::fs::create_dir_all(&self.sst_dir)?;
        let sst_path = self.sst_path(sst_id);
        let tmp_path = self.tmp_path(sst_id);

        // Write to tmp file.
        {
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(data)?;
            file.flush()?;
            file.sync_all()?;
        }

        // Atomic rename.
        std::fs::rename(&tmp_path, &sst_path)?;

        // Sync directory so the rename is durable.
        #[cfg(not(target_os = "windows"))]
        {
            let dir_file = std::fs::File::open(&self.sst_dir)?;
            dir_file.sync_all()?;
        }

        Ok(())
    }

    fn open_reader(&self, sst_id: u32, block_count_hint: usize) -> Result<SstReader> {
        let path = self.sst_path(sst_id);
        if !path.exists() {
            return Err(FlowError::Other(format!("sst {} not found", sst_id)));
        }
        SstReader::open(&path, sst_id, block_count_hint)
    }

    fn delete_sst(&self, sst_id: u32) -> Result<()> {
        let path = self.sst_path(sst_id);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    fn sst_exists(&self, sst_id: u32) -> bool {
        self.sst_path(sst_id).exists()
    }
}

// ── SingleFileStorage ───────────────────────────────────────────────

/// Magic header identifying a FlowDB single-file container.
const DB_MAGIC: &[u8; 8] = b"FLOWDB01";
/// Fixed-size header at the start of the `.db` file.
const DB_HEADER_SIZE: usize = 4096;

/// Record type tags inside the container.
const REC_SST: u8 = 0;
const REC_TOMBSTONE: u8 = 1;
const REC_CHECKPOINT: u8 = 2;

/// Description of where an SST's bytes live inside the container.
#[derive(Clone, Copy)]
struct SstRegion {
    offset: u64,
    len: u64,
}

/// Single-file container backend.
///
/// All SSTs live inside one `.db` file as a sequence of length-prefixed
/// records:
///
/// ```text
/// [HEADER — 4096 bytes: magic + version + padding]
/// [RECORDS — append-only]
///   record = [len: u64 BE][type: u8][payload: len-1 bytes]
///   type 0 (SST):  payload = [sst_id: u32 BE][raw sst bytes]
///   type 1 (Tomb): payload = [sst_id: u32 BE]
///   type 2 (Chkp): payload = serialized offset table
/// ```
///
/// Writes append to the file then fsync. Deletes append a tombstone
/// record. Recovery scans records from the header to EOF, rebuilding the
/// live SST offset table. Truncated final records are silently dropped
/// (crash safety).
pub(crate) struct SingleFileStorage {
    db_path: PathBuf,
    /// Guards all mutations (append + offset table rebuild).
    state: parking_lot::Mutex<SingleFileState>,
}

struct SingleFileState {
    /// Open file handle for appending.
    file: Option<std::fs::File>,
    /// Current write position (= file length).
    write_pos: u64,
    /// Maps live sst_id → region in the file.
    live: std::collections::HashMap<u32, SstRegion>,
}

impl SingleFileStorage {
    /// Open (or create) the `.db` container at `db_path`.
    pub fn open(db_path: &Path) -> Result<Self> {
        let exists = db_path.exists();
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(db_path)?;

        let write_pos;
        let live;

        if !exists || file.metadata()?.len() == 0 {
            // Brand-new file: write header.
            let mut hdr = vec![0u8; DB_HEADER_SIZE];
            hdr[..8].copy_from_slice(DB_MAGIC);
            hdr[8..12].copy_from_slice(&1u32.to_be_bytes()); // version
            file.write_all(&hdr)?;
            file.flush()?;
            file.sync_all()?;
            write_pos = DB_HEADER_SIZE as u64;
            live = std::collections::HashMap::new();
        } else {
            // Existing file: verify magic.
            let mut magic = [0u8; 8];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut magic)?;
            if &magic != DB_MAGIC {
                return Err(FlowError::Corruption {
                    file: db_path.display().to_string(),
                    msg: "bad magic in single-file container".into(),
                });
            }
            write_pos = file.metadata()?.len();
            live = Self::scan(&mut file, write_pos)?;
        }

        // Seek to end for appending.
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            db_path: db_path.to_path_buf(),
            state: parking_lot::Mutex::new(SingleFileState {
                file: Some(file),
                write_pos,
                live,
            }),
        })
    }

    /// Scan records from `DB_HEADER_SIZE` to `file_len` and build the
    /// live SST offset table.
    fn scan(
        file: &mut std::fs::File,
        file_len: u64,
    ) -> Result<std::collections::HashMap<u32, SstRegion>> {
        let mut live: std::collections::HashMap<u32, SstRegion> = std::collections::HashMap::new();
        let mut pos = DB_HEADER_SIZE as u64;

        while pos + 9 <= file_len {
            // Read [len: u64][type: u8]
            file.seek(SeekFrom::Start(pos))?;
            let mut hdr_buf = [0u8; 9];
            let n = file.read(&mut hdr_buf)?;
            if n < 9 {
                break; // truncated header
            }
            let len = u64::from_be_bytes(hdr_buf[0..8].try_into().unwrap());
            let rec_type = hdr_buf[8];

            // len includes the type byte but NOT the 8-byte length field.
            // Total record bytes on disk = 8 (len field) + len (type + payload).
            let record_on_disk = 8 + len;
            if pos + record_on_disk > file_len {
                break; // truncated payload — crash during write
            }

            let payload_start = pos + 9;
            match rec_type {
                REC_SST => {
                    // payload = [sst_id: u32 BE][raw sst bytes]
                    if len < 5 {
                        break;
                    }
                    file.seek(SeekFrom::Start(payload_start))?;
                    let mut id_buf = [0u8; 4];
                    file.read_exact(&mut id_buf)?;
                    let sst_id = u32::from_be_bytes(id_buf);
                    let data_offset = payload_start + 4;
                    let data_len = len - 5; // subtract type(1) + sst_id(4)
                    live.insert(sst_id, SstRegion { offset: data_offset, len: data_len });
                }
                REC_TOMBSTONE => {
                    if len < 5 {
                        break;
                    }
                    file.seek(SeekFrom::Start(payload_start))?;
                    let mut id_buf = [0u8; 4];
                    file.read_exact(&mut id_buf)?;
                    let sst_id = u32::from_be_bytes(id_buf);
                    live.remove(&sst_id);
                }
                REC_CHECKPOINT => {
                    // Checkpoint replaces the entire live table.
                    live = Self::deserialize_checkpoint(file, payload_start, len - 1)?;
                }
                _ => {
                    // Unknown record type — stop scanning (forward compat).
                    break;
                }
            }

            pos += record_on_disk;
        }

        Ok(live)
    }

    /// Deserialize a checkpoint payload.
    fn deserialize_checkpoint(
        file: &mut std::fs::File,
        payload_start: u64,
        payload_len: u64,
    ) -> Result<std::collections::HashMap<u32, SstRegion>> {
        file.seek(SeekFrom::Start(payload_start))?;
        let mut buf = vec![0u8; payload_len as usize];
        file.read_exact(&mut buf)?;

        if buf.len() < 4 {
            return Err(FlowError::Corruption {
                file: "checkpoint".into(),
                msg: "checkpoint too short".into(),
            });
        }
        let count = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
        let mut live = std::collections::HashMap::with_capacity(count);
        let mut pos = 4;
        for _ in 0..count {
            if pos + 20 > buf.len() {
                return Err(FlowError::Corruption {
                    file: "checkpoint".into(),
                    msg: "checkpoint entry truncated".into(),
                });
            }
            let sst_id = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
            let offset = u64::from_be_bytes(buf[pos + 4..pos + 12].try_into().unwrap());
            let len = u64::from_be_bytes(buf[pos + 12..pos + 20].try_into().unwrap());
            live.insert(sst_id, SstRegion { offset, len });
            pos += 20;
        }
        Ok(live)
    }

    /// Write a checkpoint record capturing the current live SST table.
    /// This bounds the scan time on the next open.
    #[allow(dead_code)]
    fn write_checkpoint(&self) -> Result<()> {
        let mut state = self.state.lock();

        // Serialize: [count: u32][(sst_id: u32, offset: u64, len: u64) × count]
        let mut payload = Vec::with_capacity(4 + state.live.len() * 20);
        payload.extend_from_slice(&(state.live.len() as u32).to_be_bytes());
        let mut entries: Vec<(u32, u64, u64)> = state
            .live
            .iter()
            .map(|(&id, r)| (id, r.offset, r.len))
            .collect();
        entries.sort();
        for (id, offset, len) in entries {
            payload.extend_from_slice(&id.to_be_bytes());
            payload.extend_from_slice(&offset.to_be_bytes());
            payload.extend_from_slice(&len.to_be_bytes());
        }

        let record_len = 1 + payload.len() as u64; // type byte + payload
        let mut record = Vec::with_capacity(8 + record_len as usize);
        record.extend_from_slice(&record_len.to_be_bytes());
        record.push(REC_CHECKPOINT);
        record.extend_from_slice(&payload);

        let pos = state.write_pos;
        state.file.as_mut().unwrap().seek(SeekFrom::Start(pos))?;
        state.file.as_mut().unwrap().write_all(&record)?;
        state.file.as_mut().unwrap().flush()?;
        state.file.as_mut().unwrap().sync_all()?;
        state.write_pos += record.len() as u64;
        Ok(())
    }

    /// Rewrite the `.db` file, keeping only live SSTs and a single
    /// checkpoint.  Call this when dead space exceeds a threshold.
    #[allow(dead_code)]
    pub fn compact_file(&self) -> Result<()> {
        let mut state = self.state.lock();

        // Read all live SSTs into memory (they're typically small after
        // compaction; for very large databases a streaming approach would
        // be used instead).
        let mut entries: Vec<(u32, SstRegion)> = state
            .live
            .iter()
            .map(|(&id, &r)| (id, r))
            .collect();
        entries.sort_by_key(|(id, _)| *id);

        let tmp_path = self.db_path.with_extension("db.tmp");
        {
            let mut tmp = std::fs::File::create(&tmp_path)?;
            // Header
            let mut hdr = vec![0u8; DB_HEADER_SIZE];
            hdr[..8].copy_from_slice(DB_MAGIC);
            hdr[8..12].copy_from_slice(&1u32.to_be_bytes());
            tmp.write_all(&hdr)?;

            let mut new_live = std::collections::HashMap::new();
            let mut pos = DB_HEADER_SIZE as u64;

            for (sst_id, region) in &entries {
                // Read SST data from the old file.
                let mut data = vec![0u8; region.len as usize];
                state.file.as_mut().unwrap().seek(SeekFrom::Start(region.offset))?;
                state.file.as_mut().unwrap().read_exact(&mut data)?;

                // Write SST record to the new file.
                let record_len = 1 + 4 + data.len() as u64; // type + sst_id + data
                tmp.write_all(&record_len.to_be_bytes())?;
                tmp.write_all(&[REC_SST])?;
                tmp.write_all(&sst_id.to_be_bytes())?;
                let data_offset = pos + 8 + 1 + 4; // pos + len(8) + type(1) + sst_id(4)
                tmp.write_all(&data)?;
                new_live.insert(
                    *sst_id,
                    SstRegion {
                        offset: data_offset,
                        len: data.len() as u64,
                    },
                );
                pos += 8 + record_len;
            }

            // Write checkpoint
            let mut cp_payload = Vec::with_capacity(4 + new_live.len() * 20);
            cp_payload.extend_from_slice(&(new_live.len() as u32).to_be_bytes());
            let mut cp_entries: Vec<(u32, u64, u64)> = new_live
                .iter()
                .map(|(&id, r)| (id, r.offset, r.len))
                .collect();
            cp_entries.sort();
            for (id, offset, len) in cp_entries {
                cp_payload.extend_from_slice(&id.to_be_bytes());
                cp_payload.extend_from_slice(&offset.to_be_bytes());
                cp_payload.extend_from_slice(&len.to_be_bytes());
            }
            let cp_len = 1 + cp_payload.len() as u64;
            tmp.write_all(&cp_len.to_be_bytes())?;
            tmp.write_all(&[REC_CHECKPOINT])?;
            tmp.write_all(&cp_payload)?;
            pos += 8 + cp_len;

            tmp.flush()?;
            tmp.sync_all()?;
            state.live = new_live;
            state.write_pos = pos;
        }

        // Close the old file handle before renaming so Windows doesn't block it.
        state.file = None;

        // Atomic swap.
        std::fs::rename(&tmp_path, &self.db_path)?;

        // Reopen the renamed file.
        let new_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.db_path)?;
        state.file = Some(new_file);

        Ok(())
    }

    /// Returns the number of live SSTs, live SST bytes, and dead-space bytes
    /// (space occupied by deleted/overwritten SSTs + tombstone records).
    /// Record framing and checkpoint metadata are not counted as dead space.
    #[allow(dead_code)]
    pub fn stats(&self) -> (usize, u64, u64) {
        let state = self.state.lock();
        let live_count = state.live.len();
        let live_data_bytes: u64 = state.live.values().map(|r| r.len).sum();
        // Each live SST occupies: 8 (len) + 1 (type) + 4 (sst_id) + data in the file.
        let live_with_framing = live_data_bytes + (13 * live_count as u64);
        let dead = state
            .write_pos
            .saturating_sub(DB_HEADER_SIZE as u64)
            .saturating_sub(live_with_framing);
        (live_count, live_data_bytes, dead)
    }
}

impl StorageBackend for SingleFileStorage {
    fn write_sst(&self, sst_id: u32, data: &[u8]) -> Result<()> {
        let mut state = self.state.lock();

        // Record layout: [len: u64][type: u8][sst_id: u32][data]
        // len = 1 (type) + 4 (sst_id) + data.len()
        let record_len = 1u64 + 4 + data.len() as u64;
        let mut record = Vec::with_capacity(8 + record_len as usize);
        record.extend_from_slice(&record_len.to_be_bytes());
        record.push(REC_SST);
        record.extend_from_slice(&sst_id.to_be_bytes());
        record.extend_from_slice(data);

        let pos = state.write_pos;
        state.file.as_mut().unwrap().seek(SeekFrom::Start(pos))?;
        state.file.as_mut().unwrap().write_all(&record)?;
        state.file.as_mut().unwrap().flush()?;
        state.file.as_mut().unwrap().sync_all()?;

        let data_offset = pos + 8 + 1 + 4; // len(8) + type(1) + sst_id(4)
        state.write_pos += record.len() as u64;
        state.live.insert(
            sst_id,
            SstRegion {
                offset: data_offset,
                len: data.len() as u64,
            },
        );

        Ok(())
    }

    fn open_reader(&self, sst_id: u32, block_count_hint: usize) -> Result<SstReader> {
        let state = self.state.lock();
        let region = state.live.get(&sst_id).ok_or_else(|| {
            FlowError::Other(format!("sst {} not found in single-file container", sst_id))
        })?;

        // Create a fresh mmap of the entire .db file.
        // mmap is O(1) — it just registers a VMA; pages are loaded lazily.
        // Multiple readers each get their own mapping; the OS page cache is shared.
        let file = std::fs::File::open(&self.db_path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };

        SstReader::open_region(
            Arc::new(mmap),
            region.offset as usize,
            region.len as usize,
            sst_id,
            block_count_hint,
        )
    }

    fn delete_sst(&self, sst_id: u32) -> Result<()> {
        let mut state = self.state.lock();

        // Idempotent: if not live, do nothing.
        if !state.live.contains_key(&sst_id) {
            return Ok(());
        }

        // Append tombstone record: [len: u64][type: u8][sst_id: u32]
        let record_len = 1u64 + 4; // type + sst_id
        let mut record = Vec::with_capacity(8 + record_len as usize);
        record.extend_from_slice(&record_len.to_be_bytes());
        record.push(REC_TOMBSTONE);
        record.extend_from_slice(&sst_id.to_be_bytes());

        let pos = state.write_pos;
        state.file.as_mut().unwrap().seek(SeekFrom::Start(pos))?;
        state.file.as_mut().unwrap().write_all(&record)?;
        state.file.as_mut().unwrap().flush()?;
        state.file.as_mut().unwrap().sync_all()?;
        state.write_pos += record.len() as u64;
        state.live.remove(&sst_id);

        Ok(())
    }

    fn sst_exists(&self, sst_id: u32) -> bool {
        self.state.lock().live.contains_key(&sst_id)
    }
}

/// Convenience constructor: picks the right backend.
pub(crate) fn open_storage(
    data_dir: &Path,
    single_file: bool,
) -> Result<Arc<dyn StorageBackend>> {
    if single_file {
        let db_path = data_dir.join("flow.db");
        Ok(Arc::new(SingleFileStorage::open(&db_path)?))
    } else {
        Ok(Arc::new(MultiFileStorage::new(data_dir)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{InternalRecord, Record};
    use crate::record::CompressionAlgorithm;
use crate::sstable::SstWriter;
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

    // ── MultiFileStorage tests ──────────────────────────────────

    #[test]
    fn test_multi_file_write_open_delete() {
        let dir = TempDir::new().unwrap();
        let storage = MultiFileStorage::new(dir.path());

        let records = make_records(30);
        let (data, _, blocks, _) =
            SstWriter::write_to_buf(&records, 10, 10, CompressionAlgorithm::Lz4).unwrap();

        assert!(!storage.sst_exists(1));
        storage.write_sst(1, &data).unwrap();
        assert!(storage.sst_exists(1));

        let reader = storage.open_reader(1, blocks.len()).unwrap();
        assert_eq!(reader.block_count(), blocks.len() as u32);
        let block = reader.read_block(0, None).unwrap();
        assert_eq!(block.records.len(), 10);
        assert_eq!(block.records[0].key, b"key_0000");

        storage.delete_sst(1).unwrap();
        assert!(!storage.sst_exists(1));

        // Idempotent delete.
        storage.delete_sst(1).unwrap();
    }

    #[test]
    fn test_multi_file_open_missing_returns_error() {
        let dir = TempDir::new().unwrap();
        let storage = MultiFileStorage::new(dir.path());
        let result = storage.open_reader(999, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_multi_file_overwrite() {
        let dir = TempDir::new().unwrap();
        let storage = MultiFileStorage::new(dir.path());

        // Write SST #1 with 10 records
        let records1 = make_records(10);
        let (data1, _, _, _) =
            SstWriter::write_to_buf(&records1, 10, 10, CompressionAlgorithm::Lz4).unwrap();
        storage.write_sst(1, &data1).unwrap();

        // Overwrite with different data (20 records)
        let records2 = make_records(20);
        let (data2, _, blocks2, _) =
            SstWriter::write_to_buf(&records2, 10, 10, CompressionAlgorithm::Lz4).unwrap();
        storage.write_sst(1, &data2).unwrap();

        let reader = storage.open_reader(1, blocks2.len()).unwrap();
        assert_eq!(reader.block_count(), 2); // 20 records / 10 per block = 2 blocks
    }

    #[test]
    fn test_open_storage_creates_multi_file() {
        let dir = TempDir::new().unwrap();
        let storage = open_storage(dir.path(), false).unwrap();
        assert!(!storage.sst_exists(0));
    }

    // ── SingleFileStorage tests ─────────────────────────────────

    fn make_db_path(dir: &Path) -> PathBuf {
        dir.join("test.db")
    }

    #[test]
    fn test_single_file_create_and_open() {
        let dir = TempDir::new().unwrap();
        let db_path = make_db_path(dir.path());

        let storage = SingleFileStorage::open(&db_path).unwrap();
        assert_eq!(storage.stats().0, 0); // 0 live SSTs

        // Reopen — should find the existing file.
        drop(storage);
        let storage2 = SingleFileStorage::open(&db_path).unwrap();
        assert_eq!(storage2.stats().0, 0);
    }

    #[test]
    fn test_single_file_write_open_delete() {
        let dir = TempDir::new().unwrap();
        let db_path = make_db_path(dir.path());
        let storage = SingleFileStorage::open(&db_path).unwrap();

        let records = make_records(30);
        let (data, _, blocks, _) = SstWriter::write_to_buf(&records, 10, 10, CompressionAlgorithm::Lz4).unwrap();

        assert!(!storage.sst_exists(1));
        storage.write_sst(1, &data).unwrap();
        assert!(storage.sst_exists(1));
        assert_eq!(storage.stats().0, 1);

        let reader = storage.open_reader(1, blocks.len()).unwrap();
        assert_eq!(reader.block_count(), blocks.len() as u32);
        let block = reader.read_block(0, None).unwrap();
        assert_eq!(block.records.len(), 10);
        assert_eq!(block.records[0].key, b"key_0000");

        // Read all blocks
        let mut all = Vec::new();
        for i in 0..reader.block_count() {
            all.extend(reader.read_block(i, None).unwrap().records);
        }
        assert_eq!(all.len(), 30);

        storage.delete_sst(1).unwrap();
        assert!(!storage.sst_exists(1));
        assert_eq!(storage.stats().0, 0);

        // Idempotent delete.
        storage.delete_sst(1).unwrap();
    }

    #[test]
    fn test_single_file_persistence_across_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = make_db_path(dir.path());

        let records = make_records(50);
        let (data, _, blocks, _) = SstWriter::write_to_buf(&records, 10, 10, CompressionAlgorithm::Lz4).unwrap();

        {
            let storage = SingleFileStorage::open(&db_path).unwrap();
            storage.write_sst(1, &data).unwrap();
            storage.write_sst(2, &data).unwrap();
            assert_eq!(storage.stats().0, 2);
        }

        // Reopen and verify SSTs are still there.
        let storage = SingleFileStorage::open(&db_path).unwrap();
        assert!(storage.sst_exists(1));
        assert!(storage.sst_exists(2));
        assert_eq!(storage.stats().0, 2);

        let reader = storage.open_reader(1, blocks.len()).unwrap();
        let block = reader.read_block(0, None).unwrap();
        assert_eq!(block.records.len(), 10);
    }

    #[test]
    fn test_single_file_delete_persistence() {
        let dir = TempDir::new().unwrap();
        let db_path = make_db_path(dir.path());

        let records = make_records(20);
        let (data, _, _, _) = SstWriter::write_to_buf(&records, 10, 10, CompressionAlgorithm::Lz4).unwrap();

        {
            let storage = SingleFileStorage::open(&db_path).unwrap();
            storage.write_sst(1, &data).unwrap();
            storage.write_sst(2, &data).unwrap();
            storage.delete_sst(1).unwrap();
        }

        let storage = SingleFileStorage::open(&db_path).unwrap();
        assert!(!storage.sst_exists(1));
        assert!(storage.sst_exists(2));
    }

    #[test]
    fn test_single_file_overwrite_sst() {
        let dir = TempDir::new().unwrap();
        let db_path = make_db_path(dir.path());
        let storage = SingleFileStorage::open(&db_path).unwrap();

        // Write SST 1 with 10 records
        let r1 = make_records(10);
        let (d1, _, _, _) =
            SstWriter::write_to_buf(&r1, 10, 10, CompressionAlgorithm::Lz4).unwrap();
        storage.write_sst(1, &d1).unwrap();

        // Overwrite SST 1 with 20 records
        let r2 = make_records(20);
        let (d2, _, blocks2, _) =
            SstWriter::write_to_buf(&r2, 10, 10, CompressionAlgorithm::Lz4).unwrap();
        storage.write_sst(1, &d2).unwrap();

        let reader = storage.open_reader(1, blocks2.len()).unwrap();
        assert_eq!(reader.block_count(), 2);

        // Reopen — the latest version should be visible.
        drop(storage);
        let storage = SingleFileStorage::open(&db_path).unwrap();
        let reader = storage.open_reader(1, blocks2.len()).unwrap();
        assert_eq!(reader.block_count(), 2);
    }

    #[test]
    fn test_single_file_compact() {
        let dir = TempDir::new().unwrap();
        let db_path = make_db_path(dir.path());
        let storage = SingleFileStorage::open(&db_path).unwrap();

        // Write 5 SSTs, delete 3.
        for id in 1..=5u32 {
            let records = make_records(20);
            let (data, _, _, _) = SstWriter::write_to_buf(&records, 10, 10, CompressionAlgorithm::Lz4).unwrap();
            storage.write_sst(id, &data).unwrap();
        }
        storage.delete_sst(1).unwrap();
        storage.delete_sst(2).unwrap();
        storage.delete_sst(3).unwrap();

        let (live, _, dead) = storage.stats();
        assert_eq!(live, 2);
        assert!(dead > 0, "should have dead space from deleted SSTs");

        let file_size_before = std::fs::metadata(&db_path).unwrap().len();

        // Compact the file.
        storage.compact_file().unwrap();

        let file_size_after = std::fs::metadata(&db_path).unwrap().len();
        assert!(
            file_size_after < file_size_before,
            "file should shrink after compaction"
        );

        // Live SSTs should still be readable.
        let reader = storage.open_reader(4, 0).unwrap();
        assert!(reader.block_count() > 0);
        let block = reader.read_block(0, None).unwrap();
        assert_eq!(block.records[0].key, b"key_0000");

        let (_, live_bytes, dead) = storage.stats();
        // After compaction, dead space is minimal (just checkpoint metadata).
        assert!(
            dead < 200,
            "minimal dead space after compaction, got {}",
            dead
        );
        assert!(live_bytes > 0);

        // Reopen after compaction.
        drop(storage);
        let storage = SingleFileStorage::open(&db_path).unwrap();
        assert!(storage.sst_exists(4));
        assert!(storage.sst_exists(5));
        assert!(!storage.sst_exists(1));
    }

    #[test]
    fn test_single_file_checkpoint() {
        let dir = TempDir::new().unwrap();
        let db_path = make_db_path(dir.path());

        // Write some SSTs.
        {
            let storage = SingleFileStorage::open(&db_path).unwrap();
            for id in 1..=5u32 {
                let records = make_records(20);
                let (data, _, _, _) = SstWriter::write_to_buf(&records, 10, 10, CompressionAlgorithm::Lz4).unwrap();
                storage.write_sst(id, &data).unwrap();
            }
            // Delete some.
            storage.delete_sst(2).unwrap();
            storage.delete_sst(4).unwrap();

            // Write checkpoint.
            storage.write_checkpoint().unwrap();
        }

        // Reopen — should recover from checkpoint.
        let storage = SingleFileStorage::open(&db_path).unwrap();
        assert!(storage.sst_exists(1));
        assert!(!storage.sst_exists(2));
        assert!(storage.sst_exists(3));
        assert!(!storage.sst_exists(4));
        assert!(storage.sst_exists(5));
    }

    #[test]
    fn test_single_file_truncated_record_recovery() {
        let dir = TempDir::new().unwrap();
        let db_path = make_db_path(dir.path());

        // Write a valid SST.
        {
            let storage = SingleFileStorage::open(&db_path).unwrap();
            let records = make_records(20);
            let (data, _, _, _) = SstWriter::write_to_buf(&records, 10, 10, CompressionAlgorithm::Lz4).unwrap();
            storage.write_sst(1, &data).unwrap();
        }

        // Simulate a crash by appending garbage to the file.
        let file_size = std::fs::metadata(&db_path).unwrap().len();
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&db_path)
                .unwrap();
            // Write a partial record header (only 3 bytes of the 8-byte len field).
            f.write_all(&[0x00, 0x00, 0x00]).unwrap();
        }

        // Reopen — the truncated record should be silently dropped.
        let storage = SingleFileStorage::open(&db_path).unwrap();
        assert!(storage.sst_exists(1), "valid SST should survive truncation");
        assert_eq!(
            std::fs::metadata(&db_path).unwrap().len(),
            file_size + 3
        );
    }

    #[test]
    fn test_single_file_many_ssts() {
        let dir = TempDir::new().unwrap();
        let db_path = make_db_path(dir.path());
        let storage = SingleFileStorage::open(&db_path).unwrap();

        // Write 100 SSTs
        for id in 1..=100u32 {
            let records = make_records(10);
            let (data, _, _, _) = SstWriter::write_to_buf(&records, 10, 10, CompressionAlgorithm::Lz4).unwrap();
            storage.write_sst(id, &data).unwrap();
        }

        assert_eq!(storage.stats().0, 100);

        // Delete every other one
        for id in (1..=100u32).step_by(2) {
            storage.delete_sst(id).unwrap();
        }
        assert_eq!(storage.stats().0, 50);

        // Verify surviving SSTs are readable
        for id in (2..=100u32).step_by(2) {
            let reader = storage.open_reader(id, 0).unwrap();
            assert!(reader.block_count() > 0);
        }

        // Reopen
        drop(storage);
        let storage = SingleFileStorage::open(&db_path).unwrap();
        assert_eq!(storage.stats().0, 50);
        for id in (2..=100u32).step_by(2) {
            assert!(storage.sst_exists(id));
        }
    }

    #[test]
    fn test_open_storage_single_file() {
        let dir = TempDir::new().unwrap();
        let storage = open_storage(dir.path(), true).unwrap();
        assert!(!storage.sst_exists(1));
    }
}
