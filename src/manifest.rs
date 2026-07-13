use crate::bloom::BloomFilter;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Serde adapter that keeps on-disk `BlockInfo.min_key` / `max_key` fields
/// backward-compatible across the `String` -> `Vec<u8>` migration.
///
/// * New manifests serialise keys as JSON arrays of bytes (`[107, 101, 121]`).
/// * Old manifests stored them as JSON strings (`"key"`). The deserialiser
///   accepts both shapes and converts strings into their UTF-8 bytes.
pub(crate) mod flex_key {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(key: &[u8], s: S) -> Result<S::Ok, S::Error> {
        key.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum KeyRepr {
            Bytes(Vec<u8>),
            Str(String),
        }
        match KeyRepr::deserialize(d)? {
            KeyRepr::Bytes(b) => Ok(b),
            KeyRepr::Str(s) => Ok(s.into_bytes()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SstInfo {
    pub id: u32,
    pub records: u64,
    pub bytes: u64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub min_expire: i64,
    pub max_expire: i64,
    #[serde(default)]
    pub bloom: Option<BloomFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BlockInfo {
    pub block_idx: u32,
    #[serde(with = "flex_key")]
    pub min_key: Vec<u8>,
    #[serde(with = "flex_key")]
    pub max_key: Vec<u8>,
    pub min_ts: i64,
    pub max_ts: i64,
    pub min_expire: i64,
    pub max_expire: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum ManifestEntry {
    #[serde(rename = "flush")]
    Flush {
        seq: u64,
        sst: SstInfo,
        blocks: Vec<BlockInfo>,
    },
    #[serde(rename = "delete_sst")]
    DeleteSst { sst_id: u32 },
    #[serde(rename = "compaction")]
    Compaction {
        removed: Vec<u32>,
        added: Vec<SstInfo>,
        blocks: Vec<(u32, Vec<BlockInfo>)>,
    },
    #[serde(rename = "checkpoint")]
    Checkpoint { last_flushed_seq: u64 },
    #[serde(rename = "gc_delete_sst")]
    GcDeleteSst { sst_id: u32 },
    /// Replace an SSTable's bloom filter in-place. Emitted by `Engine::open`
    /// when it rebuilds stale (legacy-hash) blooms after a hasher upgrade.
    /// Applying this entry does NOT change the active SST set; it only
    /// refreshes the bloom cached in `SstInfo.bloom`.
    #[serde(rename = "update_bloom")]
    UpdateBloom { sst_id: u32, bloom: BloomFilter },
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ManifestState {
    pub last_flushed_seq: u64,
    pub sstables: HashMap<u32, SstInfo>,
    pub block_infos: HashMap<u32, Vec<BlockInfo>>,
    pub active_sst_ids: Vec<u32>,
}

pub(crate) struct Manifest {
    path: PathBuf,
    state: ManifestState,
    #[cfg(not(target_os = "windows"))]
    dir_file: std::fs::File,
    /// Number of appended entries since the last snapshot.  Used by
    /// [`maybe_snapshot`] to decide when to compact the log.
    entry_count: usize,
}

/// Threshold: once the manifest exceeds this many lines, rewrite it as a
/// compact snapshot of the current state.
const SNAPSHOT_THRESHOLD: usize = 500;

impl Manifest {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        #[cfg(not(target_os = "windows"))]
        let dir_file = std::fs::File::open(dir)?;
        let path = dir.join("MANIFEST");
        let mut state = ManifestState::default();

        if path.exists() {
            let content = std::fs::read_to_string(&path)?;
            let lines: Vec<&str> = content.lines().map(|l| l.trim()).filter(|l| !l.is_empty()).collect();
            for (i, line) in lines.iter().enumerate() {
                match serde_json::from_str::<ManifestEntry>(line) {
                    Ok(entry) => apply_entry(&mut state, &entry),
                    Err(e) => {
                        if i == lines.len() - 1 {
                            tracing::warn!("Ignoring torn final manifest entry: {}", e);
                        } else {
                            return Err(crate::error::FlowError::Corruption {
                                file: path.display().to_string(),
                                msg: format!("Corrupted manifest entry at line {}: {}", i + 1, e),
                            });
                        }
                    }
                }
            }
        }

        Ok(Self {
            path,
            state,
            #[cfg(not(target_os = "windows"))]
            dir_file,
            entry_count: 0,
        })
    }

    pub fn append(&mut self, entry: &ManifestEntry) -> Result<()> {
        let line = serde_json::to_string(entry)?;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)?;
        use std::io::Write;
        writeln!(file, "{}", line)?;
        file.flush()?;
        file.sync_all()?;
        #[cfg(not(target_os = "windows"))]
        self.dir_file.sync_all()?;

        apply_entry(&mut self.state, entry);
        self.entry_count += 1;
        Ok(())
    }

    /// If the manifest log has grown past [`SNAPSHOT_THRESHOLD`] entries,
    /// rewrite it as a compact snapshot of the current state.  This bounds
    /// on-disk growth and restart-replay time for long-running engines.
    ///
    /// The snapshot is written atomically: write to a temp file, `sync_all`,
    /// then rename over the existing MANIFEST.
    pub fn maybe_snapshot(&mut self) -> Result<()> {
        if self.entry_count <= SNAPSHOT_THRESHOLD {
            return Ok(());
        }
        self.snapshot()
    }

    /// Rewrite the manifest file as a compact snapshot of the current
    /// in-memory state.  Each active SST is emitted as a `Flush` entry,
    /// followed by a `Checkpoint` with the current `last_flushed_seq`.
    fn snapshot(&mut self) -> Result<()> {
        use std::io::Write;

        let tmp_path = self.path.with_extension("MANIFEST.tmp");
        {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;

            for (sst_id, info) in &self.state.sstables {
                let blocks = self
                    .state
                    .block_infos
                    .get(sst_id)
                    .cloned()
                    .unwrap_or_default();
                let entry = ManifestEntry::Flush {
                    seq: self.state.last_flushed_seq,
                    sst: info.clone(),
                    blocks,
                };
                let line = serde_json::to_string(&entry)?;
                writeln!(file, "{}", line)?;
            }

            let checkpoint = ManifestEntry::Checkpoint {
                last_flushed_seq: self.state.last_flushed_seq,
            };
            let line = serde_json::to_string(&checkpoint)?;
            writeln!(file, "{}", line)?;

            file.flush()?;
            file.sync_all()?;
        }

        // Atomic rename
        std::fs::rename(&tmp_path, &self.path)?;
        #[cfg(not(target_os = "windows"))]
        self.dir_file.sync_all()?;
        self.entry_count = self.state.sstables.len() + 1;

        tracing::info!(
            "Manifest snapshot complete: {} active SSTs, {} entries",
            self.state.sstables.len(),
            self.entry_count
        );
        Ok(())
    }

    pub fn state(&self) -> &ManifestState {
        &self.state
    }

    pub fn next_sst_id(&self) -> u32 {
        self.state.sstables.keys().max().copied().unwrap_or(0) + 1
    }

    /// Number of entries appended since the last snapshot (or since open).
    #[cfg(test)]
    pub fn entry_count(&self) -> usize {
        self.entry_count
    }
}

fn apply_entry(state: &mut ManifestState, entry: &ManifestEntry) {
    match entry {
        ManifestEntry::Flush { seq, sst, blocks } => {
            state.last_flushed_seq = state.last_flushed_seq.max(*seq);
            state.sstables.insert(sst.id, sst.clone());
            state.block_infos.insert(sst.id, blocks.clone());
            if !state.active_sst_ids.contains(&sst.id) {
                state.active_sst_ids.push(sst.id);
            }
        }
        ManifestEntry::DeleteSst { sst_id } | ManifestEntry::GcDeleteSst { sst_id } => {
            state.sstables.remove(sst_id);
            state.block_infos.remove(sst_id);
            state.active_sst_ids.retain(|id| id != sst_id);
        }
        ManifestEntry::Compaction {
            removed,
            added,
            blocks,
        } => {
            for id in removed {
                state.sstables.remove(id);
                state.block_infos.remove(id);
                state.active_sst_ids.retain(|sid| sid != id);
            }
            for info in added {
                state.sstables.insert(info.id, info.clone());
                if !state.active_sst_ids.contains(&info.id) {
                    state.active_sst_ids.push(info.id);
                }
            }
            for (sst_id, blks) in blocks {
                state.block_infos.insert(*sst_id, blks.clone());
            }
        }
        ManifestEntry::Checkpoint { last_flushed_seq } => {
            state.last_flushed_seq = state.last_flushed_seq.max(*last_flushed_seq);
        }
        ManifestEntry::UpdateBloom { sst_id, bloom } => {
            if let Some(info) = state.sstables.get_mut(sst_id) {
                info.bloom = Some(bloom.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_manifest_append_and_replay() {
        let dir = TempDir::new().unwrap();
        let sst_dir = dir.path().join("data");
        std::fs::create_dir_all(&sst_dir).unwrap();

        {
            let mut mf = Manifest::open(&sst_dir).unwrap();
            mf.append(&ManifestEntry::Flush {
                seq: 100,
                sst: SstInfo {
                    id: 1,
                    records: 100,
                    bytes: 4096,
                    min_ts: 1000,
                    max_ts: 2000,
                    min_expire: i64::MAX,
                    max_expire: i64::MAX,
                    bloom: None,
                },
                blocks: vec![BlockInfo {
                    block_idx: 0,
                    min_key: "a".into(),
                    max_key: "b".into(),
                    min_ts: 1000,
                    max_ts: 2000,
                    min_expire: i64::MAX,
                    max_expire: i64::MAX,
                }],
            })
            .unwrap();

            mf.append(&ManifestEntry::Flush {
                seq: 200,
                sst: SstInfo {
                    id: 2,
                    records: 50,
                    bytes: 2048,
                    min_ts: 2000,
                    max_ts: 3000,
                    min_expire: i64::MAX,
                    max_expire: i64::MAX,
                    bloom: None,
                },
                blocks: vec![],
            })
            .unwrap();

            assert_eq!(mf.state().sstables.len(), 2);
            assert_eq!(mf.state().last_flushed_seq, 200);
        }

        let mf2 = Manifest::open(&sst_dir).unwrap();
        assert_eq!(mf2.state().sstables.len(), 2);
        assert_eq!(mf2.state().last_flushed_seq, 200);
    }

    #[test]
    fn test_manifest_delete() {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let mut mf = Manifest::open(&data_dir).unwrap();
        mf.append(&ManifestEntry::Flush {
            seq: 100,
            sst: SstInfo {
                id: 1,
                records: 10,
                bytes: 100,
                min_ts: 0,
                max_ts: 0,
                min_expire: 0,
                max_expire: 0,
                bloom: None,
            },
            blocks: vec![],
        })
        .unwrap();
        mf.append(&ManifestEntry::DeleteSst { sst_id: 1 }).unwrap();

        assert!(!mf.state().sstables.contains_key(&1));
        assert!(mf.state().active_sst_ids.is_empty());
    }

    /// Regression: a MANIFEST written by an old version of flowdb stores
    /// `BlockInfo.min_key` / `max_key` as JSON **strings**. After the
    /// `String -> Vec<u8>` migration the deserialiser must still accept the
    /// old shape and convert strings into bytes on the fly. Otherwise
    /// upgrades would refuse to start (manifest parse error).
    #[test]
    fn test_manifest_legacy_string_keys_load() {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        // Hand-craft an old-style MANIFEST line: keys are JSON strings,
        // bloom filter has no `hash_version` field (legacy).
        let legacy_line = concat!(
            r#"{"type":"flush","seq":10,"sst":{"id":1,"records":3,"bytes":128,"#,
            r#""min_ts":0,"max_ts":100,"min_expire":0,"max_expire":0,"#,
            r#""bloom":{"bits":[18446744073709551615,1024],"num_hashes":2}},"#,
            r#""blocks":[{"block_idx":0,"min_key":"metric.cpu","max_key":"metric.mem","#,
            r#""min_ts":0,"max_ts":100,"min_expire":0,"max_expire":0}]}"#
        );
        std::fs::write(data_dir.join("MANIFEST"), format!("{}\n", legacy_line)).unwrap();

        let mf = Manifest::open(&data_dir).unwrap();

        // SST should be loaded.
        assert_eq!(mf.state().sstables.len(), 1);
        assert!(mf.state().sstables.contains_key(&1));

        // Block keys must be the byte representation of the old strings.
        let blocks = mf.state().block_infos.get(&1).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].min_key, b"metric.cpu");
        assert_eq!(blocks[0].max_key, b"metric.mem");

        // Legacy bloom deserialises with hash_version == 0 (the `default`).
        let bloom = mf.state().sstables[&1].bloom.as_ref().unwrap();
        assert_eq!(bloom.hash_version(), 0);
    }

    /// Forward-compat: newly written manifests serialise keys as JSON arrays
    /// of bytes. Make sure they round-trip cleanly through serde.
    #[test]
    fn test_manifest_new_byte_keys_roundtrip() {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        {
            let mut mf = Manifest::open(&data_dir).unwrap();
            mf.append(&ManifestEntry::Flush {
                seq: 1,
                sst: SstInfo {
                    id: 1,
                    records: 1,
                    bytes: 32,
                    min_ts: 0,
                    max_ts: 0,
                    min_expire: 0,
                    max_expire: 0,
                    bloom: None,
                },
                blocks: vec![BlockInfo {
                    block_idx: 0,
                    min_key: b"\xff\x00key".to_vec(), // non-UTF-8 — proves we use bytes
                    max_key: b"\xff\x01key".to_vec(),
                    min_ts: 0,
                    max_ts: 0,
                    min_expire: 0,
                    max_expire: 0,
                }],
            })
            .unwrap();
        }

        let mf2 = Manifest::open(&data_dir).unwrap();
        let blocks = mf2.state().block_infos.get(&1).unwrap();
        assert_eq!(blocks[0].min_key, b"\xff\x00key");
        assert_eq!(blocks[0].max_key, b"\xff\x01key");
    }

    /// `ManifestEntry::UpdateBloom` must persist a replacement bloom and
    /// update the in-memory state so subsequent `state()` reads see it.
    #[test]
    fn test_manifest_update_bloom_entry() {
        use crate::bloom::BloomFilter;
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let mut mf = Manifest::open(&data_dir).unwrap();
        // Seed with a Flush carrying a legacy (version=0) bloom.
        let legacy_bloom = {
            let mut b = BloomFilter::from_keys_with_bits(&[b"k".to_vec()], 10);
            b.mark_current(); // pretend current
            b
        };
        mf.append(&ManifestEntry::Flush {
            seq: 1,
            sst: SstInfo {
                id: 7,
                records: 1,
                bytes: 16,
                min_ts: 0,
                max_ts: 0,
                min_expire: 0,
                max_expire: 0,
                bloom: Some(legacy_bloom),
            },
            blocks: vec![],
        })
        .unwrap();

        // Replace the bloom via UpdateBloom.
        let new_bloom = BloomFilter::from_keys_with_bits(&[b"new".to_vec()], 10);
        mf.append(&ManifestEntry::UpdateBloom {
            sst_id: 7,
            bloom: new_bloom,
        })
        .unwrap();

        // In-memory state: the bloom should be replaced.
        assert_eq!(
            mf.state().sstables[&7]
                .bloom
                .as_ref()
                .unwrap()
                .hash_version(),
            crate::bloom::CURRENT_HASH_VERSION
        );

        // Drop and reload — UpdateBloom must replay correctly.
        drop(mf);
        let mf2 = Manifest::open(&data_dir).unwrap();
        assert!(mf2.state().sstables[&7].bloom.is_some());
        assert_eq!(
            mf2.state().sstables[&7]
                .bloom
                .as_ref()
                .unwrap()
                .hash_version(),
            crate::bloom::CURRENT_HASH_VERSION
        );
        // The new bloom should still recognise its keys.
        assert!(
            mf2.state().sstables[&7]
                .bloom
                .as_ref()
                .unwrap()
                .may_contain(b"new")
        );
    }
}
