use crate::error::{FlowError, Result};
use crate::jsondb::TransactionMode;
use crate::jsondb::db::JsonDB;
use crate::jsondb::encoding::*;
use crate::jsondb::helpers::*;
use crate::jsondb::schema::*;
use crate::record::{InternalRecord, Record, ScanRange};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::ops::Bound;

/// Type alias for the OCC read-set entry.
type ReadSet = HashMap<(String, Vec<u8>), Option<Vec<u8>>>;

// ── Transaction ───────────────────────────────────────────────────

/// An explicit JsonDB transaction.
///
/// Created via [`JsonDB::transaction`]. All writes are buffered in memory until
/// [`commit`](Self::commit) is called, which applies them atomically in a single
/// batch. Dropping the transaction without calling `commit` discards all buffered
/// writes (equivalent to a rollback).
///
/// # Isolation
///
/// Transactions provide **MVCC snapshot isolation**: reads see a consistent
/// point-in-time snapshot captured at `BEGIN` (via `Engine::last_seq`).
/// Records written after the snapshot are invisible to the transaction.
///
/// At commit time, **OCC (Optimistic Concurrency Control)** validates that
/// no key the transaction read has been modified by a concurrent writer.
/// If a conflict is detected, `commit` returns an error and the caller may
/// retry the transaction.
///
/// # Read-Your-Writes
///
/// Within a transaction, subsequent reads (including index lookups) see the
/// effects of prior buffered writes, providing read-your-writes consistency.
///
/// # Example
///
/// ```no_run
/// use flowdb::jsondb::{JsonDB, TransactionMode};
/// use serde_json::json;
///
/// let db = JsonDB::open(Default::default()).unwrap();
/// db.create_object_store("users", "id").unwrap();
///
/// let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
/// tx.put("users", json!({"id": "u1"})).unwrap();
/// tx.commit().unwrap();
/// ```
pub struct Transaction<'db> {
    pub(crate) db: &'db JsonDB,
    pub(crate) mode: TransactionMode,
    // MVCC snapshot: all reads see only records with seq ≤ snapshot_seq.
    pub(crate) snapshot_seq: u64,
    // (store_name, primary_key_bytes) -> Some(doc_bytes) | None (delete)
    pub(crate) writes: HashMap<(String, Vec<u8>), Option<Vec<u8>>>,
    // OCC read-set: keys read from the engine and their observed value.
    // Validated at commit time to detect concurrent modifications.
    pub(crate) read_set: RefCell<ReadSet>,
    // Counter records (auto-increment) that must be committed atomically
    // with the document writes.
    pub(crate) counter_updates: Vec<InternalRecord>,
    // Per-store next auto-increment IDs (tracked in memory for
    // multiple put_auto calls within the same transaction).
    pub(crate) next_ids: HashMap<String, u64>,
    pub(crate) committed: bool,
}

impl fmt::Debug for Transaction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("mode", &self.mode)
            .field("writes_count", &self.writes.len())
            .field("committed", &self.committed)
            .finish()
    }
}

impl<'db> Transaction<'db> {
    /// Insert or update a document within this transaction.
    pub fn put(&mut self, store: &str, doc: Value) -> Result<Value> {
        self.require_read_write()?;
        let def = self.require_store(store)?;
        let key_val = extract_field(&doc, &def.key_path).ok_or_else(|| {
            FlowError::JsonDb(format!(
                "document missing key_path '{}' for store '{}'",
                def.key_path, store
            ))
        })?;
        let key_bytes = encode_primary_key(&key_val)?;
        let doc_bytes = encode_doc(&doc)?;
        self.writes
            .insert((store.to_string(), key_bytes), Some(doc_bytes));
        Ok(key_val)
    }

    /// Insert a document only if the key does not already exist
    /// (IndexedDB `add` semantics). Returns an error if the key exists.
    pub fn add(&mut self, store: &str, doc: Value) -> Result<Value> {
        self.require_read_write()?;
        let def = self.require_store(store)?;
        let key_val = extract_field(&doc, &def.key_path).ok_or_else(|| {
            FlowError::JsonDb(format!(
                "document missing key_path '{}' for store '{}'",
                def.key_path, store
            ))
        })?;
        let key_bytes = encode_primary_key(&key_val)?;

        // Check write buffer.
        if let Some(doc_opt) = self.writes.get(&(store.to_string(), key_bytes.clone())) {
            if doc_opt.is_some() {
                return Err(FlowError::JsonDb(format!(
                    "key already exists in store '{}'",
                    store
                )));
            }
        } else if self
            .db
            .engine
            .get_bytes_seq(&doc_key(store, &key_bytes), 0, self.snapshot_seq)
            .is_some()
        {
            return Err(FlowError::JsonDb(format!(
                "key already exists in store '{}'",
                store
            )));
        }

        let doc_bytes = encode_doc(&doc)?;
        self.writes
            .insert((store.to_string(), key_bytes), Some(doc_bytes));
        Ok(key_val)
    }

    /// Remove all documents from a store within this transaction.
    pub fn clear(&mut self, store: &str) -> Result<()> {
        self.require_read_write()?;
        let def = self.require_store(store)?;

        // Scan existing keys and mark each as deleted in the write buffer.
        let pfx = doc_prefix(store);
        let iter = self.db.engine.scan_seq(prefix_range(&pfx), self.snapshot_seq)?;
        for r in iter {
            let rec = r?;
            let key_bytes = rec.key[pfx.len()..].to_vec();
            self.writes.insert((store.to_string(), key_bytes), None);
        }
        // Also mark any buffered puts as deleted.
        let _ = def;
        let keys_to_delete: Vec<Vec<u8>> = self
            .writes
            .iter()
            .filter(|((s, _), opt)| s == store && opt.is_some())
            .map(|((_, k), _)| k.clone())
            .collect();
        for k in keys_to_delete {
            self.writes.insert((store.to_string(), k), None);
        }
        Ok(())
    }

    /// Retrieve a document by primary key.
    ///
    /// Reads from the transaction's write buffer first (read-your-writes),
    /// falling back to the engine with MVCC snapshot isolation.
    pub fn get(&self, store: &str, key: &Value) -> Result<Option<Value>> {
        let _ = self.require_store(store)?;
        let key_bytes = encode_primary_key(key)?;

        // Check write buffer.
        if let Some(doc_opt) = self.writes.get(&(store.to_string(), key_bytes.clone())) {
            return match doc_opt {
                Some(bytes) => Ok(Some(decode_doc(bytes)?)),
                None => Ok(None),
            };
        }

        // Fall back to engine (MVCC snapshot read).
        let rec = self
            .db
            .engine
            .get_bytes_seq(&doc_key(store, &key_bytes), 0, self.snapshot_seq);
        let val_bytes = rec.as_ref().map(|r| r.value.clone());
        self.read_set
            .borrow_mut()
            .insert((store.to_string(), key_bytes), val_bytes);
        match rec {
            Some(r) => Ok(Some(decode_doc(&r.value)?)),
            None => Ok(None),
        }
    }

    /// Delete a document by primary key.
    pub fn delete(&mut self, store: &str, key: &Value) -> Result<()> {
        self.require_read_write()?;
        let _ = self.require_store(store)?;
        let key_bytes = encode_primary_key(key)?;
        self.writes.insert((store.to_string(), key_bytes), None);
        Ok(())
    }

    /// Count documents (visible within this transaction).
    pub fn count(&self, store: &str) -> Result<usize> {
        let _ = self.require_store(store)?;
        let pfx = doc_prefix(store);
        let iter = self.db.engine.scan_seq(prefix_range(&pfx), self.snapshot_seq)?;
        let mut count = 0usize;
        for r in iter {
            let rec = r?;
            // Skip if the doc has been deleted in our writes.
            let key_bytes = rec.key[doc_prefix(store).len()..].to_vec();
            if let Some(doc_opt) = self.writes.get(&(store.to_string(), key_bytes))
                && doc_opt.is_none()
            {
                continue; // deleted
            }
            count += 1;
        }
        // Add buffered puts that aren't in the engine yet.
        for ((s, k), doc_opt) in &self.writes {
            if s != store {
                continue;
            }
            if doc_opt.is_none() {
                continue;
            }
            // Check if it already was counted by the scan (MVCC snapshot).
            if self
                .db
                .engine
                .get_bytes_seq(&doc_key(store, k), 0, self.snapshot_seq)
                .is_none()
            {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Retrieve all documents in a store (visible within this transaction).
    pub fn scan(&self, store: &str) -> Result<Vec<Value>> {
        let _ = self.require_store(store)?;
        let pfx = doc_prefix(store);
        let iter = self.db.engine.scan_seq(prefix_range(&pfx), self.snapshot_seq)?;
        let mut docs = Vec::new();
        for r in iter {
            let rec = r?;
            let key_bytes = rec.key[doc_prefix(store).len()..].to_vec();
            if let Some(doc_opt) = self.writes.get(&(store.to_string(), key_bytes)) {
                if let Some(bytes) = doc_opt {
                    docs.push(decode_doc(bytes)?);
                }
            } else {
                docs.push(decode_doc(&rec.value)?);
            }
        }
        // Add buffered puts not in the engine.
        for ((s, k), doc_opt) in &self.writes {
            if s != store {
                continue;
            }
            if let Some(bytes) = doc_opt
                && self
                    .db
                    .engine
                    .get_bytes_seq(&doc_key(store, k), 0, self.snapshot_seq)
                    .is_none()
            {
                docs.push(decode_doc(bytes)?);
            }
        }
        Ok(docs)
    }

    /// Look up documents by exact index value within this transaction.
    pub fn get_by_index(&self, store: &str, index: &str, value: &Value) -> Result<Vec<Value>> {
        let def = self.require_store(store)?;
        let _ = def
            .indexes
            .iter()
            .find(|i| i.name == index)
            .ok_or_else(|| {
                FlowError::JsonDb(format!("index '{}' not found on '{}'", index, store))
            })?;

        let encoded = encode_index_value(value);
        let pfx = idx_value_prefix(store, index, &encoded);
        let iter = self.db.engine.scan_seq(prefix_range(&pfx), self.snapshot_seq)?;
        let mut docs = Vec::new();

        // Find the index key_path for field checking.
        let idx_key_paths = def
            .indexes
            .iter()
            .find(|i| i.name == index)
            .map(|i| i.key_paths.clone())
            .unwrap_or_default();
        // For composite indexes, use the first key_path for basic write buffer matching.
        let first_path = idx_key_paths.first().map(|s| s.as_str()).unwrap_or("");

        for r in iter {
            let rec = r?;
            let key_bytes = &rec.value;
            // Check write buffer.
            if let Some(doc_opt) = self.writes.get(&(store.to_string(), key_bytes.clone())) {
                if let Some(bytes) = doc_opt {
                    let buffered_doc = decode_doc(bytes)?;
                    if extract_field(&buffered_doc, first_path) == Some(value.clone()) {
                        docs.push(buffered_doc);
                    }
                }
            } else if let Some(doc) = self.db.engine.get_bytes_seq(
                &doc_key(store, key_bytes),
                0,
                self.snapshot_seq,
            ) {
                docs.push(decode_doc(&doc.value)?);
            }
        }
        // Also check buffered puts whose index value matches but aren't in the
        // engine index yet (brand-new documents).
        for ((s, _k), doc_opt) in &self.writes {
            if s != store {
                continue;
            }
            if let Some(bytes) = doc_opt {
                let doc: Value = decode_doc(bytes)?;
                if extract_field(&doc, first_path) == Some(value.clone()) {
                    // Avoid duplicates that were already returned from the engine scan.
                    let already = docs.iter().any(|d| {
                        extract_field(d, &def.key_path) == extract_field(&doc, &def.key_path)
                    });
                    if !already {
                        docs.push(doc);
                    }
                }
            }
        }
        Ok(docs)
    }

    /// Look up documents by index value range within this transaction.
    pub fn range_by_index(
        &self,
        store: &str,
        index: &str,
        start: &Value,
        end: &Value,
    ) -> Result<Vec<Value>> {
        let store_def = self.require_store(store)?;
        let first_path = store_def
            .indexes
            .iter()
            .find(|i| i.name == index)
            .ok_or_else(|| {
                FlowError::JsonDb(format!("index '{}' not found on '{}'", index, store))
            })?
            .key_paths
            .first()
            .cloned()
            .unwrap_or_default();

        let pfx = idx_prefix(store, index);
        let enc_start = encode_index_value(start);
        let enc_end = encode_index_value(end);

        let range = ScanRange {
            key_start: Bound::Included([pfx.as_slice(), &enc_start].concat()),
            key_end: Bound::Excluded([pfx.as_slice(), &enc_end].concat()),
            ts_start: Bound::Unbounded,
            ts_end: Bound::Unbounded,
        };

        let iter = self.db.engine.scan_seq(range, self.snapshot_seq)?;
        let mut docs = Vec::new();

        for r in iter {
            let rec = r?;
            let key_bytes = &rec.value;
            if let Some(doc_opt) = self.writes.get(&(store.to_string(), key_bytes.clone())) {
                if let Some(bytes) = doc_opt {
                    let buffered_doc = decode_doc(bytes)?;
                    if let Some(index_val) = extract_field(&buffered_doc, &first_path) {
                        let enc = encode_index_value(&index_val);
                        if enc.as_slice() >= enc_start.as_slice()
                            && enc.as_slice() < enc_end.as_slice()
                        {
                            docs.push(buffered_doc);
                        }
                    }
                }
            } else if let Some(doc) = self.db.engine.get_bytes_seq(
                &doc_key(store, key_bytes),
                0,
                self.snapshot_seq,
            ) {
                docs.push(decode_doc(&doc.value)?);
            }
        }

        // Also check buffered puts that aren't in the engine index yet.
        for ((s, key_bytes), doc_opt) in &self.writes {
            if s != store {
                continue;
            }
            if let Some(bytes) = doc_opt {
                if self
                    .db
                    .engine
                    .get_bytes_seq(&doc_key(store, key_bytes), 0, self.snapshot_seq)
                    .is_some()
                {
                    continue;
                }
                let buffered_doc = decode_doc(bytes)?;
                if let Some(index_val) = extract_field(&buffered_doc, &first_path) {
                    let enc = encode_index_value(&index_val);
                    if enc.as_slice() >= enc_start.as_slice() && enc.as_slice() < enc_end.as_slice()
                    {
                        docs.push(buffered_doc);
                    }
                }
            }
        }
        Ok(docs)
    }

    /// Insert a document with auto-generated key (for auto-increment stores).
    pub fn put_auto(&mut self, store: &str, mut doc: Value) -> Result<Value> {
        self.require_read_write()?;
        let def = self.require_store(store)?;
        if !def.auto_increment {
            return Err(FlowError::JsonDb(format!(
                "store '{}' is not auto-increment",
                store
            )));
        }

        // Use in-memory tracking for multiple put_auto calls in the same
        // transaction. Only the first call reads the engine counter.
        let next_id = match self.next_ids.get(store) {
            Some(&existing) => {
                self.next_ids.insert(store.to_string(), existing + 1);
                existing + 1
            }
            None => {
                let (id, counter_rec) = prepare_counter(&self.db.engine, store)?;
                self.counter_updates.push(counter_rec);
                self.next_ids.insert(store.to_string(), id);
                id
            }
        };

        let key_val = Value::Number(next_id.into());

        if let Value::Object(ref mut map) = doc {
            map.insert(def.key_path.clone(), key_val.clone());
        }

        let key_bytes = next_id.to_string().into_bytes();
        let doc_bytes = encode_doc(&doc)?;
        self.writes
            .insert((store.to_string(), key_bytes), Some(doc_bytes));
        Ok(key_val)
    }

    /// Commit all buffered writes atomically.
    ///
    /// Acquires the `write_lock` to serialise against direct `put`/`delete`
    /// operations, validates the OCC read-set (detecting concurrent
    /// modifications), and applies all writes in a single atomic batch.
    pub fn commit(mut self) -> Result<()> {
        if self.committed {
            return Ok(());
        }

        // Acquire write_lock so no direct put/delete or concurrent commit
        // can race with our index-maintenance reads and atomic batch.
        let _wl = self.db.write_lock.lock().unwrap();

        // OCC validation: verify that every key we read still has the
        // same value as at our snapshot.  If any key changed, abort.
        for ((store, key), expected) in self.read_set.borrow().iter() {
            let current = self
                .db
                .engine
                .get_bytes(&doc_key(store, key), 0)
                .map(|r| r.value);
            if &current != expected {
                return Err(FlowError::JsonDb(format!(
                    "transaction conflict: key in store '{}' was modified by a concurrent \
                     transaction (snapshot seq {}, OCC validation failed)",
                    store, self.snapshot_seq,
                )));
            }
        }

        let mut records = Vec::new();

        // Include any pending counter updates (auto-increment).
        records.append(&mut self.counter_updates);

        // Process buffered document writes.
        for ((store_name, key_bytes), doc_opt) in &self.writes {
            let def =
                self.db.schema.get(store_name).ok_or_else(|| {
                    FlowError::JsonDb(format!("store '{}' not found", store_name))
                })?;

            // Read old document for index maintenance.
            // If the document is corrupted we fail hard — silent data loss
            // is worse than a failed write.
            let old_doc_str = self
                .db
                .engine
                .get_bytes(&doc_key(store_name, key_bytes), 0)
                .and_then(|r| decode_doc(&r.value).ok());

            // Delete old index entries.
            if let Some(ref old_doc_val) = old_doc_str {
                for idx in &def.indexes {
                    let old_values = extract_index_values(old_doc_val, idx);
                    for vals in old_values {
                        let encoded = encode_composite_value(&vals);
                        records.push(InternalRecord::delete(
                            idx_key(store_name, &idx.name, &encoded, key_bytes),
                            0,
                            0,
                        ));
                    }
                }
            }

            match doc_opt {
                Some(doc_bytes) => {
                    // Write new document.
                    records.push(InternalRecord::from_record(
                        &Record::new(doc_key(store_name, key_bytes), 0, doc_bytes.clone()),
                        0,
                    ));

                    // Write new index entries.
                    let new_doc = decode_doc(doc_bytes)?;
                    for idx in &def.indexes {
                        let new_values = extract_index_values(&new_doc, idx);

                        // Unique validation: check BOTH engine AND write buffer.
                        if idx.unique {
                            for vals in &new_values {
                                let encoded = encode_composite_value(vals);
                                let val_pfx = idx_value_prefix(store_name, &idx.name, &encoded);
                                let iter = self.db.engine.scan(prefix_range(&val_pfx))?;
                                for r in iter {
                                    let rec = r?;
                                    if rec.value.as_slice() != key_bytes.as_slice() {
                                        return Err(FlowError::JsonDb(format!(
                                            "unique constraint violation: index '{}' value '{:?}' already exists",
                                            idx.name, vals
                                        )));
                                    }
                                }
                                // Also check other buffered writes in this transaction.
                                for ((other_store, other_key), other_doc) in &self.writes {
                                    if other_store != store_name {
                                        continue;
                                    }
                                    if other_key == key_bytes {
                                        continue;
                                    }
                                    if let Some(other_bytes) = other_doc {
                                        let other_doc_val = decode_doc(other_bytes)?;
                                        let other_vals = extract_index_values(&other_doc_val, idx);
                                        for ov in other_vals {
                                            if encode_composite_value(&ov) == encoded {
                                                return Err(FlowError::JsonDb(format!(
                                                    "unique constraint violation in transaction: index '{}' value '{:?}'",
                                                    idx.name, vals
                                                )));
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        for vals in &new_values {
                            let encoded = encode_composite_value(vals);
                            records.push(InternalRecord::from_record(
                                &Record::new(
                                    idx_key(store_name, &idx.name, &encoded, key_bytes),
                                    0,
                                    key_bytes.clone(),
                                ),
                                0,
                            ));
                        }
                    }
                }
                None => {
                    // Delete document.
                    records.push(InternalRecord::delete(doc_key(store_name, key_bytes), 0, 0));
                }
            }
        }

        if !records.is_empty() {
            self.db.engine.write_internal(records)?;
        }
        // Only mark committed AFTER the write succeeds.
        // This lets callers retry if write_internal fails.
        self.committed = true;
        Ok(())
    }

    /// Abort the transaction (discard all buffered writes).
    pub fn abort(self) {
        // Just drop — writes are discarded.
    }

    // ── helpers ──────────────────────────────────────────────────

    /// Override the snapshot sequence number.  Used by language bindings
    /// that capture the snapshot at transaction *creation* time (before
    /// any reads or writes) rather than at the Rust `Transaction` constructor.
    pub fn set_snapshot_seq(&mut self, seq: u64) {
        self.snapshot_seq = seq;
    }

    /// Inject a read-set entry from an external source (language bindings
    /// that perform reads outside the Rust `Transaction` but still need
    /// OCC validation at commit time).
    pub fn add_read_entry(&self, store: &str, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.read_set
            .borrow_mut()
            .insert((store.to_string(), key), value);
    }

    fn require_read_write(&self) -> Result<()> {
        if self.mode == TransactionMode::ReadOnly {
            return Err(FlowError::JsonDb(
                "cannot write in a read-only transaction".into(),
            ));
        }
        Ok(())
    }

    fn require_store(&self, name: &str) -> Result<StoreDef> {
        self.db
            .schema
            .get(name)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", name)))
    }
}

impl<'db> Drop for Transaction<'db> {
    fn drop(&mut self) {
        if !self.committed && self.mode == TransactionMode::ReadWrite {
            // Auto-abort: writes are simply discarded.
        }
    }
}
