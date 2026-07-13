use crate::engine::Engine;
use crate::error::{FlowError, Result};
use crate::jsondb::cursor::{Cursor, CursorDirection, IndexCursor};
use crate::jsondb::encoding::*;
use crate::jsondb::helpers::*;
use crate::jsondb::keyrange::KeyRange;
use crate::jsondb::query::QueryBuilder;
use crate::jsondb::schema::*;
use crate::jsondb::{KeyArg, ObjectStore, Transaction, TransactionMode};
use crate::record::{Config, InternalRecord, Record, ScanRange};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::ops::Bound;

// ── JsonDB ───────────────────────────────────────────────────────

/// A JSON document database built on top of a single FlowDB engine.
///
/// Every document operation is ACID — document writes and secondary-index
/// updates are applied atomically. Explicit [`Transaction`]s group multiple
/// operations into a single atomic batch.
///
/// # Object Stores
///
/// JsonDB organises documents into **object stores**, each with a named primary
/// key field (`key_path`). Secondary indexes can be created on any field or
/// combination of fields, with optional uniqueness constraints.
///
/// # Example
///
/// ```no_run
/// use flowdb::jsondb::JsonDB;
/// use serde_json::json;
///
/// let db = JsonDB::open(Default::default()).unwrap();
/// db.create_object_store("users", "id").unwrap();
/// db.create_index("users", "by_email", &["email"], true, false).unwrap();
/// db.put("users", json!({"id": "u1", "email": "a@b.com"})).unwrap();
/// let doc = db.get("users", &json!("u1")).unwrap();
/// ```
///
/// # Example
///
/// ```no_run
/// use flowdb::jsondb::{JsonDB, TransactionMode};
/// use serde_json::json;
///
/// let db = JsonDB::open(Default::default()).unwrap();
/// db.create_object_store("users", "id").unwrap();
/// db.create_index("users", "by_email", &["email"], true, false).unwrap();
///
/// db.put("users", json!({"id": "u1", "email": "a@b.com"})).unwrap();
/// let doc = db.get("users", &json!("u1")).unwrap();
///
/// let mut tx = db.transaction(&["users"], TransactionMode::ReadWrite).unwrap();
/// tx.put("users", json!({"id": "u2", "email": "c@d.com"})).unwrap();
/// tx.commit().unwrap();
/// ```
pub struct JsonDB {
    pub(crate) engine: Engine,
    pub(crate) schema: Schema,
    // Serialises read-modify-write operations (put, delete, put_auto) so
    // concurrent threads don't compute stale index maintenance from the
    // same old-document snapshot.  Also acquired by `Transaction::commit`
    // for OCC validation.
    pub(crate) write_lock: std::sync::Mutex<()>,
}

impl fmt::Debug for JsonDB {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonDB")
            .field("store_names", &self.store_names())
            .finish()
    }
}

impl JsonDB {
    /// Open a JsonDB backed by a FlowDB engine at `config.data_dir`.
    pub fn open(config: Config) -> Result<Self> {
        let engine = Engine::open(config)?;
        Self::from_engine(engine)
    }

    /// Wrap an already-open FlowDB [`Engine`](crate::Engine) with a JsonDB layer.
    ///
    /// Existing schemas (stores and indexes) are automatically recovered from
    /// the engine's persisted schema records. New schemas can be added after
    /// construction via [`create_object_store`](Self::create_object_store).
    pub fn from_engine(engine: Engine) -> Result<Self> {
        let schema = load_schemas(|range| {
            let iter = engine.scan(range)?;
            iter.collect()
        })?;
        Ok(Self {
            engine,
            schema,
            write_lock: std::sync::Mutex::new(()),
        })
    }

    /// Shut down the underlying engine.
    pub fn shutdown(self) -> Result<()> {
        self.engine.shutdown()
    }

    /// Close (flush) without consuming.
    pub fn close(&self) -> Result<()> {
        self.engine.close()
    }

    /// Access the underlying FlowDB engine.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Returns the sequence number of the last committed write.
    /// Used by language bindings to capture a snapshot at transaction creation.
    pub fn last_seq(&self) -> u64 {
        self.engine.last_seq()
    }

    /// MVCC point read: like `get` but only sees records with `seq <= max_seq`.
    /// Used by language bindings for snapshot-isolated transaction reads.
    pub fn get_snapshot(&self, store: &str, key: &Value, max_seq: u64) -> Result<Option<Value>> {
        let _ = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let key_bytes = encode_primary_key(key)?;
        let rec = self
            .engine
            .get_bytes_seq(&doc_key(store, &key_bytes), 0, max_seq);
        match rec {
            Some(r) => Ok(Some(decode_doc(&r.value)?)),
            None => Ok(None),
        }
    }

    // ── schema management ─────────────────────────────────────────

    /// Create a new object store.
    ///
    /// `key_path` is a dotted field path (e.g. `"id"` or `"user.id"`) that
    /// identifies the primary key within each document.
    pub fn create_object_store(&self, name: &str, key_path: &str) -> Result<()> {
        validate_store_def(name, key_path)?;

        let mut def = self.schema.get(name);

        match &mut def {
            Some(d) => {
                // Store already exists – update key_path if it matches.
                if d.key_path != key_path {
                    return Err(FlowError::JsonDb(format!(
                        "store '{}' already exists with a different key_path",
                        name
                    )));
                }
                // Already exists with identical definition – no-op.
                Ok(())
            }
            None => {
                let entry = StoreDef {
                    name: name.to_string(),
                    key_path: key_path.to_string(),
                    auto_increment: false,
                    indexes: vec![],
                    next_auto_id: 0,
                };
                self.engine
                    .write_internal(vec![InternalRecord::from_record(
                        &schema_record(&entry)?,
                        0,
                    )])?;
                self.schema.insert(entry);
                Ok(())
            }
        }
    }

    /// Delete an object store (all documents, indexes, and schema).
    pub fn delete_object_store(&self, name: &str) -> Result<()> {
        let def = self.schema.get(name);
        if def.is_none() {
            return Err(FlowError::JsonDb(format!("store '{}' not found", name)));
        }
        let def = def.unwrap();

        // Range-delete all documents and index entries.
        let doc_pfx = doc_prefix(name);

        let mut records = Vec::new();

        // Delete all index entries for each index.
        for index in &def.indexes {
            let pfx = idx_prefix(name, &index.name);
            let end = crate::record::increment_prefix_bytes(&pfx);
            records.push(InternalRecord::delete_range(pfx, end, 0));
        }

        // Delete all documents.
        let doc_end = crate::record::increment_prefix_bytes(&doc_pfx);
        records.push(InternalRecord::delete_range(doc_pfx, doc_end, 0));

        // Delete schema.
        records.push(schema_delete_record(name));

        // Delete counter.
        records.push(InternalRecord::delete(counter_key(name), 0, 0));

        self.engine.write_internal(records)?;
        self.schema.remove(name);
        Ok(())
    }

    /// Create a secondary index on an existing object store.
    ///
    /// `key_paths` can be one or more field paths (e.g. `&["email"]` for a
    /// single-field index or `&["city", "age"]` for a composite index).
    /// Composite indexes enable efficient multi-field queries via
    /// [`QueryBuilder`].
    pub fn create_index(
        &self,
        store: &str,
        name: &str,
        key_paths: &[&str],
        unique: bool,
        multi_entry: bool,
    ) -> Result<()> {
        let mut def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        validate_index_def(&def, name, key_paths)?;

        let index = IndexDef {
            name: name.to_string(),
            key_paths: key_paths.iter().map(|s| s.to_string()).collect(),
            unique,
            multi_entry,
        };

        def.indexes.push(index.clone());

        // Build a single atomic batch: schema + all index entries for
        // existing documents.  This ensures consistency — if the write
        // fails, neither the schema nor any entries are committed.
        let mut records = Vec::new();
        records.push(InternalRecord::from_record(&schema_record(&def)?, 0));

        let doc_pfx = doc_prefix(store);
        let docs = self.engine.scan(prefix_range(&doc_pfx))?;
        for rec in docs {
            let doc = decode_doc(&rec?.value)?;
            let index_vals = extract_index_values(&doc, &index);
            for vals in index_vals {
                let key_bytes =
                    encode_primary_key(&extract_field(&doc, &def.key_path).unwrap_or(Value::Null))?;
                let encoded = encode_composite_value(&vals);
                records.push(InternalRecord::from_record(
                    &Record::new(
                        idx_key(store, name, &encoded, &key_bytes),
                        0,
                        key_bytes.clone(),
                    ),
                    0,
                ));
            }
        }
        self.engine.write_internal(records)?;

        self.schema.insert(def);
        Ok(())
    }

    /// Convenience: create a single-field index.  Equivalent to
    /// `create_index(store, name, &[key_path], unique)`.
    pub fn create_index_on(
        &self,
        store: &str,
        name: &str,
        key_path: &str,
        unique: bool,
    ) -> Result<()> {
        self.create_index(store, name, &[key_path], unique, false)
    }

    /// Delete a secondary index (removes all index entries).
    pub fn delete_index(&self, store: &str, name: &str) -> Result<()> {
        let mut def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;

        let pos = def.indexes.iter().position(|i| i.name == name);
        if pos.is_none() {
            return Err(FlowError::JsonDb(format!(
                "index '{}' not found on store '{}'",
                name, store
            )));
        }
        def.indexes.remove(pos.unwrap());

        // Delete all index entries.
        let pfx = idx_prefix(store, name);
        let end = crate::record::increment_prefix_bytes(&pfx);
        let mut records = vec![InternalRecord::delete_range(pfx, end, 0)];
        records.push(InternalRecord::from_record(&schema_record(&def)?, 0));

        self.engine.write_internal(records)?;
        self.schema.insert(def);
        Ok(())
    }

    /// List all store names.
    pub fn store_names(&self) -> Vec<String> {
        self.schema.list().into_iter().map(|s| s.name).collect()
    }

    /// Get a store definition.
    pub fn get_store(&self, name: &str) -> Option<StoreDef> {
        self.schema.get(name)
    }

    /// Apply a full store definition — create the store and all its indexes,
    /// or diff against the existing schema and add/remove indexes as needed.
    ///
    /// This is the recommended way to set up schema when using the builder
    /// or derive-macro pattern:
    ///
    /// ```no_run
    /// use flowdb::jsondb::{JsonDB, StoreSchema};
    /// use flowdb::Config;
    ///
    /// let db = JsonDB::open(Config::default()).unwrap();
    /// db.apply_store(
    ///     &StoreSchema::new("users", "id")
    ///         .with_index("by_email", &["email"], true)
    ///         .with_index("by_city_age", &["city", "age"], false),
    /// ).unwrap();
    /// ```
    ///
    /// Semantics:
    /// - If the store does not exist, it is created with all specified indexes.
    /// - If the store exists with the same `key_path`, missing indexes are
    ///   created (with backfill for existing documents) and extra indexes
    ///   (present on disk but not in `def`) are removed.
    /// - If the store exists with a different `key_path`, an error is returned.
    /// - If an index with the same name but different `key_paths` or `unique`
    ///   exists, an error is returned (manual intervention required).
    pub fn apply_store(&self, def: &StoreDef) -> Result<()> {
        let existing = self.schema.get(&def.name);

        match existing {
            None => {
                // Store doesn't exist — create it with all indexes.
                self.create_object_store(&def.name, &def.key_path)?;
                // Set auto_increment if requested
                if def.auto_increment {
                    let mut current = self.schema.get(&def.name).unwrap();
                    current.auto_increment = true;
                    self.engine.write_internal(vec![
                        InternalRecord::from_record(&schema_record(&current)?, 0),
                    ])?;
                    self.schema.insert(current);
                }
                for idx in &def.indexes {
                    let paths: Vec<&str> = idx.key_paths.iter().map(|s| s.as_str()).collect();
                    self.create_index(&def.name, &idx.name, &paths, idx.unique, idx.multi_entry)?;
                }
                Ok(())
            }
            Some(existing) => {
                // Store exists — validate key_path.
                if existing.key_path != def.key_path {
                    return Err(FlowError::JsonDb(format!(
                        "store '{}' already exists with a different key_path ('{}' vs '{}')",
                        def.name, existing.key_path, def.key_path
                    )));
                }

                // Detect conflicting indexes (same name, different definition).
                for idx in &def.indexes {
                    if let Some(ex_idx) = existing.indexes.iter().find(|i| i.name == idx.name)
                        && (ex_idx.key_paths != idx.key_paths || ex_idx.unique != idx.unique)
                    {
                        return Err(FlowError::JsonDb(format!(
                            "index '{}' on store '{}' already exists with different \
                             definition (key_paths={:?}, unique={}) vs requested \
                             (key_paths={:?}, unique={})",
                            idx.name, def.name, ex_idx.key_paths, ex_idx.unique,
                            idx.key_paths, idx.unique,
                        )));
                    }
                }

                // Create missing indexes.
                for idx in &def.indexes {
                    if !existing.indexes.iter().any(|i| i.name == idx.name) {
                        let paths: Vec<&str> = idx.key_paths.iter().map(|s| s.as_str()).collect();
                        self.create_index(&def.name, &idx.name, &paths, idx.unique, idx.multi_entry)?;
                    }
                }

                // Remove extra indexes (present on disk but not in desired def).
                for ex_idx in &existing.indexes {
                    if !def.indexes.iter().any(|i| i.name == ex_idx.name) {
                        self.delete_index(&def.name, &ex_idx.name)?;
                    }
                }

                Ok(())
            }
        }
    }

    /// Apply multiple store definitions at once.
    ///
    /// Calls [`apply_store`](Self::apply_store) for each definition.
    pub fn apply_schemas(&self, stores: &[StoreDef]) -> Result<()> {
        for def in stores {
            self.apply_store(def)?;
        }
        Ok(())
    }

    /// Apply the schema defined by a typed [`ObjectStore`] implementor.
    ///
    /// This is the entry point for the derive-macro pattern:
    ///
    /// ```ignore
    /// // The derive macro is re-exported at flowdb::ObjectStore.
    /// // Use `use flowdb::ObjectStore;` together with
    /// // `use flowdb::jsondb::JsonDB;`:
    /// //
    /// //   use flowdb::jsondb::JsonDB;
    /// //   use flowdb::ObjectStore;
    /// //
    /// //   #[derive(ObjectStore)]
    /// //   #[store(key_path = "id")]
    /// //   struct User {
    /// //       #[index(unique)]
    /// //       email: String,
    /// //   }
    /// //
    /// //   let db = JsonDB::open(Default::default()).unwrap();
    /// //   db.apply_schema::<User>().unwrap();
    /// ```
    pub fn apply_schema<T: ObjectStore>(&self) -> Result<()> {
        self.apply_store(&T::store_def())
    }

    // ── direct document operations (implicit transaction) ─────────

    /// Insert or update a document.
    ///
    /// The document **must** contain the store's `key_path` field.
    /// Returns the extracted primary key value.
    pub fn put(&self, store: &str, doc: Value) -> Result<Value> {
        let _lock = self.write_lock.lock().unwrap();
        let def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let key_val = extract_field(&doc, &def.key_path).ok_or_else(|| {
            FlowError::JsonDb(format!(
                "document missing key_path '{}' for store '{}'",
                def.key_path, store
            ))
        })?;
        let key_bytes = encode_primary_key(&key_val)?;
        let doc_bytes = encode_doc(&doc)?;

        let batch = build_put_batch(&def, store, &key_bytes, &doc_bytes, &doc, &self.engine)?;
        self.engine.write_internal(batch)?;
        Ok(key_val)
    }

    /// Retrieve a document by primary key.
    pub fn get(&self, store: &str, key: &Value) -> Result<Option<Value>> {
        let _def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let key_bytes = encode_primary_key(key)?;
        let rec = self.engine.get_bytes(&doc_key(store, &key_bytes), 0);
        match rec {
            Some(r) => Ok(Some(decode_doc(&r.value)?)),
            None => Ok(None),
        }
    }

    /// Retrieve a document by primary key, including its internal timestamp
    /// (`ts`) and expiry (`expire_at`) in microseconds since epoch.
    ///
    /// Returns `None` if the document does not exist.
    pub fn get_with_meta(&self, store: &str, key: &Value) -> Result<Option<(Value, i64, i64)>> {
        let _def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let key_bytes = encode_primary_key(key)?;
        let rec = self.engine.get_bytes(&doc_key(store, &key_bytes), 0);
        match rec {
            Some(r) => Ok(Some((decode_doc(&r.value)?, r.ts, r.expire_at))),
            None => Ok(None),
        }
    }

    /// Delete a document by primary key (and all associated index entries).
    pub fn delete(&self, store: &str, key: &Value) -> Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let key_bytes = encode_primary_key(key)?;

        let batch = build_delete_batch(&def, store, &key_bytes, &self.engine)?;
        self.engine.write_internal(batch)?;
        Ok(())
    }

    /// Insert a document with auto-generated key (for auto-increment stores).
    ///
    /// Returns the assigned key value.
    pub fn put_auto(&self, store: &str, mut doc: Value) -> Result<Value> {
        let _lock = self.write_lock.lock().unwrap();
        let def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        if !def.auto_increment {
            return Err(FlowError::JsonDb(format!(
                "store '{}' is not auto-increment",
                store
            )));
        }

        let (next_id, counter_rec) = prepare_counter(&self.engine, store)?;
        let key_val = Value::Number(next_id.into());
        let key_bytes = next_id.to_string().into_bytes();

        // Inject the auto key into the document.
        if let Value::Object(ref mut map) = doc {
            map.insert(def.key_path.clone(), key_val.clone());
        }

        let doc_bytes = encode_doc(&doc)?;
        let mut batch = build_put_batch(&def, store, &key_bytes, &doc_bytes, &doc, &self.engine)?;
        batch.push(counter_rec); // atomic: counter + doc + index entries
        self.engine.write_internal(batch)?;
        Ok(key_val)
    }

    /// Count documents in a store.
    pub fn count(&self, store: &str) -> Result<usize> {
        let _ = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let pfx = doc_prefix(store);
        let iter = self.engine.scan(prefix_range(&pfx))?;
        let mut count = 0;
        for r in iter {
            let _ = r?;
            count += 1;
        }
        Ok(count)
    }

    /// Retrieve all documents in a store.
    pub fn scan(&self, store: &str) -> Result<Vec<Value>> {
        let _ = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let pfx = doc_prefix(store);
        let iter = self.engine.scan(prefix_range(&pfx))?;
        let mut docs = Vec::new();
        for r in iter {
            let rec = r?;
            docs.push(decode_doc(&rec.value)?);
        }
        Ok(docs)
    }

    /// Retrieve all documents in a store, including internal timestamps
    /// (`ts`, `expire_at`) for each document.
    ///
    /// Returns a vector of `(document, ts, expire_at)` tuples.
    pub fn scan_with_meta(&self, store: &str) -> Result<Vec<(Value, i64, i64)>> {
        let _ = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let pfx = doc_prefix(store);
        let iter = self.engine.scan(prefix_range(&pfx))?;
        let mut docs = Vec::new();
        for r in iter {
            let rec = r?;
            docs.push((decode_doc(&rec.value)?, rec.ts, rec.expire_at));
        }
        Ok(docs)
    }

    /// Insert a document only if the key does not already exist.
    ///
    /// Returns an error if a document with the same primary key already exists
    /// (IndexedDB `add()` semantics).
    pub fn add(&self, store: &str, doc: Value) -> Result<Value> {
        let _lock = self.write_lock.lock().unwrap();
        let def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let key_val = extract_field(&doc, &def.key_path).ok_or_else(|| {
            FlowError::JsonDb(format!(
                "document missing key_path '{}' for store '{}'",
                def.key_path, store
            ))
        })?;
        let key_bytes = encode_primary_key(&key_val)?;

        if self.engine.get_bytes(&doc_key(store, &key_bytes), 0).is_some() {
            return Err(FlowError::JsonDb(format!(
                "a record with key '{:?}' already exists in store '{}'",
                key_val, store
            )));
        }

        let doc_bytes = encode_doc(&doc)?;
        let batch = build_put_batch(&def, store, &key_bytes, &doc_bytes, &doc, &self.engine)?;
        self.engine.write_internal(batch)?;
        Ok(key_val)
    }

    /// Remove all documents (and their index entries) from a store, preserving
    /// the store schema. Returns the number of documents removed.
    pub fn clear(&self, store: &str) -> Result<usize> {
        let _lock = self.write_lock.lock().unwrap();
        let def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;

        let count = self.count(store)?;

        let mut records = Vec::new();

        // Range-delete all index entries for each index.
        for index in &def.indexes {
            let pfx = idx_prefix(store, &index.name);
            let end = crate::record::increment_prefix_bytes(&pfx);
            records.push(InternalRecord::delete_range(pfx, end, 0));
        }

        // Range-delete all documents.
        let doc_pfx = doc_prefix(store);
        let doc_end = crate::record::increment_prefix_bytes(&doc_pfx);
        records.push(InternalRecord::delete_range(doc_pfx, doc_end, 0));

        self.engine.write_internal(records)?;
        Ok(count)
    }

    /// Retrieve documents in primary-key order, optionally filtered by a
    /// [`KeyRange`] and limited to `count` results (IndexedDB `getAll`).
    pub fn get_all(
        &self,
        store: &str,
        query: Option<&KeyRange>,
        count: Option<usize>,
    ) -> Result<Vec<Value>> {
        let _ = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;

        let range = match query {
            Some(kr) => kr.to_doc_scan_range(store)?,
            None => prefix_range(&doc_prefix(store)),
        };

        let iter = self.engine.scan(range)?;
        let mut docs = Vec::new();
        for r in iter {
            let rec = r?;
            docs.push(decode_doc(&rec.value)?);
            if count.is_some_and(|n| docs.len() >= n) {
                break;
            }
        }
        Ok(docs)
    }

    /// Retrieve only the primary keys of documents, optionally filtered by a
    /// [`KeyRange`] and limited to `count` results (IndexedDB `getAllKeys`).
    pub fn get_all_keys(
        &self,
        store: &str,
        query: Option<&KeyRange>,
        count: Option<usize>,
    ) -> Result<Vec<Value>> {
        let def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;

        let range = match query {
            Some(kr) => kr.to_doc_scan_range(store)?,
            None => prefix_range(&doc_prefix(store)),
        };

        let iter = self.engine.scan(range)?;
        let mut keys = Vec::new();
        for r in iter {
            let rec = r?;
            let doc = decode_doc(&rec.value)?;
            if let Some(key_val) = extract_field(&doc, &def.key_path) {
                keys.push(key_val);
            }
            if count.is_some_and(|n| keys.len() >= n) {
                break;
            }
        }
        Ok(keys)
    }

    /// Check whether a key exists and return the key value (IndexedDB `getKey`).
    /// Returns `None` if no document with the given key exists.
    pub fn get_key(&self, store: &str, key: &Value) -> Result<Option<Value>> {
        let _ = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let key_bytes = encode_primary_key(key)?;
        if self.engine.get_bytes(&doc_key(store, &key_bytes), 0).is_some() {
            Ok(Some(key.clone()))
        } else {
            Ok(None)
        }
    }

    /// Count documents, optionally filtered by a [`KeyRange`]
    /// (IndexedDB `store.count(query)`).
    pub fn count_with_query(&self, store: &str, query: Option<&KeyRange>) -> Result<usize> {
        let _ = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;

        let range = match query {
            Some(kr) => kr.to_doc_scan_range(store)?,
            None => prefix_range(&doc_prefix(store)),
        };

        let iter = self.engine.scan(range)?;
        let mut count = 0;
        for r in iter {
            let _ = r?;
            count += 1;
        }
        Ok(count)
    }

    /// Look up documents by an exact index value.
    pub fn get_by_index(&self, store: &str, index: &str, value: &Value) -> Result<Vec<Value>> {
        let _def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let idx_def = _def
            .indexes
            .iter()
            .find(|i| i.name == index)
            .ok_or_else(|| {
                FlowError::JsonDb(format!("index '{}' not found on '{}'", index, store))
            })?;
        let is_composite = idx_def.key_paths.len() > 1;

        let encoded = if is_composite {
            // Multi-field index: accept array ["v1","v2"] for full match,
            // or single value for prefix scan on first field.
            match value {
                Value::Array(arr) => encode_composite_value(arr),
                _ => encode_index_value(value),
            }
        } else {
            encode_index_value(value)
        };

        let pfx = idx_value_prefix(store, index, &encoded);
        let iter = self.engine.scan(prefix_range(&pfx))?;
        let mut docs = Vec::new();
        for r in iter {
            let rec = r?;
            if let Some(doc) = self.engine.get_bytes(&doc_key(store, &rec.value), 0) {
                docs.push(decode_doc(&doc.value)?);
            }
        }
        Ok(docs)
    }

    /// Look up documents by a range of index values `[start, end)`.
    ///
    /// The range is **exclusive** of `end`.
    pub fn range_by_index(
        &self,
        store: &str,
        index: &str,
        start: &Value,
        end: &Value,
    ) -> Result<Vec<Value>> {
        let _def = self
            .schema
            .get(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let idx_def = _def
            .indexes
            .iter()
            .find(|i| i.name == index)
            .ok_or_else(|| {
                FlowError::JsonDb(format!("index '{}' not found on '{}'", index, store))
            })?;
        let is_composite = idx_def.key_paths.len() > 1;

        let enc_start = if is_composite {
            match start {
                Value::Array(arr) => encode_composite_value(arr),
                _ => encode_index_value(start),
            }
        } else {
            encode_index_value(start)
        };
        let enc_end = if is_composite {
            match end {
                Value::Array(arr) => encode_composite_value(arr),
                _ => encode_index_value(end),
            }
        } else {
            encode_index_value(end)
        };

        let pfx = idx_prefix(store, index);
        let range = ScanRange {
            key_start: Bound::Included([pfx.as_slice(), &enc_start].concat()),
            key_end: Bound::Excluded([pfx.as_slice(), &enc_end].concat()),
            ts_start: Bound::Unbounded,
            ts_end: Bound::Unbounded,
        };

        let iter = self.engine.scan(range)?;
        let mut docs = Vec::new();
        for r in iter {
            let rec = r?;
            if let Some(doc) = self.engine.get_bytes(&doc_key(store, &rec.value), 0) {
                docs.push(decode_doc(&doc.value)?);
            }
        }
        Ok(docs)
    }

    // ── explicit transactions ──────────────────────────────────────

    /// Begin an explicit transaction over the given stores.
    ///
    /// Call [`Transaction::commit`] to apply buffered writes atomically.
    /// Dropping the transaction without calling `commit` discards all writes.
    pub fn transaction<'db>(
        &'db self,
        stores: &[&str],
        mode: TransactionMode,
    ) -> Result<Transaction<'db>> {
        // Validate all stores exist.
        for name in stores {
            if self.schema.get(name).is_none() {
                return Err(FlowError::JsonDb(format!("store '{}' not found", name)));
            }
        }
        Ok(Transaction {
            db: self,
            mode,
            snapshot_seq: self.engine.last_seq(),
            writes: HashMap::new(),
            read_set: RefCell::new(HashMap::new()),
            counter_updates: Vec::new(),
            next_ids: HashMap::new(),
            committed: false,
        })
    }

    /// Create a [`QueryBuilder`] for the given store.
    pub fn query<'a>(&'a self, store: &'a str) -> QueryBuilder<'a> {
        QueryBuilder::new(self, store)
    }

    // ── generic document API (struct-based) ──────────────────────

    /// Insert or update a document of any `Serialize` type.
    ///
    /// The type **must** have a field matching the store's `key_path`.
    /// Returns the extracted primary key value.
    ///
    /// ```no_run
    /// use flowdb::jsondb::{JsonDB, StoreSchema};
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct User { id: String, name: String }
    ///
    /// let db = JsonDB::open(Default::default()).unwrap();
    /// db.apply_store(&StoreSchema::new("users", "id")).unwrap();
    /// db.put_doc("users", &User { id: "u1".into(), name: "Alice".into() }).unwrap();
    /// ```
    pub fn put_doc<T: serde::Serialize>(&self, store: &str, doc: &T) -> Result<Value> {
        let json = serde_json::to_value(doc).map_err(FlowError::from)?;
        self.put(store, json)
    }

    /// Retrieve a document by primary key, deserialized to `T`.
    ///
    /// ```no_run
    /// use flowdb::jsondb::{JsonDB, StoreSchema};
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize, Serialize)]
    /// struct User { id: String, name: String }
    ///
    /// let db = JsonDB::open(Default::default()).unwrap();
    /// db.apply_store(&StoreSchema::new("users", "id")).unwrap();
    /// db.put_doc("users", &User { id: "u1".into(), name: "Alice".into() }).unwrap();
    /// let user: User = db.get_doc("users", "u1").unwrap().unwrap();
    /// ```
    pub fn get_doc<T: serde::de::DeserializeOwned>(
        &self,
        store: &str,
        key: impl KeyArg,
    ) -> Result<Option<T>> {
        let val = self.get(store, &key.into_value())?;
        match val {
            Some(v) => {
                let t: T = serde_json::from_value(v).map_err(FlowError::from)?;
                Ok(Some(t))
            }
            None => Ok(None),
        }
    }

    /// Delete a document by primary key, accepting any `KeyArg` type.
    pub fn delete_doc(&self, store: &str, key: impl KeyArg) -> Result<()> {
        self.delete(store, &key.into_value())
    }

    // ── cursors ─────────────────────────────────────────────────────

    /// Open a [`Cursor`] over a store's primary-key space, optionally filtered
    /// by a [`KeyRange`], with the given [`CursorDirection`].
    pub fn open_cursor(
        &self,
        store: &str,
        range: Option<&KeyRange>,
        direction: CursorDirection,
    ) -> Result<Cursor> {
        Cursor::open(self, store, range, direction)
    }

    /// Open an [`IndexCursor`] over an index's value space.
    pub fn open_cursor_on_index(
        &self,
        store: &str,
        index: &str,
        range: Option<&KeyRange>,
        direction: CursorDirection,
    ) -> Result<IndexCursor> {
        IndexCursor::open(self, store, index, range, direction)
    }
}
