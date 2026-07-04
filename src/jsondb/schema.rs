//! Schema definitions for JsonDB object stores and indexes.

use crate::error::{FlowError, Result};
use crate::jsondb::encoding::{schema_key, schema_prefix, validate_name};
use crate::record::{InternalRecord, ScanRange};
use parking_lot::RwLock;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize)]
pub struct IndexDef {
    #[serde(alias = "key_path")]
    pub key_paths: Vec<String>,
    pub name: String,
    pub unique: bool,
    pub multi_entry: bool,
}

/// Custom deserialize: accepts both old `"key_path": "single"` and
/// new `"key_paths": ["a", "b"]` (or `"key_path": ["a","b"]` via alias).
impl<'de> Deserialize<'de> for IndexDef {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            name: String,
            #[serde(alias = "key_paths")]
            key_path: serde_json::Value,
            unique: Option<bool>,
            multi_entry: Option<bool>,
        }
        let raw = Raw::deserialize(deserializer)?;
        let key_paths = match raw.key_path {
            serde_json::Value::String(s) => vec![s],
            serde_json::Value::Array(arr) => arr
                .into_iter()
                .filter_map(|v| match v {
                    serde_json::Value::String(s) => Some(s),
                    _ => None,
                })
                .collect(),
            _ => {
                return Err(D::Error::custom(
                    "key_paths must be a string or array of strings",
                ));
            }
        };
        if key_paths.is_empty() {
            return Err(D::Error::custom("key_paths must not be empty"));
        }
        Ok(IndexDef {
            name: raw.name,
            key_paths,
            unique: raw.unique.unwrap_or(false),
            multi_entry: raw.multi_entry.unwrap_or(false),
        })
    }
}

/// Definition of an object store (analogous to a table in SQL or an
/// object store in IndexedDB).
///
/// Construct one via [`StoreDef::new`] and optionally chain builder methods:
///
/// ```no_run
/// use flowdb::jsondb::StoreSchema;
///
/// let users = StoreSchema::new("users", "id")
///     .with_index("by_email", &["email"], true)
///     .with_index("by_city_age", &["city", "age"], false);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreDef {
    /// Store name (unique within a JsonDB instance).
    pub name: String,
    /// Dotted path to the primary key field within each document
    /// (e.g. `"id"`, `"user.id"`).
    pub key_path: String,
    /// When true, the primary key is auto-generated as a monotonically
    /// increasing integer on [`JsonDB::put_auto`].
    pub auto_increment: bool,
    /// Secondary indexes defined on this store.
    pub indexes: Vec<IndexDef>,
    /// Next auto-increment ID (only meaningful when `auto_increment` is true).
    pub next_auto_id: u64,
}

impl StoreDef {
    /// Create a new store definition with the given `name` and `key_path`.
    ///
    /// Equivalent to the old style:
    /// ```ignore
    /// db.create_object_store("users", "id")?;
    /// ```
    pub fn new(name: &str, key_path: &str) -> Self {
        Self {
            name: name.to_string(),
            key_path: key_path.to_string(),
            auto_increment: false,
            indexes: Vec::new(),
            next_auto_id: 0,
        }
    }

    /// Add a secondary index to this store definition (builder pattern).
    ///
    /// Equivalent to the old style:
    /// ```ignore
    /// db.create_index("users", "by_email", &["email"], true, false)?;
    /// ```
    pub fn with_index(mut self, name: &str, key_paths: &[&str], unique: bool) -> Self {
        self.indexes.push(IndexDef {
            name: name.to_string(),
            key_paths: key_paths.iter().map(|s| s.to_string()).collect(),
            unique,
            multi_entry: false,
        });
        self
    }

    /// Enable auto-increment on this store (builder pattern).
    ///
    /// When enabled, the primary key is auto-generated as a monotonically
    /// increasing integer on [`JsonDB::put_auto`].
    pub fn with_auto_increment(mut self) -> Self {
        self.auto_increment = true;
        self
    }
}

/// Thread-safe in-memory cache of all store schemas.
#[derive(Debug)]
pub struct Schema {
    stores: RwLock<HashMap<String, StoreDef>>,
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

impl Schema {
    pub fn new() -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, name: &str) -> Option<StoreDef> {
        self.stores.read().get(name).cloned()
    }

    pub fn insert(&self, def: StoreDef) {
        self.stores.write().insert(def.name.clone(), def);
    }

    pub fn remove(&self, name: &str) -> Option<StoreDef> {
        self.stores.write().remove(name)
    }

    pub fn list(&self) -> Vec<StoreDef> {
        self.stores.read().values().cloned().collect()
    }
}

/// Load all schemas from the engine by scanning the schema-key prefix.
pub(crate) fn load_schemas(
    scan_fn: impl Fn(ScanRange) -> crate::error::Result<Vec<crate::record::Record>>,
) -> Result<Schema> {
    let schema = Schema::new();
    let range = crate::jsondb::encoding::prefix_range(&schema_prefix());
    let records = scan_fn(range)?;
    for rec in records {
        let def: StoreDef = serde_json::from_slice(&rec.value).map_err(FlowError::from)?;
        schema.insert(def);
    }
    Ok(schema)
}

/// Validate and create a new store definition.
pub(crate) fn validate_store_def(name: &str, key_path: &str) -> Result<()> {
    validate_name(name)?;
    if key_path.is_empty() {
        return Err(FlowError::JsonDb("key_path cannot be empty".into()));
    }
    Ok(())
}

/// Validate and create a new index definition.
pub(crate) fn validate_index_def(store: &StoreDef, name: &str, key_paths: &[&str]) -> Result<()> {
    validate_name(name)?;
    if key_paths.is_empty() || key_paths.iter().any(|p| p.is_empty()) {
        return Err(FlowError::JsonDb("index key_paths cannot be empty".into()));
    }
    if store.indexes.iter().any(|i| i.name == name) {
        return Err(FlowError::JsonDb(format!(
            "index '{}' already exists in store '{}'",
            name, store.name
        )));
    }
    Ok(())
}

/// Serialize a StoreDef to a put record for the given store name.
pub(crate) fn schema_record(def: &StoreDef) -> Result<crate::record::Record> {
    Ok(crate::record::Record::new(
        schema_key(&def.name),
        0,
        serde_json::to_vec(def).map_err(FlowError::from)?,
    ))
}

/// Create a delete record for a store's schema.
pub(crate) fn schema_delete_record(store: &str) -> InternalRecord {
    InternalRecord::delete(schema_key(store), 0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_basic() {
        let schema = Schema::new();
        let def = StoreDef {
            name: "users".into(),
            key_path: "id".into(),
            auto_increment: false,
            indexes: vec![IndexDef {
                name: "by_email".into(),
                key_paths: vec!["email".into()],
                unique: true,
                multi_entry: false,
            }],
            next_auto_id: 0,
        };
        schema.insert(def.clone());
        assert_eq!(schema.get("users").unwrap().name, "users");
        assert_eq!(schema.list().len(), 1);
        schema.remove("users");
        assert!(schema.get("users").is_none());
    }

    #[test]
    fn test_validate_store_def() {
        assert!(validate_store_def("users", "id").is_ok());
        assert!(validate_store_def("", "id").is_err());
        assert!(validate_store_def("users", "").is_err());
        assert!(validate_store_def("has space", "id").is_err());
    }

    #[test]
    fn test_validate_index_def_cases() {
        let store = StoreDef {
            name: "users".into(),
            key_path: "id".into(),
            auto_increment: false,
            indexes: vec![IndexDef {
                name: "existing".into(),
                key_paths: vec!["f".into()],
                unique: true,
                multi_entry: false,
            }],
            next_auto_id: 0,
        };
        assert!(validate_index_def(&store, "", &["f"]).is_err());
        assert!(validate_index_def(&store, "new", &[""]).is_err());
        assert!(validate_index_def(&store, "existing", &["f"]).is_err());
        assert!(validate_index_def(&store, "new", &["f"]).is_ok());
        assert!(validate_index_def(&store, "new", &["a", "b"]).is_ok());
    }

    #[test]
    fn test_schema_record_roundtrip() {
        let def = StoreDef {
            name: "t".into(),
            key_path: "id".into(),
            auto_increment: true,
            indexes: vec![],
            next_auto_id: 100,
        };
        let rec = schema_record(&def).unwrap();
        let decoded: StoreDef = serde_json::from_slice(&rec.value).unwrap();
        assert_eq!(decoded.name, "t");
        assert_eq!(decoded.next_auto_id, 100);
        assert!(decoded.auto_increment);
    }

    #[test]
    fn test_schema_delete_record_roundtrip() {
        let rec = schema_delete_record("test_store");
        assert_eq!(rec.key, schema_key("test_store"));
        assert_eq!(rec.op, crate::record::Op::Delete);
    }

    #[test]
    fn test_schema_list_empty() {
        let schema = Schema::new();
        assert!(schema.list().is_empty());
        assert!(schema.get("x").is_none());
    }

    #[test]
    fn test_schema_default() {
        let schema: Schema = Default::default();
        assert!(schema.list().is_empty());
    }

    // ── backward compat deserialization ───────────────────────────

    #[test]
    fn test_index_def_deserialize_old_key_path_string() {
        let json = r#"{"name":"by_email","key_path":"email","unique":true,"multi_entry":false}"#;
        let idx: IndexDef = serde_json::from_str(json).unwrap();
        assert_eq!(idx.key_paths, vec!["email"]);
        assert!(idx.unique);
    }

    #[test]
    fn test_index_def_deserialize_old_key_path_array() {
        let json = r#"{"name":"by_city_age","key_path":["city","age"],"unique":false,"multi_entry":false}"#;
        let idx: IndexDef = serde_json::from_str(json).unwrap();
        assert_eq!(idx.key_paths, vec!["city", "age"]);
    }

    #[test]
    fn test_index_def_deserialize_new_key_paths_array() {
        let json = r#"{"name":"by_city_age","key_paths":["city","age"],"unique":false,"multi_entry":false}"#;
        let idx: IndexDef = serde_json::from_str(json).unwrap();
        assert_eq!(idx.key_paths, vec!["city", "age"]);
    }

    #[test]
    fn test_index_def_deserialize_empty_key_paths_rejected() {
        let json = r#"{"name":"bad","key_paths":[],"unique":false,"multi_entry":false}"#;
        assert!(serde_json::from_str::<IndexDef>(json).is_err());
    }

    #[test]
    fn test_index_def_serialize_new_format() {
        let idx = IndexDef {
            name: "by_city_age".into(),
            key_paths: vec!["city".into(), "age".into()],
            unique: false,
            multi_entry: false,
        };
        let json = serde_json::to_string(&idx).unwrap();
        assert!(json.contains(r#""key_paths""#));
        assert!(json.contains(r#""city""#));
        assert!(json.contains(r#""age""#));
    }
}
