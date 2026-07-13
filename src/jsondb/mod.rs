//! JsonDB – A JSON document database interface built on top of FlowDB.
//!
//! Provides an IndexedDB-like API with ACID transactions, secondary indexes,
//! and auto-increment support.

pub(crate) mod cursor;
pub(crate) mod db;
pub(crate) mod helpers;
pub(crate) mod keyrange;
pub(crate) mod query;
#[cfg(test)]
mod tests;
pub(crate) mod transaction;

mod encoding;
mod schema;

use serde_json::Value;

pub use cursor::{Cursor, CursorDirection, IndexCursor};
pub use db::JsonDB;
pub use keyrange::KeyRange;
pub use query::{QueryBuilder, SortDir};
pub use schema::{IndexDef as IndexSchema, StoreDef as StoreSchema};
pub use transaction::Transaction;

/// Trait implemented by the [`ObjectStore`](derive@ObjectStore) derive macro.
///
/// Provides a [`StoreDef`] that can be applied via [`JsonDB::apply_store`].
///
/// # Example
///
/// ```no_run
/// use flowdb::ObjectStore;
/// use flowdb::jsondb::JsonDB;
///
/// #[derive(ObjectStore)]
/// #[store(key_path = "id")]
/// struct User {
///     #[index(unique)]
///     email: String,
///     #[index]
///     age: u32,
/// }
///
/// let db = JsonDB::open(Default::default()).unwrap();
/// db.apply_schema::<User>().unwrap();
/// ```
pub trait ObjectStore {
    /// Returns the [`StoreDef`] for this struct, including all annotated indexes.
    fn store_def() -> StoreSchema;
}

/// Trait for types that can be used as a primary key argument.
///
/// Implemented for `&str`, `String`, `i64`, `i32`, `u64`, `u32`, `Value`, and `&Value`.
///
/// Used by the serde-typed methods [`JsonDB::get_doc`] and [`JsonDB::delete_doc`]
/// to accept both string and numeric keys without the caller needing to wrap them in `json!()`.
pub trait KeyArg {
    /// Convert `self` into a `serde_json::Value` suitable for use as a document key.
    fn into_value(self) -> Value;
}

impl KeyArg for &str {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }
}
impl KeyArg for String {
    fn into_value(self) -> Value {
        Value::String(self)
    }
}
impl KeyArg for i64 {
    fn into_value(self) -> Value {
        Value::Number(self.into())
    }
}
impl KeyArg for i32 {
    fn into_value(self) -> Value {
        Value::Number((self as i64).into())
    }
}
impl KeyArg for u64 {
    fn into_value(self) -> Value {
        Value::Number(self.into())
    }
}
impl KeyArg for u32 {
    fn into_value(self) -> Value {
        Value::Number((self as u64).into())
    }
}
impl KeyArg for Value {
    fn into_value(self) -> Value {
        self
    }
}
impl KeyArg for &Value {
    fn into_value(self) -> Value {
        self.clone()
    }
}

/// Transaction mode (read-only vs read-write).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    /// Read-only — queries only.
    ReadOnly,
    /// Read-write — queries, puts, deletes, and index updates.
    ReadWrite,
}
