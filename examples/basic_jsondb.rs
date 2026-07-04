//! FlowDB JsonDB — JSON document store example.
//!
//! Demonstrates object stores, secondary indexes, CRUD on documents,
//! queries with predicates, compound indexes, transactions,
//! StoreDef builder, and the ObjectStore derive macro.

use flowdb::jsondb::{JsonDB, SortDir, StoreSchema, TransactionMode};
use flowdb::{Config, ObjectStore};
use serde::{Deserialize, Serialize};
use serde_json::json;

// ── Derive macro style (new!) ──────────────────────────────────────
//
// The `#[derive(ObjectStore)]` macro generates a `StoreDef` from the
// struct definition, so you don't need to call create_object_store /
// create_index manually.  Use `db.apply_schema::<T>()` once at startup.
//
// Attributes:
//   #[store(key_path = "...")]  — required, sets the primary key field
//   #[index]                    — creates a non-unique index on the field
//   #[index(unique)]            — creates a unique index
//   #[index(name = "...")]      — custom index name (defaults to field name)

#[derive(Debug, Serialize, Deserialize, ObjectStore)]
#[store(name = "users", key_path = "id")]
struct User {
    id: String,
    #[index(name = "by_email", unique)]
    email: String,
    #[index(name = "by_age")]
    age: u32,
    city: String,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, ObjectStore)]
#[store(key_path = "id")]
struct Log {
    #[index(name = "by_level")]
    level: String,
    msg: String,
}

fn main() {
    let dir = tempfile::TempDir::with_prefix("flowdb_jsondb_").unwrap();
    let db = JsonDB::open(Config {
        data_dir: dir.path().to_path_buf(),
        ..Config::default()
    })
    .unwrap();

    // ── 0a. Apply schema via derive macro ─────────────────────────
    // This single call creates the "users" store, sets key_path="id",
    // and creates both indexes (by_email unique, by_age).
    db.apply_schema::<User>().unwrap();

    // ── 0b. Apply schema via StoreDef builder ─────────────────────
    // Equivalent to the derive macro above, but using the builder:
    db.apply_store(
        &StoreSchema::new("logs", "id")
            .with_index("by_level", &["level"], false),
    )
    .unwrap();

    println!("Schemas applied: users (derived), logs (builder)");

    // ── 1. Old-style API (still works) ────────────────────────────
    // db.create_object_store("users", "id").unwrap();
    // db.create_index("users", "by_email", &["email"], true, false).unwrap();
    // db.create_index("users", "by_city_age", &["city", "age"], false, false).unwrap();

    // ── 2. Insert documents ───────────────────────────────────────
    let docs = vec![
        json!({"id": "u1", "email": "alice@ex.com",  "age": 30, "city": "NYC"}),
        json!({"id": "u2", "email": "bob@ex.com",    "age": 25, "city": "NYC"}),
        json!({"id": "u3", "email": "carol@ex.com",  "age": 35, "city": "SF"}),
        json!({"id": "u4", "email": "dave@ex.com",   "age": 28, "city": "SF"}),
    ];
    for doc in &docs {
        db.put("users", doc.clone()).unwrap();
    }
    println!("Inserted {} users", docs.len());

    // ── 3. Point get ─────────────────────────────────────────────
    let user = db.get("users", &json!("u1")).unwrap();
    println!("get u1 → {:#}", user.unwrap());

    // ── 4. Query with index ──────────────────────────────────────
    let results = db
        .query("users")
        .where_eq("city", json!("NYC"))
        .order_by("age", SortDir::Asc)
        .collect()
        .unwrap();
    println!("NYC users (asc age):");
    for doc in &results {
        println!("  {} (age {})", doc["email"], doc["age"]);
    }

    // ── 5. Compound index query (index created manually below) ───
    // Note: the derive macro only created a single-field "age" index,
    // not a compound index.  Let's add one via the classic API:
    db.create_index("users", "by_city_age", &["city", "age"], false, false)
        .unwrap();
    let results = db
        .query("users")
        .where_eq("city", json!("SF"))
        .where_range("age", json!(20), json!(30))
        .collect()
        .unwrap();
    println!("SF users age [20,30): {} found", results.len());

    // ── 6. Unique-index lookup ───────────────────────────────────
    let by_email = db.get_by_index("users", "by_email", &json!("alice@ex.com")).unwrap();
    println!("by_email alice → {:?}", by_email.first().map(|d| &d["id"]));

    // ── 7. Range-by-index query (email prefix) ───────────────────
    let results = db
        .range_by_index("users", "by_email", &json!("a"), &json!("z"))
        .unwrap();
    println!("users by email [a..z): {}", results.len());

    // ── 8. Serde typed API ──────────────────────────────────────
    let new_user = User {
        id: "u5".into(),
        email: "eve@ex.com".into(),
        age: 32,
        city: "LA".into(),
    };
    db.put_doc("users", &new_user).unwrap();
    let back: Option<User> = db.get_doc("users", "u5").unwrap();
    println!("serde round-trip → {:?}", back.unwrap());

    // ── 9. Transactions ──────────────────────────────────────────
    {
        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        let doc = tx.get("users", &json!("u1")).unwrap().unwrap();
        let mut updated = doc.as_object().unwrap().clone();
        updated.insert("email".into(), json!("alice_new@ex.com"));
        tx.put("users", json!(updated)).unwrap();
        tx.commit().unwrap();
        println!("transaction committed");
    }
    let user = db.get("users", &json!("u1")).unwrap();
    println!("u1 email after tx → {}", user.unwrap()["email"]);

    // ── 10. Inspect schema ───────────────────────────────────────
    println!("stores: {:?}", db.store_names());
    let store = db.get_store("users").unwrap();
    println!("users key_path={}", store.key_path);
    for idx in &store.indexes {
        println!("  index {} fields={:?} unique={}", idx.name, idx.key_paths, idx.unique);
    }

    // ── 11. apply_store is idempotent ────────────────────────────
    // Calling apply_store again with the same def is a no-op.
    db.apply_schema::<User>().unwrap();
    println!("apply_schema (idempotent) OK");

    db.shutdown().unwrap();
    println!("Done (data in {})", dir.path().display());
}
