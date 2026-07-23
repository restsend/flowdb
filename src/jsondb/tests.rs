// ── tests ─────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use crate::Config;
    use crate::jsondb::encoding::*;
    use crate::jsondb::schema::*;

    use crate::jsondb::{CursorDirection, JsonDB, KeyRange, SortDir, TransactionMode};
    use crate::jsondb::StoreSchema;

    use crate::record::InternalRecord;
    use serde_json::{Value, json};
    use std::sync::Arc;
    use tempfile::TempDir;

    pub(crate) fn test_db() -> (JsonDB, TempDir) {
        let dir = TempDir::new().unwrap();
        let cfg = Config {
            data_dir: dir.path().to_path_buf(),
            auto_background: false,
            compression: crate::record::CompressionAlgorithm::Lz4,
            ..Default::default()
        };
        let db = JsonDB::open(cfg).unwrap();
        (db, dir)
    }

    pub(crate) fn setup_users(db: &JsonDB) {
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true, false)
            .unwrap();
        db.create_index("users", "by_age", &["age"], false, false).unwrap();
    }

    // ── basic CRUD ────────────────────────────────────────────────

    #[test]
    fn test_put_and_get() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        db.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();
        let doc = db.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");
    }

    #[test]
    fn test_get_nonexistent() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let doc = db.get("users", &json!("missing")).unwrap();
        assert!(doc.is_none());
    }

    #[test]
    fn test_put_and_delete() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        db.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();
        assert_eq!(db.count("users").unwrap(), 1);

        db.delete("users", &json!("u1")).unwrap();
        assert_eq!(db.count("users").unwrap(), 0);
        assert!(db.get("users", &json!("u1")).unwrap().is_none());
    }

    #[test]
    fn test_put_update() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        db.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();
        db.put("users", json!({"id": "u1", "name": "Bob"})).unwrap();

        let doc = db.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc["name"], "Bob");
        assert_eq!(db.count("users").unwrap(), 1);
    }

    #[test]
    fn test_scan() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        db.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();
        db.put("users", json!({"id": "u2", "name": "Bob"})).unwrap();
        db.put("users", json!({"id": "u3", "name": "Carol"}))
            .unwrap();

        let docs = db.scan("users").unwrap();
        assert_eq!(docs.len(), 3);
    }

    #[test]
    fn test_count() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        assert_eq!(db.count("users").unwrap(), 0);
        db.put("users", json!({"id": "u1"})).unwrap();
        assert_eq!(db.count("users").unwrap(), 1);
        db.put("users", json!({"id": "u2"})).unwrap();
        assert_eq!(db.count("users").unwrap(), 2);
        db.delete("users", &json!("u1")).unwrap();
        assert_eq!(db.count("users").unwrap(), 1);
    }

    // ── secondary indexes ─────────────────────────────────────────

    #[test]
    fn test_index_point_lookup() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put(
            "users",
            json!({"id": "u1", "email": "alice@b.com", "age": 30}),
        )
        .unwrap();
        db.put(
            "users",
            json!({"id": "u2", "email": "bob@c.com", "age": 25}),
        )
        .unwrap();

        let docs = db
            .get_by_index("users", "by_email", &json!("alice@b.com"))
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    #[test]
    fn test_index_range_lookup() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put(
            "users",
            json!({"id": "u1", "email": "alice@b.com", "age": 30}),
        )
        .unwrap();
        db.put(
            "users",
            json!({"id": "u2", "email": "bob@c.com", "age": 25}),
        )
        .unwrap();
        db.put(
            "users",
            json!({"id": "u3", "email": "carol@d.com", "age": 35}),
        )
        .unwrap();

        // age in [25, 35) — should include u2 (age=25) and u1 (age=30)
        let docs = db
            .range_by_index("users", "by_age", &json!(25), &json!(35))
            .unwrap();
        assert_eq!(docs.len(), 2, "expected 2 docs in age range [25,35)");
    }

    #[test]
    fn test_unique_index_violation() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put("users", json!({"id": "u1", "email": "same@b.com"}))
            .unwrap();

        let err = db
            .put("users", json!({"id": "u2", "email": "same@b.com"}))
            .unwrap_err();
        assert!(
            err.to_string().contains("unique"),
            "expected unique violation: {}",
            err
        );
    }

    #[test]
    fn test_index_update_on_put() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put(
            "users",
            json!({"id": "u1", "email": "old@b.com", "age": 30}),
        )
        .unwrap();

        // Update the email.
        db.put(
            "users",
            json!({"id": "u1", "email": "new@b.com", "age": 30}),
        )
        .unwrap();

        // Old email should have no docs.
        let docs = db
            .get_by_index("users", "by_email", &json!("old@b.com"))
            .unwrap();
        assert_eq!(docs.len(), 0, "old index entry should be gone");

        // New email should have the doc.
        let docs = db
            .get_by_index("users", "by_email", &json!("new@b.com"))
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    #[test]
    fn test_index_delete_removes_entries() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put("users", json!({"id": "u1", "email": "alice@b.com"}))
            .unwrap();
        db.delete("users", &json!("u1")).unwrap();

        let docs = db
            .get_by_index("users", "by_email", &json!("alice@b.com"))
            .unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn test_create_index_on_existing_data() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        // Put docs before creating index.
        db.put("users", json!({"id": "u1", "email": "a@b.com"}))
            .unwrap();
        db.put("users", json!({"id": "u2", "email": "b@c.com"}))
            .unwrap();

        // Now create the index.
        db.create_index("users", "by_email", &["email"], true, false)
            .unwrap();

        // Should be able to query by index.
        let docs = db
            .get_by_index("users", "by_email", &json!("a@b.com"))
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    // ── auto-increment ────────────────────────────────────────────

    #[test]
    fn test_auto_increment_store() {
        let (db, _dir) = test_db();
        db.create_object_store("events", "id").unwrap();
        // Make it auto-increment by setting the flag (simulated via direct schema update).
        let mut def = db.get_store("events").unwrap();
        def.auto_increment = true;
        db.engine
            .write_internal(vec![InternalRecord::from_record(
                &schema_record(&def).unwrap(),
                0,
            )])
            .unwrap();
        db.schema.insert(def);

        db.put_auto("events", json!({"type": "click"})).unwrap();
        db.put_auto("events", json!({"type": "scroll"})).unwrap();
        db.put_auto("events", json!({"type": "nav"})).unwrap();

        assert_eq!(db.count("events").unwrap(), 3);
        let doc1 = db.get("events", &json!(1)).unwrap().unwrap();
        assert_eq!(doc1["type"], "click");
        let doc3 = db.get("events", &json!(3)).unwrap().unwrap();
        assert_eq!(doc3["type"], "nav");
    }

    #[test]
    fn test_get_with_meta() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();

        // get_with_meta returns (doc, ts, expire_at).
        let result = db.get_with_meta("users", &json!("u1")).unwrap();
        assert!(result.is_some());
        let (doc, ts, expire_at) = result.unwrap();
        assert_eq!(doc["name"], "Alice");
        assert!(ts >= 0, "ts should be non-negative");
        assert_eq!(expire_at, i64::MAX, "default expire_at should be i64::MAX");

        // Non-existent key returns None.
        let missing = db.get_with_meta("users", &json!("nope")).unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_scan_with_meta() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();
        db.put("users", json!({"id": "u2", "name": "Bob"}))
            .unwrap();

        let docs = db.scan_with_meta("users").unwrap();
        assert_eq!(docs.len(), 2);
        for (doc, ts, expire_at) in &docs {
            assert!(doc["name"].is_string());
            assert!(*ts >= 0);
            assert_eq!(*expire_at, i64::MAX);
        }
    }

    #[test]
    fn test_apply_store_auto_increment() {
        let (db, _dir) = test_db();
        // Use apply_store with with_auto_increment — should work in one shot.
        let def = StoreSchema::new("events", "id").with_auto_increment();
        db.apply_store(&def).unwrap();

        // put_auto should work immediately.
        let id1 = db.put_auto("events", json!({"type": "click"})).unwrap();
        assert_eq!(id1, json!(1));
        let id2 = db.put_auto("events", json!({"type": "scroll"})).unwrap();
        assert_eq!(id2, json!(2));
    }

    // ── explicit transactions ─────────────────────────────────────

    #[test]
    fn test_transaction_commit() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        tx.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();
        tx.put("users", json!({"id": "u2", "name": "Bob"})).unwrap();
        tx.commit().unwrap();

        assert_eq!(db.count("users").unwrap(), 2);
    }

    #[test]
    fn test_transaction_abort() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        tx.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();
        tx.abort(); // drop without commit

        assert_eq!(db.count("users").unwrap(), 0);
    }

    #[test]
    fn test_transaction_read_your_writes() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        db.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        // Should see committed data.
        let doc = tx.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");

        // Buffered write should be visible.
        tx.put("users", json!({"id": "u2", "name": "Bob"})).unwrap();
        let doc2 = tx.get("users", &json!("u2")).unwrap().unwrap();
        assert_eq!(doc2["name"], "Bob");

        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_index_read_your_writes() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put("users", json!({"id": "u1", "email": "alice@b.com"}))
            .unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        tx.put("users", json!({"id": "u2", "email": "bob@c.com"}))
            .unwrap();

        // The index in the engine doesn't yet know about u2, but the
        // transaction's get_by_index should find it via the write buffer.
        let docs = tx
            .get_by_index("users", "by_email", &json!("bob@c.com"))
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u2");

        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_atomicity() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true, false)
            .unwrap();

        // This will fail because u1 already exists.
        db.put("users", json!({"id": "u1", "email": "a@b.com"}))
            .unwrap();

        // Single batch with a unique violation.
        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        tx.put("users", json!({"id": "u2", "email": "b@c.com"}))
            .unwrap();
        tx.put("users", json!({"id": "u3", "email": "a@b.com"}))
            .unwrap(); // violation

        let err = tx.commit();
        assert!(err.is_err(), "expected unique violation on commit");

        // The entire batch should be rolled back.
        assert_eq!(db.count("users").unwrap(), 1);
        assert!(db.get("users", &json!("u2")).unwrap().is_none());
    }

    #[test]
    fn test_transaction_readonly_rejects_writes() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadOnly)
            .unwrap();
        let err = tx
            .put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap_err();
        assert!(
            err.to_string().contains("read-only"),
            "expected read-only error: {}",
            err
        );
    }

    // ── schema management ─────────────────────────────────────────

    #[test]
    fn test_create_delete_store() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        assert!(db.get_store("users").is_some());

        db.put("users", json!({"id": "u1"})).unwrap();
        assert_eq!(db.count("users").unwrap(), 1);

        db.delete_object_store("users").unwrap();
        assert!(db.get_store("users").is_none());
    }

    #[test]
    fn test_create_delete_index() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true, false)
            .unwrap();

        let def = db.get_store("users").unwrap();
        assert_eq!(def.indexes.len(), 1);

        db.delete_index("users", "by_email").unwrap();
        let def = db.get_store("users").unwrap();
        assert_eq!(def.indexes.len(), 0);
    }

    #[test]
    fn test_store_names() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_object_store("orders", "id").unwrap();
        let mut names = db.store_names();
        names.sort();
        assert_eq!(names, vec!["orders", "users"]);
    }

    // ── edge cases ────────────────────────────────────────────────

    #[test]
    fn test_missing_store_returns_error() {
        let (db, _dir) = test_db();
        let err = db.get("nonexistent", &json!("1")).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_document_without_key_field() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let err = db.put("users", json!({"name": "Alice"})).unwrap_err();
        assert!(
            err.to_string().contains("missing"),
            "expected missing-key error: {}",
            err
        );
    }

    #[test]
    fn test_duplicate_put_updates() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        db.put("users", json!({"id": "u1", "val": 1})).unwrap();
        db.put("users", json!({"id": "u1", "val": 2})).unwrap();
        db.put("users", json!({"id": "u1", "val": 3})).unwrap();

        assert_eq!(db.count("users").unwrap(), 1);
        assert_eq!(db.get("users", &json!("u1")).unwrap().unwrap()["val"], 3);
    }

    #[test]
    fn test_reopen_persists_data() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // First session.
        {
            let cfg = Config {
                data_dir: path.clone(),
                auto_background: false,
                compression: crate::record::CompressionAlgorithm::Lz4,
                ..Default::default()
            };
            let db = JsonDB::open(cfg).unwrap();
            db.create_object_store("users", "id").unwrap();
            db.create_index("users", "by_email", &["email"], true, false)
                .unwrap();
            db.put("users", json!({"id": "u1", "email": "a@b.com"}))
                .unwrap();
            db.shutdown().unwrap();
        }

        // Second session — reopen.
        {
            let cfg = Config {
                data_dir: path,
                auto_background: false,
                compression: crate::record::CompressionAlgorithm::Lz4,
                ..Default::default()
            };
            let db = JsonDB::open(cfg).unwrap();
            assert!(db.get_store("users").is_some());

            let doc = db.get("users", &json!("u1")).unwrap().unwrap();
            assert_eq!(doc["email"], "a@b.com");

            let docs = db
                .get_by_index("users", "by_email", &json!("a@b.com"))
                .unwrap();
            assert_eq!(docs.len(), 1);
        }
    }

    #[test]
    fn test_non_string_primary_key() {
        let (db, _dir) = test_db();
        db.create_object_store("nums", "id").unwrap();

        db.put("nums", json!({"id": 42, "name": "answer"})).unwrap();
        db.put("nums", json!({"id": 100, "name": "hundred"}))
            .unwrap();

        assert_eq!(db.count("nums").unwrap(), 2);
        let doc = db.get("nums", &json!(42)).unwrap().unwrap();
        assert_eq!(doc["name"], "answer");

        let doc = db.get("nums", &json!(100)).unwrap().unwrap();
        assert_eq!(doc["name"], "hundred");
    }

    #[test]
    fn test_index_on_deleted_doc_removed() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put("users", json!({"id": "u1", "email": "a@b.com"}))
            .unwrap();
        db.put("users", json!({"id": "u2", "email": "b@c.com"}))
            .unwrap();

        let docs = db
            .get_by_index("users", "by_email", &json!("a@b.com"))
            .unwrap();
        assert_eq!(docs.len(), 1);

        db.delete("users", &json!("u1")).unwrap();

        let docs = db
            .get_by_index("users", "by_email", &json!("a@b.com"))
            .unwrap();
        assert_eq!(docs.len(), 0, "index entry should be removed after delete");
    }

    // ── error path tests ─────────────────────────────────────────

    #[test]
    fn test_put_missing_store() {
        let (db, _dir) = test_db();
        let err = db.put("nonexistent", json!({"id": "1"})).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_delete_missing_store() {
        let (db, _dir) = test_db();
        let err = db.delete("nonexistent", &json!("1")).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_count_missing_store() {
        let (db, _dir) = test_db();
        let err = db.count("nonexistent").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_scan_missing_store() {
        let (db, _dir) = test_db();
        let err = db.scan("nonexistent").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_get_by_index_missing_store() {
        let (db, _dir) = test_db();
        let err = db
            .get_by_index("nonexistent", "idx", &json!("v"))
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_get_by_index_missing_index() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let err = db
            .get_by_index("users", "nonexistent", &json!("v"))
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_range_by_index_missing_store() {
        let (db, _dir) = test_db();
        let err = db
            .range_by_index("nonexistent", "idx", &json!(0), &json!(10))
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_range_by_index_missing_index() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let err = db
            .range_by_index("users", "nonexistent", &json!(0), &json!(10))
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_put_auto_non_auto() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let err = db.put_auto("users", json!({"type": "x"})).unwrap_err();
        assert!(err.to_string().contains("not auto-increment"));
    }

    #[test]
    fn test_put_auto_missing_store() {
        let (db, _dir) = test_db();
        let err = db
            .put_auto("nonexistent", json!({"type": "x"}))
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_delete_object_store_missing() {
        let (db, _dir) = test_db();
        let err = db.delete_object_store("nonexistent").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_create_object_store_same_name_different_key_path() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let err = db.create_object_store("users", "uuid").unwrap_err();
        assert!(err.to_string().contains("different key_path"));
    }

    #[test]
    fn test_create_object_store_idempotent() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        // Same name + same key_path should succeed (no-op).
        assert!(db.create_object_store("users", "id").is_ok());
    }

    #[test]
    fn test_create_index_duplicate() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true, false)
            .unwrap();
        let err = db
            .create_index("users", "by_email", &["phone"], true, false)
            .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn test_create_index_missing_store() {
        let (db, _dir) = test_db();
        let err = db
            .create_index("nonexistent", "idx", &["field"], false, false)
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_delete_index_missing_store() {
        let (db, _dir) = test_db();
        let err = db.delete_index("nonexistent", "idx").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_delete_index_missing() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let err = db.delete_index("users", "nonexistent").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_transaction_missing_store() {
        let (db, _dir) = test_db();
        let err = db
            .transaction(&["nonexistent"], TransactionMode::ReadWrite)
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_transaction_delete_in_buffer() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        let doc = tx.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");

        tx.delete("users", &json!("u1")).unwrap();
        assert!(tx.get("users", &json!("u1")).unwrap().is_none());
        tx.commit().unwrap();

        assert!(db.get("users", &json!("u1")).unwrap().is_none());
    }

    #[test]
    fn test_transaction_put_auto() {
        let (db, _dir) = test_db();
        db.create_object_store("events", "id").unwrap();
        let mut def = db.get_store("events").unwrap();
        def.auto_increment = true;
        db.engine
            .write_internal(vec![InternalRecord::from_record(
                &schema_record(&def).unwrap(),
                0,
            )])
            .unwrap();
        db.schema.insert(def);

        let mut tx = db
            .transaction(&["events"], TransactionMode::ReadWrite)
            .unwrap();
        let key1 = tx.put_auto("events", json!({"type": "click"})).unwrap();
        assert_eq!(key1, json!(1));
        let key2 = tx.put_auto("events", json!({"type": "scroll"})).unwrap();
        assert_eq!(key2, json!(2));
        tx.commit().unwrap();

        assert_eq!(db.count("events").unwrap(), 2);
    }

    #[test]
    fn test_transaction_put_auto_non_auto() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        let err = tx.put_auto("users", json!({"name": "x"})).unwrap_err();
        assert!(err.to_string().contains("not auto-increment"));
    }

    #[test]
    fn test_transaction_scan_buffered_puts() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        tx.put("users", json!({"id": "u2", "name": "Bob"})).unwrap();

        // Scan should see both committed and buffered docs.
        let docs = tx.scan("users").unwrap();
        assert_eq!(docs.len(), 2);
        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_scan_with_buffered_delete() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();
        db.put("users", json!({"id": "u2", "name": "Bob"})).unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        tx.delete("users", &json!("u1")).unwrap();

        let docs = tx.scan("users").unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Bob");
        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_range_by_index() {
        let (db, _dir) = test_db();
        setup_users(&db);
        db.put("users", json!({"id": "u1", "age": 30})).unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        tx.put("users", json!({"id": "u2", "age": 25})).unwrap();

        let docs = tx
            .range_by_index("users", "by_age", &json!(20), &json!(35))
            .unwrap();
        // Should see both u1 (committed) and u2 (buffered)
        assert_eq!(docs.len(), 2);
        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_get_by_index_buffered_update() {
        let (db, _dir) = test_db();
        setup_users(&db);
        db.put("users", json!({"id": "u1", "email": "old@b.com"}))
            .unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        // Update email in buffer (not yet committed)
        tx.delete("users", &json!("u1")).unwrap();
        tx.put("users", json!({"id": "u1", "email": "new@b.com"}))
            .unwrap();

        // get_by_index should NOT find old email (doc deleted in buffer)
        let docs = tx
            .get_by_index("users", "by_email", &json!("old@b.com"))
            .unwrap();
        assert_eq!(docs.len(), 0, "old email should not be visible");

        // Should find new email from buffer
        let docs = tx
            .get_by_index("users", "by_email", &json!("new@b.com"))
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_get_by_index_buffered_delete_only() {
        let (db, _dir) = test_db();
        setup_users(&db);
        db.put("users", json!({"id": "u1", "email": "a@b.com"}))
            .unwrap();

        let tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        // Without any buffered writes, read-only should work
        let docs = tx
            .get_by_index("users", "by_email", &json!("a@b.com"))
            .unwrap();
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn test_transaction_abort_drop() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        {
            let mut tx = db
                .transaction(&["users"], TransactionMode::ReadWrite)
                .unwrap();
            tx.put("users", json!({"id": "u1", "name": "Alice"}))
                .unwrap();
            // Drop without commit = abort
        }
        assert_eq!(db.count("users").unwrap(), 0);
    }

    #[test]
    fn test_transaction_double_commit() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        // tx.put... but we need to capture the result of first commit
        // empty commit should work
        assert!(tx.commit().is_ok());
    }

    #[test]
    fn test_transaction_empty_commit() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        let tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        assert!(tx.commit().is_ok());
    }

    // ── delete_object_store with data ─────────────────────────────

    #[test]
    fn test_delete_store_with_indexes_and_data() {
        let (db, _dir) = test_db();
        setup_users(&db);
        db.put("users", json!({"id": "u1", "email": "a@b.com", "age": 30}))
            .unwrap();
        db.put("users", json!({"id": "u2", "email": "b@c.com", "age": 25}))
            .unwrap();
        assert_eq!(db.count("users").unwrap(), 2);

        db.delete_object_store("users").unwrap();
        assert!(db.get_store("users").is_none());
        // Create a new store with the same name to verify clean state
        db.create_object_store("users", "id").unwrap();
        assert_eq!(db.count("users").unwrap(), 0);
    }

    #[test]
    fn test_delete_store_with_auto_increment() {
        let (db, _dir) = test_db();
        db.create_object_store("events", "id").unwrap();
        let mut def = db.get_store("events").unwrap();
        def.auto_increment = true;
        db.engine
            .write_internal(vec![InternalRecord::from_record(
                &schema_record(&def).unwrap(),
                0,
            )])
            .unwrap();
        db.schema.insert(def);

        db.put_auto("events", json!({"type": "click"})).unwrap();
        db.delete_object_store("events").unwrap();
        assert!(db.get_store("events").is_none());
    }

    // ── secondary index with multiple values ──────────────────────

    #[test]
    fn test_index_on_nested_field() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_city", &["address.city"], false, false)
            .unwrap();
        db.put("users", json!({"id": "u1", "address": {"city": "NYC"}}))
            .unwrap();
        db.put("users", json!({"id": "u2", "address": {"city": "SF"}}))
            .unwrap();

        let docs = db.get_by_index("users", "by_city", &json!("NYC")).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    #[test]
    fn test_index_on_field_not_present() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], false, false)
            .unwrap();
        // Doc without the indexed field
        db.put("users", json!({"id": "u1"})).unwrap();
        // Should not create index entry, so query returns no results
        let docs = db.get_by_index("users", "by_email", &json!("x")).unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn test_index_float_values() {
        let (db, _dir) = test_db();
        db.create_object_store("scores", "id").unwrap();
        db.create_index("scores", "by_score", &["score"], false, false)
            .unwrap();

        db.put("scores", json!({"id": "a", "score": 95.5})).unwrap();
        db.put("scores", json!({"id": "b", "score": 87.3})).unwrap();
        db.put("scores", json!({"id": "c", "score": 95.5})).unwrap();

        let docs = db.get_by_index("scores", "by_score", &json!(95.5)).unwrap();
        assert_eq!(docs.len(), 2);

        let docs = db
            .range_by_index("scores", "by_score", &json!(80.0), &json!(90.0))
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "b");
    }

    #[test]
    fn test_index_bool_values() {
        let (db, _dir) = test_db();
        db.create_object_store("items", "id").unwrap();
        db.create_index("items", "by_active", &["active"], false, false)
            .unwrap();

        db.put("items", json!({"id": 1, "active": true})).unwrap();
        db.put("items", json!({"id": 2, "active": false})).unwrap();

        let docs = db.get_by_index("items", "by_active", &json!(true)).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], 1);

        let docs = db
            .get_by_index("items", "by_active", &json!(false))
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], 2);
    }

    #[test]
    fn test_index_null_values() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], false, false)
            .unwrap();

        db.put("users", json!({"id": "u1", "email": null})).unwrap();

        let docs = db
            .get_by_index("users", "by_email", &json!(Value::Null))
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    // ── many documents / performance stress ───────────────────────

    #[test]
    fn test_many_documents() {
        let (db, _dir) = test_db();
        db.create_object_store("large", "id").unwrap();
        db.create_index("large", "by_val", &["val"], false, false).unwrap();

        let n = 200;
        for i in 0..n {
            db.put("large", json!({"id": i, "val": i % 50})).unwrap();
        }
        assert_eq!(db.count("large").unwrap(), n);

        // Index point query
        let docs = db.get_by_index("large", "by_val", &json!(0)).unwrap();
        assert_eq!(docs.len(), n / 50);

        // Index range query
        let docs = db
            .range_by_index("large", "by_val", &json!(10), &json!(20))
            .unwrap();
        assert!(!docs.is_empty());

        // Non-string key check
        let doc = db.get("large", &json!(0)).unwrap();
        assert!(doc.is_some());
    }

    // ── multiple stores ───────────────────────────────────────────

    #[test]
    fn test_multiple_stores_independent() {
        let (db, _dir) = test_db();
        db.create_object_store("a", "id").unwrap();
        db.create_object_store("b", "id").unwrap();
        db.create_index("a", "by_val", &["val"], false, false).unwrap();

        db.put("a", json!({"id": "a1", "val": 1})).unwrap();
        db.put("b", json!({"id": "b1", "val": 2})).unwrap();

        assert_eq!(db.count("a").unwrap(), 1);
        assert_eq!(db.count("b").unwrap(), 1);

        let docs = db.get_by_index("a", "by_val", &json!(1)).unwrap();
        assert_eq!(docs.len(), 1);
    }

    // ── from_engine ───────────────────────────────────────────────

    #[test]
    fn test_from_engine_with_existing_data() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Create with schema and data, then reopen via JsonDB::open
        {
            let cfg = Config {
                data_dir: path.clone(),
                auto_background: false,
                compression: crate::record::CompressionAlgorithm::Lz4,
                ..Default::default()
            };
            let db = JsonDB::open(cfg).unwrap();
            db.create_object_store("users", "id").unwrap();
            db.put("users", json!({"id": "u1"})).unwrap();
            db.shutdown().unwrap();
        }

        // Reopen via JsonDB::open (which internally calls Engine::open)
        {
            let cfg = Config {
                data_dir: path,
                auto_background: false,
                compression: crate::record::CompressionAlgorithm::Lz4,
                ..Default::default()
            };
            let db = JsonDB::open(cfg).unwrap();
            assert!(db.get_store("users").is_some());
            assert_eq!(db.count("users").unwrap(), 1);
        }
    }

    // ── Transaction count with mixture ───────────────────────────

    #[test]
    fn test_transaction_count_mixed() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1"})).unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        // u1 is committed, u2 is buffered
        tx.put("users", json!({"id": "u2"})).unwrap();
        assert_eq!(tx.count("users").unwrap(), 2);

        tx.delete("users", &json!("u1")).unwrap();
        // u1 deleted in buffer, u2 still buffered
        assert_eq!(tx.count("users").unwrap(), 1);
        tx.commit().unwrap();
    }

    // ── Edge cases for encoding module ────────────────────────────

    #[test]
    fn test_primary_key_numeric_types() {
        let (db, _dir) = test_db();
        db.create_object_store("items", "id").unwrap();

        // Bool key
        db.put("items", json!({"id": true, "name": "yes"})).unwrap();
        let doc = db.get("items", &json!(true)).unwrap().unwrap();
        assert_eq!(doc["name"], "yes");

        // Null key
        db.put("items", json!({"id": null, "name": "nothing"}))
            .unwrap();

        // Negative number key
        db.put("items", json!({"id": -5, "name": "neg"})).unwrap();
        let doc = db.get("items", &json!(-5)).unwrap().unwrap();
        assert_eq!(doc["name"], "neg");
    }

    // ── create_index on existing data with unique constraint ──────

    #[test]
    fn test_create_index_on_existing_data_unique_violation() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "email": "same@b.com"}))
            .unwrap();
        db.put("users", json!({"id": "u2", "email": "same@b.com"}))
            .unwrap();

        // Creating a unique index on existing data with duplicates succeeds
        // but subsequent put attempts will fail with unique violation.
        db.create_index("users", "by_email", &["email"], true, false)
            .unwrap();

        let err = db
            .put("users", json!({"id": "u3", "email": "same@b.com"}))
            .unwrap_err();
        assert!(
            err.to_string().contains("unique"),
            "expected unique violation"
        );
    }

    // ── validate_name edge cases ──────────────────────────────────

    #[test]
    fn test_validate_name_in_creation() {
        let (db, _dir) = test_db();
        let err = db.create_object_store("", "id").unwrap_err();
        assert!(err.to_string().contains("empty"));
        let err = db.create_object_store("has space", "id").unwrap_err();
        assert!(err.to_string().contains("invalid"));
    }

    // ── shutdown/close ────────────────────────────────────────────

    #[test]
    fn test_json_db_shutdown_close() {
        let dir = TempDir::new().unwrap();
        let cfg = Config {
            data_dir: dir.path().to_path_buf(),
            auto_background: false,
            compression: crate::record::CompressionAlgorithm::Lz4,
            ..Default::default()
        };
        let db = JsonDB::open(cfg).unwrap();
        db.close().unwrap();
        // After close the engine is flushed but still usable
        assert!(db.store_names().is_empty());
    }

    // ── schema module edge cases ──────────────────────────────────

    #[test]
    fn test_validate_index_def_edge_cases() {
        let def = StoreDef {
            name: "users".into(),
            key_path: "id".into(),
            auto_increment: false,
            indexes: vec![IndexDef {
                name: "existing".into(),
                key_paths: vec!["f".into()],
                unique: false,
                multi_entry: false,
            }],
            next_auto_id: 0,
        };
        assert!(validate_index_def(&def, "", &["f"]).is_err());
        assert!(validate_index_def(&def, "existing", &["f"]).is_err());
        assert!(validate_index_def(&def, "new", &[""]).is_err());
    }

    // Test the internal key encoding functions directly
    #[test]
    fn test_encoding_u64_number() {
        // u64 that doesn't fit in i64
        let val = Value::Number(serde_json::Number::from(18446744073709551615u64));
        let encoded = encode_index_value(&val);
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encoding_negative_float() {
        let val = json!(-3.5e10);
        let encoded = encode_index_value(&val);
        let val2 = json!(-3.5e10 + 1.0);
        let _encoded2 = encode_index_value(&val2);
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_primary_key_object_type() {
        // Object primary key (fallback to JSON ser)
        let val = json!({"a": 1, "b": 2});
        let pk = encode_primary_key(&val).unwrap();
        assert!(!pk.is_empty());
    }

    #[test]
    fn test_extract_field_deep_path() {
        let doc = json!({"a": {"b": {"c": [1, 2, 3]}}});
        assert_eq!(extract_field(&doc, "a.b.c.0"), Some(json!(1)));
        assert_eq!(extract_field(&doc, "a.b.c.3"), None);
        assert_eq!(extract_field(&doc, "a.b.c"), Some(json!([1, 2, 3])));
    }

    #[test]
    fn test_extract_field_from_non_object() {
        let doc_str = json!("hello");
        assert_eq!(extract_field(&doc_str, "anything"), None);

        let doc_arr = json!([1, 2, 3]);
        assert_eq!(extract_field(&doc_arr, "0"), Some(json!(1)));
        assert_eq!(extract_field(&doc_arr, "5"), None);
    }

    // ── transaction range_by_index via direct method ──────────────

    #[test]
    fn test_range_by_index_empty_range() {
        let (db, _dir) = test_db();
        setup_users(&db);
        db.put("users", json!({"id": "u1", "age": 30})).unwrap();

        // Empty range [100, 100) should return no results
        let docs = db
            .range_by_index("users", "by_age", &json!(100), &json!(100))
            .unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn test_range_by_index_start_equals_end() {
        let (db, _dir) = test_db();
        setup_users(&db);
        db.put("users", json!({"id": "u1", "age": 30})).unwrap();

        // Range [30, 30) should be empty (exclusive end)
        let docs = db
            .range_by_index("users", "by_age", &json!(30), &json!(30))
            .unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn test_many_concurrent_transactions() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let mut threads = Vec::new();
        let db = Arc::new(db);
        for i in 0..10 {
            let db = db.clone();
            threads.push(std::thread::spawn(move || {
                let mut tx = db
                    .transaction(&["users"], TransactionMode::ReadWrite)
                    .unwrap();
                tx.put("users", json!({"id": format!("t{}", i)})).unwrap();
                tx.commit().unwrap();
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(db.count("users").unwrap(), 10);
    }

    #[test]
    fn test_put_in_transaction_updates_index() {
        let (db, _dir) = test_db();
        setup_users(&db);

        db.put("users", json!({"id": "u1", "email": "old@b.com"}))
            .unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        tx.delete("users", &json!("u1")).unwrap();
        tx.put("users", json!({"id": "u1", "email": "new@b.com"}))
            .unwrap();
        tx.commit().unwrap();

        // After commit, index should be updated
        let docs = db
            .get_by_index("users", "by_email", &json!("new@b.com"))
            .unwrap();
        assert_eq!(docs.len(), 1);

        let docs = db
            .get_by_index("users", "by_email", &json!("old@b.com"))
            .unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn test_reopen_with_indexes() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        {
            let cfg = Config {
                data_dir: path.clone(),
                auto_background: false,
                compression: crate::record::CompressionAlgorithm::Lz4,
                ..Default::default()
            };
            let db = JsonDB::open(cfg).unwrap();
            db.create_object_store("users", "id").unwrap();
            db.create_index("users", "by_name", &["name"], false, false)
                .unwrap();
            db.put("users", json!({"id": "u1", "name": "Alice"}))
                .unwrap();
            db.shutdown().unwrap();
        }
        {
            let cfg = Config {
                data_dir: path,
                auto_background: false,
                compression: crate::record::CompressionAlgorithm::Lz4,
                ..Default::default()
            };
            let db = JsonDB::open(cfg).unwrap();
            let docs = db
                .get_by_index("users", "by_name", &json!("Alice"))
                .unwrap();
            assert_eq!(docs.len(), 1);
        }
    }

    // ── durability ───────────────────────────────────────────────
    //
    // All writes go through Engine::write_internal → WAL + memtable.
    // With SyncMode::Always (default) every batch is fsynced before
    // returning.  The test below exercises the full shutdown / reopen
    // cycle to confirm data survives.

    #[test]
    fn test_durability_shutdown_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        {
            let cfg = Config {
                data_dir: path.clone(),
                auto_background: false,
                compression: crate::record::CompressionAlgorithm::Lz4,
                ..Default::default()
            };
            let db = JsonDB::open(cfg).unwrap();
            db.create_object_store("store", "id").unwrap();
            db.create_index("store", "by_val", &["val"], true, false).unwrap();
            for i in 0u64..20 {
                db.put("store", json!({"id": i, "val": format!("v{}", i)}))
                    .unwrap();
            }
            // Flush + fsync before shutdown.
            db.close().unwrap();
        }
        // Reopen — all 20 docs and their index entries must be present.
        {
            let cfg = Config {
                data_dir: path,
                auto_background: false,
                compression: crate::record::CompressionAlgorithm::Lz4,
                ..Default::default()
            };
            let db = JsonDB::open(cfg).unwrap();
            assert_eq!(db.count("store").unwrap(), 20);
            for i in 0u64..20 {
                let doc = db.get("store", &json!(i)).unwrap();
                assert!(doc.is_some(), "doc {} missing after reopen", i);
            }
            let docs = db.get_by_index("store", "by_val", &json!("v5")).unwrap();
            assert_eq!(docs.len(), 1);
            assert_eq!(docs[0]["id"], 5);
        }
    }

    // ── performance benchmark (not a correctness test) ───────────

    #[test]
    fn test_throughput_sequential_writes() {
        let (db, _dir) = test_db();
        db.create_object_store("bench", "id").unwrap();
        db.create_index("bench", "by_val", &["val"], false, false).unwrap();

        let n = 1_000;
        let start = std::time::Instant::now();
        for i in 0u64..n {
            db.put("bench", json!({"id": i, "val": i % 100})).unwrap();
        }
        let elapsed = start.elapsed();
        let ops_per_sec = n as f64 / elapsed.as_secs_f64();
        eprintln!(
            "JsonDB sequential write: {} ops in {:.3}s = {:.0} ops/s",
            n,
            elapsed.as_secs_f64(),
            ops_per_sec
        );
        assert_eq!(db.count("bench").unwrap(), n as usize);
    }

    #[test]
    fn test_throughput_point_reads() {
        let (db, _dir) = test_db();
        db.create_object_store("bench", "id").unwrap();
        db.create_index("bench", "by_val", &["val"], false, false).unwrap();

        let n = 1_000;
        for i in 0u64..n {
            db.put("bench", json!({"id": i, "val": i * 2})).unwrap();
        }

        let start = std::time::Instant::now();
        for i in 0u64..n {
            let _ = db.get("bench", &json!(i)).unwrap();
        }
        let elapsed = start.elapsed();
        let ops_per_sec = n as f64 / elapsed.as_secs_f64();
        eprintln!(
            "JsonDB point read: {} ops in {:.3}s = {:.0} ops/s",
            n,
            elapsed.as_secs_f64(),
            ops_per_sec
        );
    }

    #[test]
    fn test_throughput_index_query() {
        let (db, _dir) = test_db();
        db.create_object_store("bench", "id").unwrap();
        db.create_index("bench", "by_val", &["val"], false, false).unwrap();

        let n = 1_000;
        for i in 0u64..n {
            db.put("bench", json!({"id": i, "val": i % 50})).unwrap();
        }

        let start = std::time::Instant::now();
        for v in 0..50 {
            let docs = db.get_by_index("bench", "by_val", &json!(v)).unwrap();
            assert_eq!(docs.len(), n as usize / 50);
        }
        let elapsed = start.elapsed();
        let ops_per_sec = 50f64 / elapsed.as_secs_f64();
        eprintln!(
            "JsonDB index query (50 lookups): {:.3}s total, {:.0} queries/s",
            elapsed.as_secs_f64(),
            ops_per_sec
        );
    }

    #[test]
    fn test_throughput_transaction_batch() {
        let (db, _dir) = test_db();
        db.create_object_store("bench", "id").unwrap();

        let batch_size = 100;
        let batches = 50;
        let total = batch_size * batches;

        let start = std::time::Instant::now();
        for b in 0..batches {
            let mut tx = db
                .transaction(&["bench"], TransactionMode::ReadWrite)
                .unwrap();
            for i in 0..batch_size {
                let id = b * batch_size + i;
                tx.put("bench", json!({"id": id as u64})).unwrap();
            }
            tx.commit().unwrap();
        }
        let elapsed = start.elapsed();
        let ops_per_sec = total as f64 / elapsed.as_secs_f64();
        eprintln!(
            "JsonDB transaction batch ({} × {} docs): {} docs in {:.3}s = {:.0} docs/s",
            batches,
            batch_size,
            total,
            elapsed.as_secs_f64(),
            ops_per_sec
        );
        assert_eq!(db.count("bench").unwrap(), total);
    }

    #[test]
    fn test_throughput_auto_increment() {
        let (db, _dir) = test_db();
        db.create_object_store("auto", "id").unwrap();
        let mut def = db.get_store("auto").unwrap();
        def.auto_increment = true;
        db.engine
            .write_internal(vec![InternalRecord::from_record(
                &schema_record(&def).unwrap(),
                0,
            )])
            .unwrap();
        db.schema.insert(def);

        let n = 50;
        let start = std::time::Instant::now();
        for _ in 0..n {
            db.put_auto("auto", json!({"data": "x"})).unwrap();
        }
        let elapsed = start.elapsed();
        let ops_per_sec = n as f64 / elapsed.as_secs_f64();
        eprintln!(
            "JsonDB auto-increment ({} ops): {:.3}s = {:.0} ops/s",
            n,
            elapsed.as_secs_f64(),
            ops_per_sec
        );
        assert_eq!(db.count("auto").unwrap(), n);
    }

    // ── composite indexes ────────────────────────────────────────

    #[test]
    fn test_composite_index_equality() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_city_age", &["city", "age"], false, false)
            .unwrap();

        db.put("users", json!({"id": "u1", "city": "NYC", "age": 30}))
            .unwrap();
        db.put("users", json!({"id": "u2", "city": "NYC", "age": 25}))
            .unwrap();
        db.put("users", json!({"id": "u3", "city": "SF", "age": 30}))
            .unwrap();

        // Query by first field only (prefix scan)
        let docs = db
            .get_by_index("users", "by_city_age", &json!("NYC"))
            .unwrap();
        assert_eq!(docs.len(), 2);

        // Query by all fields (exact match)
        let docs = db
            .get_by_index("users", "by_city_age", &json!(["NYC", 30]))
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    #[test]
    fn test_composite_index_update() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_city_age", &["city", "age"], true, false)
            .unwrap();

        db.put("users", json!({"id": "u1", "city": "NYC", "age": 30}))
            .unwrap();

        // Update city → old index entry removed, new one created
        db.put("users", json!({"id": "u1", "city": "SF", "age": 30}))
            .unwrap();

        let docs = db
            .get_by_index("users", "by_city_age", &json!(["NYC", 30]))
            .unwrap();
        assert_eq!(docs.len(), 0, "old composite value should be gone");

        let docs = db
            .get_by_index("users", "by_city_age", &json!(["SF", 30]))
            .unwrap();
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn test_composite_index_unique() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_city_age", &["city", "age"], true, false)
            .unwrap();

        db.put("users", json!({"id": "u1", "city": "NYC", "age": 30}))
            .unwrap();
        let err = db
            .put("users", json!({"id": "u2", "city": "NYC", "age": 30}))
            .unwrap_err();
        assert!(
            err.to_string().contains("unique"),
            "composite unique should fail"
        );

        // Same city, different age → should succeed
        db.put("users", json!({"id": "u2", "city": "NYC", "age": 25}))
            .unwrap();
        assert_eq!(db.count("users").unwrap(), 2);
    }

    #[test]
    fn test_composite_index_on_existing_data() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "city": "NYC", "age": 30}))
            .unwrap();
        db.put("users", json!({"id": "u2", "city": "SF", "age": 25}))
            .unwrap();

        // Create composite on existing data
        db.create_index("users", "by_city_age", &["city", "age"], false, false)
            .unwrap();

        let docs = db
            .get_by_index("users", "by_city_age", &json!(["NYC", 30]))
            .unwrap();
        assert_eq!(docs.len(), 1);
    }

    // ── QueryBuilder ────────────────────────────────────────────

    #[test]
    fn test_query_builder_eq() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true, false)
            .unwrap();
        db.put("users", json!({"id": "u1", "email": "a@b.com"}))
            .unwrap();
        db.put("users", json!({"id": "u2", "email": "b@c.com"}))
            .unwrap();

        let docs = db
            .query("users")
            .where_eq("email", json!("a@b.com"))
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    #[test]
    fn test_query_builder_composite_eq() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_city_age", &["city", "age"], false, false)
            .unwrap();
        db.put("users", json!({"id": "u1", "city": "NYC", "age": 30}))
            .unwrap();
        db.put("users", json!({"id": "u2", "city": "NYC", "age": 25}))
            .unwrap();
        db.put("users", json!({"id": "u3", "city": "SF", "age": 30}))
            .unwrap();

        let docs = db
            .query("users")
            .where_eq("city", json!("NYC"))
            .where_eq("age", json!(30))
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], "u1");
    }

    #[test]
    fn test_query_builder_range() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_age", &["age"], false, false).unwrap();
        db.put("users", json!({"id": "u1", "age": 30})).unwrap();
        db.put("users", json!({"id": "u2", "age": 25})).unwrap();
        db.put("users", json!({"id": "u3", "age": 35})).unwrap();

        let docs = db
            .query("users")
            .where_range("age", json!(25), json!(35))
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 2); // age 25 ≤ docs < 35
    }

    #[test]
    fn test_query_builder_eq_and_range() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_city_age", &["city", "age"], false, false)
            .unwrap();
        db.put("users", json!({"id": "u1", "city": "NYC", "age": 30}))
            .unwrap();
        db.put("users", json!({"id": "u2", "city": "NYC", "age": 25}))
            .unwrap();
        db.put("users", json!({"id": "u3", "city": "SF", "age": 30}))
            .unwrap();

        let docs = db
            .query("users")
            .where_eq("city", json!("NYC"))
            .where_range("age", json!(20), json!(30))
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 1); // age 20-30 in NYC → u2 (age 25). age 30 is exclusive.
        assert_eq!(docs[0]["id"], "u2");
    }

    #[test]
    fn test_query_builder_limit_offset() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        for i in 0..10 {
            db.put("users", json!({"id": i, "val": i})).unwrap();
        }

        let docs = db.query("users").limit(3).collect().unwrap();
        assert_eq!(docs.len(), 3);

        let docs = db.query("users").offset(5).limit(3).collect().unwrap();
        assert_eq!(docs.len(), 3);
    }

    #[test]
    fn test_query_builder_order_by_asc() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_age", &["age"], false, false).unwrap();
        db.put("users", json!({"id": "u1", "age": 30})).unwrap();
        db.put("users", json!({"id": "u2", "age": 25})).unwrap();
        db.put("users", json!({"id": "u3", "age": 35})).unwrap();

        let docs = db
            .query("users")
            .order_by("age", SortDir::Asc)
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[0]["id"], "u2"); // age 25
        assert_eq!(docs[1]["id"], "u1"); // age 30
        assert_eq!(docs[2]["id"], "u3"); // age 35
    }

    #[test]
    fn test_query_builder_order_by_desc() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "age": 30})).unwrap();
        db.put("users", json!({"id": "u2", "age": 25})).unwrap();

        let docs = db
            .query("users")
            .order_by("age", SortDir::Desc)
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0]["id"], "u1"); // age 30
        assert_eq!(docs[1]["id"], "u2"); // age 25
    }

    #[test]
    fn test_query_builder_where_in() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "city": "NYC"})).unwrap();
        db.put("users", json!({"id": "u2", "city": "SF"})).unwrap();
        db.put("users", json!({"id": "u3", "city": "LA"})).unwrap();

        let docs = db
            .query("users")
            .where_in("city", vec![json!("NYC"), json!("LA")])
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn test_query_builder_no_matching_index() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "color": "red"}))
            .unwrap();
        db.put("users", json!({"id": "u2", "color": "blue"}))
            .unwrap();

        // No index on "color" → full scan with filter
        let docs = db
            .query("users")
            .where_eq("color", json!("red"))
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn test_query_builder_store_not_found() {
        let (db, _dir) = test_db();
        let err = db.query("nonexistent").collect().unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_query_builder_bool_index() {
        let (db, _dir) = test_db();
        db.create_object_store("items", "id").unwrap();
        db.create_index("items", "by_active", &["active"], false, false)
            .unwrap();
        db.put("items", json!({"id": 1, "active": true})).unwrap();
        db.put("items", json!({"id": 2, "active": false})).unwrap();

        let docs = db
            .query("items")
            .where_eq("active", json!(true))
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["id"], 1);
    }

    #[test]
    fn test_query_builder_with_transaction_roundtrip() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true, false)
            .unwrap();
        db.put("users", json!({"id": "u1", "email": "a@b.com"}))
            .unwrap();

        // QueryBuilder sees committed data
        let docs = db
            .query("users")
            .where_eq("email", json!("a@b.com"))
            .collect()
            .unwrap();
        assert_eq!(docs.len(), 1);
    }

    // ── generic struct API ────────────────────────────────────────

    #[derive(serde::Serialize, serde::Deserialize)]
    struct TestUser {
        id: String,
        name: String,
        email: String,
    }

    #[test]
    fn test_put_doc_and_get_doc() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true, false)
            .unwrap();

        let user = TestUser {
            id: "u1".into(),
            name: "Alice".into(),
            email: "a@b.com".into(),
        };
        let key = db.put_doc("users", &user).unwrap();
        assert_eq!(key, json!("u1"));

        let retrieved: TestUser = db.get_doc("users", "u1").unwrap().unwrap();
        assert_eq!(retrieved.name, "Alice");
        assert_eq!(retrieved.email, "a@b.com");
    }

    #[test]
    fn test_put_doc_numeric_key() {
        let (db, _dir) = test_db();
        db.create_object_store("items", "id").unwrap();

        #[derive(serde::Serialize, serde::Deserialize)]
        struct Item {
            id: i64,
            name: String,
        }

        let item = Item {
            id: 42,
            name: "answer".into(),
        };
        let key = db.put_doc("items", &item).unwrap();
        assert_eq!(key, json!(42));

        let retrieved: Item = db.get_doc("items", 42).unwrap().unwrap();
        assert_eq!(retrieved.name, "answer");
    }

    #[test]
    fn test_get_doc_not_found() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let user: Option<TestUser> = db.get_doc("users", "nonexistent").unwrap();
        assert!(user.is_none());
    }

    #[test]
    fn test_delete_doc() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let user = TestUser {
            id: "u1".into(),
            name: "Alice".into(),
            email: "a@b.com".into(),
        };
        db.put_doc("users", &user).unwrap();
        assert!(db.count("users").unwrap() == 1);

        db.delete_doc("users", "u1").unwrap();
        assert!(db.count("users").unwrap() == 0);
    }

    #[test]
    fn test_collect_doc() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_email", &["email"], true, false)
            .unwrap();

        db.put_doc(
            "users",
            &TestUser {
                id: "u1".into(),
                name: "Alice".into(),
                email: "a@b.com".into(),
            },
        )
        .unwrap();

        let users: Vec<TestUser> = db
            .query("users")
            .where_eq("email", json!("a@b.com"))
            .collect_doc()
            .unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].name, "Alice");
    }

    #[test]
    fn test_put_doc_in_transaction() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        let user = TestUser {
            id: "u2".into(),
            name: "Bob".into(),
            email: "b@b.com".into(),
        };
        let json = serde_json::to_value(&user).unwrap();
        tx.put("users", json).unwrap();
        tx.commit().unwrap();

        let retrieved: TestUser = db.get_doc("users", "u2").unwrap().unwrap();
        assert_eq!(retrieved.name, "Bob");
    }

    // ── IndexedDB compatibility tests ─────────────────────────────

    #[test]
    fn test_add_rejects_duplicate() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.add("users", json!({"id": "u1", "name": "Alice"}))
            .unwrap();
        let err = db
            .add("users", json!({"id": "u1", "name": "Bob"}))
            .unwrap_err();
        assert!(err.to_string().contains("already exists"));
        // Original doc unchanged.
        let doc = db.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");
    }

    #[test]
    fn test_clear_store() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1"})).unwrap();
        db.put("users", json!({"id": "u2"})).unwrap();
        db.put("users", json!({"id": "u3"})).unwrap();
        assert_eq!(db.count("users").unwrap(), 3);

        let removed = db.clear("users").unwrap();
        assert_eq!(removed, 3);
        assert_eq!(db.count("users").unwrap(), 0);
        // Schema still exists.
        let names = db.store_names();
        assert!(names.contains(&"users".to_string()));
    }

    #[test]
    fn test_get_all_with_count() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        for i in 0..10 {
            db.put("users", json!({"id": format!("u{}", i)})).unwrap();
        }
        let all = db.get_all("users", None, None).unwrap();
        assert_eq!(all.len(), 10);
        let limited = db.get_all("users", None, Some(3)).unwrap();
        assert_eq!(limited.len(), 3);
    }

    #[test]
    fn test_get_all_keys() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1"})).unwrap();
        db.put("users", json!({"id": "u2"})).unwrap();
        let keys = db.get_all_keys("users", None, None).unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_get_key() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1"})).unwrap();
        assert!(db.get_key("users", &json!("u1")).unwrap().is_some());
        assert!(db.get_key("users", &json!("missing")).unwrap().is_none());
    }

    #[test]
    fn test_count_with_keyrange() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        for i in 0..10 {
            db.put("users", json!({"id": format!("u{}", i)})).unwrap();
        }
        let kr = KeyRange::bound(json!("u3"), json!("u6"), false, false);
        let n = db.count_with_query("users", Some(&kr)).unwrap();
        assert_eq!(n, 4); // u3, u4, u5, u6
    }

    #[test]
    fn test_multi_entry_index() {
        let (db, _dir) = test_db();
        db.create_object_store("items", "id").unwrap();
        db.create_index("items", "by_tag", &["tags"], false, true)
            .unwrap();
        db.put("items", json!({"id": "i1", "tags": ["red", "blue"]})).unwrap();
        db.put("items", json!({"id": "i2", "tags": ["blue", "green"]})).unwrap();
        // Lookup by each tag should work.
        let red = db.get_by_index("items", "by_tag", &json!("red")).unwrap();
        assert_eq!(red.len(), 1);
        let blue = db.get_by_index("items", "by_tag", &json!("blue")).unwrap();
        assert_eq!(blue.len(), 2);
    }

    #[test]
    fn test_key_range_only() {
        let kr = KeyRange::only(json!("alice"));
        assert!(kr.includes(&json!("alice")));
        assert!(!kr.includes(&json!("bob")));
    }

    #[test]
    fn test_key_range_bound_closed() {
        let kr = KeyRange::bound(json!(10), json!(20), false, false);
        assert!(kr.includes(&json!(10)));
        assert!(kr.includes(&json!(15)));
        assert!(kr.includes(&json!(20)));
        assert!(!kr.includes(&json!(9)));
        assert!(!kr.includes(&json!(21)));
    }

    #[test]
    fn test_key_range_bound_open() {
        let kr = KeyRange::bound(json!(10), json!(20), true, true);
        assert!(!kr.includes(&json!(10)));
        assert!(kr.includes(&json!(11)));
        assert!(!kr.includes(&json!(20)));
        assert!(kr.includes(&json!(19)));
    }

    #[test]
    fn test_key_range_lower_bound() {
        let kr = KeyRange::lower_bound(json!(18), false);
        assert!(kr.includes(&json!(18)));
        assert!(kr.includes(&json!(100)));
        assert!(!kr.includes(&json!(17)));
    }

    #[test]
    fn test_key_range_upper_bound() {
        let kr = KeyRange::upper_bound(json!(65), true);
        assert!(!kr.includes(&json!(65)));
        assert!(kr.includes(&json!(64)));
        assert!(!kr.includes(&json!(66)));
    }

    #[test]
    fn test_cursor_next_forward() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "a"})).unwrap();
        db.put("users", json!({"id": "b"})).unwrap();
        db.put("users", json!({"id": "c"})).unwrap();

        let mut cursor = db
            .open_cursor("users", None, CursorDirection::Next)
            .unwrap();
        let (k1, _) = cursor.next_value().unwrap();
        assert_eq!(k1, json!("a"));
        let (k2, _) = cursor.next_value().unwrap();
        assert_eq!(k2, json!("b"));
        let (k3, _) = cursor.next_value().unwrap();
        assert_eq!(k3, json!("c"));
        assert!(cursor.next_value().is_none());
    }

    #[test]
    fn test_cursor_prev_reverse() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "a"})).unwrap();
        db.put("users", json!({"id": "b"})).unwrap();
        db.put("users", json!({"id": "c"})).unwrap();

        let mut cursor = db
            .open_cursor("users", None, CursorDirection::Prev)
            .unwrap();
        let (k1, _) = cursor.next_value().unwrap();
        assert_eq!(k1, json!("c"));
        let (k2, _) = cursor.next_value().unwrap();
        assert_eq!(k2, json!("b"));
        let (k3, _) = cursor.next_value().unwrap();
        assert_eq!(k3, json!("a"));
    }

    #[test]
    fn test_cursor_advance() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        for i in 0..10 {
            db.put("users", json!({"id": format!("u{}", i)})).unwrap();
        }

        let mut cursor = db
            .open_cursor("users", None, CursorDirection::Next)
            .unwrap();
        let (k, _) = cursor.advance(5).unwrap();
        assert_eq!(k, json!("u5"));
        let (k, _) = cursor.next_value().unwrap();
        assert_eq!(k, json!("u6"));
    }

    #[test]
    fn test_cursor_with_keyrange() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        for i in 0..10 {
            db.put("users", json!({"id": format!("u{}", i)})).unwrap();
        }

        let kr = KeyRange::bound(json!("u3"), json!("u5"), false, false);
        let mut cursor = db
            .open_cursor("users", Some(&kr), CursorDirection::Next)
            .unwrap();
        let mut keys = Vec::new();
        while let Some((k, _)) = cursor.next_value() {
            keys.push(k);
        }
        assert_eq!(keys, vec![json!("u3"), json!("u4"), json!("u5")]);
    }

    #[test]
    fn test_index_cursor() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.create_index("users", "by_age", &["age"], false, false)
            .unwrap();
        db.put("users", json!({"id": "u1", "age": 25})).unwrap();
        db.put("users", json!({"id": "u2", "age": 30})).unwrap();
        db.put("users", json!({"id": "u3", "age": 35})).unwrap();

        let mut cursor = db
            .open_cursor_on_index("users", "by_age", None, CursorDirection::Next)
            .unwrap();
        let (idx_val, pk, doc) = cursor.next_value().unwrap();
        assert_eq!(idx_val, json!(25));
        assert_eq!(pk, json!("u1"));
        assert_eq!(doc["age"], 25);
    }

    #[test]
    fn test_transaction_add_clear() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        // add should fail for existing key.
        let err = tx.add("users", json!({"id": "u1"})).unwrap_err();
        assert!(err.to_string().contains("already exists"));
        // add should succeed for new key.
        tx.add("users", json!({"id": "u2", "name": "Bob"})).unwrap();
        // clear should remove all.
        tx.clear("users").unwrap();
        tx.commit().unwrap();

        assert_eq!(db.count("users").unwrap(), 0);
    }

    // ── MVCC snapshot isolation tests ───────────────────────────────

    #[test]
    fn test_mvcc_snapshot_repeatable_read() {
        // A transaction should see the same value throughout its lifetime,
        // even if other writers commit changes.
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();

        let tx = db
            .transaction(&["users"], TransactionMode::ReadOnly)
            .unwrap();

        // Read before external modification.
        let doc1 = tx.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc1["name"], "Alice");

        // External modification (direct put, not through the transaction).
        db.put("users", json!({"id": "u1", "name": "Bob"})).unwrap();

        // Read after external modification — should still see the snapshot value.
        let doc2 = tx.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(
            doc2["name"], "Alice",
            "MVCC snapshot: transaction should see the snapshot value, not the latest"
        );
    }

    #[test]
    fn test_mvcc_snapshot_hides_uncommitted_writes() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "v": 1})).unwrap();

        // T1 begins and reads.
        let tx1 = db
            .transaction(&["users"], TransactionMode::ReadOnly)
            .unwrap();
        let doc = tx1.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc["v"], 1);

        // External write of v=2.
        db.put("users", json!({"id": "u1", "v": 2})).unwrap();

        // T2 begins after the external write.
        let tx2 = db
            .transaction(&["users"], TransactionMode::ReadOnly)
            .unwrap();
        let doc2 = tx2.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc2["v"], 2, "new transaction should see latest committed");

        // T1 still sees v=1.
        let doc1 = tx1.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc1["v"], 1, "old transaction should still see snapshot");
    }

    #[test]
    fn test_mvcc_read_your_writes() {
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        tx.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();

        // Should see our own uncommitted write.
        let doc = tx.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");

        tx.commit().unwrap();
        assert_eq!(db.count("users").unwrap(), 1);
    }

    // ── OCC conflict detection tests ───────────────────────────────

    #[test]
    fn test_occ_conflict_concurrent_modification() {
        // If a transaction reads a key and another writer modifies it
        // before commit, the commit should fail (OCC conflict).
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();

        let tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();

        // Read the document (tracked in OCC read-set).
        let doc = tx.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");

        // External concurrent modification.
        db.put("users", json!({"id": "u1", "name": "Bob"})).unwrap();

        // Attempt to commit — should fail due to OCC conflict.
        let result = tx.commit();
        assert!(
            result.is_err(),
            "commit should fail with OCC conflict when a read key was modified"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("conflict"),
            "error should mention conflict, got: {}",
            err
        );
    }

    #[test]
    fn test_occ_no_conflict_disjoint_keys() {
        // Two transactions writing disjoint keys should both succeed.
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();
        db.put("users", json!({"id": "u2", "name": "Bob"})).unwrap();

        // T1 reads u1 and writes u1.
        let mut tx1 = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        let _doc = tx1.get("users", &json!("u1")).unwrap().unwrap();
        tx1.put("users", json!({"id": "u1", "name": "Alice2"})).unwrap();
        tx1.commit().unwrap();

        // T2 reads u2 and writes u2 (disjoint from T1).
        let mut tx2 = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        let _doc = tx2.get("users", &json!("u2")).unwrap().unwrap();
        tx2.put("users", json!({"id": "u2", "name": "Bob2"})).unwrap();
        tx2.commit().unwrap();

        // Both commits should have succeeded.
        let doc1 = db.get("users", &json!("u1")).unwrap().unwrap();
        assert_eq!(doc1["name"], "Alice2");
        let doc2 = db.get("users", &json!("u2")).unwrap().unwrap();
        assert_eq!(doc2["name"], "Bob2");
    }

    #[test]
    fn test_occ_no_conflict_write_only() {
        // A transaction that only writes (no reads of existing data)
        // should not have OCC conflicts.
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();

        let mut tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        tx.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();
        tx.commit().unwrap();

        assert_eq!(db.count("users").unwrap(), 1);
    }

    #[test]
    fn test_occ_conflict_on_delete() {
        // If a transaction reads a key and another writer deletes it,
        // the commit should fail.
        let (db, _dir) = test_db();
        db.create_object_store("users", "id").unwrap();
        db.put("users", json!({"id": "u1", "name": "Alice"})).unwrap();

        let tx = db
            .transaction(&["users"], TransactionMode::ReadWrite)
            .unwrap();
        let _doc = tx.get("users", &json!("u1")).unwrap().unwrap();

        // External concurrent deletion.
        db.delete("users", &json!("u1")).unwrap();

        let result = tx.commit();
        assert!(result.is_err(), "commit should fail when read key was deleted");
    }

    // ── Flush-gap consistency test ─────────────────────────────────

    #[test]
    fn test_flush_gap_records_remain_visible() {
        // During a flush, records should remain visible to readers.
        // This tests the flushing-list mechanism.
        use crate::record::Record;

        let dir = TempDir::new().unwrap();
        let cfg = Config {
            data_dir: dir.path().to_path_buf(),
            auto_background: false,
            wal_sync_mode: crate::record::SyncMode::IntervalMs(u64::MAX),
            memtable_size_mb: 64,
            compression: crate::record::CompressionAlgorithm::Lz4,
            ..Default::default()
        };
        let engine = crate::engine::Engine::open(cfg).unwrap();

        // Write a record.
        engine.write_batch_owned(vec![Record::new("key1", 1, b"value1".to_vec())]).unwrap();

        // Manually move the memtable to the flushing state.
        let memtables = engine.memtables_ref();
        assert!(memtables.freeze());
        let records = memtables.pop_frozen_to_flushing().unwrap();
        assert!(!records.is_empty(), "should have records in flushing");

        // The record should still be queryable while in the flushing list.
        let found = memtables.get(b"key1", 1, i64::MAX);
        assert!(found.is_some(), "record should be visible during flush (in flushing list)");

        // Also check query_prefix sees flushing records.
        let prefix_results = memtables.query_prefix(b"key1", i64::MAX);
        assert!(!prefix_results.is_empty(), "scan should see flushing records");

        // Complete the flush.
        memtables.complete_flush();

        // After complete_flush, the flushing list is empty.
        // The record is no longer in any memtable (it was never written to SST).
        let not_found = memtables.get(b"key1", 1, i64::MAX);
        assert!(not_found.is_none(), "record should be gone after flush completes");

        engine.shutdown().unwrap();
    }

    // ── Cross-crash durability test ────────────────────────────────

    #[test]
    fn test_durability_survives_shutdown_reopen() {
        let dir = TempDir::new().unwrap();
        let cfg = Config {
            data_dir: dir.path().to_path_buf(),
            auto_background: false,
            wal_sync_mode: crate::record::SyncMode::Always,
            compression: crate::record::CompressionAlgorithm::Lz4,
            ..Default::default()
        };

        // Write and shut down.
        {
            let db = JsonDB::open(cfg.clone()).unwrap();
            db.create_object_store("kv", "id").unwrap();
            db.put("kv", json!({"id": "k1", "v": 42})).unwrap();
            db.shutdown().unwrap();
        }

        // Reopen — data must survive.
        {
            let db = JsonDB::open(cfg).unwrap();
            let doc = db.get("kv", &json!("k1")).unwrap().unwrap();
            assert_eq!(doc["v"], 42);
        }
    }

    #[test]
    fn test_durability_batch_atomicity_on_replay() {
        // Multiple records in a batch should all survive or none.
        let dir = TempDir::new().unwrap();
        let cfg = Config {
            data_dir: dir.path().to_path_buf(),
            auto_background: false,
            wal_sync_mode: crate::record::SyncMode::Always,
            compression: crate::record::CompressionAlgorithm::Lz4,
            ..Default::default()
        };

        {
            let db = JsonDB::open(cfg.clone()).unwrap();
            db.create_object_store("kv", "id").unwrap();

            let mut tx = db.transaction(&["kv"], TransactionMode::ReadWrite).unwrap();
            tx.put("kv", json!({"id": "a", "v": 1})).unwrap();
            tx.put("kv", json!({"id": "b", "v": 2})).unwrap();
            tx.put("kv", json!({"id": "c", "v": 3})).unwrap();
            tx.commit().unwrap();
            db.shutdown().unwrap();
        }

        {
            let db = JsonDB::open(cfg).unwrap();
            assert_eq!(db.count("kv").unwrap(), 3);
            assert_eq!(db.get("kv", &json!("a")).unwrap().unwrap()["v"], 1);
            assert_eq!(db.get("kv", &json!("b")).unwrap().unwrap()["v"], 2);
            assert_eq!(db.get("kv", &json!("c")).unwrap().unwrap()["v"], 3);
        }
    }
}
