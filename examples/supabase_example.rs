//! Supabase-like pattern using FlowDB + JsonDB as an embedded database.
//!
//! This example mimics how a Supabase Edge Function or local dev tool
//! might use FlowDB to manage users, sessions, and application data —
//! all without a remote PostgreSQL instance.
//!
//! Concepts shown:
//!   - Auth: users + sessions stores with unique indexes
//!   - Row-Level Security (RLS) simulation via queries scoped by user_id
//!   - App data: todos owned by users with status/priority indexes
//!   - Atomic transactions for sign-up (create user + session)
//!   - Compound index queries ("my open todos, ordered by priority")
//!   - Schema defined via #[derive(ObjectStore)] macro

use flowdb::jsondb::{JsonDB, TransactionMode};
use flowdb::{Config, ObjectStore};
use serde_json::Value;

// ── Schema definitions via derive macro ───────────────────────────

#[allow(dead_code)]
#[derive(ObjectStore)]
#[store(name = "users", key_path = "id")]
struct UserStore {
    id: String,
    #[index(name = "by_email", unique)]
    email: String,
}

#[allow(dead_code)]
#[derive(ObjectStore)]
#[store(name = "sessions", key_path = "token")]
struct SessionStore {
    token: String,
    #[index(name = "by_user")]
    user_id: String,
}

#[allow(dead_code)]
#[derive(ObjectStore)]
#[store(name = "todos", key_path = "id")]
struct TodoStore {
    id: String,
    #[index(name = "by_user")]
    user_id: String,
    status: String,
    priority: i64,
}

/// A tiny Supabase-like client built on JsonDB.
struct SupaBase {
    db: JsonDB,
}

impl SupaBase {
    fn open(path: &std::path::Path) -> Self {
        let db = JsonDB::open(Config {
            data_dir: path.to_path_buf(),
            ..Config::default()
        })
        .unwrap();

        // ── Schema via derive macro ──────────────────────────────
        db.apply_schema::<UserStore>().unwrap();
        db.apply_schema::<SessionStore>().unwrap();
        db.apply_schema::<TodoStore>().unwrap();

        // Compound indexes not expressible via single-field #[index]:
        db.create_index("todos", "by_user_status", &["user_id", "status"], false).unwrap();
        db.create_index("todos", "by_user_priority", &["user_id", "priority"], false).unwrap();

        Self { db }
    }

    // ── Auth ───────────────────────────────────────────────────

    /// Sign up: insert user + create session atomically.
    fn sign_up(&self, email: &str, password: &str) -> Value {
        let user_id = uuid_v4();
        let token = uuid_v4();

        let mut tx = self
            .db
            .transaction(&["users", "sessions"], TransactionMode::ReadWrite)
            .unwrap();

        tx.put(
            "users",
            serde_json::json!({
                "id": user_id,
                "email": email,
                "password": password,
                "created_at": now_iso(),
            }),
        )
        .unwrap();

        tx.put(
            "sessions",
            serde_json::json!({
                "token": token,
                "user_id": user_id,
                "expires_at": "2026-12-31T23:59:59Z",
            }),
        )
        .unwrap();

        tx.commit().unwrap();
        serde_json::json!({"user_id": user_id, "token": token})
    }

    /// Look up user by email (unique-index query).
    fn get_user_by_email(&self, email: &str) -> Option<Value> {
        self.db
            .get_by_index("users", "by_email", &serde_json::json!(email))
            .unwrap()
            .into_iter()
            .next()
    }

    /// Validate a session token and return the user_id.
    fn validate_session(&self, token: &str) -> Option<String> {
        let session = self.db.get("sessions", &serde_json::json!(token)).unwrap()?;
        session.get("user_id").and_then(|v| v.as_str()).map(String::from)
    }

    // ── Todos (RLS-style) ──────────────────────────────────────

    fn create_todo(&self, user_id: &str, title: &str, priority: i64) -> Value {
        let doc = serde_json::json!({
            "id": uuid_v4(),
            "user_id": user_id,
            "title": title,
            "status": "open",
            "priority": priority,
            "created_at": now_iso(),
        });
        self.db.put("todos", doc.clone()).unwrap();
        doc
    }

    /// My open todos, highest priority first.
    fn my_open_todos(&self, user_id: &str) -> Vec<Value> {
        self.db
            .query("todos")
            .where_eq("user_id", serde_json::json!(user_id))
            .where_eq("status", serde_json::json!("open"))
            .order_by("priority", flowdb::jsondb::SortDir::Desc)
            .collect()
            .unwrap()
    }

    fn complete_todo(&self, user_id: &str, todo_id: &str) -> bool {
        let mut doc = match self.db.get("todos", &serde_json::json!(todo_id)).unwrap() {
            Some(d) => d,
            None => return false,
        };
        if doc["user_id"] != user_id {
            return false;
        }
        doc["status"] = serde_json::json!("done");
        self.db.put("todos", doc).unwrap();
        true
    }

    // ── Stats ──────────────────────────────────────────────────

    fn count_users(&self) -> usize {
        self.db.count("users").unwrap()
    }

    fn count_todos(&self) -> usize {
        self.db.count("todos").unwrap()
    }

    fn count_by_status(&self, status: &str) -> usize {
        self.db
            .scan("todos")
            .unwrap()
            .into_iter()
            .filter(|d| d["status"] == status)
            .count()
    }
}

fn main() {
    let dir = tempfile::TempDir::with_prefix("flowdb_supabase_").unwrap();
    let app = SupaBase::open(dir.path());

    // ── Sign up two users ──────────────────────────────────────
    let alice = app.sign_up("alice@ex.com", "p4ss1");
    let bob = app.sign_up("bob@ex.com", "p4ss2");
    println!(
        "Users: alice={} bob={}",
        alice["user_id"].as_str().unwrap(),
        bob["user_id"].as_str().unwrap()
    );

    // ── Session validation ─────────────────────────────────────
    let uid = app
        .validate_session(alice["token"].as_str().unwrap())
        .unwrap();
    println!("Session valid → user_id={}", uid);

    // ── Create todos ───────────────────────────────────────────
    app.create_todo(&uid, "Buy milk", 2);
    app.create_todo(&uid, "Write docs", 1);
    app.create_todo(&uid, "Fix bug", 5);
    app.create_todo(bob["user_id"].as_str().unwrap(), "Review PR", 3);

    // ── RLS query: only Alice's open todos ─────────────────────
    let open = app.my_open_todos(&uid);
    println!("Alice's open todos:");
    for t in &open {
        println!("  [p{}] {} ({})", t["priority"], t["title"], t["status"]);
    }

    // ── Complete a todo (RLS check) ───────────────────────────
    let todo_id = open[0]["id"].as_str().unwrap();
    let ok = app.complete_todo(&uid, todo_id);
    println!("Complete {} by owner → {}", todo_id, ok);

    // Bob tries to complete Alice's todo → rejected
    let rejected = app.complete_todo(bob["user_id"].as_str().unwrap(), todo_id);
    println!("Complete {} by other → {}", todo_id, rejected);

    // ── Lookup by email ───────────────────────────────────────
    let user = app.get_user_by_email("bob@ex.com").unwrap();
    println!("Email lookup bob → id={}", user["id"]);

    // ── Stats ──────────────────────────────────────────────────
    println!(
        "Stats: {} users, {} todos ({} done, {} open)",
        app.count_users(),
        app.count_todos(),
        app.count_by_status("done"),
        app.count_by_status("open"),
    );

    // ── Shutdown ──────────────────────────────────────────────
    app.db.shutdown().unwrap();
    println!("Done (data in {})", dir.path().display());
}

// ── Helpers ────────────────────────────────────────────────────────

fn uuid_v4() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}", n, 0, 0, 0, 0)
}

fn now_iso() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let days = secs / 86400;
    let time = secs % 86400;
    let mut y = 1970i64;
    let mut remaining = days as i64;
    loop {
        let year_days = if is_leap(y) { 366 } else { 365 };
        if remaining < year_days {
            break;
        }
        remaining -= year_days;
        y += 1;
    }
    let mut m = 1u32;
    for days_in_month in &[31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31] {
        let dim = if m == 2 && is_leap(y) { 29 } else { *days_in_month };
        if remaining < dim as i64 {
            break;
        }
        remaining -= dim as i64;
        m += 1;
    }
    let d = remaining as u32 + 1;
    let h = time / 3600;
    let min = (time % 3600) / 60;
    let s = time % 60;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y, m, d, h, min, s
    )
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}
