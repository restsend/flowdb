//! Supabase-like server — Axum web UI + FlowDB JsonDB backend.
//!
//! Run:
//!   cargo run --example supabase-server
//!   # then open http://localhost:3000

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{Html, Json},
    routing::{get, post, put},
    Router,
};
use flowdb::jsondb::{JsonDB, SortDir, TransactionMode};
use flowdb::ObjectStore;
use flowdb::Config;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tower_http::cors::CorsLayer;

// ── App State ─────────────────────────────────────────────────────

struct AppState {
    db: JsonDB,
}

// ── Request / Response types ──────────────────────────────────────

#[derive(Deserialize)]
struct SignUpReq {
    email: String,
    password: String,
}

#[derive(Serialize)]
struct AuthRes {
    user_id: String,
    token: String,
}

#[derive(Deserialize)]
struct LoginReq {
    email: String,
    password: String,
}

#[derive(Deserialize)]
struct CreateTodoReq {
    title: String,
    #[serde(default = "default_priority")]
    priority: i64,
}

fn default_priority() -> i64 {
    0
}

#[derive(Deserialize)]
struct UpdateTodoReq {
    title: Option<String>,
    status: Option<String>,
    priority: Option<i64>,
}

// ── Helpers ───────────────────────────────────────────────────────

fn uuid_v4() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
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
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m, d, h, min, s)
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn extract_user_id(auth: &str, db: &JsonDB) -> Result<String, StatusCode> {
    let token = auth
        .strip_prefix("Bearer ")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let session = db
        .get("sessions", &json!(token))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::UNAUTHORIZED)?;
    session["user_id"]
        .as_str()
        .map(String::from)
        .ok_or(StatusCode::UNAUTHORIZED)
}

// ── Handlers ──────────────────────────────────────────────────────

/// Serve the HTML UI (loaded from `supabase-ui.html` at compile time).
async fn index_handler() -> Html<&'static str> {
    Html(include_str!("supabase-ui.html"))
}

/// POST /api/signup
async fn signup_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SignUpReq>,
) -> Result<Json<AuthRes>, StatusCode> {
    let user_id = uuid_v4();
    let token = uuid_v4();

    let mut tx = state
        .db
        .transaction(&["users", "sessions"], TransactionMode::ReadWrite)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    tx.put(
        "users",
        json!({
            "id": user_id,
            "email": req.email,
            "password": req.password,
            "created_at": now_iso(),
        }),
    )
    .map_err(|_| StatusCode::CONFLICT)?;

    tx.put(
        "sessions",
        json!({
            "token": token,
            "user_id": user_id,
            "created_at": now_iso(),
        }),
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    tx.commit().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(AuthRes { user_id, token }))
}

/// POST /api/login
async fn login_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginReq>,
) -> Result<Json<AuthRes>, StatusCode> {
    let users = state
        .db
        .get_by_index("users", "by_email", &json!(req.email))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let user = users.into_iter().next().ok_or(StatusCode::UNAUTHORIZED)?;
    if user["password"] != req.password {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let user_id = user["id"].as_str().unwrap();
    let token = uuid_v4();

    state
        .db
        .put(
            "sessions",
            json!({
                "token": token,
                "user_id": user_id,
                "created_at": now_iso(),
            }),
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(AuthRes {
        user_id: user_id.to_string(),
        token,
    }))
}

/// GET /api/todos
async fn list_todos_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>, StatusCode> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let user_id = extract_user_id(auth, &state.db)?;

    let todos = state
        .db
        .query("todos")
        .where_eq("user_id", json!(user_id))
        .order_by("priority", SortDir::Desc)
        .collect()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(todos))
}

/// POST /api/todos
async fn create_todo_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateTodoReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let user_id = extract_user_id(auth, &state.db)?;

    let doc = json!({
        "id": uuid_v4(),
        "user_id": user_id,
        "title": req.title,
        "status": "open",
        "priority": req.priority,
        "created_at": now_iso(),
    });

    state
        .db
        .put("todos", doc.clone())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(doc))
}

/// PUT /api/todos/:id
async fn update_todo_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<UpdateTodoReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let user_id = extract_user_id(auth, &state.db)?;

    let mut doc = state
        .db
        .get("todos", &json!(id))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    // RLS: only the owner can update
    if doc["user_id"] != user_id {
        return Err(StatusCode::FORBIDDEN);
    }

    if let Some(title) = req.title {
        doc["title"] = json!(title);
    }
    if let Some(status) = req.status {
        doc["status"] = json!(status);
    }
    if let Some(priority) = req.priority {
        doc["priority"] = json!(priority);
    }

    state
        .db
        .put("todos", doc.clone())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(doc))
}

/// DELETE /api/todos/:id
async fn delete_todo_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let user_id = extract_user_id(auth, &state.db)?;

    let doc = state
        .db
        .get("todos", &json!(id))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    if doc["user_id"] != user_id {
        return Err(StatusCode::FORBIDDEN);
    }

    state
        .db
        .delete("todos", &json!(id))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(json!({"deleted": true})))
}

// ── HTML UI is loaded from `supabase-ui.html` via include_str! ─────

// ── Schema definitions via derive macro ───────────────────────────

#[allow(dead_code)]
#[derive(ObjectStore)]
#[store(name = "users", key_path = "id")]
struct UserStore {
    id: String,
    #[index(name = "by_email", unique)]
    email: String,
}

#[derive(ObjectStore)]
#[store(name = "sessions", key_path = "token")]
#[allow(dead_code)]
struct SessionStore {
    token: String,
    #[index(name = "by_user")]
    user_id: String,
}

#[derive(ObjectStore)]
#[allow(dead_code)]
#[store(name = "todos", key_path = "id")]
struct TodoStore {
    id: String,
    #[index(name = "by_user")]
    user_id: String,
    status: String,
    priority: i64,
}

// ── Main ──────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Open JsonDB with an auto-removed temp directory.
    let dir = tempfile::TempDir::with_prefix("flowdb-supabase-server_").unwrap();
    let db_path = dir.path().to_owned();

    let db = JsonDB::open(Config {
        data_dir: db_path.clone(),
        auto_background: true,
        compression: flowdb::record::CompressionAlgorithm::Lz4,
        ..Config::default()
    })
    .expect("failed to open JsonDB");

    // Schema via derive macro
    db.apply_schema::<UserStore>().unwrap();
    db.apply_schema::<SessionStore>().unwrap();
    db.apply_schema::<TodoStore>().unwrap();

    // Compound indexes (multi-field, not expressible via single #[index])
    db.create_index("todos", "by_user_status", &["user_id", "status"], false, false).unwrap();
    db.create_index("todos", "by_user_priority", &["user_id", "priority"], false, false).unwrap();

    let state = Arc::new(AppState { db });

    println!("FlowDB Supabase server running at http://localhost:3000");
    println!("  (data directory: {})", db_path.display());

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/signup", post(signup_handler))
        .route("/api/login", post(login_handler))
        .route("/api/todos", get(list_todos_handler).post(create_todo_handler))
        .route("/api/todos/{id}", put(update_todo_handler).delete(delete_todo_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
