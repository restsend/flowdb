use napi::bindgen_prelude::*;
use napi::JsUnknown;
use napi_derive::napi;
use std::sync::Arc;

use flowdb::jsondb::{JsonDB, TransactionMode};
use flowdb::record::Config;
use serde_json::Value;

// ── Helpers ─────────────────────────────────────────────────────────

fn flow_err(e: impl ToString) -> napi::Error {
    napi::Error::from_reason(e.to_string())
}

fn parse_mode(s: &str) -> Result<TransactionMode> {
    match s {
        "readonly" => Ok(TransactionMode::ReadOnly),
        "readwrite" => Ok(TransactionMode::ReadWrite),
        _ => Err(napi::Error::from_reason(
            "mode must be 'readonly' or 'readwrite'".to_string(),
        )),
    }
}

// Convert serde_json::Value → JsUnknown (native JS value)
fn value_to_js(env: &Env, val: Value) -> Result<JsUnknown> {
    env.to_js_value(&val)
}

fn value_opt_to_js(env: &Env, val: Option<Value>) -> Result<JsUnknown> {
    match val {
        Some(v) => value_to_js(env, v),
        None => value_to_js(env, Value::Null),
    }
}

fn values_to_js_vec(env: &Env, vals: Vec<Value>) -> Result<Vec<JsUnknown>> {
    vals.into_iter()
        .map(|v| value_to_js(env, v))
        .collect()
}

// ── JsConfig ────────────────────────────────────────────────────────

#[napi(object)]
#[derive(Default)]
pub struct JsConfig {
    pub data_dir: String,
    pub create_if_missing: Option<bool>,
    pub default_ttl_secs: Option<i64>,
    pub memtable_size_mb: Option<i64>,
    pub block_cache_capacity_mb: Option<i64>,
    pub bloom_bits_per_key: Option<i64>,
    pub compaction_interval_ms: Option<i64>,
}

// ── FlowDb ──────────────────────────────────────────────────────────

#[napi]
pub struct FlowDb {
    inner: Arc<JsonDB>,
}

// ── Open (synchronous — TypeScript layer wraps in Promise) ──────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn open(config: JsConfig) -> Result<FlowDb> {
        let mut cfg = Config::default();
        cfg.data_dir = config.data_dir.into();
        if let Some(v) = config.create_if_missing {
            cfg.create_if_missing = v;
        }
        if let Some(v) = config.default_ttl_secs {
            if v < 0 { return Err(napi::Error::from_reason("default_ttl_secs cannot be negative")); }
            cfg.default_ttl_secs = Some(v as u64);
        }
        if let Some(v) = config.memtable_size_mb {
            if v <= 0 { return Err(napi::Error::from_reason("memtable_size_mb must be > 0")); }
            cfg.memtable_size_mb = v as usize;
        }
        if let Some(v) = config.block_cache_capacity_mb {
            if v < 0 { return Err(napi::Error::from_reason("block_cache_capacity_mb cannot be negative")); }
            cfg.block_cache_capacity_mb = v as usize;
        }
        if let Some(v) = config.bloom_bits_per_key {
            if v <= 0 { return Err(napi::Error::from_reason("bloom_bits_per_key must be > 0")); }
            cfg.bloom_bits_per_key = v as usize;
        }
        if let Some(v) = config.compaction_interval_ms {
            if v <= 0 { return Err(napi::Error::from_reason("compaction_interval_ms must be > 0")); }
            cfg.compaction_interval_ms = v as u64;
        }
        let db = JsonDB::open(cfg).map_err(flow_err)?;
        Ok(FlowDb {
            inner: Arc::new(db),
        })
    }
}

// ── CloseTask ───────────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn close(&self) -> AsyncTask<CloseTask> {
        AsyncTask::new(CloseTask {
            inner: self.inner.clone(),
        })
    }
}

pub struct CloseTask {
    inner: Arc<JsonDB>,
}

impl Task for CloseTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        self.inner.close().map_err(flow_err)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

// ── PutTask ─────────────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn put(&self, store: String, value: Value) -> AsyncTask<PutTask> {
        AsyncTask::new(PutTask {
            inner: self.inner.clone(),
            store,
            value,
        })
    }
}

pub struct PutTask {
    inner: Arc<JsonDB>,
    store: String,
    value: Value,
}

impl Task for PutTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let value = std::mem::take(&mut self.value);
        self.inner.put(&self.store, value).map_err(flow_err)?;
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

// ── PutAutoTask ─────────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn put_auto(&self, store: String, value: Value) -> AsyncTask<PutAutoTask> {
        AsyncTask::new(PutAutoTask {
            inner: self.inner.clone(),
            store,
            value,
        })
    }
}

pub struct PutAutoTask {
    inner: Arc<JsonDB>,
    store: String,
    value: Value,
}

impl Task for PutAutoTask {
    type Output = Value;
    type JsValue = JsUnknown;

    fn compute(&mut self) -> Result<Self::Output> {
        let value = std::mem::take(&mut self.value);
        self.inner.put_auto(&self.store, value).map_err(flow_err)
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        value_to_js(&env, output)
    }
}

// ── GetTask ─────────────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn get(&self, store: String, key: Value) -> AsyncTask<GetTask> {
        AsyncTask::new(GetTask {
            inner: self.inner.clone(),
            store,
            key,
        })
    }
}

pub struct GetTask {
    inner: Arc<JsonDB>,
    store: String,
    key: Value,
}

impl Task for GetTask {
    type Output = Option<Value>;
    type JsValue = JsUnknown;

    fn compute(&mut self) -> Result<Self::Output> {
        let key = std::mem::take(&mut self.key);
        self.inner.get(&self.store, &key).map_err(flow_err)
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        value_opt_to_js(&env, output)
    }
}

// ── DeleteTask ──────────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn delete(&self, store: String, key: Value) -> AsyncTask<DeleteTask> {
        AsyncTask::new(DeleteTask {
            inner: self.inner.clone(),
            store,
            key,
        })
    }
}

pub struct DeleteTask {
    inner: Arc<JsonDB>,
    store: String,
    key: Value,
}

impl Task for DeleteTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let key = std::mem::take(&mut self.key);
        self.inner.delete(&self.store, &key).map_err(flow_err)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

// ── ScanTask ────────────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn scan(&self, store: String) -> AsyncTask<ScanTask> {
        AsyncTask::new(ScanTask {
            inner: self.inner.clone(),
            store,
        })
    }
}

pub struct ScanTask {
    inner: Arc<JsonDB>,
    store: String,
}

impl Task for ScanTask {
    type Output = Vec<Value>;
    type JsValue = Vec<JsUnknown>;

    fn compute(&mut self) -> Result<Self::Output> {
        self.inner.scan(&self.store).map_err(flow_err)
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        values_to_js_vec(&env, output)
    }
}

// ── StoreNames (sync — cheap) ───────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn store_names(&self) -> Vec<String> {
        self.inner.store_names()
    }
}

// ── CreateObjectStoreTask ───────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn create_object_store(
        &self,
        name: String,
        key_path: String,
    ) -> AsyncTask<CreateObjectStoreTask> {
        AsyncTask::new(CreateObjectStoreTask {
            inner: self.inner.clone(),
            name,
            key_path,
        })
    }
}

pub struct CreateObjectStoreTask {
    inner: Arc<JsonDB>,
    name: String,
    key_path: String,
}

impl Task for CreateObjectStoreTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        self.inner
            .create_object_store(&self.name, &self.key_path)
            .map_err(flow_err)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

// ── DeleteObjectStoreTask ───────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn delete_object_store(&self, name: String) -> AsyncTask<DeleteObjectStoreTask> {
        AsyncTask::new(DeleteObjectStoreTask {
            inner: self.inner.clone(),
            name,
        })
    }
}

pub struct DeleteObjectStoreTask {
    inner: Arc<JsonDB>,
    name: String,
}

impl Task for DeleteObjectStoreTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        self.inner.delete_object_store(&self.name).map_err(flow_err)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

// ── CreateIndexTask ─────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn create_index(
        &self,
        store: String,
        name: String,
        key_paths: Vec<String>,
        unique: Option<bool>,
    ) -> AsyncTask<CreateIndexTask> {
        AsyncTask::new(CreateIndexTask {
            inner: self.inner.clone(),
            store,
            name,
            key_paths,
            unique: unique.unwrap_or(false),
        })
    }
}

pub struct CreateIndexTask {
    inner: Arc<JsonDB>,
    store: String,
    name: String,
    key_paths: Vec<String>,
    unique: bool,
}

impl Task for CreateIndexTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let refs: Vec<&str> = self.key_paths.iter().map(|s| s.as_str()).collect();
        self.inner
            .create_index(&self.store, &self.name, &refs, self.unique)
            .map_err(flow_err)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

// ── DeleteIndexTask ─────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn delete_index(&self, store: String, name: String) -> AsyncTask<DeleteIndexTask> {
        AsyncTask::new(DeleteIndexTask {
            inner: self.inner.clone(),
            store,
            name,
        })
    }
}

pub struct DeleteIndexTask {
    inner: Arc<JsonDB>,
    store: String,
    name: String,
}

impl Task for DeleteIndexTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        self.inner
            .delete_index(&self.store, &self.name)
            .map_err(flow_err)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

// ── CountTask ───────────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn count(&self, store: String) -> AsyncTask<CountTask> {
        AsyncTask::new(CountTask {
            inner: self.inner.clone(),
            store,
        })
    }
}

pub struct CountTask {
    inner: Arc<JsonDB>,
    store: String,
}

impl Task for CountTask {
    type Output = i64;
    type JsValue = i64;

    fn compute(&mut self) -> Result<Self::Output> {
        self.inner
            .count(&self.store)
            .map(|c| c as i64)
            .map_err(flow_err)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output)
    }
}

// ── GetByIndexTask ──────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn get_by_index(
        &self,
        store: String,
        index: String,
        value: Value,
    ) -> AsyncTask<GetByIndexTask> {
        AsyncTask::new(GetByIndexTask {
            inner: self.inner.clone(),
            store,
            index,
            value,
        })
    }
}

pub struct GetByIndexTask {
    inner: Arc<JsonDB>,
    store: String,
    index: String,
    value: Value,
}

impl Task for GetByIndexTask {
    type Output = Vec<Value>;
    type JsValue = Vec<JsUnknown>;

    fn compute(&mut self) -> Result<Self::Output> {
        let value = std::mem::take(&mut self.value);
        self.inner
            .get_by_index(&self.store, &self.index, &value)
            .map_err(flow_err)
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        values_to_js_vec(&env, output)
    }
}

// ── RangeByIndexTask ────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn range_by_index(
        &self,
        store: String,
        index: String,
        start: Value,
        end: Value,
    ) -> AsyncTask<RangeByIndexTask> {
        AsyncTask::new(RangeByIndexTask {
            inner: self.inner.clone(),
            store,
            index,
            start,
            end,
        })
    }
}

pub struct RangeByIndexTask {
    inner: Arc<JsonDB>,
    store: String,
    index: String,
    start: Value,
    end: Value,
}

impl Task for RangeByIndexTask {
    type Output = Vec<Value>;
    type JsValue = Vec<JsUnknown>;

    fn compute(&mut self) -> Result<Self::Output> {
        let start = std::mem::take(&mut self.start);
        let end = std::mem::take(&mut self.end);
        self.inner
            .range_by_index(&self.store, &self.index, &start, &end)
            .map_err(flow_err)
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        values_to_js_vec(&env, output)
    }
}

// ── Transaction ─────────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn transaction(&self, stores: Vec<String>, mode: String) -> Result<JsTransaction> {
        let tx_mode = parse_mode(&mode)?;
        Ok(JsTransaction {
            db: self.inner.clone(),
            mode: tx_mode,
            stores,
            ops: std::sync::Mutex::new(Vec::new()),
        })
    }
}

// ── JsTransaction ───────────────────────────────────────────────────

enum TxOp {
    Put { store: String, value: Value },
    Delete { store: String, key: Value },
}

#[napi]
pub struct JsTransaction {
    db: Arc<JsonDB>,
    mode: TransactionMode,
    stores: Vec<String>,
    ops: std::sync::Mutex<Vec<TxOp>>,
}

#[napi]
impl JsTransaction {
    #[napi]
    pub fn put(&self, store: String, value: Value) -> Result<()> {
        self.ops
            .lock()
            .map_err(|_| napi::Error::from_reason("transaction lock poisoned"))?
            .push(TxOp::Put { store, value });
        Ok(())
    }

    #[napi]
    pub fn delete(&self, store: String, key: Value) -> Result<()> {
        self.ops
            .lock()
            .map_err(|_| napi::Error::from_reason("transaction lock poisoned"))?
            .push(TxOp::Delete { store, key });
        Ok(())
    }

    #[napi]
    pub fn commit(&self) -> AsyncTask<CommitTask> {
        let ops = {
            let mut guard = self.ops.lock().expect("transaction lock poisoned");
            std::mem::take(&mut *guard)
        };
        AsyncTask::new(CommitTask {
            db: self.db.clone(),
            mode: self.mode,
            stores: self.stores.clone(),
            ops,
        })
    }

    #[napi]
    pub fn abort(&self) -> Result<()> {
        let mut guard = self
            .ops
            .lock()
            .map_err(|_| napi::Error::from_reason("transaction lock poisoned"))?;
        guard.clear();
        Ok(())
    }
}

// ── CommitTask ──────────────────────────────────────────────────────

pub struct CommitTask {
    db: Arc<JsonDB>,
    mode: TransactionMode,
    stores: Vec<String>,
    ops: Vec<TxOp>,
}

impl Task for CommitTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let store_refs: Vec<&str> = self.stores.iter().map(|s| s.as_str()).collect();
        let mut tx = self
            .db
            .transaction(&store_refs, self.mode)
            .map_err(flow_err)?;

        let ops = std::mem::take(&mut self.ops);
        for op in ops {
            match op {
                TxOp::Put { store, value } => {
                    tx.put(&store, value).map_err(flow_err)?;
                }
                TxOp::Delete { store, key } => {
                    tx.delete(&store, &key).map_err(flow_err)?;
                }
            }
        }

        tx.commit().map_err(flow_err)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}
