use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
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

fn inject_meta(doc: Value, ts: i64, expire_at: i64) -> Value {
    match doc {
        Value::Object(mut map) => {
            // Timestamps are in microseconds. Convert to milliseconds for
            // JS Number compatibility (microsecond epoch fits in i64 but
            // may exceed Number.MAX_SAFE_INTEGER in the JS runtime).
            map.insert("_tsMs".to_string(), Value::from(ts / 1000));
            // i64::MAX means "never expires".
            if expire_at == i64::MAX {
                map.insert("_expireAtMs".to_string(), Value::Null);
            } else {
                map.insert("_expireAtMs".to_string(), Value::from(expire_at / 1000));
            }
            Value::Object(map)
        }
        other => other,
    }
}

fn values_to_js_vec(env: &Env, vals: Vec<Value>) -> Result<Vec<JsUnknown>> {
    vals.into_iter()
        .map(|v| value_to_js(env, v))
        .collect()
}

fn parse_key_range(val: &Option<Value>) -> Result<Option<flowdb::jsondb::KeyRange>> {
    match val {
        None => Ok(None),
        Some(v) => {
            let obj = v.as_object().ok_or_else(|| {
                napi::Error::from_reason("KeyRange must be an object")
            })?;
            let lower = obj.get("lower").filter(|v| !v.is_null()).cloned();
            let upper = obj.get("upper").filter(|v| !v.is_null()).cloned();
            let lower_open = obj
                .get("lowerOpen")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let upper_open = obj
                .get("upperOpen")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            Ok(Some(flowdb::jsondb::KeyRange {
                lower,
                upper,
                lower_open,
                upper_open,
            }))
        }
    }
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
        let mut cfg = Config {
            data_dir: config.data_dir.into(),
            ..Default::default()
        };
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

    #[napi]
    pub fn get_with_meta(&self, store: String, key: Value) -> AsyncTask<GetWithMetaTask> {
        AsyncTask::new(GetWithMetaTask {
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

pub struct GetWithMetaTask {
    inner: Arc<JsonDB>,
    store: String,
    key: Value,
}

impl Task for GetWithMetaTask {
    type Output = Option<Value>;
    type JsValue = JsUnknown;

    fn compute(&mut self) -> Result<Self::Output> {
        let key = std::mem::take(&mut self.key);
        let result = self.inner.get_with_meta(&self.store, &key).map_err(flow_err)?;
        Ok(result.map(|(doc, ts, expire_at)| {
            inject_meta(doc, ts, expire_at)
        }))
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

    #[napi]
    pub fn scan_with_meta(&self, store: String) -> AsyncTask<ScanWithMetaTask> {
        AsyncTask::new(ScanWithMetaTask {
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

pub struct ScanWithMetaTask {
    inner: Arc<JsonDB>,
    store: String,
}

impl Task for ScanWithMetaTask {
    type Output = Vec<Value>;
    type JsValue = Vec<JsUnknown>;

    fn compute(&mut self) -> Result<Self::Output> {
        let docs = self.inner.scan_with_meta(&self.store).map_err(flow_err)?;
        Ok(docs.into_iter().map(|(doc, ts, expire_at)| inject_meta(doc, ts, expire_at)).collect())
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
        auto_increment: bool,
    ) -> AsyncTask<CreateObjectStoreTask> {
        AsyncTask::new(CreateObjectStoreTask {
            inner: self.inner.clone(),
            name,
            key_path,
            auto_increment,
        })
    }
}

pub struct CreateObjectStoreTask {
    inner: Arc<JsonDB>,
    name: String,
    key_path: String,
    auto_increment: bool,
}

impl Task for CreateObjectStoreTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        if self.auto_increment {
            let def = flowdb::jsondb::StoreSchema::new(&self.name, &self.key_path)
                .with_auto_increment();
            self.inner.apply_store(&def).map_err(flow_err)?;
        } else {
            self.inner
                .create_object_store(&self.name, &self.key_path)
                .map_err(flow_err)?;
        }
        Ok(())
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
        multi_entry: Option<bool>,
    ) -> AsyncTask<CreateIndexTask> {
        AsyncTask::new(CreateIndexTask {
            inner: self.inner.clone(),
            store,
            name,
            key_paths,
            unique: unique.unwrap_or(false),
            multi_entry: multi_entry.unwrap_or(false),
        })
    }
}

pub struct CreateIndexTask {
    inner: Arc<JsonDB>,
    store: String,
    name: String,
    key_paths: Vec<String>,
    unique: bool,
    multi_entry: bool,
}

impl Task for CreateIndexTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let refs: Vec<&str> = self.key_paths.iter().map(|s| s.as_str()).collect();
        self.inner
            .create_index(
                &self.store,
                &self.name,
                &refs,
                self.unique,
                self.multi_entry,
            )
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

// ── AddTask (IndexedDB add — fail on duplicate key) ─────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn add(&self, store: String, value: Value) -> AsyncTask<AddTask> {
        AsyncTask::new(AddTask {
            inner: self.inner.clone(),
            store,
            value,
        })
    }
}

pub struct AddTask {
    inner: Arc<JsonDB>,
    store: String,
    value: Value,
}

impl Task for AddTask {
    type Output = Value;
    type JsValue = JsUnknown;

    fn compute(&mut self) -> Result<Self::Output> {
        let value = std::mem::take(&mut self.value);
        self.inner.add(&self.store, value).map_err(flow_err)
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        value_to_js(&env, output)
    }
}

// ── ClearTask ───────────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn clear(&self, store: String) -> AsyncTask<ClearTask> {
        AsyncTask::new(ClearTask {
            inner: self.inner.clone(),
            store,
        })
    }
}

pub struct ClearTask {
    inner: Arc<JsonDB>,
    store: String,
}

impl Task for ClearTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        self.inner.clear(&self.store).map_err(flow_err)?;
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

// ── GetAllTask (IndexedDB getAll with optional KeyRange + count) ────

#[napi]
impl FlowDb {
    #[napi]
    pub fn get_all(
        &self,
        store: String,
        query: Option<Value>,
        count: Option<i64>,
    ) -> AsyncTask<GetAllTask> {
        AsyncTask::new(GetAllTask {
            inner: self.inner.clone(),
            store,
            query,
            count: count.map(|c| c as usize),
        })
    }
}

pub struct GetAllTask {
    inner: Arc<JsonDB>,
    store: String,
    query: Option<Value>,
    count: Option<usize>,
}

impl Task for GetAllTask {
    type Output = Vec<Value>;
    type JsValue = Vec<JsUnknown>;

    fn compute(&mut self) -> Result<Self::Output> {
        let kr = parse_key_range(&self.query)?;
        self.inner
            .get_all(&self.store, kr.as_ref(), self.count)
            .map_err(flow_err)
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        values_to_js_vec(&env, output)
    }
}

// ── GetAllKeysTask ──────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn get_all_keys(
        &self,
        store: String,
        query: Option<Value>,
        count: Option<i64>,
    ) -> AsyncTask<GetAllKeysTask> {
        AsyncTask::new(GetAllKeysTask {
            inner: self.inner.clone(),
            store,
            query,
            count: count.map(|c| c as usize),
        })
    }
}

pub struct GetAllKeysTask {
    inner: Arc<JsonDB>,
    store: String,
    query: Option<Value>,
    count: Option<usize>,
}

impl Task for GetAllKeysTask {
    type Output = Vec<Value>;
    type JsValue = Vec<JsUnknown>;

    fn compute(&mut self) -> Result<Self::Output> {
        let kr = parse_key_range(&self.query)?;
        self.inner
            .get_all_keys(&self.store, kr.as_ref(), self.count)
            .map_err(flow_err)
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        values_to_js_vec(&env, output)
    }
}

// ── GetKeyTask ──────────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn get_key(&self, store: String, key: Value) -> AsyncTask<GetKeyTask> {
        AsyncTask::new(GetKeyTask {
            inner: self.inner.clone(),
            store,
            key,
        })
    }
}

pub struct GetKeyTask {
    inner: Arc<JsonDB>,
    store: String,
    key: Value,
}

impl Task for GetKeyTask {
    type Output = Option<Value>;
    type JsValue = JsUnknown;

    fn compute(&mut self) -> Result<Self::Output> {
        let key = std::mem::take(&mut self.key);
        self.inner.get_key(&self.store, &key).map_err(flow_err)
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        value_opt_to_js(&env, output)
    }
}

// ── CountQueryTask ──────────────────────────────────────────────────

#[napi]
impl FlowDb {
    #[napi]
    pub fn count_query(
        &self,
        store: String,
        query: Option<Value>,
    ) -> AsyncTask<CountQueryTask> {
        AsyncTask::new(CountQueryTask {
            inner: self.inner.clone(),
            store,
            query,
        })
    }
}

pub struct CountQueryTask {
    inner: Arc<JsonDB>,
    store: String,
    query: Option<Value>,
}

impl Task for CountQueryTask {
    type Output = usize;
    type JsValue = i64;

    fn compute(&mut self) -> Result<Self::Output> {
        let kr = parse_key_range(&self.query)?;
        self.inner
            .count_with_query(&self.store, kr.as_ref())
            .map_err(flow_err)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output as i64)
    }
}

// ── Cursor (callback-style + async iterator) ────────────────────────

#[napi]
impl FlowDb {
    /// Open a cursor over a store and call `callback` for each item.
    /// The callback receives `{ key, value }` or `null` when done.
    /// Return `true` to continue, `false` to stop.
    #[napi]
    pub fn open_cursor(
        &self,
        store: String,
        query: Option<Value>,
        direction: String,
        callback: ThreadsafeFunction<CursorItem, ErrorStrategy::Fatal>,
    ) -> AsyncTask<OpenCursorTask> {
        AsyncTask::new(OpenCursorTask {
            inner: self.inner.clone(),
            store,
            query,
            direction,
            callback,
            is_index: false,
            index: None,
        })
    }

    /// Open a cursor over an index and call `callback` for each item.
    /// The callback receives `{ key, primaryKey, value }` or `null` when done.
    #[napi]
    pub fn open_cursor_by_index(
        &self,
        store: String,
        index: String,
        query: Option<Value>,
        direction: String,
        callback: ThreadsafeFunction<CursorItem, ErrorStrategy::Fatal>,
    ) -> AsyncTask<OpenCursorTask> {
        AsyncTask::new(OpenCursorTask {
            inner: self.inner.clone(),
            store,
            query,
            direction,
            callback,
            is_index: true,
            index: Some(index),
        })
    }
}

#[napi(object)]
pub struct CursorItem {
    pub key: Value,
    pub primary_key: Option<Value>,
    pub value: Value,
    pub done: bool,
}

pub struct OpenCursorTask {
    inner: Arc<JsonDB>,
    store: String,
    query: Option<Value>,
    direction: String,
    callback: ThreadsafeFunction<CursorItem, ErrorStrategy::Fatal>,
    is_index: bool,
    index: Option<String>,
}

impl Task for OpenCursorTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let kr = parse_key_range(&self.query)?;
        let dir = flowdb::jsondb::CursorDirection::parse(&self.direction)
            .map_err(flow_err)?;

        if self.is_index {
            let index = self.index.as_ref().unwrap();
            let mut cursor = self
                .inner
                .open_cursor_on_index(&self.store, index, kr.as_ref(), dir)
                .map_err(flow_err)?;
            while let Some((idx_key, pk, doc)) = cursor.next_value() {
                let item = CursorItem {
                    key: idx_key,
                    primary_key: Some(pk),
                    value: doc,
                    done: false,
                };
                let status = self.callback.call(
                    item,
                    ThreadsafeFunctionCallMode::Blocking,
                );
                if status != napi::Status::Ok {
                    break;
                }
            }
            // Signal done.
            let _ = self.callback.call(
                CursorItem {
                    key: Value::Null,
                    primary_key: None,
                    value: Value::Null,
                    done: true,
                },
                ThreadsafeFunctionCallMode::Blocking,
            );
        } else {
            let mut cursor = self
                .inner
                .open_cursor(&self.store, kr.as_ref(), dir)
                .map_err(flow_err)?;
            while let Some((key, doc)) = cursor.next_value() {
                let item = CursorItem {
                    key,
                    primary_key: None,
                    value: doc,
                    done: false,
                };
                let status = self.callback.call(
                    item,
                    ThreadsafeFunctionCallMode::Blocking,
                );
                if status != napi::Status::Ok {
                    break;
                }
            }
            let _ = self.callback.call(
                CursorItem {
                    key: Value::Null,
                    primary_key: None,
                    value: Value::Null,
                    done: true,
                },
                ThreadsafeFunctionCallMode::Blocking,
            );
        }
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
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
            snapshot_seq: self.inner.last_seq(),
            reads: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    }
}

// ── JsTransaction ───────────────────────────────────────────────────

#[derive(Clone)]
enum TxOp {
    Put { store: String, value: Value },
    PutAuto { store: String, value: Value },
    Delete { store: String, key: Value },
}

#[napi]
pub struct JsTransaction {
    db: Arc<JsonDB>,
    mode: TransactionMode,
    stores: Vec<String>,
    ops: std::sync::Mutex<Vec<TxOp>>,
    /// MVCC snapshot captured at transaction creation.
    snapshot_seq: u64,
    /// OCC read-set: (store, key_bytes, value_bytes) tracked for
    /// conflict detection at commit time.  Shared with async read tasks
    /// via Arc so reads are visible at commit.
    reads: std::sync::Arc<std::sync::Mutex<Vec<ReadEntry>>>,
}

struct ReadEntry {
    store: String,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

/// Async task for MVCC snapshot-aware point reads within a transaction.
/// Checks the buffered write-ops first (read-your-writes), then falls
/// back to a snapshot-isolated engine read.
pub struct TxGetTask {
    db: Arc<JsonDB>,
    store: String,
    key: Value,
    snapshot_seq: u64,
    reads: std::sync::Arc<std::sync::Mutex<Vec<ReadEntry>>>,
    ops_snapshot: Vec<TxOp>,
}

impl Task for TxGetTask {
    type Output = Option<Value>;
    type JsValue = napi::JsUnknown;

    fn compute(&mut self) -> Result<Self::Output> {
        // Read-your-writes: check buffered ops for a matching put/delete.
        let key_path = self
            .db
            .get_store(&self.store)
            .map(|s| s.key_path)
            .unwrap_or_default();
        for op in &self.ops_snapshot {
            match op {
                TxOp::Put { store, value } | TxOp::PutAuto { store, value }
                    if store == &self.store =>
                {
                    if value.get(&key_path) == Some(&self.key) {
                        return Ok(Some(value.clone()));
                    }
                }
                TxOp::Delete { store, key } if store == &self.store && key == &self.key => {
                    return Ok(None);
                }
                _ => {}
            }
        }

        // Fall back to MVCC snapshot read.
        let result = self
            .db
            .get_snapshot(&self.store, &self.key, self.snapshot_seq)
            .map_err(flow_err)?;
        // Track in OCC read-set.
        let key_bytes = serde_json::to_vec(&self.key).unwrap_or_default();
        let val_bytes = result.as_ref().map(|v| serde_json::to_vec(v).unwrap_or_default());
        if let Ok(mut guard) = self.reads.lock() {
            guard.push(ReadEntry {
                store: self.store.clone(),
                key: key_bytes,
                value: val_bytes,
            });
        }
        Ok(result)
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        value_opt_to_js(&env, output)
    }
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
    pub fn put_auto(&self, store: String, value: Value) -> Result<()> {
        self.ops
            .lock()
            .map_err(|_| napi::Error::from_reason("transaction lock poisoned"))?
            .push(TxOp::PutAuto { store, value });
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
    pub fn get(&self, store: String, key: Value) -> AsyncTask<TxGetTask> {
        let ops_snapshot = {
            let guard = self.ops.lock().expect("transaction lock poisoned");
            guard.clone()
        };
        AsyncTask::new(TxGetTask {
            db: self.db.clone(),
            store,
            key,
            snapshot_seq: self.snapshot_seq,
            reads: self.reads.clone(),
            ops_snapshot,
        })
    }

    #[napi]
    pub fn count(&self, store: String) -> AsyncTask<CountTask> {
        AsyncTask::new(CountTask {
            inner: self.db.clone(),
            store,
        })
    }

    #[napi]
    pub fn scan(&self, store: String) -> AsyncTask<ScanTask> {
        AsyncTask::new(ScanTask {
            inner: self.db.clone(),
            store,
        })
    }

    #[napi]
    pub fn get_by_index(
        &self,
        store: String,
        index: String,
        value: Value,
    ) -> AsyncTask<GetByIndexTask> {
        AsyncTask::new(GetByIndexTask {
            inner: self.db.clone(),
            store,
            index,
            value,
        })
    }

    #[napi]
    pub fn range_by_index(
        &self,
        store: String,
        index: String,
        start: Value,
        end: Value,
    ) -> AsyncTask<RangeByIndexTask> {
        AsyncTask::new(RangeByIndexTask {
            inner: self.db.clone(),
            store,
            index,
            start,
            end,
        })
    }

    #[napi]
    pub fn commit(&self) -> AsyncTask<CommitTask> {
        let ops = {
            let mut guard = self.ops.lock().expect("transaction lock poisoned");
            std::mem::take(&mut *guard)
        };
        let reads = {
            let mut guard = self.reads.lock().expect("transaction reads lock poisoned");
            std::mem::take(&mut *guard)
        };
        AsyncTask::new(CommitTask {
            db: self.db.clone(),
            mode: self.mode,
            stores: self.stores.clone(),
            ops,
            snapshot_seq: self.snapshot_seq,
            reads,
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
    snapshot_seq: u64,
    reads: Vec<ReadEntry>,
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

        // Override snapshot with the one captured at JsTransaction creation.
        tx.set_snapshot_seq(self.snapshot_seq);

        // Inject OCC read-set entries collected during the transaction.
        for entry in &self.reads {
            tx.add_read_entry(&entry.store, entry.key.clone(), entry.value.clone());
        }

        let ops = std::mem::take(&mut self.ops);
        for op in ops {
            match op {
                TxOp::Put { store, value } => {
                    tx.put(&store, value).map_err(flow_err)?;
                }
                TxOp::PutAuto { store, value } => {
                    tx.put_auto(&store, value).map_err(flow_err)?;
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
