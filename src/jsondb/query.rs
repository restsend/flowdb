use crate::error::{FlowError, Result};
use crate::jsondb::db::JsonDB;
use crate::jsondb::encoding::*;
use crate::jsondb::schema::*;
use crate::record::ScanRange;
use serde_json::Value;
use std::ops::Bound;

// ── QueryBuilder ───────────────────────────────────────────────────

/// Sort direction for [`QueryBuilder::order_by`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SortDir {
    /// Ascending order (smallest first).
    Asc,
    /// Descending order (largest first).
    Desc,
}

/// A type-safe query builder for JsonDB object stores.
///
/// Supports single-field and composite indexes, predicate pushdown, and
/// optional `order_by`/`limit`/`offset`.
///
/// # Example
///
/// ```no_run
/// use flowdb::jsondb::{JsonDB, SortDir};
/// use serde_json::json;
///
/// let db = JsonDB::open(Default::default()).unwrap();
/// db.create_object_store("users", "id").unwrap();
/// db.create_index("users", "by_city_age", &["city", "age"], false, false).unwrap();
/// db.put("users", json!({"id": "u1", "city": "NYC", "age": 30})).unwrap();
///
/// let docs: Vec<serde_json::Value> = db.query("users")
///     .where_eq("city", json!("NYC"))
///     .where_range("age", json!(25), json!(35))
///     .order_by("age", SortDir::Asc)
///     .limit(10)
///     .collect()
///     .unwrap();
/// ```
pub struct QueryBuilder<'a> {
    db: &'a JsonDB,
    store: &'a str,
    filters: Vec<QueryFilter>,
    order_field: Option<String>,
    order_dir: SortDir,
    limit: Option<usize>,
    offset: usize,
}

#[derive(Debug, Clone)]
pub(crate) enum QueryFilter {
    Eq(String, Value),
    Range(String, Value, Value),
    In(String, Vec<Value>),
}

impl<'a> QueryBuilder<'a> {
    /// Create a new query builder for the given store.
    pub fn new(db: &'a JsonDB, store: &'a str) -> Self {
        Self {
            db,
            store,
            filters: Vec::new(),
            order_field: None,
            order_dir: SortDir::Asc,
            limit: None,
            offset: 0,
        }
    }

    /// Filter: field == value.
    pub fn where_eq(mut self, field: &str, value: Value) -> Self {
        self.filters.push(QueryFilter::Eq(field.to_string(), value));
        self
    }

    /// Filter: `start <= field < end` (exclusive upper bound).
    pub fn where_range(mut self, field: &str, start: Value, end: Value) -> Self {
        self.filters
            .push(QueryFilter::Range(field.to_string(), start, end));
        self
    }

    /// Filter: field IN [...values].
    pub fn where_in(mut self, field: &str, values: Vec<Value>) -> Self {
        self.filters
            .push(QueryFilter::In(field.to_string(), values));
        self
    }

    /// Sort by `field` in ascending or descending order.
    ///
    /// When the sort field matches the first field of the best-matching index
    /// **and** direction is `Asc`, the results are already in the correct order
    /// and no extra sort is performed.
    pub fn order_by(mut self, field: &str, dir: SortDir) -> Self {
        self.order_field = Some(field.to_string());
        self.order_dir = dir;
        self
    }

    /// Limit results to `n` documents.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Skip the first `n` documents.
    pub fn offset(mut self, n: usize) -> Self {
        self.offset = n;
        self
    }

    /// Execute the query and return matching documents.
    pub fn collect(self) -> Result<Vec<Value>> {
        let store_def = self
            .db
            .schema
            .get(self.store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", self.store)))?;

        // 1. Find best matching index and compute scan range
        let (scan_result, used_index) = self.plan_scan(&store_def);

        // Determine whether an in-memory sort is needed (after the scan,
        // before offset/limit).  This also controls early-termination in
        // step 2 — when no sort is needed we can stop scanning early.
        let needs_sort = match &self.order_field {
            Some(field) => {
                let index_provides_order = used_index
                    .as_ref()
                    .map(|idx: &IndexDef| {
                        self.order_dir == SortDir::Asc
                            && idx.key_paths.first().map(|s| s.as_str()) == Some(field.as_str())
                    })
                    .unwrap_or(false);
                !index_provides_order
            }
            None => false,
        };

        // 2. Execute scan with optional early-termination for limit.
        //    When the scan order matches the natural index order we can
        //    stop after collecting (limit + offset) results.
        let limit_target = self.limit.map(|l| l + self.offset);

        let mut docs: Vec<Value> = match scan_result {
            ScanPlan::Index {
                prefix,
                range_start,
                range_end,
            } => {
                let range = if let (Some(s), Some(e)) = (&range_start, &range_end) {
                    ScanRange {
                        key_start: Bound::Included(s.to_vec()),
                        key_end: Bound::Excluded(e.to_vec()),
                        ts_start: Bound::Unbounded,
                        ts_end: Bound::Unbounded,
                    }
                } else {
                    prefix_range(&prefix)
                };
                let iter = self.db.engine.scan(range)?;
                let mut results = Vec::new();
                for r in iter {
                    let rec = r?;
                    if let Some(doc) = self
                        .db
                        .engine
                        .get_bytes(&doc_key(self.store, &rec.value), 0)
                    {
                        results.push(decode_doc(&doc.value)?);
                    }
                    // Early-terminate when limit is set and no sort is needed.
                    if !needs_sort
                        && let Some(target) = limit_target
                        && results.len() >= target
                    {
                        break;
                    }
                }
                results
            }
            ScanPlan::FullScan => {
                let pfx = doc_prefix(self.store);
                let iter = self.db.engine.scan(prefix_range(&pfx))?;
                let mut results = Vec::new();
                for r in iter {
                    let rec = r?;
                    results.push(decode_doc(&rec.value)?);
                    // Early-terminate when limit is set.
                    if let Some(target) = limit_target
                        && results.len() >= target
                    {
                        break;
                    }
                }
                results
            }
        };

        // 3. Apply predicate pushdown (remaining filters not covered by index)
        for filter in &self.filters {
            docs.retain(|doc| filter_matches(doc, filter));
        }

        // 4. Sort if needed.
        if needs_sort && let Some(ref field) = self.order_field {
            docs.sort_by(|a, b| {
                let va = extract_field(a, field).unwrap_or(Value::Null);
                let vb = extract_field(b, field).unwrap_or(Value::Null);
                let cmp = encode_index_value(&va).cmp(&encode_index_value(&vb));
                match self.order_dir {
                    SortDir::Asc => cmp,
                    SortDir::Desc => cmp.reverse(),
                }
            });
        }

        // 5. Offset + limit
        let docs: Vec<Value> = docs
            .into_iter()
            .skip(self.offset)
            .take(self.limit.unwrap_or(usize::MAX))
            .collect();

        Ok(docs)
    }

    /// Execute the query and deserialize results to `T`.
    ///
    /// ```no_run
    /// use flowdb::jsondb::{JsonDB, StoreSchema};
    /// use serde_json::json;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize, Serialize)]
    /// struct User { id: String, email: String }
    ///
    /// let db = JsonDB::open(Default::default()).unwrap();
    /// db.apply_store(&StoreSchema::new("users", "id")
    ///     .with_index("by_email", &["email"], true)).unwrap();
    /// db.put("users", json!({"id": "u1", "email": "a@b.com"})).unwrap();
    ///
    /// let users: Vec<User> = db.query("users")
    ///     .where_eq("email", json!("a@b.com"))
    ///     .collect_doc().unwrap();
    /// ```
    pub fn collect_doc<T: serde::de::DeserializeOwned>(self) -> Result<Vec<T>> {
        let values: Vec<Value> = self.collect()?;
        values
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(FlowError::from))
            .collect()
    }
}

enum ScanPlan {
    Index {
        prefix: Vec<u8>,
        range_start: Option<Vec<u8>>,
        range_end: Option<Vec<u8>>,
    },
    FullScan,
}

/// Check whether a document matches a single filter.
pub(crate) fn filter_matches(doc: &Value, filter: &QueryFilter) -> bool {
    match filter {
        QueryFilter::Eq(field, val) => extract_field(doc, field).as_ref() == Some(val),
        QueryFilter::Range(field, start, end) => match extract_field(doc, field) {
            Some(ref v) => {
                let enc = encode_index_value(v);
                let enc_start = encode_index_value(start);
                let enc_end = encode_index_value(end);
                enc.as_slice() >= enc_start.as_slice() && enc.as_slice() < enc_end.as_slice()
            }
            None => false,
        },
        QueryFilter::In(field, values) => match extract_field(doc, field) {
            Some(ref v) => values
                .iter()
                .any(|val| encode_index_value(v) == encode_index_value(val)),
            None => false,
        },
    }
}

impl<'a> QueryBuilder<'a> {
    fn plan_scan(&self, store_def: &StoreDef) -> (ScanPlan, Option<IndexDef>) {
        // Score each index by how many prefix fields match Eq/Range filters
        let mut best: Option<(usize, &IndexDef)> = None;
        for idx in &store_def.indexes {
            let score = self.score_index(idx);
            if score > best.map(|(s, _)| s).unwrap_or(0) {
                best = Some((score, idx));
            }
        }

        let (used_idx, plan) = match best {
            Some((_, idx)) => {
                let plan = self.build_index_scan(idx);
                (Some(idx.clone()), plan)
            }
            None => (None, ScanPlan::FullScan),
        };

        (plan, used_idx)
    }

    /// Number of prefix key_paths covered by Eq/Range/In filters.
    fn score_index(&self, idx: &IndexDef) -> usize {
        let mut score = 0usize;
        for path in &idx.key_paths {
            let matched = self.filters.iter().any(|f| match f {
                QueryFilter::Eq(field, _) => field == path,
                QueryFilter::Range(field, _, _) => field == path,
                QueryFilter::In(field, _) => field == path,
            });
            if matched {
                score += 1;
            } else {
                break; // prefix stop: we can only use prefix of composite
            }
        }
        score
    }

    fn build_index_scan(&self, idx: &IndexDef) -> ScanPlan {
        // Collect one encoded value per key_path, building the prefix key.
        let mut partial = idx_prefix(self.store, &idx.name);
        let mut range_end_bytes: Option<Vec<u8>> = None;

        for path in &idx.key_paths {
            let matched = self.filters.iter().find(|f| match f {
                QueryFilter::Eq(field, _) => field == path,
                QueryFilter::Range(field, _, _) => field == path,
                QueryFilter::In(field, _) => field == path,
            });

            match matched {
                Some(QueryFilter::Eq(_, val)) => {
                    let enc = encode_index_value(val);
                    partial.extend_from_slice(&enc);
                    partial.push(SEP);
                }
                Some(QueryFilter::Range(_, start, end)) => {
                    let enc_start = encode_index_value(start);
                    let enc_end = encode_index_value(end);
                    partial.extend_from_slice(&enc_start);
                    let mut end_key = idx_prefix(self.store, &idx.name);
                    for prev_path in &idx.key_paths {
                        if prev_path == path {
                            end_key.extend_from_slice(&enc_end);
                            break;
                        }
                        if let Some(QueryFilter::Eq(_, v)) =
                            self.filters.iter().find(|filt| match filt {
                                QueryFilter::Eq(field, _) => field == prev_path,
                                _ => false,
                            })
                        {
                            end_key.extend_from_slice(&encode_index_value(v));
                            end_key.push(SEP);
                        }
                    }
                    range_end_bytes = Some(end_key);
                    break;
                }
                Some(QueryFilter::In(_, _)) => break,
                None => break,
            }
        }

        if partial.last() == Some(&SEP) {
            partial.pop();
        }

        ScanPlan::Index {
            range_start: if !partial.is_empty() {
                Some(partial.clone())
            } else {
                None
            },
            range_end: range_end_bytes,
            prefix: partial,
        }
    }
}
