use crate::error::{FlowError, Result};
use crate::jsondb::encoding::*;
use crate::jsondb::keyrange::KeyRange;
use crate::jsondb::JsonDB;
use serde_json::Value;

/// Cursor direction, mirroring IndexedDB cursor direction values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorDirection {
    Next,
    NextUnique,
    Prev,
    PrevUnique,
}

impl CursorDirection {
    pub fn is_reverse(&self) -> bool {
        matches!(self, Self::Prev | Self::PrevUnique)
    }
    pub fn is_unique(&self) -> bool {
        matches!(self, Self::NextUnique | Self::PrevUnique)
    }
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "next" => Ok(Self::Next),
            "nextunique" => Ok(Self::NextUnique),
            "prev" => Ok(Self::Prev),
            "prevunique" => Ok(Self::PrevUnique),
            _ => Err(FlowError::JsonDb(format!(
                "invalid cursor direction '{}'; expected next/prev/nextunique/prevunique",
                s
            ))),
        }
    }
}

/// A cursor over documents in primary-key order.
pub struct Cursor {
    items: Vec<(Value, Value)>,
    pos: usize,
}

impl Cursor {
    pub(crate) fn open(
        db: &JsonDB,
        store: &str,
        range: Option<&KeyRange>,
        direction: CursorDirection,
    ) -> Result<Self> {
        let def = db
            .get_store(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;

        let scan_range = match range {
            Some(kr) => kr.to_doc_scan_range(store)?,
            None => prefix_range(&doc_prefix(store)),
        };

        let iter = db.engine.scan(scan_range)?;
        let mut items: Vec<(Value, Value)> = Vec::new();
        for r in iter {
            let rec = r?;
            let doc = decode_doc(&rec.value)?;
            let key_val = extract_field(&doc, &def.key_path).unwrap_or(Value::Null);
            items.push((key_val, doc));
        }

        if direction.is_reverse() {
            items.reverse();
        }
        if direction.is_unique() {
            let mut seen = std::collections::HashSet::new();
            items.retain(|(k, _)| seen.insert(k.clone()));
        }

        Ok(Self { items, pos: 0 })
    }

    pub fn next_value(&mut self) -> Option<(Value, Value)> {
        if self.pos >= self.items.len() {
            return None;
        }
        let item = self.items[self.pos].clone();
        self.pos += 1;
        Some(item)
    }

    pub fn advance(&mut self, count: usize) -> Option<(Value, Value)> {
        self.pos = (self.pos + count).min(self.items.len());
        self.next_value()
    }

    pub fn remaining(&self) -> usize {
        self.items.len().saturating_sub(self.pos)
    }

    pub fn current_key(&self) -> Option<&Value> {
        if self.pos == 0 {
            None
        } else {
            self.items.get(self.pos - 1).map(|(k, _)| k)
        }
    }
}

/// A cursor over an index, yielding (index_value, primary_key, doc) triples.
pub struct IndexCursor {
    items: Vec<(Value, Value, Value)>,
    pos: usize,
}

impl IndexCursor {
    pub(crate) fn open(
        db: &JsonDB,
        store: &str,
        index: &str,
        range: Option<&KeyRange>,
        direction: CursorDirection,
    ) -> Result<Self> {
        let def = db
            .get_store(store)
            .ok_or_else(|| FlowError::JsonDb(format!("store '{}' not found", store)))?;
        let idx_def = def
            .indexes
            .iter()
            .find(|i| i.name == index)
            .ok_or_else(|| {
                FlowError::JsonDb(format!("index '{}' not found on '{}'", index, store))
            })?;

        let scan_range = match range {
            Some(kr) => kr.to_index_scan_range(store, index)?,
            None => prefix_range(&idx_prefix(store, index)),
        };

        let iter = db.engine.scan(scan_range)?;
        let mut items: Vec<(Value, Value, Value)> = Vec::new();
        for r in iter {
            let rec = r?;
            let pk_bytes = &rec.value;
            if let Some(doc_rec) = db.engine.get_bytes(&doc_key(store, pk_bytes), 0) {
                let doc = decode_doc(&doc_rec.value)?;
                let pk = extract_field(&doc, &def.key_path).unwrap_or(Value::Null);
                // Extract the index value from the document for display.
                let idx_val = if idx_def.key_paths.len() == 1 {
                    extract_field(&doc, &idx_def.key_paths[0]).unwrap_or(Value::Null)
                } else {
                    let arr: Vec<Value> = idx_def
                        .key_paths
                        .iter()
                        .map(|p| extract_field(&doc, p).unwrap_or(Value::Null))
                        .collect();
                    Value::Array(arr)
                };
                items.push((idx_val, pk, doc));
            }
        }

        if direction.is_reverse() {
            items.reverse();
        }
        if direction.is_unique() {
            let mut seen = std::collections::HashSet::new();
            items.retain(|(_, pk, _)| seen.insert(pk.clone()));
        }

        Ok(Self { items, pos: 0 })
    }

    pub fn next_value(&mut self) -> Option<(Value, Value, Value)> {
        if self.pos >= self.items.len() {
            return None;
        }
        let item = self.items[self.pos].clone();
        self.pos += 1;
        Some(item)
    }

    pub fn advance(&mut self, count: usize) -> Option<(Value, Value, Value)> {
        self.pos = (self.pos + count).min(self.items.len());
        self.next_value()
    }

    pub fn remaining(&self) -> usize {
        self.items.len().saturating_sub(self.pos)
    }
}
