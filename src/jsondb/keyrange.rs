use crate::jsondb::encoding::{encode_index_value, encode_primary_key};
use crate::record::ScanRange;
use serde_json::Value;
use std::ops::Bound;

/// An IndexedDB-compatible key range for filtering documents or index entries.
///
/// # Factories
///
/// ```ignore
/// use flowdb::jsondb::KeyRange;
///
/// KeyRange::only(json!("alice"));
/// KeyRange::bound(json!("a"), json!("z"), false, false); // [a, z]
/// KeyRange::lower_bound(json!(18), false);               // [18, +inf)
/// KeyRange::upper_bound(json!(65), true);                 // (-inf, 65)
/// ```
#[derive(Debug, Clone)]
pub struct KeyRange {
    pub lower: Option<Value>,
    pub upper: Option<Value>,
    pub lower_open: bool,
    pub upper_open: bool,
}

impl Default for KeyRange {
    fn default() -> Self {
        Self {
            lower: None,
            upper: None,
            lower_open: false,
            upper_open: false,
        }
    }
}

impl KeyRange {
    /// Match exactly one key.
    pub fn only(key: Value) -> Self {
        Self {
            lower: Some(key.clone()),
            upper: Some(key),
            lower_open: false,
            upper_open: false,
        }
    }

    /// Match keys in `[lower, upper]` (or with open bounds).
    pub fn bound(lower: Value, upper: Value, lower_open: bool, upper_open: bool) -> Self {
        Self {
            lower: Some(lower),
            upper: Some(upper),
            lower_open,
            upper_open,
        }
    }

    /// Match keys `>= lower` (or `> lower` when `open` is true).
    pub fn lower_bound(key: Value, open: bool) -> Self {
        Self {
            lower: Some(key),
            upper: None,
            lower_open: open,
            upper_open: false,
        }
    }

    /// Match keys `<= upper` (or `< upper` when `open` is true).
    pub fn upper_bound(key: Value, open: bool) -> Self {
        Self {
            lower: None,
            upper: Some(key),
            lower_open: false,
            upper_open: open,
        }
    }

    /// Check whether a key falls within this range.
    pub fn includes(&self, key: &Value) -> bool {
        if let Some(ref lower) = self.lower {
            let cmp = compare_values(key, lower);
            if self.lower_open {
                if cmp != std::cmp::Ordering::Greater {
                    return false;
                }
            } else if cmp == std::cmp::Ordering::Less {
                return false;
            }
        }
        if let Some(ref upper) = self.upper {
            let cmp = compare_values(key, upper);
            if self.upper_open {
                if cmp != std::cmp::Ordering::Less {
                    return false;
                }
            } else if cmp == std::cmp::Ordering::Greater {
                return false;
            }
        }
        true
    }

    pub fn is_unbounded(&self) -> bool {
        self.lower.is_none() && self.upper.is_none()
    }

    // ── byte-level conversion for document keys ───────────────────

    /// Convert to a [`ScanRange`] over the **primary-key space** of a store.
    pub(crate) fn to_doc_scan_range(&self, store: &str) -> crate::error::Result<ScanRange> {
        let pfx = crate::jsondb::encoding::doc_prefix(store);
        let key_start = match &self.lower {
            None => Bound::Included(pfx.clone()),
            Some(val) => {
                let key_bytes = encode_primary_key(val)?;
                let full = [pfx.as_slice(), &key_bytes].concat();
                if self.lower_open {
                    Bound::Excluded(full)
                } else {
                    Bound::Included(full)
                }
            }
        };
        let key_end = match &self.upper {
            None => {
                let end = crate::record::increment_prefix_bytes(&pfx);
                Bound::Excluded(end)
            }
            Some(val) => {
                let key_bytes = encode_primary_key(val)?;
                let full = [pfx.as_slice(), &key_bytes].concat();
                if self.upper_open {
                    Bound::Excluded(full)
                } else {
                    Bound::Included(full)
                }
            }
        };
        Ok(ScanRange {
            key_start,
            key_end,
            ts_start: Bound::Unbounded,
            ts_end: Bound::Unbounded,
        })
    }

    /// Convert to a [`ScanRange`] over the **index-value space** of an index.
    pub(crate) fn to_index_scan_range(
        &self,
        store: &str,
        index: &str,
    ) -> crate::error::Result<ScanRange> {
        let pfx = crate::jsondb::encoding::idx_prefix(store, index);
        let key_start = match &self.lower {
            None => Bound::Included(pfx.clone()),
            Some(val) => {
                let encoded = encode_index_value(val);
                let full = [pfx.as_slice(), &encoded].concat();
                if self.lower_open {
                    Bound::Excluded(full)
                } else {
                    Bound::Included(full)
                }
            }
        };
        let key_end = match &self.upper {
            None => {
                let end = crate::record::increment_prefix_bytes(&pfx);
                Bound::Excluded(end)
            }
            Some(val) => {
                let encoded = encode_index_value(val);
                let full = [pfx.as_slice(), &encoded].concat();
                if self.upper_open {
                    Bound::Excluded(full)
                } else {
                    Bound::Included(full)
                }
            }
        };
        Ok(ScanRange {
            key_start,
            key_end,
            ts_start: Bound::Unbounded,
            ts_end: Bound::Unbounded,
        })
    }
}

/// Compare two JSON values for ordering (type-aware, mirrors encode_index_value ordering).
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    let enc_a = encode_index_value(a);
    let enc_b = encode_index_value(b);
    enc_a.cmp(&enc_b)
}
