use crate::engine::{EvictBudget, EvictReport, NamespaceQuota, NamespaceStats, RecordVersion, Result, kv::retention as ttl};

use super::{Connection, State};

impl Connection {
    /// Insert or replace a raw byte record keyed by `key`.
    ///
    /// Inline row storage is **allowed** but **not recommended** for large
    /// payloads — prefer [`Self::put_chunk`] / [`Self::put_file_chunked`] into
    /// the unified `objects/` CAS (see `INLINE_BYTES_MAX`).
    pub fn put(&self, key: impl Into<String>, value: impl AsRef<[u8]>) -> Result<()> {
        let _guard = self.operation_lock.lock().unwrap_or_else(|p| p.into_inner());
        self.mutate_active_state_unlocked(|state| ttl::put_record(&mut state.records, &key.into(), value.as_ref()))
    }

    /// Store a record that expires after `ttl`.
    pub fn put_with_ttl(&self, key: impl Into<String>, value: impl AsRef<[u8]>, ttl: std::time::Duration) -> Result<RecordVersion> {
        let _guard = self.operation_lock.lock().unwrap_or_else(|p| p.into_inner());
        let mut state = self.active_state_unlocked()?;
        let version = ttl::put_with_ttl(&mut state.records, &key.into(), value.as_ref(), ttl.as_millis() as u64)?;
        self.replace_active_state_unlocked(state)?;
        Ok(version)
    }

    /// Configure quota limits for a namespace prefix such as `cache/response/`.
    pub fn set_namespace_quota(&self, namespace: &str, quota: NamespaceQuota) -> Result<()> {
        let _guard = self.operation_lock.lock().unwrap_or_else(|p| p.into_inner());
        self.mutate_active_state_unlocked(|state| {
            ttl::set_namespace_quota(&mut state.records, namespace, quota);
            Ok(())
        })
    }

    /// Return byte and record usage for a namespace prefix.
    pub fn namespace_stats(&self, namespace: &str) -> Result<NamespaceStats> {
        let _guard = self.operation_lock.lock().unwrap_or_else(|p| p.into_inner());
        Ok(ttl::namespace_stats(&self.active_state_unlocked()?.records, namespace))
    }

    /// Remove expired TTL records under `namespace_prefix` up to `budget`.
    pub fn evict_expired(&self, namespace_prefix: &str, budget: EvictBudget) -> Result<EvictReport> {
        let _guard = self.operation_lock.lock().unwrap_or_else(|p| p.into_inner());
        let mut state = self.active_state_unlocked()?;
        let report = ttl::evict_expired(&mut state.records, namespace_prefix, budget)?;
        self.replace_active_state_unlocked(state)?;
        Ok(report)
    }

    /// Fetch a raw byte record by key.
    ///
    /// Without an open transaction this reaches the two leaves that hold the
    /// record and its expiry deadline instead of materializing every record in
    /// the image, so a point read stays flat as the database grows. An open
    /// transaction already owns an in-memory snapshot and reads from it.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let _guard = self.operation_lock.lock().unwrap_or_else(|p| p.into_inner());
        if !self.has_open_transaction() {
            return self.read_record_unlocked(key);
        }
        let state = self.active_state_unlocked()?;
        if ttl::is_expired(&state.records, key) {
            return Ok(None);
        }
        Ok(state.records.get(key).cloned())
    }

    /// Commit a batch in one journal snapshot. `None` deletes the key.
    ///
    /// The batch names every row it writes, so an open transaction applies it in
    /// place inside those keys and the commit publishes only the journal delta.
    /// The whole-state rebuild is kept for images the narrowed path cannot prove,
    /// which [`Self::mutate_narrowed_write_unlocked`] decides.
    /// Operations are serialized on this connection, not across separate handles or processes.
    pub fn write_batch(&self, changes: &[(String, Option<Vec<u8>>)]) -> Result<()> {
        let _guard = self.operation_lock.lock().unwrap_or_else(|p| p.into_inner());
        let plan = |_: &State| {
            let windows = changes
                .iter()
                .map(|(key, _)| database_query::local::ReadWindow { start: key.clone(), end: Some(format!("{key}\0")) })
                .collect();
            Ok(Some(windows))
        };
        self.mutate_narrowed_write_unlocked(plan, |state| {
            for (key, value) in changes {
                match value {
                    Some(bytes) => {
                        state.records.insert(key.clone(), bytes.clone());
                    }
                    None => {
                        state.records.remove(key);
                    }
                }
            }
            Ok(())
        })
    }

    /// Replace or delete a key only if its current bytes match `expected`.
    /// `None` as expected requires an absent key. Atomic on a shared connection.
    pub fn compare_exchange(&self, key: &str, expected: Option<&[u8]>, replacement: Option<&[u8]>) -> Result<bool> {
        let _guard = self.operation_lock.lock().unwrap_or_else(|p| p.into_inner());
        let mut state = self.active_state_unlocked()?;
        if state.records.get(key).map(Vec::as_slice) != expected {
            return Ok(false);
        }
        match replacement {
            Some(bytes) => {
                state.records.insert(key.to_owned(), bytes.to_vec());
            }
            None => {
                state.records.remove(key);
            }
        }
        self.replace_active_state_unlocked(state)?;
        Ok(true)
    }

    /// Scan keys in lexical order with an exclusive continuation key and a result limit.
    ///
    /// The walk is bounded by the prefix and by `limit`, so the first page costs
    /// one root-to-leaf path plus the leaves it covers instead of one map entry
    /// per record in the image.
    pub fn scan_prefix(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<Vec<(String, Vec<u8>)>> {
        let _guard = self.operation_lock.lock().unwrap_or_else(|p| p.into_inner());
        if !self.has_open_transaction() {
            return self.read_prefix_unlocked(prefix, after, limit);
        }
        let state = self.active_state_unlocked()?;
        let whole_table = prefix.is_empty();
        Ok(state
            .records
            .range(prefix.to_owned()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .filter(|(key, _)| after.is_none_or(|cursor| key.as_str() > cursor))
            .filter(|(key, _)| !(whole_table && crate::engine::kv::retention::is_reserved_key(key)))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}
