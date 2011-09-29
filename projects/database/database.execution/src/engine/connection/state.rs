use std::collections::{BTreeMap, BTreeSet};

use crate::engine::{
    Result,
    journal::JournalMode,
    kv::{lease, retention as ttl},
    objects::EMBEDDED_OBJECT_PREFIX,
    storage::snapshot,
};
use database_storage::local::snapshot::RecordWindow;

use super::{Backend, Connection, State, TxnJournal, is_yydx_main_path};

impl Connection {
    /// Reconcile expired leases without materializing the whole record tree.
    ///
    /// Opening a database does not need every row. Leases live under one reserved
    /// key prefix, so only that window is read and the open cost stays
    /// proportional to the lease count instead of the database size. The rare
    /// case where a lease actually expired falls back to a full read, because
    /// publication replaces the durable image wholesale.
    pub(crate) fn reconcile_leases_on_open(&self) -> Result<()> {
        let _guard = self.operation_lock.lock().unwrap_or_else(|p| p.into_inner());
        let metadata = self.read_state_metadata()?;
        let window =
            database_query::local::ReadWindow { start: lease::LEASE_PREFIX.to_owned(), end: Some(format!("{}0", lease::LEASE_PREFIX)) };
        let mut leases = self.read_state_window_with_metadata(&window, metadata)?;
        if !lease::reconcile_expired_leases(&mut leases.records)? {
            return Ok(());
        }
        let mut state = self.read_state()?;
        lease::reconcile_expired_leases(&mut state.records)?;
        self.replace_active_state_unlocked(state)
    }

    pub(crate) fn active_state_unlocked(&self) -> Result<State> {
        if let Some(state) = self.txn.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).as_ref() {
            Ok(state.clone())
        }
        else {
            self.read_state()
        }
    }

    /// Read one record without materializing the record tree.
    ///
    /// A point read reaches the leaf that holds `key` and the reserved row that
    /// carries its expiry deadline, so the cost is two root-to-leaf paths instead
    /// of one map entry per record in the image. Reserved `__yydb/meta/` rows are
    /// not records and read as absent, like [`Self::read_state`].
    pub(crate) fn read_record_unlocked(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self.backend.as_ref() {
            Backend::File { database, .. } => {
                let mut database = database.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                let deadline = database.get_record(&ttl::ttl_meta_key(key))?;
                if ttl::is_expired_bytes(deadline.as_deref()) {
                    return Ok(None);
                }
                if self.is_single_file_yydb() && key.starts_with(EMBEDDED_OBJECT_PREFIX) {
                    return Ok(None);
                }
                database.get_record(key)
            }
            Backend::Memory { state } => {
                let state = state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                if ttl::is_expired(&state.records, key) {
                    return Ok(None);
                }
                Ok(state.records.get(key).cloned())
            }
        }
    }

    /// Read at most `limit` records under `prefix` after `after` without
    /// materializing the record tree.
    ///
    /// The walk is bounded by the prefix window and by `limit`, so a first page
    /// costs one root-to-leaf path plus the leaves it covers. `after` keeps the
    /// exclusive continuation contract of the whole-state scan.
    pub(crate) fn read_prefix_unlocked(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<Vec<(String, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        match self.backend.as_ref() {
            Backend::File { database, .. } => {
                let start = match after {
                    Some(cursor) if cursor >= prefix => cursor.to_owned(),
                    _ => prefix.to_owned(),
                };
                let end = Self::prefix_upper_bound(prefix);
                if end.as_ref().is_some_and(|end| start >= *end) {
                    return Ok(Vec::new());
                }
                // One extra row covers the cursor itself, which the walk sees
                // because tree ranges are inclusive at the lower bound. Rows the
                // whole-state scan does not expose are skipped here too, so the
                // walk is widened until `limit` records remain.
                let mut want = limit.saturating_add(1);
                let whole_table = prefix.is_empty();
                loop {
                    let mut rows =
                        database.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).scan_records(&start, end.as_deref(), want)?;
                    let fetched = rows.len();
                    rows.retain(|(key, _)| {
                        key.starts_with(prefix)
                            && after.is_none_or(|cursor| key.as_str() > cursor)
                            && !(self.is_single_file_yydb() && key.starts_with(EMBEDDED_OBJECT_PREFIX))
                            && !(whole_table && ttl::is_reserved_key(key))
                    });
                    if rows.len() >= limit || fetched < want {
                        rows.truncate(limit);
                        return Ok(rows);
                    }
                    want = want.saturating_mul(2);
                }
            }
            Backend::Memory { .. } => {
                let state = self.active_state_unlocked()?;
                let whole_table = prefix.is_empty();
                Ok(state
                    .records
                    .range(prefix.to_owned()..)
                    .take_while(|(key, _)| key.starts_with(prefix))
                    .filter(|(key, _)| after.is_none_or(|cursor| key.as_str() > cursor))
                    .filter(|(key, _)| !(whole_table && ttl::is_reserved_key(key)))
                    .take(limit)
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect())
            }
        }
    }

    /// Smallest key above every key starting with `prefix`.
    fn prefix_upper_bound(prefix: &str) -> Option<String> {
        let chars: Vec<char> = prefix.chars().collect();
        for index in (0..chars.len()).rev() {
            if let Some(next) = char::from_u32(u32::from(chars[index]) + 1) {
                let mut end: String = chars[..index].iter().collect();
                end.push(next);
                return Some(end);
            }
        }
        None
    }

    /// Whether a transaction snapshot is currently open.
    pub(crate) fn has_open_transaction(&self) -> bool {
        self.txn.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).is_some()
    }

    /// Discard the journal so the next transaction starts with an empty key set.
    pub(crate) fn reset_transaction_journal(&self) {
        let mut journal = self.txn_journal.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        journal.keys.clear();
        journal.prior.clear();
        journal.wholesale = false;
    }

    /// Take the journal of the transaction that is ending.
    pub(crate) fn take_transaction_journal(&self) -> TxnJournal {
        let mut journal = self.txn_journal.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        std::mem::take(&mut *journal)
    }

    /// Record that this transaction's change set cannot be published by key.
    pub(crate) fn mark_transaction_wholesale_publish(&self) {
        let mut journal = self.txn_journal.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        journal.wholesale = true;
    }

    pub(crate) fn replace_active_state_unlocked(&self, state: State) -> Result<()> {
        let mut slot = self.txn.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if slot.is_some() {
            *slot = Some(state);
            self.mark_transaction_wholesale_publish();
            Ok(())
        }
        else {
            drop(slot);
            self.write_state(&state)?;
            Ok(())
        }
    }

    pub(crate) fn mutate_active_state_unlocked<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&mut State) -> Result<()>,
    {
        let mut state = self.active_state_unlocked()?;
        f(&mut state)?;
        self.replace_active_state_unlocked(state)
    }

    /// Read reserved metadata only and decode the schema and contract without
    /// materializing a single table row.
    ///
    /// Reads use this as the first phase: the decoded contract tells the query
    /// layer which record-key window one statement can actually reach.
    pub(crate) fn read_state_metadata(&self) -> Result<State> {
        match self.backend.as_ref() {
            Backend::File { database, .. } => {
                let mut database = database.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                snapshot::state_from_snapshot(&database.snapshot_windows(&[])?)
            }
            Backend::Memory { state } => Ok(state.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()),
        }
    }

    /// Read metadata plus the rows inside `window`, reusing already-decoded
    /// metadata instead of decoding the schema and contract a second time.
    ///
    /// The window is a **superset** of what the statement may read: execution
    /// still applies every filter, so an over-wide window can only cost extra
    /// reads and never change the result. Single-file `.yydb` images also pull
    /// the embedded object prefix, because
    /// [`Self::read_state`] would otherwise hand the resolver an incomplete
    /// object catalog.
    pub(crate) fn read_state_window_with_metadata(&self, window: &database_query::local::ReadWindow, metadata: State) -> Result<State> {
        match self.backend.as_ref() {
            Backend::File { database, .. } => {
                let mut windows = vec![RecordWindow { start: window.start.clone(), end: window.end.clone() }];
                if self.is_single_file_yydb() {
                    windows.push(RecordWindow { start: EMBEDDED_OBJECT_PREFIX.to_owned(), end: Some(format!("{EMBEDDED_OBJECT_PREFIX}g")) });
                }
                let snapshot = {
                    let mut database = database.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    database.snapshot_windows(&windows)?
                };
                let mut state = snapshot::state_with_window(metadata, snapshot)?;
                if self.is_single_file_yydb() {
                    self.objects.take_embedded_records(&mut state.records)?;
                }
                Ok(state)
            }
            Backend::Memory { .. } => Ok(metadata),
        }
    }

    /// Read metadata plus a borrowed view of the rows inside `window`.
    ///
    /// Same window contract as [`Self::read_state_window_with_metadata`]: the
    /// window is a superset of every row the statement can read, and execution
    /// still filters inside it. The difference is that table rows stay in the
    /// decoded page images instead of being copied into a record map, so a
    /// window read pays one page handle per leaf instead of one map entry per
    /// row. Single-file `.yydb` images still pull the embedded object prefix into
    /// `state.records`, because the resolver and publication consume an owned
    /// object catalog.
    ///
    /// Returns no window for a memory-backed connection: that backend already
    /// keeps every row in `state.records`, so it needs no borrowed view.
    pub(crate) fn read_state_window_rows(
        &self,
        window: &database_query::local::ReadWindow,
        metadata: State,
    ) -> Result<(State, Option<database_storage::local::RowWindow>)> {
        match self.backend.as_ref() {
            Backend::File { database, .. } => {
                let mut windows = vec![RecordWindow { start: window.start.clone(), end: window.end.clone() }];
                if self.is_single_file_yydb() {
                    windows.push(RecordWindow { start: EMBEDDED_OBJECT_PREFIX.to_owned(), end: Some(format!("{EMBEDDED_OBJECT_PREFIX}g")) });
                }
                let (snapshot, rows) = {
                    let mut database = database.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    database.snapshot_windows_with_rows(&windows)?
                };
                let mut state = snapshot::state_with_window(metadata, snapshot)?;
                if self.is_single_file_yydb() {
                    for row in rows.iter() {
                        let row = row?;
                        if row.key.starts_with(EMBEDDED_OBJECT_PREFIX) {
                            state.records.insert(row.key.to_owned(), row.bytes.to_vec());
                        }
                    }
                    self.objects.take_embedded_records(&mut state.records)?;
                }
                Ok((state, Some(rows)))
            }
            // Memory-backed connections already hold every row in `metadata`.
            Backend::Memory { .. } => Ok((metadata, None)),
        }
    }

    /// Read reserved metadata plus exactly the rows inside `windows`, together
    /// with what the image held for them before the statement ran.
    ///
    /// A single-key window is `[key, key + NUL)`, so the record tree descends to
    /// that leaf per key instead of walking every row. A single-file image also
    /// pulls the embedded object prefix, because publication has to know which
    /// object records are already durable.
    fn read_state_windows(&self, windows: &[database_query::local::ReadWindow]) -> Result<(State, snapshot::PriorRecords)> {
        let metadata = self.read_state_metadata()?;
        self.read_state_windows_with_metadata(windows, metadata)
    }

    /// Read metadata plus exactly the rows inside `windows`, reusing
    /// already-decoded metadata.
    fn read_state_windows_with_metadata(
        &self,
        windows: &[database_query::local::ReadWindow],
        metadata: State,
    ) -> Result<(State, snapshot::PriorRecords)> {
        match self.backend.as_ref() {
            Backend::File { database, .. } => {
                let mut record_windows: Vec<RecordWindow> =
                    windows.iter().map(|window| RecordWindow { start: window.start.clone(), end: window.end.clone() }).collect();
                if self.is_single_file_yydb() {
                    record_windows
                        .push(RecordWindow { start: EMBEDDED_OBJECT_PREFIX.to_owned(), end: Some(format!("{EMBEDDED_OBJECT_PREFIX}g")) });
                }
                let snapshot = {
                    let mut database = database.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    database.snapshot_windows(&record_windows)?
                };
                let mut state = snapshot::state_with_window(metadata, snapshot)?;
                let prior = snapshot::prior_records(&state.records);
                if self.is_single_file_yydb() {
                    self.objects.take_embedded_records(&mut state.records)?;
                }
                Ok((state, prior))
            }
            Backend::Memory { state } => {
                let state = state.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
                let prior = snapshot::prior_records(&state.records);
                Ok((state, prior))
            }
        }
    }

    pub(crate) fn read_state(&self) -> Result<State> {
        let mut state = match self.backend.as_ref() {
            Backend::File { database, .. } => {
                let mut database = database.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                snapshot::state_from_snapshot(&database.snapshot()?)?
            }
            Backend::Memory { state } => state.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone(),
        };
        if self.is_single_file_yydb() {
            self.objects.take_embedded_records(&mut state.records)?;
        }
        Ok(state)
    }

    /// Publish a state snapshot, advancing the durable commit sequence by one.
    ///
    /// This is the single publish point for every durable state change, so the
    /// `__yydb/meta/commit_sequence` record doubles as the transaction fencing
    /// base: `begin` captures it and `commit` refuses to publish once it moved.
    pub(crate) fn write_state(&self, state: &State) -> Result<database_types::CommitSequence> {
        let mut state = state.clone();
        if self.is_single_file_yydb() {
            self.objects.embed_into(&mut state.records)?;
        }
        let sequence = lease::bump_commit_sequence(&mut state.records);
        match self.backend.as_ref() {
            Backend::File { database, .. } => {
                let blob_objects = if self.path().is_some_and(is_yydx_main_path) && self.journal_mode() == JournalMode::Wal {
                    Some(self.objects())
                }
                else {
                    None
                };
                let mut database = database.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                let next = snapshot::snapshot_from_state(&state)?;
                let blob_refs = match blob_objects {
                    Some(objects) => {
                        let before = database.snapshot()?.records;
                        snapshot::blob_deltas(&before, &next.records, objects)?
                    }
                    None => Vec::new(),
                };
                database.commit(&next, &blob_refs)?;
            }
            Backend::Memory { state: slot } => {
                *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = state;
            }
        }
        Ok(sequence)
    }

    /// Publish only the records one narrowed write changed.
    ///
    /// `prior` holds what the image contained for the keys the statement could
    /// reach, so the published delta is the exact change set and publication
    /// never scans the record tree to discover what changed.
    fn write_state_delta(&self, mut state: State, prior: &snapshot::PriorRecords) -> Result<database_types::CommitSequence> {
        if self.is_single_file_yydb() {
            self.objects.embed_into(&mut state.records)?;
        }
        let sequence = lease::bump_commit_sequence(&mut state.records);
        let next = snapshot::snapshot_from_state(&state)?;
        let delta = snapshot::record_delta(&next.records, prior);
        match self.backend.as_ref() {
            Backend::File { database, .. } => {
                let mut database = database.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                database.commit_delta(&next.metadata, &delta, &[])?;
            }
            Backend::Memory { state: slot } => {
                *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = state;
            }
        }
        Ok(sequence)
    }

    /// Apply one write and publish only its records when the statement names
    /// the rows it can touch.
    ///
    /// `plan` receives the active state and returns the record windows the
    /// statement can reach, or `None` when that cannot be proven. An open
    /// transaction applies the statement in place inside those windows instead
    /// of cloning the whole transaction state per write. Everything unprovable,
    /// the in-memory backend and `.yydx` images keep the full publish path.
    pub(crate) fn mutate_narrowed_write_unlocked<P, F>(&self, plan: P, f: F) -> Result<()>
    where
        P: FnOnce(&State) -> Result<Option<Vec<database_query::local::ReadWindow>>>,
        F: FnOnce(&mut State) -> Result<()>,
    {
        if self.publishes_everything() || !self.is_single_file_yydb() {
            return self.mutate_active_state_unlocked(f);
        }
        if self.has_open_transaction() {
            let windows = {
                let slot = self.txn.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                match slot.as_ref() {
                    Some(state) => plan(state)?,
                    None => None,
                }
            };
            return match windows {
                Some(windows) => self.mutate_transaction_windows(&windows, f),
                None => self.mutate_active_state_unlocked(f),
            };
        }
        let metadata = self.read_state_metadata()?;
        let Some(windows) = plan(&metadata)?
        else {
            return self.mutate_active_state_unlocked(f);
        };
        let (mut state, prior) = self.read_state_windows_with_metadata(&windows, metadata)?;
        f(&mut state)?;
        self.write_state_delta(state, &prior)?;
        Ok(())
    }

    /// Apply `f` to the open transaction state in place and undo it inside
    /// `windows` if `f` fails.
    ///
    /// The rows inside the windows are copied first, so a failing statement
    /// leaves the transaction exactly as it was without cloning the whole
    /// record map per write. The window copy is the only per-statement state
    /// cost, and it is bounded by what the statement can reach.
    fn mutate_transaction_windows<F>(&self, windows: &[database_query::local::ReadWindow], f: F) -> Result<()>
    where
        F: FnOnce(&mut State) -> Result<()>,
    {
        let mut slot = self.txn.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(state) = slot.as_mut()
        else {
            drop(slot);
            return self.mutate_active_state_unlocked(f);
        };
        let prior = window_rows(&state.records, windows);
        match f(&mut *state) {
            Ok(()) => {
                // The published delta is the symmetric difference of the window
                // before and after this statement. Keys that disappeared must
                // stay named, so both sides are contributed, and only the first
                // pre-image of a key is kept because later statements must diff
                // against what the transaction started from.
                let mut journal = self.txn_journal.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                for (key, value) in prior {
                    if journal.keys.insert(key.clone()) {
                        journal.prior.insert(key, value);
                    }
                }
                for (key, _) in window_rows(&state.records, windows) {
                    journal.keys.insert(key);
                }
                Ok(())
            }
            Err(error) => {
                restore_window_rows(&mut state.records, windows, prior);
                Err(error)
            }
        }
    }

    /// Publish only the records an open transaction touched.
    ///
    /// The journal already holds the first pre-image of every touched key, so
    /// the user-row delta is a pure in-memory diff against it. The durable
    /// image is read only for the embedded object prefix, which tells publication
    /// which content-addressed object rows are already there. Metadata rows are
    /// rewritten as a group, like every other publication.
    pub(crate) fn write_transaction_delta(&self, mut state: State, journal: &TxnJournal) -> Result<database_types::CommitSequence> {
        self.objects.embed_into(&mut state.records)?;
        let sequence = lease::bump_commit_sequence(&mut state.records);
        let metadata = snapshot::metadata_from_state(&state)?;
        let (_, prior) = self.read_state_windows(&[])?;
        let mut delta = database_storage::local::snapshot::RecordDelta::default();
        for key in &journal.keys {
            match (state.records.get(key), journal.prior.get(key)) {
                (Some(value), Some(old)) if value == old => {}
                (Some(value), _) => delta.puts.push((key.clone(), value.clone())),
                (None, Some(_)) => delta.deletes.push(key.clone()),
                (None, None) => {}
            }
        }
        // Embedded object rows are content addressed, so an absent key means an
        // object the image does not carry yet and an existing key is unchanged.
        for (key, value) in &state.records {
            if key.starts_with(EMBEDDED_OBJECT_PREFIX) && !prior.objects.contains(key) {
                delta.puts.push((key.clone(), value.clone()));
            }
        }
        match self.backend.as_ref() {
            Backend::File { database, .. } => {
                let mut database = database.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                database.commit_delta(&metadata, &delta, &[])?;
            }
            Backend::Memory { state: slot } => {
                *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = state;
            }
        }
        Ok(sequence)
    }
}

/// Borrowed table rows handed to a narrowed read.
///
/// A window read keeps row bytes in the decoded leaf pages, so the query layer
/// receives references that live exactly as long as the window does. Every row
/// is decoded into an owned `QueryRow` inside the operator, so no borrow crosses
/// this boundary.
pub(crate) struct WindowRows<'a>(pub(crate) &'a database_storage::local::RowWindow);

impl database_query::local::RowSource for WindowRows<'_> {
    fn rows_with_prefix<'a>(&'a self, prefix: &str) -> Box<dyn Iterator<Item = Result<database_query::local::BorrowedRow<'a>>> + 'a> {
        let prefix = prefix.to_owned();
        Box::new(self.0.iter().filter_map(move |row| match row {
            Ok(row) if row.key.starts_with(&prefix) => Some(Ok(database_query::local::BorrowedRow { key: row.key, bytes: row.bytes })),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        }))
    }
}

/// Rows inside `windows`, copied so a failing statement can restore them.
fn window_rows(records: &BTreeMap<String, Vec<u8>>, windows: &[database_query::local::ReadWindow]) -> Vec<(String, Vec<u8>)> {
    let mut rows = Vec::new();
    for window in windows {
        let collected = match window.end.as_ref() {
            Some(end) => records.range(window.start.clone()..end.clone()),
            None => records.range(window.start.clone()..),
        };
        rows.extend(collected.map(|(key, value)| (key.clone(), value.clone())));
    }
    rows
}

/// Restore `records` inside `windows` to exactly `prior`.
fn restore_window_rows(records: &mut BTreeMap<String, Vec<u8>>, windows: &[database_query::local::ReadWindow], prior: Vec<(String, Vec<u8>)>) {
    let keep: BTreeSet<&str> = prior.iter().map(|(key, _)| key.as_str()).collect();
    for (key, _) in window_rows(records, windows) {
        if !keep.contains(key.as_str()) {
            records.remove(&key);
        }
    }
    for (key, value) in prior {
        records.insert(key, value);
    }
}
