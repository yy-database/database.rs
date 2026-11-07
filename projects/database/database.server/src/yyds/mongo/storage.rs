//! `KeyValueExecutor` 的 `MongoStorage` 适配器。
//!
//! 文档落在 `<database>.<collection>` 对应的执行器 namespace，集合选项与索引写成
//! [`mongo::store::collection_record`] 生成的同一份 catalog 记录，放在预留的
//! `!catalog` namespace 里。命令层只看到 `MongoStorage`，所以这里不复制任何命令
//! 语义，只把读写、目录与账目落到执行器原语上。
//!
//! 会话与 TTL 账目是进程内状态。网关不广告逻辑会话，也不广告副本集，因此每个帧
//! 新建一个适配器实例即可，跨帧的事务不属于本网关的能力。

use std::{collections::BTreeMap, sync::Mutex};

use database_execution::cluster::KeyValueExecutor;
use database_query::cluster_kv::{KeyValueAction, KeyValueCommand, KeyValueResult};
use database_types::cluster::Namespace;
use mongo::{
    oplog::OplogEntry,
    storage::{
        CollectionOptions, IndexSpec, InsertReport, MongoStorage, SessionWriteState, StoreError, TransactionAbort, TransactionBegin,
        TransactionCommit, TransactionWrite, TtlMonitorState,
    },
    store::{collection_record, parse_collection_record},
};

const CATALOG_NAMESPACE: &str = "!catalog";

/// 把命令层的数据操作翻译成执行器原语。
pub struct ExecutorStorage<'a> {
    executor: &'a dyn KeyValueExecutor,
    sessions: Mutex<Sessions>,
    ttl: Mutex<TtlMonitorState>,
}

impl<'a> ExecutorStorage<'a> {
    /// 为一次请求建立适配器；调用方保证执行器在本帧内有效。
    pub fn new(executor: &'a dyn KeyValueExecutor) -> Self {
        Self { executor, sessions: Mutex::new(Sessions::default()), ttl: Mutex::new(TtlMonitorState::default()) }
    }

    fn run(&self, namespace: &str, key: Vec<u8>, action: KeyValueAction) -> Result<KeyValueResult, StoreError> {
        self.executor
            .execute(None, KeyValueCommand { namespace: Namespace(namespace.to_string()), key, action })
            .map_err(|error| StoreError::Shard(error.to_string()))
    }

    fn read(&self, namespace: &str, key: &[u8]) -> Option<Vec<u8>> {
        match self.run(namespace, key.to_vec(), KeyValueAction::Get) {
            Ok(KeyValueResult::Get(value)) => value,
            _ => None,
        }
    }

    fn write(&self, namespace: &str, key: Vec<u8>, value: Vec<u8>) -> Result<(), StoreError> {
        self.run(namespace, key, KeyValueAction::Put(value)).map(|_| ())
    }

    fn scan_namespace(&self, namespace: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.executor.execute_scan(None, &Namespace(namespace.to_string()), b"", None, None, 0).unwrap_or_default()
    }

    fn catalog_record(&self, namespace: &str) -> Option<(CollectionOptions, Vec<IndexSpec>)> {
        let bytes = self.read(CATALOG_NAMESPACE, namespace.as_bytes())?;
        let text = String::from_utf8(bytes).ok()?;
        parse_collection_record(namespace, &text).ok()
    }
}

/// 会话、事务暂存与 TTL 睡眠窗口。只描述在途工作，不落盘。
#[derive(Debug, Default)]
struct Sessions {
    transactions: BTreeMap<String, Transaction>,
    newest: BTreeMap<String, i64>,
}

#[derive(Debug, Default)]
struct Transaction {
    number: i64,
    writes: BTreeMap<(String, Vec<u8>), Option<Vec<u8>>>,
    aborted: bool,
}

impl MongoStorage for ExecutorStorage<'_> {
    fn get(&self, namespace: &str, key: &[u8]) -> Option<Vec<u8>> {
        self.read(namespace, key)
    }

    fn insert(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)], ordered: bool) -> Result<InsertReport, StoreError> {
        let mut report = InsertReport { inserted: 0, duplicates: Vec::new(), invalid: Vec::new() };
        for (position, (key, value)) in documents.iter().enumerate() {
            match self.run(namespace, key.clone(), KeyValueAction::Exists) {
                Ok(KeyValueResult::Exists(true)) => {
                    report.duplicates.push(position);
                    if ordered {
                        break;
                    }
                }
                Ok(_) => {
                    self.write(namespace, key.clone(), value.clone())?;
                    report.inserted += 1;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(report)
    }

    fn scan(&self, namespace: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scan_namespace(namespace)
    }

    fn delete(&self, namespace: &str, keys: &[Vec<u8>]) -> Result<usize, StoreError> {
        let mut removed = 0;
        for key in keys {
            if let Ok(KeyValueResult::Delete { removed: true }) = self.run(namespace, key.clone(), KeyValueAction::Delete) {
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn replace(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)]) -> Result<usize, StoreError> {
        for (key, value) in documents {
            self.write(namespace, key.clone(), value.clone())?;
        }
        Ok(documents.len())
    }

    fn import(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)]) -> Result<usize, StoreError> {
        self.replace(namespace, documents)
    }

    fn namespaces(&self) -> Vec<String> {
        let mut namespaces: Vec<String> =
            self.scan_namespace(CATALOG_NAMESPACE).into_iter().filter_map(|(key, _)| String::from_utf8(key).ok()).collect();
        namespaces.sort();
        namespaces
    }

    fn namespace_exists(&self, namespace: &str) -> bool {
        matches!(self.run(CATALOG_NAMESPACE, namespace.as_bytes().to_vec(), KeyValueAction::Exists), Ok(KeyValueResult::Exists(true)))
    }

    fn create_collection(&self, namespace: &str, options: CollectionOptions) -> Result<bool, StoreError> {
        if self.namespace_exists(namespace) {
            return Ok(false);
        }
        self.write(CATALOG_NAMESPACE, namespace.as_bytes().to_vec(), collection_record(namespace, &options, &[]).into_bytes())?;
        Ok(true)
    }

    fn drop_namespace(&self, namespace: &str) -> Result<bool, StoreError> {
        let existing = self.namespace_exists(namespace);
        let keys: Vec<Vec<u8>> = self.scan_namespace(namespace).into_iter().map(|(key, _)| key).collect();
        self.delete(namespace, &keys)?;
        self.run(CATALOG_NAMESPACE, namespace.as_bytes().to_vec(), KeyValueAction::Delete)?;
        Ok(existing)
    }

    fn drop_database(&self, database: &str) -> Result<usize, StoreError> {
        let prefix = format!("{database}.");
        let mut dropped = 0;
        for namespace in self.namespaces().into_iter().filter(|namespace| namespace.starts_with(&prefix)) {
            self.drop_namespace(&namespace)?;
            dropped += 1;
        }
        Ok(dropped)
    }

    fn collection_options(&self, namespace: &str) -> CollectionOptions {
        self.catalog_record(namespace).map(|(options, _)| options).unwrap_or_default()
    }

    fn set_collection_options(&self, namespace: &str, options: CollectionOptions) -> Result<(), StoreError> {
        let indexes = self.index_specs(namespace);
        self.write(CATALOG_NAMESPACE, namespace.as_bytes().to_vec(), collection_record(namespace, &options, &indexes).into_bytes())
    }

    fn index_specs(&self, namespace: &str) -> Vec<IndexSpec> {
        self.catalog_record(namespace).map(|(_, indexes)| indexes).unwrap_or_default()
    }

    fn add_indexes(&self, namespace: &str, indexes: &[IndexSpec]) -> Result<(), StoreError> {
        let options = self.collection_options(namespace);
        let mut existing = self.index_specs(namespace);
        existing.extend(indexes.iter().cloned());
        self.write(CATALOG_NAMESPACE, namespace.as_bytes().to_vec(), collection_record(namespace, &options, &existing).into_bytes())
    }

    fn drop_indexes(&self, namespace: &str, name: &str) -> Result<usize, StoreError> {
        let options = self.collection_options(namespace);
        let existing = self.index_specs(namespace);
        let retained: Vec<IndexSpec> =
            if name == "*" { Vec::new() } else { existing.iter().filter(|index| index.name != name).cloned().collect() };
        let dropped = existing.len() - retained.len();
        self.write(CATALOG_NAMESPACE, namespace.as_bytes().to_vec(), collection_record(namespace, &options, &retained).into_bytes())?;
        Ok(dropped)
    }

    fn ttl_monitor_state(&self) -> TtlMonitorState {
        *self.ttl.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    fn set_ttl_monitor_sleep_seconds(&self, seconds: u32) {
        let mut state = self.ttl.lock().unwrap_or_else(|poison| poison.into_inner());
        state.sleep_seconds = seconds.max(1);
    }

    fn claim_ttl_pass(&self, now_ms: i64) -> bool {
        let mut state = self.ttl.lock().unwrap_or_else(|poison| poison.into_inner());
        let due = state.last_scan_ms.map_or(true, |last| now_ms - last >= i64::from(state.sleep_seconds) * 1_000);
        if due {
            state.last_scan_ms = Some(now_ms);
        }
        due
    }

    fn enable_oplog(&self, _namespace: &str, _max_bytes: i64) -> Result<(), StoreError> {
        Ok(())
    }

    fn oplog_enabled(&self) -> bool {
        false
    }

    fn oplog_namespace(&self) -> Option<String> {
        None
    }

    fn oplog_status(&self) -> Option<(String, i64, i64, i64)> {
        None
    }

    fn stage_oplog(&self, _entry: OplogEntry) {}

    fn discard_staged_oplog(&self) {}

    fn commit_oplog(&self, _wall_millis: i64) -> Result<Option<(u32, u32)>, StoreError> {
        Ok(None)
    }

    fn begin_transaction(&self, session: &str, txn_number: i64) -> TransactionBegin {
        let mut sessions = self.sessions.lock().unwrap_or_else(|poison| poison.into_inner());
        if let Some(newest) = sessions.newest.get(session) {
            if *newest == txn_number {
                return TransactionBegin::AlreadyStarted;
            }
            if *newest > txn_number {
                return TransactionBegin::TooOld { newer: *newest };
            }
        }
        sessions.newest.insert(session.to_string(), txn_number);
        sessions.transactions.insert(session.to_string(), Transaction { number: txn_number, ..Transaction::default() });
        TransactionBegin::Started
    }

    fn session_write_state(&self, session: &str, txn_number: i64) -> SessionWriteState {
        let sessions = self.sessions.lock().unwrap_or_else(|poison| poison.into_inner());
        match sessions.transactions.get(session) {
            Some(transaction) if transaction.number == txn_number && !transaction.aborted => SessionWriteState::Active,
            _ => SessionWriteState::NoTransaction { active: sessions.newest.get(session).copied().unwrap_or(0) },
        }
    }

    fn transaction_active(&self, session: &str) -> bool {
        let sessions = self.sessions.lock().unwrap_or_else(|poison| poison.into_inner());
        sessions.transactions.get(session).is_some_and(|transaction| !transaction.aborted)
    }

    fn transaction_write(&self, session: &str, namespace: &str, key: &[u8], value: Option<Vec<u8>>) -> TransactionWrite {
        let mut sessions = self.sessions.lock().unwrap_or_else(|poison| poison.into_inner());
        let Some(transaction) = sessions.transactions.get_mut(session)
        else {
            return TransactionWrite::Conflict;
        };
        transaction.writes.insert((namespace.to_string(), key.to_vec()), value);
        TransactionWrite::Staged
    }

    fn transaction_scan(&self, session: &str, namespace: &str) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let sessions = self.sessions.lock().unwrap_or_else(|poison| poison.into_inner());
        let transaction = sessions.transactions.get(session)?;
        let mut documents: BTreeMap<Vec<u8>, Vec<u8>> = self.scan_namespace(namespace).into_iter().collect();
        for ((target, key), value) in &transaction.writes {
            if target != namespace {
                continue;
            }
            match value {
                Some(value) => {
                    documents.insert(key.clone(), value.clone());
                }
                None => {
                    documents.remove(key);
                }
            }
        }
        Some(documents.into_iter().collect())
    }

    fn transaction_get(&self, session: &str, namespace: &str, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let sessions = self.sessions.lock().unwrap_or_else(|poison| poison.into_inner());
        let transaction = sessions.transactions.get(session)?;
        Some(transaction.writes.get(&(namespace.to_string(), key.to_vec())).cloned().unwrap_or_else(|| self.read(namespace, key)))
    }

    fn transaction_commit(&self, session: &str, txn_number: i64) -> TransactionCommit {
        let mut sessions = self.sessions.lock().unwrap_or_else(|poison| poison.into_inner());
        let Some(transaction) = sessions.transactions.remove(session)
        else {
            return TransactionCommit::NoTransaction { active: sessions.newest.get(session).copied().unwrap_or(0) };
        };
        if transaction.number != txn_number {
            return TransactionCommit::NoTransaction { active: transaction.number };
        }
        if transaction.aborted {
            return TransactionCommit::AlreadyAborted;
        }
        for ((namespace, key), value) in transaction.writes {
            let result = match value {
                Some(value) => self.write(&namespace, key, value),
                None => self.run(&namespace, key, KeyValueAction::Delete).map(|_| ()),
            };
            if result.is_err() {
                return TransactionCommit::Conflict;
            }
        }
        TransactionCommit::Committed
    }

    fn transaction_abort(&self, session: &str, txn_number: i64) -> TransactionAbort {
        let mut sessions = self.sessions.lock().unwrap_or_else(|poison| poison.into_inner());
        let Some(transaction) = sessions.transactions.get_mut(session)
        else {
            return TransactionAbort::NoTransaction { active: sessions.newest.get(session).copied().unwrap_or(0) };
        };
        if transaction.number != txn_number {
            return TransactionAbort::NoTransaction { active: transaction.number };
        }
        if transaction.aborted {
            return TransactionAbort::AlreadyAborted;
        }
        transaction.aborted = true;
        transaction.writes.clear();
        TransactionAbort::Aborted
    }
}
