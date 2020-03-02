//! Mongo namespace 的 `.yyds` catalog + `.yykv` shard 后端。
//!
//! `.yyds` catalog 是产品级登记簿：它登记承载数据的唯一 `.yykv` shard，并在
//! schema 槽里记录每个 `<database>.<collection>` namespace。字节布局沿用共享的
//! `yyds-formats` 合同（magic `YYDS\x01`、小端字段、可选的
//! schema/routing/contract/owners 段、一个 `ShardId`）。
//!
//! shard 里保留一份 `!catalog` 旧镜像记录 namespace 标签，只在 catalog 缺失时用来
//! 播种，并在每次 `open` 时并回 catalog，因此删掉 `.yyds` 不会让已有集合消失。
//!
//! 写入顺序是先 shard 后 catalog，catalog 通过同步临时文件加 rename 原子替换。两次
//! 写入之间崩溃只会留下 `.yykv`，下一次 `open` 用 shard 镜像播种 catalog 完成自愈。
//! 反向残留（只有 catalog 没有 shard）本 crate 不会产生，`open` 会结构性拒绝，只能
//! 通过显式的 `repair_shard` 入口修复。

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

use database_cluster::catalog;
use database_protocol_mongo::{
    bson::{BsonDocument, BsonValue, encode_bson, parse_bson, value_to_key},
    json,
};
use database_storage::cluster_kv::{FileShard, InlineValue, Key, KvStore, ShardChange, StoredValue};
use database_types::cluster::{CATALOG_SUFFIX, CatalogSchema, Namespace, ShardId};

use crate::oplog::OplogEntry;

/// One user-created index. `_id_` is implicit and is never stored here.
#[derive(Debug, Clone)]
pub struct IndexSpec {
    /// Namespace the index belongs to.
    pub namespace: String,
    /// Index name drivers and `listIndexes` report.
    pub name: String,
    /// Ordered key specification, exactly as it was sent.
    pub key: BsonDocument,
    /// Whether the index rejects duplicate keys.
    pub unique: bool,
    /// `sparse` exactly as requested. `mongod 8.0.9` reports an explicit
    /// `sparse: false` in `listIndexes`, so the requested value is kept.
    pub sparse: Option<bool>,
    /// `hidden: true` keeps the index out of the planner but in the catalog.
    pub hidden: bool,
    /// TTL expiry in seconds, applied to single-field date values.
    pub expire_after_seconds: Option<i32>,
    /// `collation` normalized the way `listIndexes` reports it.
    pub collation: Option<BsonDocument>,
    /// `partialFilterExpression`: only matching documents join the index.
    pub partial_filter_expression: Option<BsonDocument>,
    /// `wildcardProjection` of a wildcard index, as `listIndexes` reports it.
    pub wildcard_projection: Option<BsonDocument>,
    /// 索引字段出现过数组值。两侧范围谓词在 multikey 上不能按同一元素取区间。
    pub multikey: bool,
}

impl IndexSpec {
    /// Effective `sparse` flag.
    pub fn is_sparse(&self) -> bool {
        self.sparse.unwrap_or(false)
    }

    /// Whether the index is usable by the planner (`hidden` is not).
    pub fn is_visible(&self) -> bool {
        !self.hidden
    }
}

/// Collection-level options kept in the catalog. Only non-default values are
/// stored, so collections without validators keep the original catalog shape.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CollectionOptions {
    /// `$jsonSchema` (or other validator) exactly as it was sent.
    pub validator: Option<serde_json::Value>,
    /// `strict` (default), `moderate`, or `off`.
    pub validation_level: Option<String>,
    /// `error` (default) or `warn`.
    pub validation_action: Option<String>,
}

/// Default `ttlMonitorSleepSecs`, matching `mongod 8.0.9`.
pub const DEFAULT_TTL_MONITOR_SLEEP_SECONDS: u32 = 60;

/// TTL monitor state shared by every command call.
///
/// `expireAfterSeconds` deletion is periodic in `mongod`: the monitor wakes up
/// every `sleep_seconds` and removes whatever expired since the previous
/// wake-up. Any expired document therefore stays visible for at most one
/// window before it disappears, never at its exact deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TtlMonitorState {
    /// Wake-up interval, `ttlMonitorSleepSecs`.
    pub sleep_seconds: u32,
    /// Clock reading of the last pass, `None` until the first pass.
    pub last_scan_ms: Option<i64>,
}

impl Default for TtlMonitorState {
    fn default() -> Self {
        Self { sleep_seconds: DEFAULT_TTL_MONITOR_SLEEP_SECONDS, last_scan_ms: None }
    }
}

/// Namespace label used to remember which Mongo namespaces exist.
///
/// Mongo collection names cannot contain `$` or a NUL byte and a database name
/// cannot be `!catalog`, so this label never collides with user data.
const CATALOG_NAMESPACE: &str = "!catalog";

/// 索引项命名空间标记。
///
/// 用户可见的库名与集合名都不能包含 `$`，所以 `<collection>$idx$<index>`
/// 不会与用户集合冲突。索引项与文档共用同一个 shard，只是命名空间不进入目录。
pub const INDEX_NAMESPACE_MARKER: &str = "$idx$";

/// 该 namespace 是否属于索引项保留命名空间。
pub fn is_index_namespace(namespace: &str) -> bool {
    namespace.contains(INDEX_NAMESPACE_MARKER)
}

/// 由一个集合 namespace 与索引名拼出索引项 namespace。
pub fn index_namespace(namespace: &str, index: &str) -> String {
    format!("{namespace}{INDEX_NAMESPACE_MARKER}{index}")
}

/// Version of the product document stored in the `.yyds` schema slot.
const CATALOG_DOCUMENT_VERSION: u32 = 1;

/// Errors surfaced by the storage adapter.
#[derive(Debug)]
pub enum StoreError {
    /// The shard rejected the command.
    Shard(String),
    /// The catalog is missing pieces, malformed, or points at nothing.
    Catalog(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Shard(message) => write!(formatter, "{message}"),
            Self::Catalog(message) => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for StoreError {}

/// Outcome of one `insert` batch.
#[derive(Debug, Default)]
pub struct InsertReport {
    /// Documents written.
    pub inserted: i64,
    /// Positions whose `_id` already existed.
    pub duplicates: Vec<usize>,
    /// Positions that were not documents at all.
    pub invalid: Vec<usize>,
}

#[derive(Debug)]
struct CatalogState {
    path: PathBuf,
    shard_id: String,
    namespaces: BTreeSet<String>,
    indexes: Vec<IndexSpec>,
    collections: BTreeMap<String, CollectionOptions>,
}

impl CatalogState {
    fn document(&self) -> String {
        let mut document = format!("{{\"version\":{CATALOG_DOCUMENT_VERSION},\"namespaces\":[");
        for (index, namespace) in self.namespaces.iter().enumerate() {
            if index > 0 {
                document.push(',');
            }
            document.push_str(&serde_json::to_string(namespace).unwrap_or_else(|_| "\"\"".to_string()));
        }
        document.push(']');
        // The `indexes` array is additive: catalogs without user indexes keep
        // the original byte length, so existing catalog assertions stay valid.
        if !self.indexes.is_empty() {
            document.push_str(",\"indexes\":[");
            for (position, index) in self.indexes.iter().enumerate() {
                if position > 0 {
                    document.push(',');
                }
                document.push_str(&index.to_json());
            }
            document.push(']');
        }
        // The `collections` array is additive for the same reason: it only
        // appears once a validator or a non-default validation level exists.
        if !self.collections.is_empty() {
            document.push_str(",\"collections\":[");
            for (position, (namespace, options)) in self.collections.iter().enumerate() {
                if position > 0 {
                    document.push(',');
                }
                document.push_str(&collection_options_json(namespace, options));
            }
            document.push(']');
        }
        document.push('}');
        document
    }

    fn flush(&self) -> Result<(), StoreError> {
        let schema = CatalogSchema { version: CATALOG_DOCUMENT_VERSION, document: self.document() };
        let bytes = catalog::encode(Some(&schema), None, &[ShardId(self.shard_id.clone())], None, &[])
            .map_err(|error| StoreError::Catalog(format!("cannot encode catalog `{}`: {error}", self.path.display())))?;
        write_catalog(&self.path, &bytes)
    }
}

/// Writes catalog bytes through a synced temporary file and an atomic rename.
///
/// A partially written catalog would be rejected on the next open with no way
/// back, so the catalog never becomes visible in a torn state.
fn write_catalog(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    use std::io::Write;

    let temporary = path.with_extension("tmp");
    let write = (|| -> std::io::Result<()> {
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, path)
    })();
    write.map_err(|error| StoreError::Catalog(format!("cannot write catalog `{}`: {error}", path.display())))
}

fn parse_namespaces(document: &str) -> Result<BTreeSet<String>, StoreError> {
    let value: serde_json::Value =
        serde_json::from_str(document).map_err(|error| StoreError::Catalog(format!("catalog schema document is not JSON: {error}")))?;
    let version = value.get("version").and_then(serde_json::Value::as_u64);
    if version != Some(u64::from(CATALOG_DOCUMENT_VERSION)) {
        return Err(StoreError::Catalog(format!("catalog schema document version {version:?}, expected {CATALOG_DOCUMENT_VERSION}")));
    }
    let Some(entries) = value.get("namespaces").and_then(serde_json::Value::as_array)
    else {
        return Err(StoreError::Catalog("catalog schema document has no `namespaces` array".to_string()));
    };
    let mut namespaces = BTreeSet::new();
    for entry in entries {
        let Some(label) = entry.as_str()
        else {
            return Err(StoreError::Catalog("catalog schema document has a non-string namespace".to_string()));
        };
        namespaces.insert(label.to_string());
    }
    Ok(namespaces)
}

fn collection_options_json(namespace: &str, options: &CollectionOptions) -> String {
    let field = |text: &str| serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string());
    let mut text = format!("{{\"namespace\":{}", field(namespace));
    if let Some(validator) = &options.validator {
        text.push_str(&format!(",\"validator\":{}", serde_json::to_string(validator).unwrap_or_else(|_| "null".to_string())));
    }
    if let Some(level) = &options.validation_level {
        text.push_str(&format!(",\"validationLevel\":{}", field(level)));
    }
    if let Some(action) = &options.validation_action {
        text.push_str(&format!(",\"validationAction\":{}", field(action)));
    }
    text.push('}');
    text
}

fn parse_collections(document: &str) -> Result<BTreeMap<String, CollectionOptions>, StoreError> {
    let value: serde_json::Value =
        serde_json::from_str(document).map_err(|error| StoreError::Catalog(format!("catalog schema document is not JSON: {error}")))?;
    let Some(entries) = value.get("collections")
    else {
        return Ok(BTreeMap::new());
    };
    let Some(entries) = entries.as_array()
    else {
        return Err(StoreError::Catalog("catalog schema document has a non-array `collections` field".to_string()));
    };
    let mut collections = BTreeMap::new();
    for entry in entries {
        let namespace = entry
            .get("namespace")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| StoreError::Catalog("catalog collection entry has no namespace".to_string()))?;
        let options = CollectionOptions {
            validator: entry.get("validator").cloned(),
            validation_level: entry.get("validationLevel").and_then(serde_json::Value::as_str).map(str::to_string),
            validation_action: entry.get("validationAction").and_then(serde_json::Value::as_str).map(str::to_string),
        };
        collections.insert(namespace.to_string(), options);
    }
    Ok(collections)
}

impl IndexSpec {
    /// One catalog entry. The key is stored as ordered `[field, value]` pairs so
    /// a compound index keeps its creation order across a reopen.
    fn to_json(&self) -> String {
        let field = |text: &str| serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string());
        let mut text =
            format!("{{\"namespace\":{},\"name\":{},\"unique\":{},\"key\":[", field(&self.namespace), field(&self.name), self.unique);
        for (position, (name, value)) in self.key.fields.iter().enumerate() {
            if position > 0 {
                text.push(',');
            }
            text.push('[');
            text.push_str(&field(name));
            text.push(',');
            text.push_str(&serde_json::to_string(&json::to_json(value)).unwrap_or_else(|_| "null".to_string()));
            text.push(']');
        }
        text.push(']');
        if let Some(sparse) = self.sparse {
            text.push_str(&format!(",\"sparse\":{sparse}"));
        }
        if self.hidden {
            text.push_str(",\"hidden\":true");
        }
        if self.multikey {
            text.push_str(",\"multikey\":true");
        }
        else {
            // 显式写 false：旧目录没有这个字段，解析时按「可能 multikey」保守处理。
            text.push_str(",\"multikey\":false");
        }
        if let Some(seconds) = self.expire_after_seconds {
            text.push_str(&format!(",\"expireAfterSeconds\":{seconds}"));
        }
        if let Some(collation) = &self.collation {
            text.push_str(&format!(
                ",\"collation\":{}",
                serde_json::to_string(&json::to_json(&BsonValue::Document(collation.clone()))).unwrap_or_else(|_| "null".to_string())
            ));
        }
        if let Some(filter) = &self.partial_filter_expression {
            text.push_str(&format!(
                ",\"partialFilterExpression\":{}",
                serde_json::to_string(&json::to_json(&BsonValue::Document(filter.clone()))).unwrap_or_else(|_| "null".to_string())
            ));
        }
        if let Some(projection) = &self.wildcard_projection {
            text.push_str(&format!(
                ",\"wildcardProjection\":{}",
                serde_json::to_string(&json::to_json(&BsonValue::Document(projection.clone()))).unwrap_or_else(|_| "null".to_string())
            ));
        }
        text.push('}');
        text
    }
}

fn parse_indexes(document: &str) -> Result<Vec<IndexSpec>, StoreError> {
    let value: serde_json::Value =
        serde_json::from_str(document).map_err(|error| StoreError::Catalog(format!("catalog schema document is not JSON: {error}")))?;
    let Some(entries) = value.get("indexes")
    else {
        return Ok(Vec::new());
    };
    let Some(entries) = entries.as_array()
    else {
        return Err(StoreError::Catalog("catalog schema document has a non-array `indexes` field".to_string()));
    };
    let mut indexes = Vec::with_capacity(entries.len());
    for entry in entries {
        let namespace = entry
            .get("namespace")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| StoreError::Catalog("catalog index entry has no namespace".to_string()))?;
        let name = entry
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| StoreError::Catalog("catalog index entry has no name".to_string()))?;
        let sparse = entry.get("sparse").and_then(serde_json::Value::as_bool);
        let hidden = entry.get("hidden").and_then(serde_json::Value::as_bool).unwrap_or(false);
        // 旧目录没有该字段时按 multikey 处理：宁可退回集合扫描，也不漏数组文档。
        let multikey = entry.get("multikey").and_then(serde_json::Value::as_bool).unwrap_or(true);
        let expire_after_seconds = entry.get("expireAfterSeconds").and_then(serde_json::Value::as_i64).map(|seconds| seconds as i32);
        let collation = match entry.get("collation") {
            Some(value) => Some(
                document_option(value).map_err(|error| StoreError::Catalog(format!("catalog index `{name}` collation is invalid: {error}")))?,
            ),
            None => None,
        };
        let partial_filter_expression = match entry.get("partialFilterExpression") {
            Some(value) => Some(
                document_option(value)
                    .map_err(|error| StoreError::Catalog(format!("catalog index `{name}` partial filter is invalid: {error}")))?,
            ),
            None => None,
        };
        let wildcard_projection = match entry.get("wildcardProjection") {
            Some(value) => Some(
                document_option(value)
                    .map_err(|error| StoreError::Catalog(format!("catalog index `{name}` wildcard projection is invalid: {error}")))?,
            ),
            None => None,
        };
        let Some(pairs) = entry.get("key").and_then(serde_json::Value::as_array)
        else {
            return Err(StoreError::Catalog(format!("catalog index `{name}` has no key array")));
        };
        let mut key = BsonDocument::new();
        for pair in pairs {
            let Some(fields) = pair.as_array()
            else {
                return Err(StoreError::Catalog(format!("catalog index `{name}` has a malformed key entry")));
            };
            if fields.len() != 2 {
                return Err(StoreError::Catalog(format!("catalog index `{name}` has a malformed key entry")));
            }
            let Some(field) = fields[0].as_str()
            else {
                return Err(StoreError::Catalog(format!("catalog index `{name}` has a non-string key field")));
            };
            let value = json::from_json(&fields[1])
                .map_err(|error| StoreError::Catalog(format!("catalog index `{name}` key value is invalid: {error}")))?;
            key.push(field.to_string(), value);
        }
        let unique = entry.get("unique").and_then(serde_json::Value::as_bool).unwrap_or(false);
        indexes.push(IndexSpec {
            namespace: namespace.to_string(),
            name: name.to_string(),
            key,
            unique,
            sparse,
            hidden,
            expire_after_seconds,
            collation,
            partial_filter_expression,
            wildcard_projection,
            multikey,
        });
    }
    Ok(indexes)
}

fn document_option(value: &serde_json::Value) -> Result<BsonDocument, String> {
    match json::from_json(value) {
        Ok(BsonValue::Document(document)) => Ok(document),
        Ok(_) => Err("not a document".to_string()),
        Err(error) => Err(error.to_string()),
    }
}

/// 把单个集合的选项与索引编码成后端无关的 catalog 记录。
///
/// `.yykv` 后端把它写进 `.yyds` catalog，执行器后端把它写进预留的
/// `!catalog` namespace，两者共用同一份字节格式，因此切换后端不需要重写元数据。
pub fn collection_record(namespace: &str, options: &CollectionOptions, indexes: &[IndexSpec]) -> String {
    let mut text = format!("{{\"version\":{CATALOG_DOCUMENT_VERSION},\"collections\":[");
    text.push_str(&collection_options_json(namespace, options));
    text.push_str("],\"indexes\":[");
    for (position, index) in indexes.iter().filter(|index| index.namespace == namespace).enumerate() {
        if position > 0 {
            text.push(',');
        }
        text.push_str(&index.to_json());
    }
    text.push_str("]}");
    text
}

/// 解析 [`collection_record`] 生成的记录，只返回该 namespace 自己的部分。
pub fn parse_collection_record(namespace: &str, text: &str) -> Result<(CollectionOptions, Vec<IndexSpec>), StoreError> {
    let options = parse_collections(text)?.remove(namespace).unwrap_or_default();
    let indexes =
        parse_indexes(text)?.into_iter().filter(|index| index.namespace == namespace).collect();
    Ok((options, indexes))
}

fn legacy_namespaces(shard: &FileShard) -> Vec<String> {
    match shard.scan(&Namespace(CATALOG_NAMESPACE.to_string()), b"", None, None, 0) {
        Ok(records) => records.into_iter().filter_map(|record| String::from_utf8(record.key.bytes).ok()).collect(),
        Err(_) => Vec::new(),
    }
}

fn valid_shard_id(shard_id: &str) -> bool {
    !shard_id.is_empty() && shard_id != "." && shard_id != ".." && !shard_id.contains(['/', '\\', '\0'])
}

fn catalog_path_for(requested: &Path) -> PathBuf {
    requested.with_extension(CATALOG_SUFFIX)
}

fn shard_path_for(catalog_path: &Path, shard_id: &str) -> PathBuf {
    let directory = catalog_path.parent().unwrap_or_else(|| Path::new("."));
    directory.join(format!("{shard_id}.yykv"))
}

fn load_catalog(catalog_path: &Path) -> Result<CatalogState, StoreError> {
    let bytes = std::fs::read(catalog_path)
        .map_err(|error| StoreError::Catalog(format!("cannot read catalog `{}`: {error}", catalog_path.display())))?;
    let (schema, _routing, shards, _contract, _owners) =
        catalog::decode(&bytes).map_err(|error| StoreError::Catalog(format!("catalog `{}` is invalid: {error}", catalog_path.display())))?;
    if shards.len() != 1 {
        return Err(StoreError::Catalog(format!("catalog `{}` registers {} shards, expected 1", catalog_path.display(), shards.len())));
    }
    let shard_id = shards[0].0.clone();
    if !valid_shard_id(&shard_id) {
        return Err(StoreError::Catalog(format!("catalog `{}` has an unusable shard id", catalog_path.display())));
    }
    let namespaces = match &schema {
        Some(schema) => {
            if schema.version != CATALOG_DOCUMENT_VERSION {
                return Err(StoreError::Catalog(format!(
                    "catalog `{}` schema version {}, expected {CATALOG_DOCUMENT_VERSION}",
                    catalog_path.display(),
                    schema.version
                )));
            }
            parse_namespaces(&schema.document)?
        }
        None => BTreeSet::new(),
    };
    let indexes = match &schema {
        Some(schema) => parse_indexes(&schema.document)?,
        None => Vec::new(),
    };
    let collections = match &schema {
        Some(schema) => parse_collections(&schema.document)?,
        None => BTreeMap::new(),
    };
    Ok(CatalogState { path: catalog_path.to_path_buf(), shard_id, namespaces, indexes, collections })
}

fn new_catalog(requested: &Path, catalog_path: &Path) -> Result<CatalogState, StoreError> {
    let stem = requested
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .ok_or_else(|| StoreError::Catalog(format!("shard path `{}` has no file stem", requested.display())))?;
    Ok(CatalogState {
        path: catalog_path.to_path_buf(),
        shard_id: stem.to_string(),
        namespaces: BTreeSet::new(),
        indexes: Vec::new(),
        collections: BTreeMap::new(),
    })
}

/// One in-flight transaction: staged writes plus the store version it started
/// from. `writes` is `None` for a staged delete.
#[derive(Debug, Clone)]
pub struct TransactionState {
    /// Store version observed when the transaction started.
    pub start_version: u64,
    /// Staged `(namespace, encoded _id) -> document bytes`.
    pub writes: BTreeMap<(String, Vec<u8>), Option<Vec<u8>>>,
    /// Set when a write conflict or `abortTransaction` ended the transaction.
    pub aborted: bool,
}

/// Result of staging one write inside a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionWrite {
    /// The write was recorded in the transaction overlay.
    Staged,
    /// Another active transaction already staged the same document.
    Conflict,
}

/// Lifecycle of the newest transaction number a session used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// The transaction is open and accepts reads and writes.
    Active,
    /// The newest transaction number was committed.
    Committed,
    /// The newest transaction number was aborted, by the client or a conflict.
    Aborted,
}

/// Result of starting a transaction for one session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionBegin {
    /// A transaction was opened at the requested number.
    Started,
    /// The same number is already active, committed, or aborted.
    AlreadyStarted,
    /// The requested number is older than the session's newest number.
    TooOld {
        /// Newest number recorded for the session.
        newer: i64,
    },
}

/// What the session state allows for a write outside `startTransaction`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionWriteState {
    /// The write belongs to the open transaction.
    Active,
    /// The number is older than the session's newest number.
    TooOld {
        /// Newest number recorded for the session.
        newer: i64,
    },
    /// The number was already committed and cannot be reused.
    AlreadyCommitted,
    /// The number was already aborted and cannot be reused.
    AlreadyAborted,
    /// The number does not match any in-progress transaction.
    NoTransaction {
        /// Newest number recorded for the session.
        active: i64,
    },
}

/// Result of committing a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionCommit {
    /// The staged writes were applied.
    Committed,
    /// A document was modified after the transaction started.
    Conflict,
    /// The number was already committed, so the commit is a no-op success.
    AlreadyCommitted,
    /// The number was aborted earlier, by the client or a conflict.
    AlreadyAborted,
    /// The number is older than the session's newest number.
    TooOld {
        /// Newest number recorded for the session.
        newer: i64,
    },
    /// The number does not match the newest transaction of the session.
    NoTransaction {
        /// Newest number recorded for the session.
        active: i64,
    },
}

/// Result of aborting a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionAbort {
    /// The open transaction was dropped.
    Aborted,
    /// The number belongs to a committed transaction.
    AlreadyCommitted,
    /// The number was already aborted.
    AlreadyAborted,
    /// The number is older than the session's newest number.
    TooOld {
        /// Newest number recorded for the session.
        newer: i64,
    },
    /// The number does not match the newest transaction of the session.
    NoTransaction {
        /// Newest number recorded for the session.
        active: i64,
    },
}

/// Sessions, their staged writes, and the per-document write versions used for
/// conflict detection. Kept in memory: it describes in-flight work, not data.
#[derive(Debug, Default)]
struct SessionRegistry {
    transactions: BTreeMap<String, TransactionState>,
    last_numbers: BTreeMap<String, i64>,
    last_states: BTreeMap<String, SessionState>,
    versions: BTreeMap<(String, Vec<u8>), u64>,
    version: u64,
}

/// Oplog persistence and reclamation state.
///
/// The entries themselves live in the shard under the oplog namespace, keyed by
/// their BSON timestamp so scans return them in the order `mongod` writes them.
/// This struct only tracks the accounting needed for capped retention, plus the
/// entries a write command staged but has not committed yet.
#[derive(Debug, Default)]
struct OplogState {
    namespace: String,
    max_bytes: i64,
    bytes: i64,
    count: i64,
    last: Option<(u32, u32)>,
    staged: Vec<crate::oplog::OplogEntry>,
}

/// Document store backed by one `.yyds` catalog and one `.yykv` shard.
#[derive(Debug)]
pub struct Store {
    shard: Mutex<FileShard>,
    catalog: Mutex<CatalogState>,
    sessions: Mutex<SessionRegistry>,
    ttl: Mutex<TtlMonitorState>,
    oplog: Mutex<Option<OplogState>>,
}

impl Store {
    /// Opens the store whose shard would be `path`, reading `<path>.yyds`.
    ///
    /// - Catalog present: its single `ShardId` selects `<dir>/<id>.yykv`; the
    ///   shard must exist. Invalid bytes, a different shard count, a bad
    ///   schema document, or a missing shard are structured rejections.
    /// - Catalog missing: it is created, seeded from the shard's `!catalog`
    ///   mirror, and the shard is created when absent.
    ///
    /// The shard is opened (and created) before the catalog is written, so a
    /// crash in between leaves a self-healing directory instead of a catalog
    /// that can never be opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let requested = path.as_ref().to_path_buf();
        let catalog_path = catalog_path_for(&requested);
        let catalog_exists = catalog_path.exists();

        let mut catalog = if catalog_exists { load_catalog(&catalog_path)? } else { new_catalog(&requested, &catalog_path)? };

        let shard_path = if catalog_exists { shard_path_for(&catalog_path, &catalog.shard_id) } else { requested };
        if catalog_exists && !shard_path.exists() {
            return Err(StoreError::Catalog(format!(
                "catalog `{}` points at missing shard `{}`",
                catalog_path.display(),
                shard_path.display()
            )));
        }

        let shard = FileShard::open(&shard_path).map_err(|error| StoreError::Shard(error.to_string()))?;

        let mut changed = !catalog_exists;
        for label in legacy_namespaces(&shard) {
            if catalog.namespaces.insert(label) {
                changed = true;
            }
        }
        if changed {
            catalog.flush()?;
        }

        Ok(Self {
            shard: Mutex::new(shard),
            catalog: Mutex::new(catalog),
            sessions: Mutex::new(SessionRegistry::default()),
            ttl: Mutex::new(TtlMonitorState::default()),
            oplog: Mutex::new(None),
        })
    }

    fn ttl_lock(&self) -> MutexGuard<'_, TtlMonitorState> {
        match self.ttl.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Current TTL monitor state.
    pub fn ttl_monitor_state(&self) -> TtlMonitorState {
        *self.ttl_lock()
    }

    /// Changes `ttlMonitorSleepSecs`. `mongod` treats the value as whole seconds
    /// and always sleeps at least one, so `0` is clamped to `1`.
    pub fn set_ttl_monitor_sleep_seconds(&self, seconds: u32) {
        self.ttl_lock().sleep_seconds = seconds.max(1);
    }

    fn oplog_lock(&self) -> MutexGuard<'_, Option<OplogState>> {
        match self.oplog.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Turns on `local.oplog.rs` persistence with capped retention.
    ///
    /// Existing entries are read once so accounting survives a restart: the
    /// byte total, the entry count and the newest timestamp all come from the
    /// stored documents. Entries above `max_bytes` are reclaimed immediately,
    /// oldest first, exactly like a capped collection created with that size.
    pub fn enable_oplog(&self, namespace: &str, max_bytes: i64) -> Result<(), StoreError> {
        self.register_namespace(namespace)?;
        let records = self.scan(namespace)?;
        let mut bytes = 0_i64;
        let mut last = None;
        for (_, payload) in &records {
            bytes += payload.len() as i64;
            let Ok(document) = parse_bson(payload)
            else {
                continue;
            };
            if let Some(BsonValue::Timestamp { t, i }) = document.get("ts") {
                last = Some((*t, *i));
            }
        }
        {
            let mut guard = self.oplog_lock();
            *guard =
                Some(OplogState { namespace: namespace.to_string(), max_bytes, bytes, count: records.len() as i64, last, staged: Vec::new() });
        }
        self.evict_oplog()
    }

    /// Whether a replica-set oplog is configured.
    pub fn oplog_enabled(&self) -> bool {
        self.oplog_lock().is_some()
    }

    /// Namespace of the configured oplog, when persistence is enabled.
    pub fn oplog_namespace(&self) -> Option<String> {
        self.oplog_lock().as_ref().map(|state| state.namespace.clone())
    }

    /// Capped retention figures: namespace, `max_bytes`, count and bytes.
    pub fn oplog_status(&self) -> Option<(String, i64, i64, i64)> {
        let guard = self.oplog_lock();
        let state = guard.as_ref()?;
        Some((state.namespace.clone(), state.max_bytes, state.count, state.bytes))
    }

    /// Queues one entry for the next [`Store::commit_oplog`]. A no-op when the
    /// oplog is not enabled.
    pub fn stage_oplog(&self, entry: OplogEntry) {
        let mut guard = self.oplog_lock();
        if let Some(state) = guard.as_mut() {
            state.staged.push(entry);
        }
    }

    /// Drops entries a failed command staged, so they can never leak into a
    /// later commit.
    pub fn discard_staged_oplog(&self) {
        let mut guard = self.oplog_lock();
        if let Some(state) = guard.as_mut() {
            state.staged.clear();
        }
    }

    /// Persists every staged entry with increasing timestamps.
    ///
    /// `wall_millis` is the host's clock. Timestamps follow `mongod`: within one
    /// second the increment grows by one per entry, and a new second restarts at
    /// one. Returns the timestamp of the last entry, or `None` when nothing was
    /// staged.
    pub fn commit_oplog(&self, wall_millis: i64) -> Result<Option<(u32, u32)>, StoreError> {
        let mut guard = self.oplog_lock();
        let Some(state) = guard.as_mut()
        else {
            return Ok(None);
        };
        if state.staged.is_empty() {
            return Ok(None);
        }
        let staged = std::mem::take(&mut state.staged);
        let seconds = (wall_millis.max(0) / 1000) as u32;
        let mut last = state.last;
        let mut writes = Vec::with_capacity(staged.len());
        for entry in &staged {
            let (t, i) = match last {
                Some((last_t, last_i)) if last_t == seconds => (seconds, last_i.saturating_add(1)),
                _ => (seconds, 1),
            };
            last = Some((t, i));
            let document = entry.render(t, i, 1, wall_millis);
            writes.push((value_to_key(&BsonValue::Timestamp { t, i }), encode_bson(&document)));
        }
        let namespace = state.namespace.clone();
        let inserted = self.insert(&namespace, &writes, true)?.inserted;
        state.count += inserted as i64;
        state.bytes += writes.iter().map(|(_, payload)| payload.len() as i64).sum::<i64>();
        state.last = last;
        drop(guard);
        self.evict_oplog()?;
        Ok(last)
    }

    /// Reclaims the oldest entries until the capped size fits.
    fn evict_oplog(&self) -> Result<(), StoreError> {
        let guard = self.oplog_lock();
        let Some(state) = guard.as_ref()
        else {
            return Ok(());
        };
        if state.bytes <= state.max_bytes {
            return Ok(());
        }
        let namespace = state.namespace.clone();
        let max_bytes = state.max_bytes;
        let records = self.scan(&namespace)?;
        drop(guard);

        let total = records.len();
        let mut doomed = Vec::new();
        let mut remaining = records.iter().map(|(_, payload)| payload.len() as i64).sum::<i64>();
        for (position, (key, payload)) in records.into_iter().enumerate() {
            // A capped collection always keeps at least its newest document, so a
            // single oversized entry is retained rather than deleted.
            if remaining <= max_bytes || position + 1 == total {
                break;
            }
            remaining -= payload.len() as i64;
            doomed.push(key);
        }
        if doomed.is_empty() {
            return Ok(());
        }
        let removed = self.delete(&namespace, &doomed)? as i64;
        let mut guard = self.oplog_lock();
        if let Some(state) = guard.as_mut() {
            state.bytes = remaining;
            state.count = (state.count - removed).max(0);
        }
        Ok(())
    }

    /// Claims one monitor pass when the sleep window has elapsed.
    ///
    /// The caller supplies the clock, so a host drives the monitor with its own
    /// time source and tests compress the window. The window itself is never
    /// bypassed: a document whose deadline just passed stays until some later
    /// call claims a pass, which is the periodic `mongod` semantics.
    pub fn claim_ttl_pass(&self, now_ms: i64) -> bool {
        let mut state = self.ttl_lock();
        let window = i64::from(state.sleep_seconds.max(1)) * 1_000;
        let due = match state.last_scan_ms {
            None => true,
            Some(last) => now_ms >= last.saturating_add(window),
        };
        if due {
            state.last_scan_ms = Some(now_ms);
        }
        due
    }

    fn lock(&self) -> MutexGuard<'_, FileShard> {
        match self.shard.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn catalog_lock(&self) -> MutexGuard<'_, CatalogState> {
        match self.catalog.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn sessions_lock(&self) -> MutexGuard<'_, SessionRegistry> {
        match self.sessions.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Notes one committed batch in the per-document version map. Active
    /// transactions use these versions to detect a write conflict at commit.
    fn note_writes(&self, namespace: &str, keys: &[Vec<u8>]) {
        let mut registry = self.sessions_lock();
        registry.version += 1;
        let version = registry.version;
        for key in keys {
            registry.versions.insert((namespace.to_string(), key.clone()), version);
        }
    }

    /// Starts a transaction for one logical session. A number older than the
    /// session's newest number is rejected, and the newest number can never be
    /// started twice.
    pub fn begin_transaction(&self, session: &str, txn_number: i64) -> TransactionBegin {
        let mut registry = self.sessions_lock();
        if let Some(&last) = registry.last_numbers.get(session) {
            if txn_number < last {
                return TransactionBegin::TooOld { newer: last };
            }
            if txn_number == last {
                return TransactionBegin::AlreadyStarted;
            }
        }
        let start_version = registry.version;
        registry.transactions.insert(session.to_string(), TransactionState { start_version, writes: BTreeMap::new(), aborted: false });
        registry.last_numbers.insert(session.to_string(), txn_number);
        registry.last_states.insert(session.to_string(), SessionState::Active);
        TransactionBegin::Started
    }

    /// What the session state allows for a command carrying `txn_number`
    /// without `startTransaction`.
    pub fn session_write_state(&self, session: &str, txn_number: i64) -> SessionWriteState {
        let registry = self.sessions_lock();
        let Some(&last) = registry.last_numbers.get(session)
        else {
            return SessionWriteState::NoTransaction { active: -1 };
        };
        if txn_number < last {
            return SessionWriteState::TooOld { newer: last };
        }
        if txn_number > last {
            return SessionWriteState::NoTransaction { active: last };
        }
        match registry.last_states.get(session).copied().unwrap_or(SessionState::Active) {
            SessionState::Active => SessionWriteState::Active,
            SessionState::Committed => SessionWriteState::AlreadyCommitted,
            SessionState::Aborted => SessionWriteState::AlreadyAborted,
        }
    }

    /// Whether the session currently has a usable transaction.
    pub fn transaction_active(&self, session: &str) -> bool {
        self.sessions_lock().transactions.get(session).map(|state| !state.aborted).unwrap_or(false)
    }

    /// Stages one write in a transaction, rejecting a document another active
    /// transaction already holds.
    pub fn transaction_write(&self, session: &str, namespace: &str, key: &[u8], value: Option<Vec<u8>>) -> TransactionWrite {
        let mut registry = self.sessions_lock();
        let target = (namespace.to_string(), key.to_vec());
        let mut conflict = false;
        for (other, state) in &registry.transactions {
            if other == session || state.aborted {
                continue;
            }
            if state.writes.contains_key(&target) {
                conflict = true;
                break;
            }
        }
        if conflict {
            if let Some(state) = registry.transactions.get_mut(session) {
                state.aborted = true;
            }
            registry.last_states.insert(session.to_string(), SessionState::Aborted);
            return TransactionWrite::Conflict;
        }
        if let Some(state) = registry.transactions.get_mut(session) {
            state.writes.insert(target, value);
        }
        TransactionWrite::Staged
    }

    /// The namespace as the transaction sees it, `None` when the session has no
    /// active transaction and the caller should read the committed data.
    pub fn transaction_scan(&self, session: &str, namespace: &str) -> Result<Option<Vec<(Vec<u8>, Vec<u8>)>>, StoreError> {
        let staged: Vec<(Vec<u8>, Option<Vec<u8>>)> = {
            let registry = self.sessions_lock();
            let Some(state) = registry.transactions.get(session)
            else {
                return Ok(None);
            };
            if state.aborted {
                return Ok(None);
            }
            state.writes.iter().filter(|((owner, _), _)| owner == namespace).map(|((_, key), value)| (key.clone(), value.clone())).collect()
        };
        let mut documents = self.scan(namespace)?;
        for (key, value) in staged {
            match value {
                Some(payload) => match documents.iter_mut().find(|(candidate, _)| candidate == &key) {
                    Some(existing) => existing.1 = payload,
                    None => documents.push((key, payload)),
                },
                None => documents.retain(|(candidate, _)| candidate != &key),
            }
        }
        Ok(Some(documents))
    }

    /// One document as the transaction sees it. The outer `Option` is `None`
    /// when the key is not staged, so the caller reads the committed value.
    pub fn transaction_get(&self, session: &str, namespace: &str, key: &[u8]) -> Result<Option<Option<Vec<u8>>>, StoreError> {
        let registry = self.sessions_lock();
        let Some(state) = registry.transactions.get(session)
        else {
            return Ok(None);
        };
        if state.aborted {
            return Ok(None);
        }
        Ok(state.writes.get(&(namespace.to_string(), key.to_vec())).cloned().map(Some).unwrap_or(None))
    }

    /// Applies every staged write, or reports the conflict that prevents it.
    pub fn transaction_commit(&self, session: &str, txn_number: i64) -> TransactionCommit {
        let mut registry = self.sessions_lock();
        let Some(&last) = registry.last_numbers.get(session)
        else {
            return TransactionCommit::NoTransaction { active: -1 };
        };
        if txn_number < last {
            return TransactionCommit::TooOld { newer: last };
        }
        if txn_number > last {
            return TransactionCommit::NoTransaction { active: last };
        }
        match registry.last_states.get(session).copied() {
            Some(SessionState::Committed) => return TransactionCommit::AlreadyCommitted,
            Some(SessionState::Aborted) => {
                registry.transactions.remove(session);
                return TransactionCommit::AlreadyAborted;
            }
            _ => {}
        }
        let (start_version, aborted, writes) = {
            let Some(state) = registry.transactions.get(session)
            else {
                return TransactionCommit::NoTransaction { active: last };
            };
            (state.start_version, state.aborted, state.writes.clone())
        };
        if aborted {
            registry.transactions.remove(session);
            registry.last_states.insert(session.to_string(), SessionState::Aborted);
            return TransactionCommit::AlreadyAborted;
        }
        let conflict = writes.keys().any(|key| registry.versions.get(key).map(|version| *version > start_version).unwrap_or(false));
        if conflict {
            if let Some(state) = registry.transactions.get_mut(session) {
                state.aborted = true;
            }
            registry.last_states.insert(session.to_string(), SessionState::Aborted);
            return TransactionCommit::Conflict;
        }
        type NamespaceWriteSet = Vec<(Vec<u8>, Option<Vec<u8>>)>;
        let mut by_namespace: BTreeMap<String, NamespaceWriteSet> = BTreeMap::new();
        for ((namespace, key), value) in writes {
            by_namespace.entry(namespace).or_default().push((key, value));
        }
        drop(registry);
        for (namespace, entries) in by_namespace {
            let replaced: Vec<(Vec<u8>, Vec<u8>)> =
                entries.iter().filter_map(|(key, value)| value.clone().map(|payload| (key.clone(), payload))).collect();
            if !replaced.is_empty() {
                let _ = self.replace(&namespace, &replaced);
            }
            let deleted: Vec<Vec<u8>> = entries.iter().filter(|(_, value)| value.is_none()).map(|(key, _)| key.clone()).collect();
            if !deleted.is_empty() {
                let _ = self.delete(&namespace, &deleted);
            }
        }
        let mut registry = self.sessions_lock();
        registry.transactions.remove(session);
        registry.last_states.insert(session.to_string(), SessionState::Committed);
        TransactionCommit::Committed
    }

    /// Drops the open transaction, or reports that the number was already
    /// committed or aborted.
    pub fn transaction_abort(&self, session: &str, txn_number: i64) -> TransactionAbort {
        let mut registry = self.sessions_lock();
        let Some(&last) = registry.last_numbers.get(session)
        else {
            return TransactionAbort::NoTransaction { active: -1 };
        };
        if txn_number < last {
            return TransactionAbort::TooOld { newer: last };
        }
        if txn_number > last {
            return TransactionAbort::NoTransaction { active: last };
        }
        match registry.last_states.get(session).copied() {
            Some(SessionState::Committed) => return TransactionAbort::AlreadyCommitted,
            Some(SessionState::Aborted) => {
                registry.transactions.remove(session);
                return TransactionAbort::AlreadyAborted;
            }
            _ => {}
        }
        if registry.transactions.remove(session).is_none() {
            return TransactionAbort::NoTransaction { active: last };
        }
        registry.last_states.insert(session.to_string(), SessionState::Aborted);
        TransactionAbort::Aborted
    }

    fn register_namespace(&self, namespace: &str) -> Result<(), StoreError> {
        let mut catalog = self.catalog_lock();
        if !catalog.namespaces.insert(namespace.to_string()) {
            return Ok(());
        }
        catalog.flush()
    }

    /// Reads the collection options, defaulting to no validator and `strict` /
    /// `error` when the catalog has no entry.
    pub fn collection_options(&self, namespace: &str) -> Result<CollectionOptions, StoreError> {
        Ok(self.catalog_lock().collections.get(namespace).cloned().unwrap_or_default())
    }

    /// Writes collection options and registers the namespace in one durable
    /// catalog update. An all-default option set removes the entry again.
    pub fn set_collection_options(&self, namespace: &str, options: CollectionOptions) -> Result<(), StoreError> {
        let mut catalog = self.catalog_lock();
        catalog.namespaces.insert(namespace.to_string());
        if options == CollectionOptions::default() {
            catalog.collections.remove(namespace);
        }
        else {
            catalog.collections.insert(namespace.to_string(), options);
        }
        catalog.flush()
    }

    /// Drops one collection: its documents, user indexes and options.
    ///
    /// The catalog is rewritten first (it is the authority for namespace
    /// existence), then every record of the namespace and its `!catalog` mirror
    /// label are removed in one durable shard commit. Returns whether the
    /// namespace existed, so `drop` can distinguish `nIndexesWas` from an
    /// absent collection.
    pub fn drop_namespace(&self, namespace: &str) -> Result<bool, StoreError> {
        let mut catalog = self.catalog_lock();
        let existed = catalog.namespaces.remove(namespace);
        let removed_index = catalog.indexes.iter().any(|index| index.namespace == namespace);
        catalog.indexes.retain(|index| index.namespace != namespace);
        let removed_options = catalog.collections.remove(namespace).is_some();
        if existed || removed_index || removed_options {
            catalog.flush()?;
        }

        let mut shard = self.lock();
        let mut changes = Vec::new();
        match shard.scan(&Namespace(namespace.to_string()), b"", None, None, 0) {
            Ok(records) => changes.extend(records.into_iter().map(|record| ShardChange::Delete { key: record.key })),
            Err(error) => return Err(StoreError::Shard(error.to_string())),
        }
        match shard.scan(&Namespace(CATALOG_NAMESPACE.to_string()), namespace.as_bytes(), None, None, 0) {
            Ok(records) => changes.extend(records.into_iter().map(|record| ShardChange::Delete { key: record.key })),
            Err(error) => return Err(StoreError::Shard(error.to_string())),
        }
        if !changes.is_empty() {
            shard.commit_changes(changes).map_err(|error| StoreError::Shard(error.to_string()))?;
        }
        Ok(existed)
    }

    /// Creates a collection. Returns `false` when the namespace already exists.
    pub fn create_collection(&self, namespace: &str, options: CollectionOptions) -> Result<bool, StoreError> {
        let mut catalog = self.catalog_lock();
        if !catalog.namespaces.insert(namespace.to_string()) {
            return Ok(false);
        }
        if options != CollectionOptions::default() {
            catalog.collections.insert(namespace.to_string(), options);
        }
        catalog.flush()?;
        Ok(true)
    }

    /// Reads one document by its encoded `_id`.
    ///
    /// A shard read failure is reported instead of being collapsed into "no such
    /// document", so a broken shard cannot masquerade as missing data.
    pub fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let shard = self.lock();
        let record = shard.get(&Key::new(namespace, key.to_vec())).map_err(|error| StoreError::Shard(error.to_string()))?;
        Ok(match record {
            Some(record) => match record.value {
                StoredValue::Inline(InlineValue(bytes)) => Some(bytes),
                StoredValue::Object(_) => None,
            },
            None => None,
        })
    }

    /// Atomically creates one document when its `_id` is absent.
    ///
    /// The existence check and the write share one shard commit under the store
    /// lock, so two concurrent inserts of the same `_id` cannot both land.
    pub fn insert_if_absent(&self, namespace: &str, key: Vec<u8>, value: Vec<u8>) -> Result<bool, StoreError> {
        Ok(self.insert(namespace, &[(key, value)], true)?.inserted == 1)
    }

    /// Writes one batch and reports duplicate / invalid positions.
    ///
    /// The whole batch lands through one durable commit. `ordered` stops staging
    /// at the first duplicate, `ordered: false` keeps the remaining valid documents.
    pub fn insert(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)], ordered: bool) -> Result<InsertReport, StoreError> {
        let mut report = InsertReport::default();
        let mut written: Vec<Vec<u8>> = Vec::new();
        {
            let mut shard = self.lock();
            let mut changes = Vec::with_capacity(documents.len());
            let mut staged: BTreeSet<Vec<u8>> = BTreeSet::new();

            for (index, (key_bytes, payload)) in documents.iter().enumerate() {
                let key = Key::new(namespace, key_bytes.clone());
                // `staged.insert` is the same key encoding used for single-document
                // lookups, so intra-batch duplicates share the numeric equivalence
                // rules (int32/int64/integral double) without a second comparator.
                let already_staged = !staged.insert(key_bytes.clone());
                let exists =
                    if already_staged { true } else { shard.get(&key).map_err(|error| StoreError::Shard(error.to_string()))?.is_some() };
                if exists {
                    report.duplicates.push(index);
                    if ordered {
                        break;
                    }
                    continue;
                }
                changes.push(ShardChange::Put { key, value: StoredValue::Inline(InlineValue(payload.clone())), expires_at: None });
                written.push(key_bytes.clone());
                report.inserted += 1;
            }

            if changes.is_empty() {
                return Ok(report);
            }

            let catalog_key = Key::new(CATALOG_NAMESPACE, namespace.as_bytes().to_vec());
            if !is_index_namespace(namespace) && shard.get(&catalog_key).ok().flatten().is_none() {
                changes.push(ShardChange::Put { key: catalog_key, value: StoredValue::Inline(InlineValue(Vec::new())), expires_at: None });
            }

            shard.commit_changes(changes).map_err(|error| StoreError::Shard(error.to_string()))?;
        }
        if !is_index_namespace(namespace) {
            self.note_writes(namespace, &written);
            self.register_namespace(namespace)?;
        }
        Ok(report)
    }

    /// Every document in one namespace, ordered by encoded `_id`.
    ///
    /// A scan failure is reported instead of being turned into an empty set.
    pub fn scan(&self, namespace: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let shard = self.lock();
        let records =
            shard.scan(&Namespace(namespace.to_string()), b"", None, None, 0).map_err(|error| StoreError::Shard(error.to_string()))?;
        Ok(records
            .into_iter()
            .filter_map(|record| match record.value {
                StoredValue::Inline(InlineValue(bytes)) => Some((record.key.bytes, bytes)),
                StoredValue::Object(_) => None,
            })
            .collect())
    }

    /// 扫描保留命名空间下以 `prefix` 开头的键值对。
    ///
    /// 索引项与文档共享同一个 shard，所以这里直接走底层前缀扫描，不经过目录。
    pub fn scan_reserved(&self, namespace: &str, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let shard = self.lock();
        let records =
            shard.scan(&Namespace(namespace.to_string()), prefix, None, None, 0).map_err(|error| StoreError::Shard(error.to_string()))?;
        Ok(records
            .into_iter()
            .filter_map(|record| match record.value {
                StoredValue::Inline(InlineValue(bytes)) => Some((record.key.bytes, bytes)),
                StoredValue::Object(_) => None,
            })
            .collect())
    }

    /// 保留命名空间下的有界区间扫描，直接把游标与条数下推到底层 shard。
    ///
    /// `after` / `before` 都是排他游标，`limit` 为 `0` 表示不设上限。
    pub fn scan_reserved_range(
        &self,
        namespace: &str,
        prefix: &[u8],
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let shard = self.lock();
        let records = shard
            .scan(&Namespace(namespace.to_string()), prefix, after, before, limit)
            .map_err(|error| StoreError::Shard(error.to_string()))?;
        Ok(records
            .into_iter()
            .filter_map(|record| match record.value {
                StoredValue::Inline(InlineValue(bytes)) => Some((record.key.bytes, bytes)),
                StoredValue::Object(_) => None,
            })
            .collect())
    }

    /// 判断保留命名空间是否至少有一条条目，只读一条就返回。
    pub fn reserved_has_entries(&self, namespace: &str) -> Result<bool, StoreError> {
        let shard = self.lock();
        let records =
            shard.scan(&Namespace(namespace.to_string()), b"", None, None, 1).map_err(|error| StoreError::Shard(error.to_string()))?;
        Ok(!records.is_empty())
    }

    /// 写入保留命名空间，不登记目录、不伪造 `!catalog` 镜像。
    pub fn put_reserved(&self, namespace: &str, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<usize, StoreError> {
        if entries.is_empty() {
            return Ok(0);
        }
        let mut shard = self.lock();
        let changes = entries
            .iter()
            .map(|(key, payload)| ShardChange::Put {
                key: Key::new(namespace, key.clone()),
                value: StoredValue::Inline(InlineValue(payload.clone())),
                expires_at: None,
            })
            .collect();
        shard.commit_changes(changes).map_err(|error| StoreError::Shard(error.to_string()))?;
        Ok(entries.len())
    }

    /// Deletes the given encoded `_id` keys and reports how many existed.
    pub fn delete(&self, namespace: &str, keys: &[Vec<u8>]) -> Result<usize, StoreError> {
        let mut shard = self.lock();
        let mut changes = Vec::new();
        for key in keys {
            let candidate = Key::new(namespace, key.clone());
            if matches!(shard.get(&candidate), Ok(Some(_))) {
                changes.push(ShardChange::Delete { key: candidate });
            }
        }
        if changes.is_empty() {
            return Ok(0);
        }
        let removed = changes.len();
        let deleted: Vec<Vec<u8>> = keys.to_vec();
        shard.commit_changes(changes).map_err(|error| StoreError::Shard(error.to_string()))?;
        drop(shard);
        self.note_writes(namespace, &deleted);
        Ok(removed)
    }

    /// Overwrites documents in place without duplicate checks, used by `update`.
    pub fn replace(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)]) -> Result<usize, StoreError> {
        self.write_batch(namespace, documents)
    }

    /// Persists one batch without duplicate checks, used by `mongodump` import.
    pub fn import(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)]) -> Result<usize, StoreError> {
        self.write_batch(namespace, documents)
    }

    fn write_batch(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)]) -> Result<usize, StoreError> {
        {
            let mut shard = self.lock();
            let mut changes = Vec::with_capacity(documents.len() + 1);
            for (key_bytes, payload) in documents {
                changes.push(ShardChange::Put {
                    key: Key::new(namespace, key_bytes.clone()),
                    value: StoredValue::Inline(InlineValue(payload.clone())),
                    expires_at: None,
                });
            }
            if !is_index_namespace(namespace) {
                changes.push(ShardChange::Put {
                    key: Key::new(CATALOG_NAMESPACE, namespace.as_bytes().to_vec()),
                    value: StoredValue::Inline(InlineValue(Vec::new())),
                    expires_at: None,
                });
            }
            shard.commit_changes(changes).map_err(|error| StoreError::Shard(error.to_string()))?;
        }
        if !is_index_namespace(namespace) {
            let keys: Vec<Vec<u8>> = documents.iter().map(|(key, _)| key.clone()).collect();
            self.note_writes(namespace, &keys);
            self.register_namespace(namespace)?;
        }
        Ok(documents.len())
    }

    /// Namespaces registered in the `.yyds` catalog, sorted.
    pub fn namespaces(&self) -> Result<Vec<String>, StoreError> {
        Ok(self.catalog_lock().namespaces.iter().cloned().collect())
    }

    /// Whether `namespace` is registered in the catalog.
    pub fn namespace_exists(&self, namespace: &str) -> Result<bool, StoreError> {
        Ok(self.catalog_lock().namespaces.contains(namespace))
    }

    /// User indexes of one namespace in creation order, `_id_` excluded.
    pub fn index_specs(&self, namespace: &str) -> Result<Vec<IndexSpec>, StoreError> {
        Ok(self.catalog_lock().indexes.iter().filter(|index| index.namespace == namespace).cloned().collect())
    }

    /// Registers `namespace` and appends the new index definitions in one flush.
    ///
    /// `createIndexes` creates the collection, so an index can exist on a
    /// namespace that holds no documents yet.
    pub fn add_indexes(&self, namespace: &str, indexes: &[IndexSpec]) -> Result<(), StoreError> {
        let mut catalog = self.catalog_lock();
        catalog.namespaces.insert(namespace.to_string());
        catalog.indexes.extend(indexes.iter().cloned());
        catalog.flush()
    }

    /// 持久化 multikey 标记，已经置上时不重复写目录。
    pub fn mark_index_multikey(&self, namespace: &str, name: &str) -> Result<(), StoreError> {
        let mut catalog = self.catalog_lock();
        let mut changed = false;
        for index in &mut catalog.indexes {
            if index.namespace == namespace && index.name == name && !index.multikey {
                index.multikey = true;
                changed = true;
            }
        }
        if changed {
            catalog.flush()?;
        }
        Ok(())
    }

    /// Drops the named user index, or every user index when `name` is `*`.
    pub fn drop_indexes(&self, namespace: &str, name: &str) -> Result<usize, StoreError> {
        let mut catalog = self.catalog_lock();
        let before = catalog.indexes.len();
        catalog.indexes.retain(|index| {
            if index.namespace != namespace {
                return true;
            }
            name != "*" && index.name != name
        });
        let removed = before - catalog.indexes.len();
        if removed > 0 {
            catalog.flush()?;
        }
        Ok(removed)
    }

    /// Drops every collection, index, collection option and the `!catalog`
    /// mirror label of one database.
    ///
    /// The `.yyds` catalog is the authority for namespace existence, so it is
    /// rewritten first through its atomic replace. Every `<database>.` record
    /// and mirror label is then removed in one durable shard commit. A failed
    /// shard commit restores the catalog snapshot, so a failed drop leaves the
    /// database exactly as it was; a crash between the two writes leaves the
    /// shard mirror intact and the next `open` merges it back, which means
    /// readers only ever see the database fully present or fully absent.
    ///
    /// Returns the number of collections the catalog listed for `database`.
    pub fn drop_database(&self, database: &str) -> Result<usize, StoreError> {
        let prefix = format!("{database}.");
        let mut catalog = self.catalog_lock();
        let namespaces = catalog.namespaces.clone();
        let indexes = catalog.indexes.clone();
        let collections = catalog.collections.clone();

        let removed = catalog.namespaces.iter().filter(|namespace| namespace.starts_with(&prefix)).count();
        catalog.namespaces.retain(|namespace| !namespace.starts_with(&prefix));
        catalog.indexes.retain(|index| !index.namespace.starts_with(&prefix));
        catalog.collections.retain(|namespace, _| !namespace.starts_with(&prefix));

        if removed > 0
            && let Err(error) = catalog.flush()
        {
            catalog.namespaces = namespaces;
            catalog.indexes = indexes;
            catalog.collections = collections;
            return Err(error);
        }

        // Restoring the snapshot keeps a failed shard commit from leaving a
        // catalog that has forgotten the database while its data is still there.
        let restore = |catalog: &mut CatalogState| {
            catalog.namespaces = namespaces.clone();
            catalog.indexes = indexes.clone();
            catalog.collections = collections.clone();
            let _ = catalog.flush();
        };

        let mut shard = self.lock();
        let mut changes = Vec::new();
        let live = match shard.namespaces() {
            Ok(namespaces) => namespaces,
            Err(error) => {
                drop(shard);
                restore(&mut catalog);
                return Err(StoreError::Shard(error.to_string()));
            }
        };
        for namespace in live {
            if !namespace.0.starts_with(&prefix) {
                continue;
            }
            let records = match shard.scan(&namespace, b"", None, None, 0) {
                Ok(records) => records,
                Err(error) => {
                    drop(shard);
                    restore(&mut catalog);
                    return Err(StoreError::Shard(error.to_string()));
                }
            };
            changes.extend(records.into_iter().map(|record| ShardChange::Delete { key: record.key }));
        }
        let mirror = match shard.scan(&Namespace(CATALOG_NAMESPACE.to_string()), prefix.as_bytes(), None, None, 0) {
            Ok(records) => records,
            Err(error) => {
                drop(shard);
                restore(&mut catalog);
                return Err(StoreError::Shard(error.to_string()));
            }
        };
        changes.extend(mirror.into_iter().map(|record| ShardChange::Delete { key: record.key }));

        if !changes.is_empty()
            && let Err(error) = shard.commit_changes(changes)
        {
            drop(shard);
            restore(&mut catalog);
            return Err(StoreError::Shard(error.to_string()));
        }
        Ok(removed)
    }

    /// Creates the shard named by the catalog when only the catalog survived.
    ///
    /// Deliberately narrow: it never invents a catalog, never overwrites an
    /// existing shard (even an unreadable one), never touches the catalog
    /// bytes, and is a no-op on a healthy directory. `open` never calls this;
    /// the host has to ask for it explicitly.
    pub fn repair_shard(path: impl AsRef<Path>) -> Result<RepairReport, StoreError> {
        let requested = path.as_ref().to_path_buf();
        let catalog_path = catalog_path_for(&requested);
        if !catalog_path.exists() {
            return Err(StoreError::Catalog(format!(
                "catalog `{}` is missing; repair only rebuilds a shard named by an existing catalog",
                catalog_path.display()
            )));
        }
        let catalog = load_catalog(&catalog_path)?;
        let shard_path = shard_path_for(&catalog_path, &catalog.shard_id);
        if shard_path.exists() {
            return Ok(RepairReport { created: false, shard: catalog.shard_id });
        }
        FileShard::open(&shard_path).map_err(|error| StoreError::Shard(error.to_string()))?;
        Ok(RepairReport { created: true, shard: catalog.shard_id })
    }
}

/// Outcome of an explicit [`Store::repair_shard`].
#[derive(Debug)]
pub struct RepairReport {
    /// Whether an empty shard was written.
    pub created: bool,
    /// Shard id named by the catalog.
    pub shard: String,
}

impl RepairReport {
    /// JSON description exchanged with the language bindings.
    pub fn to_json(&self) -> String {
        serde_json::json!({ "created": self.created, "shard": self.shard }).to_string()
    }
}

/// `.yykv` / `.yyds` 后端把全部语义落到 [`crate::storage::MongoStorage`]。
///
/// 命令层只通过该 trait 访问数据，因此换成 `KeyValueExecutor` 适配器时不需要改
/// 任何命令实现。
impl crate::storage::MongoStorage for Store {
    fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        Store::get(self, namespace, key)
    }

    fn insert_if_absent(&self, namespace: &str, key: Vec<u8>, value: Vec<u8>) -> Result<bool, StoreError> {
        Store::insert_if_absent(self, namespace, key, value)
    }

    fn insert(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)], ordered: bool) -> Result<InsertReport, StoreError> {
        Store::insert(self, namespace, documents, ordered)
    }

    fn scan(&self, namespace: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        Store::scan(self, namespace)
    }

    fn delete(&self, namespace: &str, keys: &[Vec<u8>]) -> Result<usize, StoreError> {
        Store::delete(self, namespace, keys)
    }

    fn replace(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)]) -> Result<usize, StoreError> {
        Store::replace(self, namespace, documents)
    }

    fn scan_reserved(&self, namespace: &str, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        Store::scan_reserved(self, namespace, prefix)
    }

    fn scan_reserved_range(
        &self,
        namespace: &str,
        prefix: &[u8],
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        Store::scan_reserved_range(self, namespace, prefix, after, before, limit)
    }

    fn put_reserved(&self, namespace: &str, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<usize, StoreError> {
        Store::put_reserved(self, namespace, entries)
    }

    fn reserved_has_entries(&self, namespace: &str) -> Result<bool, StoreError> {
        Store::reserved_has_entries(self, namespace)
    }

    fn import(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)]) -> Result<usize, StoreError> {
        Store::import(self, namespace, documents)
    }

    fn namespaces(&self) -> Result<Vec<String>, StoreError> {
        Store::namespaces(self)
    }

    fn namespace_exists(&self, namespace: &str) -> Result<bool, StoreError> {
        Store::namespace_exists(self, namespace)
    }

    fn create_collection(&self, namespace: &str, options: CollectionOptions) -> Result<bool, StoreError> {
        Store::create_collection(self, namespace, options)
    }

    fn drop_namespace(&self, namespace: &str) -> Result<bool, StoreError> {
        Store::drop_namespace(self, namespace)
    }

    fn drop_database(&self, database: &str) -> Result<usize, StoreError> {
        Store::drop_database(self, database)
    }

    fn collection_options(&self, namespace: &str) -> Result<CollectionOptions, StoreError> {
        Store::collection_options(self, namespace)
    }

    fn set_collection_options(&self, namespace: &str, options: CollectionOptions) -> Result<(), StoreError> {
        Store::set_collection_options(self, namespace, options)
    }

    fn index_specs(&self, namespace: &str) -> Result<Vec<IndexSpec>, StoreError> {
        Store::index_specs(self, namespace)
    }

    fn add_indexes(&self, namespace: &str, indexes: &[IndexSpec]) -> Result<(), StoreError> {
        Store::add_indexes(self, namespace, indexes)
    }

    fn mark_index_multikey(&self, namespace: &str, name: &str) -> Result<(), StoreError> {
        Store::mark_index_multikey(self, namespace, name)
    }

    fn drop_indexes(&self, namespace: &str, name: &str) -> Result<usize, StoreError> {
        Store::drop_indexes(self, namespace, name)
    }

    fn ttl_monitor_state(&self) -> TtlMonitorState {
        Store::ttl_monitor_state(self)
    }

    fn set_ttl_monitor_sleep_seconds(&self, seconds: u32) {
        Store::set_ttl_monitor_sleep_seconds(self, seconds);
    }

    fn claim_ttl_pass(&self, now_ms: i64) -> bool {
        Store::claim_ttl_pass(self, now_ms)
    }

    fn enable_oplog(&self, namespace: &str, max_bytes: i64) -> Result<(), StoreError> {
        Store::enable_oplog(self, namespace, max_bytes)
    }

    fn oplog_enabled(&self) -> bool {
        Store::oplog_enabled(self)
    }

    fn oplog_namespace(&self) -> Option<String> {
        Store::oplog_namespace(self)
    }

    fn oplog_status(&self) -> Option<(String, i64, i64, i64)> {
        Store::oplog_status(self)
    }

    fn stage_oplog(&self, entry: OplogEntry) {
        Store::stage_oplog(self, entry);
    }

    fn discard_staged_oplog(&self) {
        Store::discard_staged_oplog(self);
    }

    fn commit_oplog(&self, wall_millis: i64) -> Result<Option<(u32, u32)>, StoreError> {
        Store::commit_oplog(self, wall_millis)
    }

    fn begin_transaction(&self, session: &str, txn_number: i64) -> TransactionBegin {
        Store::begin_transaction(self, session, txn_number)
    }

    fn session_write_state(&self, session: &str, txn_number: i64) -> SessionWriteState {
        Store::session_write_state(self, session, txn_number)
    }

    fn transaction_active(&self, session: &str) -> bool {
        Store::transaction_active(self, session)
    }

    fn transaction_write(&self, session: &str, namespace: &str, key: &[u8], value: Option<Vec<u8>>) -> TransactionWrite {
        Store::transaction_write(self, session, namespace, key, value)
    }

    fn transaction_scan(&self, session: &str, namespace: &str) -> Result<Option<Vec<(Vec<u8>, Vec<u8>)>>, StoreError> {
        Store::transaction_scan(self, session, namespace)
    }

    fn transaction_get(&self, session: &str, namespace: &str, key: &[u8]) -> Result<Option<Option<Vec<u8>>>, StoreError> {
        Store::transaction_get(self, session, namespace, key)
    }

    fn transaction_commit(&self, session: &str, txn_number: i64) -> TransactionCommit {
        Store::transaction_commit(self, session, txn_number)
    }

    fn transaction_abort(&self, session: &str, txn_number: i64) -> TransactionAbort {
        Store::transaction_abort(self, session, txn_number)
    }
}
