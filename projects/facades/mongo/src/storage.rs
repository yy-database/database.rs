//! 命令层唯一的存储入口。
//!
//! 命令语义只写一份，底座差异收敛在 [`MongoStorage`] 的实现里：
//!
//! - 产品壳 `MongoDB` 注入 [`crate::store::Store`]，数据落在 `.yykv` shard，
//!   集合与索引元数据落在 `.yyds` catalog。
//! - `yyds` 侧注入 `KeyValueExecutor` 适配器，数据落在执行器承载的 KV 命名空间。
//!
//! 两个后端都必须满足同一组语义：`_id` 唯一、扫描按编码 `_id` 有序、目录是
//! namespace 存在性的权威来源、事务未提交前只对所属会话可见、oplog 按封顶容量
//! 淘汰。命令层不得感知后端类型，也不得各自携带一份存储实现。

pub use crate::store::{
    CollectionOptions, IndexSpec, InsertReport, SessionState, SessionWriteState, StoreError, TransactionAbort, TransactionBegin,
    TransactionCommit, TransactionWrite, TtlMonitorState,
};

use crate::oplog::OplogEntry;

/// MongoDB 文档存储后端。
///
/// 所有方法都以 namespace 为第一参数，namespace 是 `<database>.<collection>`。
/// 实现必须是线程安全的：同一个实例会被命令层的多个调用共享。
pub trait MongoStorage: Send + Sync {
    /// 读取一个文档，按编码后的 `_id` 定位。
    fn get(&self, namespace: &str, key: &[u8]) -> Option<Vec<u8>>;

    /// 写入一批文档，返回写入数、重复 `_id` 位置与非法位置。
    ///
    /// `ordered` 为真时遇到第一个重复就停止，否则跳过重复继续写。
    fn insert(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)], ordered: bool) -> Result<InsertReport, StoreError>;

    /// 按编码 `_id` 升序读取整个 namespace。
    fn scan(&self, namespace: &str) -> Vec<(Vec<u8>, Vec<u8>)>;

    /// 删除给定的编码 `_id`，返回实际存在的条数。
    fn delete(&self, namespace: &str, keys: &[Vec<u8>]) -> Result<usize, StoreError>;

    /// 覆盖写一批文档，不做重复检查，供 `update` 使用。
    fn replace(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)]) -> Result<usize, StoreError>;

    /// 覆盖写一批文档，不做重复检查，供 `mongodump` 导入使用。
    fn import(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)]) -> Result<usize, StoreError>;

    /// 已注册的 namespace，按字典序排列。
    fn namespaces(&self) -> Vec<String>;

    /// namespace 是否已在目录中注册。
    fn namespace_exists(&self, namespace: &str) -> bool;

    /// 创建集合，已存在时返回 `false`。
    fn create_collection(&self, namespace: &str, options: CollectionOptions) -> Result<bool, StoreError>;

    /// 删除集合：文档、用户索引与集合选项一并删除，返回集合此前是否存在。
    fn drop_namespace(&self, namespace: &str) -> Result<bool, StoreError>;

    /// 删除整个数据库下的集合、索引与选项，返回目录中列出的集合数。
    fn drop_database(&self, database: &str) -> Result<usize, StoreError>;

    /// 读取集合选项，目录无记录时返回默认值。
    fn collection_options(&self, namespace: &str) -> CollectionOptions;

    /// 写入集合选项并注册 namespace。
    fn set_collection_options(&self, namespace: &str, options: CollectionOptions) -> Result<(), StoreError>;

    /// 一个 namespace 的用户索引，按创建顺序排列，不含 `_id_`。
    fn index_specs(&self, namespace: &str) -> Vec<IndexSpec>;

    /// 注册 namespace 并追加索引定义。
    fn add_indexes(&self, namespace: &str, indexes: &[IndexSpec]) -> Result<(), StoreError>;

    /// 删除指定用户索引，`name` 为 `*` 时删除全部用户索引，返回删除条数。
    fn drop_indexes(&self, namespace: &str, name: &str) -> Result<usize, StoreError>;

    /// 当前 TTL 监视器状态。
    fn ttl_monitor_state(&self) -> TtlMonitorState;

    /// 修改 `ttlMonitorSleepSecs`，`0` 按 `mongod` 语义夹到 `1`。
    fn set_ttl_monitor_sleep_seconds(&self, seconds: u32);

    /// 睡眠窗口过去后占用一次监视器扫描，返回本次是否该扫描。
    fn claim_ttl_pass(&self, now_ms: i64) -> bool;

    /// 打开 `local.oplog.rs` 持久化与封顶容量。
    fn enable_oplog(&self, namespace: &str, max_bytes: i64) -> Result<(), StoreError>;

    /// 是否配置了副本集 oplog。
    fn oplog_enabled(&self) -> bool;

    /// 已配置的 oplog namespace。
    fn oplog_namespace(&self) -> Option<String>;

    /// 封顶容量账目：namespace、`max_bytes`、条数、字节数。
    fn oplog_status(&self) -> Option<(String, i64, i64, i64)>;

    /// 暂存一条待提交的 oplog 条目；oplog 未启用时为空操作。
    fn stage_oplog(&self, entry: OplogEntry);

    /// 丢弃命令失败时暂存的条目，避免漏入下一次提交。
    fn discard_staged_oplog(&self);

    /// 按递增时间戳持久化全部暂存条目，返回最后一条的时间戳。
    fn commit_oplog(&self, wall_millis: i64) -> Result<Option<(u32, u32)>, StoreError>;

    /// 为一个逻辑会话开启事务。比会话最新编号更旧或相同的编号会被拒绝。
    fn begin_transaction(&self, session: &str, txn_number: i64) -> TransactionBegin;

    /// 不带 `startTransaction` 的命令，当前会话状态允许它做什么。
    fn session_write_state(&self, session: &str, txn_number: i64) -> SessionWriteState;

    /// 会话当前是否有可用事务。
    fn transaction_active(&self, session: &str) -> bool;

    /// 在事务里暂存一条写入，被其他活跃事务占用时返回冲突。
    fn transaction_write(&self, session: &str, namespace: &str, key: &[u8], value: Option<Vec<u8>>) -> TransactionWrite;

    /// 事务视角下的整个 namespace；会话无活跃事务时返回 `None`，调用方读已提交数据。
    fn transaction_scan(&self, session: &str, namespace: &str) -> Option<Vec<(Vec<u8>, Vec<u8>)>>;

    /// 事务视角下的单个文档。外层 `None` 表示该键未被暂存，调用方读已提交值。
    fn transaction_get(&self, session: &str, namespace: &str, key: &[u8]) -> Option<Option<Vec<u8>>>;

    /// 提交事务；被其他事务改过的文档会让本次提交冲突。
    fn transaction_commit(&self, session: &str, txn_number: i64) -> TransactionCommit;

    /// 放弃事务；已提交或已放弃的编号会按各自状态返回。
    fn transaction_abort(&self, session: &str, txn_number: i64) -> TransactionAbort;
}
