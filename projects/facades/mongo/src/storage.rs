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
    CollectionOptions, INDEX_NAMESPACE_MARKER, IndexSpec, InsertReport, SessionState, SessionWriteState, StoreError, TransactionAbort,
    TransactionBegin, TransactionCommit, TransactionWrite, TtlMonitorState, index_namespace, is_index_namespace,
};

use crate::oplog::OplogEntry;

/// MongoDB 文档存储后端。
///
/// 所有方法都以 namespace 为第一参数，namespace 是 `<database>.<collection>`。
/// 实现必须是线程安全的：同一个实例会被命令层的多个调用共享。
pub trait MongoStorage: Send + Sync {
    /// 读取一个文档，按编码后的 `_id` 定位。
    ///
    /// 读取失败必须作为 `Err` 上报：把失败当成「文档不存在」会让客户端看到
    /// 一份静默缺失的数据。
    fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError>;

    /// 原子条件写：仅当 `_id` 尚不存在时写入一条文档，返回是否落地。
    ///
    /// `_id` 唯一性只能由这条原语保证。实现必须使用底座的原子条件写，不得
    /// 先探测再写；否则并发插入同一 `_id` 会同时成功。
    fn insert_if_absent(&self, namespace: &str, key: Vec<u8>, value: Vec<u8>) -> Result<bool, StoreError>;

    /// 写入一批文档，返回写入数、重复 `_id` 位置与非法位置。
    ///
    /// 默认实现逐条走 [`Self::insert_if_absent`]，因此并发下也不会出现两个
    /// 相同 `_id`。`ordered` 为真时遇到第一个重复就停止，否则跳过重复继续写。
    /// 后端可以覆盖为一次批量提交，但必须保持同样的条件写语义。
    fn insert(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)], ordered: bool) -> Result<InsertReport, StoreError> {
        let mut report = InsertReport::default();
        for (position, (key, value)) in documents.iter().enumerate() {
            if self.insert_if_absent(namespace, key.clone(), value.clone())? {
                report.inserted += 1;
            }
            else {
                report.duplicates.push(position);
                if ordered {
                    break;
                }
            }
        }
        Ok(report)
    }

    /// 按编码 `_id` 升序读取整个 namespace。
    ///
    /// 扫描失败必须作为 `Err` 上报：把失败当成空集合会让查询静默返回不完整
    /// 的结果。
    fn scan(&self, namespace: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError>;

    /// 删除给定的编码 `_id`，返回实际存在的条数。
    fn delete(&self, namespace: &str, keys: &[Vec<u8>]) -> Result<usize, StoreError>;

    /// 覆盖写一批文档，不做重复检查，供 `update` 使用。
    fn replace(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)]) -> Result<usize, StoreError>;

    /// 扫描保留命名空间下以 `prefix` 开头的键值对。
    ///
    /// 索引项与文档共用同一 shard，只是命名空间不进入目录，因此这里既没有
    /// 第二套存储后端，也不改变 `.yykv` 的字节布局。默认实现整段扫描后按
    /// 前缀过滤，后端可以覆盖成原生前缀扫描。
    fn scan_reserved(&self, namespace: &str, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        Ok(self.scan(namespace)?.into_iter().filter(|(key, _)| key.starts_with(prefix)).collect())
    }

    /// 在保留命名空间下做有界区间扫描。
    ///
    /// 语义与 [`Self::scan_reserved`] 相同，但只返回键严格大于 `after`（不含）
    /// 且严格小于 `before`（不含）的条目，`limit` 为 `0` 表示不设上限。
    /// 默认实现先整段取出再裁剪，能覆盖的后端应下推到原生游标，避免为了
    /// 早停而把整段索引前缀读进内存。
    fn scan_reserved_range(
        &self,
        namespace: &str,
        prefix: &[u8],
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let mut entries = self.scan_reserved(namespace, prefix)?;
        entries.retain(|(key, _)| after.is_none_or(|after| key.as_slice() > after) && before.is_none_or(|before| key.as_slice() < before));
        if limit > 0 {
            entries.truncate(limit);
        }
        Ok(entries)
    }

    /// 写入保留命名空间，返回写入条数。
    ///
    /// 实现不得把该命名空间登记进目录：索引命名空间只承载键值，不参与
    /// `listCollections` / `listDatabases`，也不出现在 `drop_namespace` 的目录账目里。
    fn put_reserved(&self, namespace: &str, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<usize, StoreError>;

    /// 判断保留命名空间是否至少有一条条目。
    ///
    /// 索引项为空而集合非空只可能来自旧目录未重建或中断的删除，查询层据此
    /// 退回集合扫描，避免把「索引里没有」误判成「数据不存在」。
    fn reserved_has_entries(&self, namespace: &str) -> Result<bool, StoreError> {
        Ok(!self.scan_reserved(namespace, b"")?.is_empty())
    }

    /// 清空一个保留命名空间，返回删除条数。
    fn clear_reserved(&self, namespace: &str) -> Result<usize, StoreError> {
        let keys: Vec<Vec<u8>> = self.scan_reserved(namespace, b"")?.into_iter().map(|(key, _)| key).collect();
        if keys.is_empty() { Ok(0) } else { self.delete(namespace, &keys) }
    }

    /// 覆盖写一批文档，不做重复检查，供 `mongodump` 导入使用。
    fn import(&self, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)]) -> Result<usize, StoreError>;

    /// 已注册的 namespace，按字典序排列。
    fn namespaces(&self) -> Result<Vec<String>, StoreError>;

    /// namespace 是否已在目录中注册。
    fn namespace_exists(&self, namespace: &str) -> Result<bool, StoreError>;

    /// 创建集合，已存在时返回 `false`。
    fn create_collection(&self, namespace: &str, options: CollectionOptions) -> Result<bool, StoreError>;

    /// 删除集合：文档、用户索引与集合选项一并删除，返回集合此前是否存在。
    fn drop_namespace(&self, namespace: &str) -> Result<bool, StoreError>;

    /// 删除整个数据库下的集合、索引与选项，返回目录中列出的集合数。
    fn drop_database(&self, database: &str) -> Result<usize, StoreError>;

    /// 读取集合选项，目录无记录时返回默认值。
    fn collection_options(&self, namespace: &str) -> Result<CollectionOptions, StoreError>;

    /// 写入集合选项并注册 namespace。
    fn set_collection_options(&self, namespace: &str, options: CollectionOptions) -> Result<(), StoreError>;

    /// 一个 namespace 的用户索引，按创建顺序排列，不含 `_id_`。
    fn index_specs(&self, namespace: &str) -> Result<Vec<IndexSpec>, StoreError>;

    /// 注册 namespace 并追加索引定义。
    fn add_indexes(&self, namespace: &str, indexes: &[IndexSpec]) -> Result<(), StoreError>;

    /// 把一个索引标记为 multikey（索引字段出现过数组值）。
    ///
    /// 标记一旦置上就不撤销：数组元素分布决定了两侧范围谓词在 multikey 上
    /// 不能只按同一元素取值，查询层需要据此退回集合扫描。后端若无法持久化
    /// 该标记，必须返回 `Err`，不能假装成功。
    fn mark_index_multikey(&self, namespace: &str, name: &str) -> Result<(), StoreError> {
        let _ = (namespace, name);
        Err(StoreError::Catalog("store does not persist index metadata".to_string()))
    }

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
    fn transaction_scan(&self, session: &str, namespace: &str) -> Result<Option<Vec<(Vec<u8>, Vec<u8>)>>, StoreError>;

    /// 事务视角下的单个文档。外层 `None` 表示该键未被暂存，调用方读已提交值。
    fn transaction_get(&self, session: &str, namespace: &str, key: &[u8]) -> Result<Option<Option<Vec<u8>>>, StoreError>;

    /// 提交事务；被其他事务改过的文档会让本次提交冲突。
    fn transaction_commit(&self, session: &str, txn_number: i64) -> TransactionCommit;

    /// 放弃事务；已提交或已放弃的编号会按各自状态返回。
    fn transaction_abort(&self, session: &str, txn_number: i64) -> TransactionAbort;
}
