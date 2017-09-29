//! MySQL 实例装配：`.yyds` 目录、`.yykv` 分片、持久化 SQL schema 与节点租约。
//!
//! 握手字节、命令分发、`OK`/`ERR`/`EOF` 帧与 SQL 运行期决策都在
//! [`super::connection`] 与 [`super::service`] 内。本模块只把数据目录的四件产物
//! 打开并交给 [`MysqlService`]，不解释任何协议语义。
//!
//! 复用规则：在增加命令或帧语义之前先搜 `database.server::yyds::mysql`。命中就接线，
//! 不写第二份实现。

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use database_cluster::{
    catalog::Catalog,
    control::{NodeIdentity, NodeLease},
};
use database_execution::cluster::{KeyValueExecutor, LocalExecutor};
use database_query::SqlSchemaRegistry;
use database_storage::cluster_kv::FileShard;
use database_types::cluster::{ShardEpoch, ShardId};

use super::service::MysqlService;

/// MySQL 数据目录使用的单节点分片 id。
pub const LOCAL_SHARD_ID: &str = "local-0";
/// 数据目录内的 `.yyds` 目录文件名。
pub const CATALOG_FILE: &str = "catalog.yyds";
/// 数据目录内的持久化 SQL schema 文件名。
pub const SCHEMA_FILE: &str = "sql-schema.v0.json";

fn map_error(context: &str, error: impl std::fmt::Display) -> String {
    format!("{context}: {error}")
}

/// 把单节点分片路由发布进 `.yyds` 目录。
///
/// 同一纪元与同一分片列表重复发布是无操作，重启可以直接复用同一数据目录。
pub fn publish_routing(catalog_path: impl AsRef<Path>, epoch: u64, shards: &[String]) -> Result<(), String> {
    let mut catalog = Catalog::open(catalog_path).map_err(|error| map_error("open catalog", error))?;
    let wanted: Vec<ShardId> = shards.iter().cloned().map(ShardId).collect();
    if let Some(existing) = catalog.routing().map_err(|error| map_error("read catalog routing", error))? {
        if existing.epoch().0 == epoch && existing.shards() == wanted.as_slice() {
            return Ok(());
        }
    }
    catalog.publish_routing(ShardEpoch(epoch), wanted).map_err(|error| map_error("publish catalog routing", error))?;
    catalog.flush().map_err(|error| map_error("flush catalog", error))
}

/// 为同一数据目录抢占独占的节点租约。
pub fn acquire_lease(data_dir: impl AsRef<Path>, cluster_id: &str, node_id: &str) -> Result<NodeLease, String> {
    let identity = NodeIdentity::new(cluster_id.to_string(), node_id.to_string()).map_err(|error| map_error("node identity", error))?;
    NodeLease::acquire(data_dir, identity).map_err(|error| map_error("acquire node lease", error))
}

/// 打开由目录分片支撑的本地 KV 执行器。
///
/// 目录必须已经发布路由。没有静默的 `local-0` 回落：目录缺失或未发布路由都是错误。
pub fn open_executor(data_dir: impl AsRef<Path>, catalog_path: impl AsRef<Path>) -> Result<Arc<dyn KeyValueExecutor>, String> {
    let directory = data_dir.as_ref();
    let catalog = Catalog::open(catalog_path.as_ref()).map_err(|error| map_error("open catalog", error))?;
    let routing = catalog
        .routing()
        .map_err(|error| map_error("read catalog routing", error))?
        .ok_or_else(|| String::from("catalog has no published routing; publish routing first"))?;
    let mut shards = HashMap::new();
    for shard_id in routing.shards() {
        let path = directory.join(format!("{}.yykv", shard_id.0));
        let shard = FileShard::open(&path).map_err(|error| map_error("open .yykv shard", error))?;
        shards.insert(shard_id.clone(), shard);
    }
    let executor = LocalExecutor::new(routing, shards, Vec::new()).map_err(|error| map_error("build local executor", error))?;
    Ok(Arc::new(executor))
}

/// 打开持久化 SQL schema 注册表，缺失时创建空状态。
pub fn open_schema(path: impl Into<PathBuf>) -> Result<Arc<SqlSchemaRegistry>, String> {
    SqlSchemaRegistry::open(path).map(Arc::new).map_err(|error| format!("open SQL schema: {}", error.message))
}

/// 装配好、可停止的 MySQL 协议实例，内部挂载上游监听器。
#[derive(Debug)]
pub struct MysqlInstance {
    inner: Option<MysqlService>,
}

impl MysqlInstance {
    /// 在 `127.0.0.1:<port>` 上启动上游监听器。
    ///
    /// 端口传 `0` 时由操作系统分配空闲端口，绑定后的地址通过
    /// [`MysqlInstance::address`] 读回。
    pub fn start(
        lease: Arc<NodeLease>,
        executor: Arc<dyn KeyValueExecutor>,
        schema: Arc<SqlSchemaRegistry>,
        port: u16,
    ) -> Result<Self, String> {
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let service =
            MysqlService::start_with_registry(lease, address, executor, schema).map_err(|error| map_error("start MySQL service", error))?;
        Ok(Self { inner: Some(service) })
    }

    /// 绑定地址，端口为 `0` 时含系统分配的实际端口。
    pub fn address(&self) -> Option<SocketAddr> {
        self.inner.as_ref().map(MysqlService::address)
    }

    /// 接收线程是否已经退出。
    pub fn is_finished(&self) -> bool {
        self.inner.as_ref().is_none_or(MysqlService::is_finished)
    }

    /// 停止监听器并等待其工作线程退出。
    pub fn stop(&mut self) -> Result<(), String> {
        let service = self.inner.take().ok_or_else(|| String::from("MySQL service is not running"))?;
        service.stop().map_err(|error| map_error("stop MySQL service", error))
    }
}
