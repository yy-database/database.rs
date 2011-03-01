/// 分布式部署中的租户 / 命名空间边界。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Namespace(pub String);

/// 某个节点拥有的分片标识。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ShardId(pub String);

/// 控制面目录文件的磁盘后缀（格式层合同，取值保持稳定）。
pub const CATALOG_SUFFIX: &str = "yyds";

/// 分布式 KV 分片文件的磁盘后缀（格式层合同，取值保持稳定）。
pub const SHARD_SUFFIX: &str = "yykv";
