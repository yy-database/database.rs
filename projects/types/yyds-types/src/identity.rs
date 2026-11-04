/// Opaque tenant / namespace boundary for a distributed YYDS deployment.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Namespace(pub String);

/// Identifies one `.yykv` shard owned by a node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardId(pub String);

/// Catalog file suffix for the distributed control plane (not a `.yydb` file).
pub const CATALOG_SUFFIX: &str = "yyds";

/// Shard file suffix for the distributed KV plane.
pub const SHARD_SUFFIX: &str = "yykv";
