/// 分布式部署中的租户 / 命名空间边界。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Namespace(pub String);

/// 某个节点拥有的分片标识。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ShardId(pub String);

impl ShardId {
    /// Validates and builds a shard identifier.
    ///
    /// A shard id names one file stem, so it must be a single non-empty path
    /// segment: `.` and `..` are rejected along with `/` and `\`. Callers that
    /// read the id from disk or from a catalog use this constructor instead of
    /// joining raw text to a directory.
    pub fn try_new(value: impl Into<String>) -> Result<Self, super::Error> {
        let value = value.into();
        if value.is_empty() || value == "." || value == ".." || value.contains(['/', '\\']) {
            return Err(super::Error::InvalidShardId { value });
        }
        Ok(Self(value))
    }
}

/// 控制面目录文件的磁盘后缀（格式层合同，取值保持稳定）。
pub const CATALOG_SUFFIX: &str = "yyds";

/// 分布式 KV 分片文件的磁盘后缀（格式层合同，取值保持稳定）。
pub const SHARD_SUFFIX: &str = "yykv";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_id_rejects_path_like_names() {
        for value in ["", ".", "..", "a/b", "a\\b", "/abs"] {
            assert!(matches!(ShardId::try_new(value), Err(super::super::Error::InvalidShardId { .. })));
        }
    }

    #[test]
    fn shard_id_accepts_a_single_segment() {
        assert_eq!(ShardId::try_new("shard-1").expect("valid").0, "shard-1");
    }
}
