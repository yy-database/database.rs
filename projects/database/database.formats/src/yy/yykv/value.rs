/// Inline payload stored directly inside a shard record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineValue(pub Vec<u8>);

/// 共享对象平面引用。哈希语义与本地 CAS 对齐，但落点由分片拥有。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRef {
    /// Lowercase hex content hash.
    pub hash_hex: String,
}

/// 分片 KV 存贮的记录载荷。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredValue {
    /// Small or hot values kept in the shard index/log.
    Inline(InlineValue),
    /// Large or cold values addressed by content hash.
    Object(ObjectRef),
}

impl StoredValue {
    /// Returns true when the payload is an object reference.
    pub fn is_object(&self) -> bool {
        matches!(self, Self::Object(_))
    }
}
