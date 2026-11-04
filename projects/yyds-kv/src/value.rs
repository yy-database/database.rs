/// Inline payload stored directly inside a shard record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineValue(pub Vec<u8>);

/// Reference into the shared object plane. Hash semantics align with YYDB CAS, but
/// placement is shard-owned rather than single-file `.yydb` rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRef {
    /// Lowercase hex content hash.
    pub hash_hex: String,
}

/// Record payload stored by `yyds-kv`.
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
