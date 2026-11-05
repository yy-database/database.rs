//! Machine-readable reasons for corrupt on-disk bytes.
//!
//! [`Error::Corrupt`](crate::Error) keeps a static message so existing call
//! sites stay cheap. A decoder that can classify the failure uses
//! [`Error::CorruptReason`](crate::Error::CorruptReason) instead, which carries
//! this code alongside the message.

/// Why a byte layout was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ReasonCode {
    /// The leading magic does not match the format.
    BadMagic,
    /// The file uses a legacy version that must be recreated rather than read.
    LegacyVersion,
    /// The bytes ended before a required field was read.
    Truncated,
    /// The stored integrity checksum does not match the body.
    ChecksumMismatch,
    /// A tag or discriminator holds a value the format does not define.
    UnknownTag,
    /// Two records claim the same identity.
    DuplicateKey,
    /// Bytes remain after the last declared section.
    TrailingBytes,
}

impl ReasonCode {
    /// Stable lowercase spelling for diagnostics and tests.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BadMagic => "bad_magic",
            Self::LegacyVersion => "legacy_version",
            Self::Truncated => "truncated",
            Self::ChecksumMismatch => "checksum_mismatch",
            Self::UnknownTag => "unknown_tag",
            Self::DuplicateKey => "duplicate_key",
            Self::TrailingBytes => "trailing_bytes",
        }
    }
}

impl std::fmt::Display for ReasonCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
