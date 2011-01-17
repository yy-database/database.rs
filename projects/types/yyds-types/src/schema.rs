//! VOS schema documents shared with [`vos`](https://github.com/voml/vos-language)
//! (`git`, branch `dev`).
//!
//! YYDS uses **VOS for DDL and query**, same as YYDB. It does not invent a private
//! schema dialect. Catalog truth in `.yyds` files stores versioned VOS source text.
//! Shard records in `.yykv` files hold opaque bytes keyed under that catalog.

use crate::{Error, Result};

/// Canonical remote used by this workspace for shared VOS semantics.
pub const VOS_GIT_DEV: &str = "https://github.com/voml/vos-language.git#branch=dev";

/// Catalog-owned VOS schema document and version (cluster truth).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSchema {
    /// Monotonic schema version inside the catalog.
    pub version: u32,
    /// VOS source text validated before commit.
    pub document: String,
}

/// Validate a schema document before it becomes catalog truth.
///
/// Uses Oak parsing and VOS semantic projection before catalog publication.
pub fn validate_document(document: &str) -> Result<()> {
    if document.contains('\0') {
        return Err(Error::Schema { message: "VOS schema document must not contain NUL bytes".into() });
    }
    vos::validate_schema(document).map(|_| ()).map_err(|message| Error::Schema { message })
}

/// Validate and adopt a catalog schema revision.
pub fn adopt_catalog_schema(version: u32, document: &str) -> Result<CatalogSchema> {
    validate_document(document)?;
    Ok(CatalogSchema { version, document: document.to_string() })
}
