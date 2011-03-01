//! 与 [`vos`](https://github.com/voml/vos-language)（`git` / `dev`）共享的 schema 文档合同。
//!
//! 分布式与嵌入式共用 **VOS 作为 DDL / 查询作者面**，不另发明私有方言。
//! 目录真值存版本化 VOS 源文；分片记录在对应存储后缀下按目录键持有不透明字节。

use crate::cluster::{Error, Result};

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
