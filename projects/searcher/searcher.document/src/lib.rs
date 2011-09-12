//! 可索引文档模型与写入操作。
//!
//! 只接收已抽取字段；不实现抓取、robots、frontier 或页面抽取。

#![deny(missing_docs)]

use searcher_types::{CollectionId, DocumentId, DocumentRevision};
use serde::{Deserialize, Serialize};

/// 字段值。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldValue {
    /// 可分词文本。
    Text(String),
    /// 精确关键字。
    Keyword(String),
    /// 整数。
    Integer(i64),
    /// 浮点。
    Float(f64),
    /// 布尔。
    Boolean(bool),
    /// 空值。
    Null,
}

/// 单个字段。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    /// 字段名。
    pub name: String,
    /// 字段值。
    pub value: FieldValue,
}

/// 进入索引前的显式文档。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDocument {
    /// 集合。
    pub collection: CollectionId,
    /// 文档身份。
    pub id: DocumentId,
    /// 外部版本。
    pub revision: DocumentRevision,
    /// 字段集合。
    pub fields: Vec<Field>,
}

/// 文档变更。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DocumentOp {
    /// 写入或覆盖。
    Put(IndexDocument),
    /// 删除。
    Delete {
        /// 集合。
        collection: CollectionId,
        /// 文档身份。
        id: DocumentId,
        /// 删除也必须带 revision。
        revision: DocumentRevision,
    },
}
