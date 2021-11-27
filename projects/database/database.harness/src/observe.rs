//! 观测值模型。
//!
//! 调用方把两边（期望与实测）已经产生的观测结果送进来：行、原始字节、错误
//! 结构或不可用标记。本模块不解码线协议、不判断语义正确性，只规定值的形态。
//!
//! 期望值一侧必须携带 [`TruthProvenance`]：真机引擎、官方语料或冻结基线。
//! 缺失来源的期望值在比对结果里标 `unverified`，不得计为通过。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::corpus::Digest;

/// 期望值的来源。
///
/// 判定器无法在运行时证明期望值真的来自外部引擎；它只强制调用方把来源写进
/// 结构，并在报告里对缺失来源的条目标 `unverified`。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TruthProvenance {
    /// 真实外部引擎产出的值。
    ExternalEngine {
        /// 引擎标识，例如 `better-sqlite3` 或 `mongod`。
        id: String,
        /// 引擎版本，例如 `3.53.2`。
        version: String,
    },
    /// 官方语料中的固定期望。
    OfficialCorpus {
        /// 语料标识。
        id: String,
        /// 语料摘要。
        digest: Digest,
    },
    /// 冻结基线（一次捕获后只读）。
    FrozenBaseline {
        /// 基线标识。
        id: String,
        /// 基线摘要。
        digest: Digest,
    },
}

/// 行集合里的一个单元格。
///
/// 只覆盖数据库行值的公共形态；产品特有的类型由调用方先归一到这里。
/// 外部标签形态（`"null"`、`{"int":7}`、`{"text":"a"}`）保证 JSON 往返
/// 确定，`null` 与字符串不会互相误配。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cell {
    /// SQL `NULL`。
    Null,
    /// 布尔。
    Bool(bool),
    /// 64 位整数。
    Int(i64),
    /// 浮点。
    Float(f64),
    /// 文本。
    Text(String),
    /// 原始字节。
    Blob(Vec<u8>),
}

/// 错误结构。
///
/// 各产品的错误字段集不同（PostgreSQL 的 `severity`/`code`/`message`、MongoDB 的
/// `ok`/`code`/`codeName`/`errmsg`、MySQL 的报文头字段），统一为「字段名 → 稳定
/// 字符串值」。哪些字段允许归一、哪些文案算稳定由调用方决定并负责。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorShape {
    /// 字段名到稳定字符串值的映射，键序固定便于逐字节比较。
    pub fields: BTreeMap<String, String>,
}

/// 一次观测的载荷。
///
/// 行集合与原始字节使用结构体变体（`{"kind":"rows","rows":[...]}`、
/// `{"kind":"wire","bytes":[...]}`）。内部标签只能与 map/struct 载荷组合，
/// 序列（`Vec`）载荷必须给字段名，否则 serde 会拒绝序列化，TS 侧也无法
/// 构造出可穿过 ABI 的形状。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObservedBody {
    /// 行集合：每行是单元格序列。
    Rows {
        /// 行集合，行内为单元格序列。
        rows: Vec<Vec<Cell>>,
    },
    /// 原始字节（线协议报文、文件镜像等）。
    Wire {
        /// 原始字节序列。
        bytes: Vec<u8>,
    },
    /// 错误结构。
    Error(ErrorShape),
    /// 引擎或语句不可用：带错误码与原因，不参与值比对。
    Unavailable {
        /// 稳定错误码。
        code: String,
        /// 人类可读原因。
        reason: String,
    },
}

/// 一侧的观测值：来源（仅期望侧需要）加载荷。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observed {
    /// 期望值一侧必须给出来源；实测侧通常为 `None`。
    pub provenance: Option<TruthProvenance>,
    /// 观测载荷。
    pub body: ObservedBody,
}
