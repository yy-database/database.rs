//! `odm-generate`：生成模型、Dejavu 渲染与多语言发射器。
//!
//! 模板与 GenerationModel 由本包统一维护；各语言宿主只消费渲染结果。

#![deny(missing_docs)]

pub mod emit;

pub use emit::*;
