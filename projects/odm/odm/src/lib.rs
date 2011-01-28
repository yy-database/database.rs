//! `odm` 是对象—数据库映射（ODM / Object-Database Mapping）能力域的唯一公开门面。
//!
//! 只按能力再导出，不实现业务逻辑。本域涵盖 ORM、文档向映射、OGM、ONDM 等；
//! 命名与目录职责见 `projects/odm/readme.md`。
//!
//! DDL 作者面通过 `odm.input.{graphql,sql,vos}` 降到统一映射模型。

#![deny(missing_docs)]

/// 对象映射合同、统一模型、会话、计划与投影状态。
#[cfg(feature = "types")]
pub mod types {
    pub use odm_types::*;
}

/// GraphQL SDL → 统一映射模型。
#[cfg(feature = "input-graphql")]
pub mod input_graphql {
    pub use odm_input_graphql::*;
}

/// SQL DDL → 统一映射模型。
#[cfg(feature = "input-sql")]
pub mod input_sql {
    pub use odm_input_sql::*;
}

/// VOS DDL → 统一映射模型。
#[cfg(feature = "input-vos")]
pub mod input_vos {
    pub use odm_input_vos::*;
}

/// 外存与本地数据源适配。
#[cfg(feature = "adapter")]
pub mod adapter {
    pub use odm_adapter::*;
}

/// 模型与代码生成。
#[cfg(feature = "generate")]
pub mod generate {
    pub use odm_generate::*;
}
