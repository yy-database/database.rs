//! `iris` 是对象—数据库映射（ODM / Object-Database Mapping）能力域的唯一公开门面。
//!
//! 只按能力再导出，不实现业务逻辑。本域涵盖 ORM、文档向映射、OGM、ONDM 等；
//! 命名与目录职责见 `projects/odm/readme.md`。
//!
//! DDL 作者面通过 `odm.input.{graphql,sql,vos}` 降到统一映射模型。
//!
//! 宿主调用面与历史 `iris` 门面逐字对齐：`iris::host`、`iris::project`、
//! `iris::generator`、`iris::wasm`、`iris::migrate_plan` / `iris::migrate_run`。

#![deny(missing_docs)]

/// 门面版本（与 Cargo 包版本一致）。
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// 宿主接受的合同信封格式。
#[cfg(feature = "types")]
pub const ACCEPTED_CONTRACT_FORMAT: &str = odm_types::ACCEPTED_CONTRACT_FORMAT;

/// 工程配置文档（`iris.von`）。
#[cfg(feature = "types")]
pub use odm_types::IrisProject;

/// 对象映射合同、统一模型、会话、计划与投影状态。
#[cfg(feature = "types")]
pub mod types {
    pub use odm_types::*;
}

/// 工程加载与 schema I/O（与历史 `iris::project` 同形）。
#[cfg(feature = "types")]
pub mod project;

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

/// 宿主会话门面（`HostSession`、`open_*` 与错误信封）。
#[cfg(feature = "host")]
pub mod host {
    pub use odm_adapter::host::*;
}

/// 模型与代码生成。
#[cfg(feature = "generate")]
pub mod generate {
    pub use odm_generate::*;
}

/// 模型与代码生成（历史路径 `iris::generator`）。
#[cfg(feature = "generate")]
pub mod generator {
    pub use odm_generate::*;
}

/// 只读 schema introspection JSON（历史路径 `iris::wasm`）。
#[cfg(feature = "wasm-bridge")]
pub mod wasm;

#[cfg(feature = "migrate")]
mod migrate;

#[cfg(feature = "migrate")]
pub use migrate::{ApplyReport, migrate_apply, migrate_plan, migrate_run, migrate_verify};
