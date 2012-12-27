//! Iris 的 SQLite 外存适配器。
//!
//! SQL 解析与执行都在 `sqlite` 产品门面（`projects/facades/sqlite`）内；本 crate
//! 只做连接持有、参数/结果类型映射与 Iris 物理计划编排。不引入 `rusqlite`。
//!
//! 内存库已可执行；门面尚未提供的能力仍 fail-closed：文件打开（`Engine` 没有
//! 从既有镜像构造的入口）、事务（`Engine` 没有 `begin` / `commit` / `rollback`）、
//! `last_insert_rowid` 取值。缺口坐标见 `provider.rs` 模块文档。

#![deny(missing_docs)]

mod catalog;
mod execute;
mod migrate;
mod outbox;
mod provider;
mod types;
pub use types::{from_sql_value, from_sql_value_typed, literal_to_sql_value, to_sql_value};

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use crate::{Error, Result};

use database_query::ir::{CommitToken, IrVersion, OutboxRecord, PhysicalPlan};
use odm_types::{
    CapabilitySet, DriftReport, LogicalMigrationPlan, MappingManifest, ObservedCatalog, QueryCaps, Row, RowWrite, TypedLogicalPlan, WriteCaps,
};
use provider::SqliteError;

pub use catalog::adopt_plan;
pub use migrate::{PushReport, apply_push, plan_push};
pub use outbox::AuthorityTxn;
pub use provider::{CatalogColumn, CatalogSnapshot, CatalogTable, RusqliteProvider, SqliteProvider, SqliteValue, StatementResult};

/// 适配器后端标识。
pub const BACKEND_ID: &str = "sqlite";

/// 本 crate 版本标签。
pub const ADAPTER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// SQLite 外存数据源。真正执行等待产品 L3 接续内核。
pub struct SqliteSource {
    provider: Mutex<SqliteProvider>,
    path: Option<PathBuf>,
}

impl std::fmt::Debug for SqliteSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteSource").field("backend", &BACKEND_ID).field("path", &self.path).finish_non_exhaustive()
    }
}

impl SqliteSource {
    /// Capability set for this adapter.
    pub fn capabilities() -> CapabilitySet {
        CapabilitySet {
            backend_id: BACKEND_ID.into(),
            backend_version: ADAPTER_VERSION.into(),
            ir_version_max: IrVersion::PHASE1,
            query: QueryCaps::full(),
            write: WriteCaps::full(),
            budget: odm_types::CompensationBudget::default(),
        }
    }

    /// Open an in-memory SQLite database.
    pub fn open_in_memory() -> Result<Self> {
        Ok(Self { provider: Mutex::new(SqliteProvider::open_in_memory()?), path: None })
    }

    /// Open or create a file-backed SQLite database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let provider = SqliteProvider::open(&path)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(Self { provider: Mutex::new(provider), path: Some(path) })
    }

    /// On-disk path when file-backed.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Active upstream SQLite engine version string.
    pub fn sqlite_version(&self) -> Result<String> {
        let provider = self.provider.lock().expect("sqlite mutex");
        provider.sqlite_version().map_err(Error::from)
    }

    /// Active upstream SQLite source id string.
    pub fn sqlite_source_id(&self) -> Result<String> {
        let provider = self.provider.lock().expect("sqlite mutex");
        provider.source_id().map_err(Error::from)
    }

    /// Inspect the foreign catalog (Adopt Existing input).
    pub fn inspect(&self) -> Result<ObservedCatalog> {
        let provider = self.provider.lock().expect("sqlite mutex");
        catalog::inspect_catalog(&provider)
    }

    /// Build an adopt mapping against a VOS schema document (does not invent semantics).
    pub fn adopt(&self, vos_schema: &str) -> Result<MappingManifest> {
        let catalog = self.inspect()?;
        let document = parse_vos(vos_schema)?;
        Ok(catalog::adopt_plan(&document, &catalog))
    }

    /// Plan a Managed Push from a VOS schema (reviewable logical plan).
    pub fn plan_managed_push(&self, vos_schema: &str) -> Result<LogicalMigrationPlan> {
        let document = parse_vos(vos_schema)?;
        let observed = self.inspect()?;
        migrate::plan_push(&document, &observed)
    }

    /// Apply a previously reviewed Managed Push plan.
    pub fn apply_managed_push(&self, plan: &LogicalMigrationPlan, vos_schema: &str) -> Result<PushReport> {
        let document = parse_vos(vos_schema)?;
        let provider = self.provider.lock().expect("sqlite mutex");
        migrate::apply_push(&provider, plan, &document)
    }

    /// Convenience: plan + apply when the database has no conflicting tables.
    pub fn managed_push(&self, vos_schema: &str) -> Result<PushReport> {
        let plan = self.plan_managed_push(vos_schema)?;
        if plan.destructive {
            return Err(Error::Policy("destructive managed push requires explicit review/apply".into()));
        }
        self.apply_managed_push(&plan, vos_schema)
    }

    /// Drift check: local VOS vs observed catalog.
    pub fn drift(&self, vos_schema: &str) -> Result<DriftReport> {
        let document = parse_vos(vos_schema)?;
        let catalog = self.inspect()?;
        Ok(catalog::drift_report(&document, &catalog))
    }

    /// Execute an Iris physical plan (reads).
    pub fn execute_plan(&self, plan: &PhysicalPlan) -> Result<Vec<Row>> {
        if plan.is_rejected() {
            return Err(Error::Policy(plan.rejection_note().unwrap_or("plan rejected").to_string()));
        }
        let provider = self.provider.lock().expect("sqlite mutex");
        execute::execute_plan(&provider, plan)
    }

    /// Execute a typed logical plan and validate every result cell against the
    /// contract's canonical result types.
    pub fn execute_typed_plan(&self, plan: &TypedLogicalPlan) -> Result<Vec<Row>> {
        if plan.physical.is_rejected() {
            return Err(Error::Policy(plan.physical.rejection_note().unwrap_or("plan rejected").to_string()));
        }
        let provider = self.provider.lock().expect("sqlite mutex");
        execute::execute_typed_plan(&provider, plan)
    }

    /// Insert a row (typed values; SQL stays private).
    pub fn insert(&self, write: &RowWrite) -> Result<()> {
        let provider = self.provider.lock().expect("sqlite mutex");
        execute::insert_row(&provider, write)
    }

    /// Update by primary key.
    pub fn update(&self, write: &RowWrite) -> Result<usize> {
        let provider = self.provider.lock().expect("sqlite mutex");
        execute::update_row(&provider, write)
    }

    /// Delete by primary key.
    pub fn delete(&self, table: &str, primary_key: &str, key: &odm_types::Value) -> Result<usize> {
        let provider = self.provider.lock().expect("sqlite mutex");
        execute::delete_row(&provider, table, primary_key, key)
    }

    /// Begin a transaction.
    pub fn begin(&self) -> Result<()> {
        let provider = self.provider.lock().expect("sqlite mutex");
        provider.begin_immediate().map_err(Error::from)
    }

    /// Commit.
    pub fn commit(&self) -> Result<()> {
        let provider = self.provider.lock().expect("sqlite mutex");
        provider.commit().map_err(Error::from)
    }

    /// Rollback.
    pub fn rollback(&self) -> Result<()> {
        let provider = self.provider.lock().expect("sqlite mutex");
        provider.rollback().map_err(Error::from)
    }

    /// Install authority commit counter + outbox tables (idempotent).
    pub fn ensure_authority_outbox(&self) -> Result<()> {
        let provider = self.provider.lock().expect("sqlite mutex");
        outbox::ensure_schema(&provider)
    }

    /// Current authority [`CommitToken`] (last successful composite commit).
    pub fn current_commit_token(&self) -> Result<CommitToken> {
        let provider = self.provider.lock().expect("sqlite mutex");
        outbox::current_token(&provider)
    }

    /// Authority transaction: business mutations + outbox appends commit atomically.
    ///
    /// Success means authority committed and durable outbox accepted propagation duty --
    /// not that cache/search/vector projections have applied the events.
    pub fn authority_commit<R>(&self, f: impl FnOnce(&mut AuthorityTxn<'_>) -> Result<R>) -> Result<(R, CommitToken)> {
        let provider = self.provider.lock().expect("sqlite mutex");
        outbox::authority_commit(&provider, f)
    }

    /// Poll durable outbox events after a sequence (projector shape).
    pub fn outbox_after(&self, after_seq: u64, limit: usize) -> Result<Vec<OutboxRecord>> {
        let provider = self.provider.lock().expect("sqlite mutex");
        outbox::outbox_after(&provider, after_seq, limit)
    }

    /// Outbox backlog size.
    pub fn outbox_backlog(&self) -> Result<u64> {
        let provider = self.provider.lock().expect("sqlite mutex");
        outbox::outbox_backlog(&provider)
    }
}

impl From<SqliteError> for Error {
    fn from(value: SqliteError) -> Self {
        Self::Sqlite(value.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

fn parse_vos(vos_schema: &str) -> Result<vos::SchemaProjection> {
    odm_types::parse_schema(vos_schema).map_err(|message| Error::Vos(format!("parse schema: {message}")))
}
