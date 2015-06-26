//! Iris 的 MySQL 外存适配器。
//!
//! 线协议经 [`database_client::mysql::Session`]（与 YYDS MySQL 伪装服务同帧）。
//! 不依赖第三方 `mysql` crate。属 L1 服务模拟，不接管对方数据目录。

#![deny(missing_docs)]

mod catalog;
mod execute;
mod migrate;
mod schema_map;
mod uuid_util;
pub use uuid_util::{try_parse_uuid_bytes, uuid_bytes_to_str};

use std::{collections::HashSet, sync::Mutex, time::Duration};

use crate::{Error, Result};

pub use database_client::mysql::Session;
use database_query::ir::{IrVersion, PhysicalPlan};
use odm_types::{CapabilitySet, DriftReport, LogicalMigrationPlan, MappingManifest, ObservedCatalog, QueryCaps, Row, RowWrite, WriteCaps};
use vos::SchemaProjection;

pub use catalog::{adopt_plan, classify_type};
pub use migrate::{PushReport, apply_push, plan_push};
pub use schema_map::collect_uuid_fields;

/// Connection handle for generated `Txn` (adapter-owned wire session).
pub type PooledConn = Session;

/// Adapter identifier.
pub const BACKEND_ID: &str = "mysql";

/// Adapter crate version label.
pub const ADAPTER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// MySQL foreign datasource backed by the gateway wire client.
pub struct MysqlSource {
    url: String,
    session: Mutex<Session>,
    uuid_fields: HashSet<(String, String)>,
}

impl std::fmt::Debug for MysqlSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlSource").field("backend", &BACKEND_ID).finish_non_exhaustive()
    }
}

impl MysqlSource {
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

    /// Connect using a mysql URL (`mysql://user:pass@host:3306/db`).
    ///
    /// The gateway wire client has no handshake / `COM_QUERY` extraction in
    /// this dev line, so every execute path would fail after a successful
    /// open. The session is refused here instead of exposing a half-open
    /// backend.
    pub fn connect(url: &str) -> Result<Self> {
        Err(Error::Unsupported(format!(
            "mysql session refused for `{url}` — gateway wire client handshake/COM_QUERY is not available in this dev line (use sqlite)"
        )))
    }

    /// Register VOS schema so `uuid` columns encode/decode as MySQL `BINARY(16)`.
    pub fn with_vos_schema(mut self, vos_schema: &str) -> Result<Self> {
        let document = parse_vos(vos_schema)?;
        self.uuid_fields = collect_uuid_fields(&document);
        Ok(self)
    }

    /// Register uuid columns from generated bindings without re-parsing `.iris`.
    pub fn with_uuid_fields<I, T, F>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = (T, F)>,
        T: Into<String>,
        F: Into<String>,
    {
        self.uuid_fields = fields.into_iter().map(|(table, field)| (table.into(), field.into())).collect();
        self
    }

    /// Register uuid columns from a parsed VOS schema projection.
    pub fn with_schema_document(mut self, document: &SchemaProjection) -> Self {
        self.uuid_fields = collect_uuid_fields(document);
        self
    }

    fn with_session<R>(&self, f: impl FnOnce(&mut Session) -> Result<R>) -> Result<R> {
        let mut guard = self.session.lock().map_err(|_| Error::Policy("mysql session mutex poisoned".into()))?;
        f(&mut guard)
    }

    /// Connectivity smoke.
    pub fn ping(&self) -> Result<()> {
        self.with_session(|conn| conn.ping().map_err(Error::Wire))
    }

    /// Inspect the foreign catalog (Adopt Existing input).
    pub fn inspect(&self) -> Result<ObservedCatalog> {
        self.with_session(catalog::inspect_catalog)
    }

    /// Build an adopt mapping against a VOS schema document.
    pub fn adopt(&self, vos_schema: &str) -> Result<MappingManifest> {
        let catalog = self.inspect()?;
        let document = parse_vos(vos_schema)?;
        Ok(catalog::adopt_plan(&document, &catalog))
    }

    /// Plan a Managed Push from a VOS schema.
    pub fn plan_managed_push(&self, vos_schema: &str) -> Result<LogicalMigrationPlan> {
        let document = parse_vos(vos_schema)?;
        let observed = self.inspect()?;
        migrate::plan_push(&document, &observed)
    }

    /// Apply a previously reviewed Managed Push plan.
    pub fn apply_managed_push(&self, plan: &LogicalMigrationPlan, vos_schema: &str) -> Result<PushReport> {
        let document = parse_vos(vos_schema)?;
        self.with_session(|conn| migrate::apply_push(conn, plan, &document))
    }

    /// Convenience: plan + apply when non-destructive.
    pub fn managed_push(&self, vos_schema: &str) -> Result<PushReport> {
        let plan = self.plan_managed_push(vos_schema)?;
        if plan.destructive {
            return Err(Error::Policy("destructive managed push requires explicit review/apply".into()));
        }
        self.apply_managed_push(&plan, vos_schema)
    }

    /// Drift report against a VOS schema.
    pub fn drift(&self, vos_schema: &str) -> Result<DriftReport> {
        let document = parse_vos(vos_schema)?;
        let catalog = self.inspect()?;
        Ok(catalog::drift_report(&document, &catalog))
    }

    /// Execute a physical plan.
    pub fn execute_plan(&self, plan: &PhysicalPlan) -> Result<Vec<Row>> {
        self.with_session(|conn| execute::execute_plan(conn, plan, &self.uuid_fields))
    }

    /// Execute a physical plan on an explicit session.
    pub fn execute_plan_on(&self, conn: &mut Session, plan: &PhysicalPlan) -> Result<Vec<Row>> {
        execute::execute_plan(conn, plan, &self.uuid_fields)
    }

    /// Insert helper used by generated bindings.
    pub fn insert_on(&self, conn: &mut Session, write: &RowWrite) -> Result<()> {
        execute::insert_row(conn, write, &self.uuid_fields)
    }

    /// Update helper used by generated bindings.
    pub fn update_on(&self, conn: &mut Session, write: &RowWrite) -> Result<u64> {
        execute::update_row(conn, write, &self.uuid_fields)
    }

    /// Delete helper used by generated bindings.
    pub fn delete_on(&self, conn: &mut Session, table: &str, primary_key: &str, key: &odm_types::Value) -> Result<u64> {
        execute::delete_row(conn, table, primary_key, key, &self.uuid_fields)
    }

    /// Insert helper used by generated bindings.
    pub fn insert(&self, write: &RowWrite) -> Result<()> {
        self.with_session(|conn| execute::insert_row(conn, write, &self.uuid_fields))
    }

    /// Update helper used by generated bindings.
    pub fn update(&self, write: &RowWrite) -> Result<u64> {
        self.with_session(|conn| execute::update_row(conn, write, &self.uuid_fields))
    }

    /// Delete helper used by generated bindings.
    pub fn delete(&self, table: &str, primary_key: &str, key: &odm_types::Value) -> Result<u64> {
        self.with_session(|conn| execute::delete_row(conn, table, primary_key, key, &self.uuid_fields))
    }

    /// Begin a transaction on a dedicated wire session.
    pub fn begin_transaction(&self) -> Result<MysqlTransaction> {
        let mut conn = Session::connect(&self.url, Duration::from_secs(5)).map_err(Error::Wire)?;
        conn.execute("START TRANSACTION").map_err(Error::Wire)?;
        Ok(MysqlTransaction { conn, uuid_fields: self.uuid_fields.clone(), open: true })
    }

    /// Run a closure inside a transaction.
    pub fn transaction<R>(&self, f: impl FnOnce(&mut Session) -> Result<R>) -> Result<R> {
        let mut txn = self.begin_transaction()?;
        match f(&mut txn.conn) {
            Ok(value) => {
                txn.commit()?;
                Ok(value)
            }
            Err(err) => {
                let _ = txn.rollback();
                Err(err)
            }
        }
    }

    /// Run a closure and always roll back (test helper).
    pub fn with_rollback<R>(&self, f: impl FnOnce(&mut Session) -> Result<R>) -> Result<R> {
        let mut txn = self.begin_transaction()?;
        let result = f(&mut txn.conn);
        let _ = txn.rollback();
        result
    }
}

/// Open MySQL transaction handle.
pub struct MysqlTransaction {
    conn: Session,
    uuid_fields: HashSet<(String, String)>,
    open: bool,
}

impl MysqlTransaction {
    /// Execute a physical plan inside the transaction.
    pub fn execute_plan(&mut self, plan: &PhysicalPlan) -> Result<Vec<Row>> {
        execute::execute_plan(&mut self.conn, plan, &self.uuid_fields)
    }

    /// Commit and release.
    pub fn commit(&mut self) -> Result<()> {
        self.conn.execute("COMMIT").map_err(Error::Wire)?;
        self.open = false;
        Ok(())
    }

    /// Roll back and release.
    pub fn rollback(&mut self) -> Result<()> {
        self.conn.execute("ROLLBACK").map_err(Error::Wire)?;
        self.open = false;
        Ok(())
    }
}

impl Drop for MysqlTransaction {
    fn drop(&mut self) {
        if self.open {
            let _ = self.conn.execute("ROLLBACK");
        }
    }
}

fn parse_vos(vos_schema: &str) -> Result<SchemaProjection> {
    odm_types::parse_schema(vos_schema).map_err(|message| Error::Vos(format!("parse schema: {message}")))
}
