//! Iris 的 PostgreSQL 线协议适配器。
//!
//! 经 [`database_client::pgsql::Session`] 说 protocol-v3（与 YYDS PostgreSQL
//! 伪装服务同帧）。网关背后是 YYDS 存储（`.yyds` / `.yykv`），不是对方数据目录。
//! 不依赖第三方 `postgres` / `r2d2`。属 L1 服务模拟。

#![deny(missing_docs)]

mod catalog;
mod execute;
mod migrate;
mod outbox;

use std::{sync::Mutex, time::Duration};

use crate::{Error, Result};

pub use database_client::pgsql::Session;
use database_query::ir::{CommitToken, IrVersion, OutboxRecord, PhysicalPlan};
use odm_types::{CapabilitySet, DriftReport, LogicalMigrationPlan, MappingManifest, ObservedCatalog, QueryCaps, Row, RowWrite, WriteCaps};

pub use catalog::{adopt_plan, classify_type};
pub use migrate::{PushReport, apply_push, plan_push};
pub use outbox::AuthorityTxn;

/// Adapter identifier.
pub const BACKEND_ID: &str = "postgres";

/// Adapter crate version label.
pub const ADAPTER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// PostgreSQL-dialect datasource backed by the YYDS gateway wire client.
pub struct PostgresSource {
    endpoint: String,
    session: Mutex<Session>,
}

impl std::fmt::Debug for PostgresSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSource").field("backend", &BACKEND_ID).finish_non_exhaustive()
    }
}

impl PostgresSource {
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

    /// Connect using a `postgres://` URL or libpq-style string.
    pub fn connect(endpoint: &str) -> Result<Self> {
        Self::connect_with_timeout(endpoint, Duration::from_secs(5))
    }

    /// Connect with an explicit timeout.
    pub fn connect_with_timeout(endpoint: &str, timeout: Duration) -> Result<Self> {
        let session = Session::connect(endpoint, timeout).map_err(Error::Wire)?;
        Ok(Self { endpoint: endpoint.to_owned(), session: Mutex::new(session) })
    }

    /// Connect with pool-timeout alias kept for call-site compatibility.
    pub fn connect_with_pool_timeout(endpoint: &str, timeout: Duration) -> Result<Self> {
        Self::connect_with_timeout(endpoint, timeout)
    }

    /// Historical pool metric — wire client is single-session.
    pub fn pool_connections(&self) -> u32 {
        1
    }

    fn with_session<R>(&self, f: impl FnOnce(&mut Session) -> Result<R>) -> Result<R> {
        let mut guard = self.session.lock().map_err(|_| Error::Policy("postgres session mutex poisoned".into()))?;
        f(&mut guard)
    }

    /// Inspect the dialect catalog via the gateway.
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
        self.with_session(|session| migrate::apply_push(session, plan, &document))
    }

    /// Convenience: plan + apply when non-destructive.
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
        self.with_session(|session| execute::execute_plan(session, plan))
    }

    /// Begin a transaction that owns a dedicated wire session.
    pub fn begin_transaction(&self) -> Result<PostgresTransaction> {
        let mut session = Session::connect(&self.endpoint, Duration::from_secs(5)).map_err(Error::Wire)?;
        session.execute("BEGIN").map_err(Error::Wire)?;
        Ok(PostgresTransaction { session, open: true })
    }

    /// Run `f` on a checked-out session.
    pub fn with_connection<R>(&self, f: impl FnOnce(&mut Session) -> Result<R>) -> Result<R> {
        self.with_session(f)
    }

    /// Run `f` inside a transaction.
    pub fn transaction<R>(&self, f: impl FnOnce(&mut Session) -> Result<R>) -> Result<R> {
        let mut txn = self.begin_transaction()?;
        match f(&mut txn.session) {
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

    /// Ensure outbox schema exists.
    pub fn ensure_outbox_schema(&self) -> Result<()> {
        self.with_session(outbox::ensure_schema)
    }

    /// Current outbox token.
    pub fn current_outbox_token(&self) -> Result<CommitToken> {
        self.with_session(outbox::current_token)
    }

    /// Authority commit with outbox.
    pub fn authority_commit<R>(&self, f: impl FnOnce(&mut Session) -> Result<R>) -> Result<(R, CommitToken)> {
        self.with_session(|session| outbox::authority_commit(session, f))
    }

    /// Read outbox after a sequence.
    pub fn outbox_after(&self, after_seq: u64, limit: usize) -> Result<Vec<OutboxRecord>> {
        self.with_session(|session| outbox::outbox_after(session, after_seq, limit))
    }

    /// Outbox backlog size.
    pub fn outbox_backlog(&self) -> Result<u64> {
        self.with_session(outbox::outbox_backlog)
    }
}

/// Open transaction that owns one PostgreSQL wire session.
pub struct PostgresTransaction {
    session: Session,
    open: bool,
}

impl PostgresTransaction {
    /// Execute a physical plan inside the transaction.
    pub fn execute_plan(&mut self, plan: &PhysicalPlan) -> Result<Vec<Row>> {
        execute::execute_plan(&mut self.session, plan)
    }

    /// Insert helper for generated bindings.
    pub fn insert(&mut self, write: &RowWrite) -> Result<()> {
        execute::insert_row(&mut self.session, write)
    }

    /// Commit and release the session.
    pub fn commit(&mut self) -> Result<()> {
        self.session.execute("COMMIT").map_err(Error::Wire)?;
        self.open = false;
        Ok(())
    }

    /// Roll back and release the session.
    pub fn rollback(&mut self) -> Result<()> {
        self.session.execute("ROLLBACK").map_err(Error::Wire)?;
        self.open = false;
        Ok(())
    }
}

impl Drop for PostgresTransaction {
    fn drop(&mut self) {
        if self.open {
            let _ = self.session.execute("ROLLBACK");
        }
    }
}

fn parse_vos(vos_schema: &str) -> Result<vos::SchemaProjection> {
    odm_types::parse_schema(vos_schema).map_err(|message| Error::Vos(format!("parse schema: {message}")))
}
