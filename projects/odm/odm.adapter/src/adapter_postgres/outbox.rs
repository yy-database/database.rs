//! Authority CommitToken + transactional outbox over the PostgreSQL wire client.
//!
//! SQL stays **private**. Public values are Iris commit/outbox shapes.
//! Parameterized query helpers still await `yyds-gateway-pgsql::client` extraction;
//! schema install uses simple-query text until then.

use database_client::pgsql::Session;
use database_query::ir::{CommitToken, DEFAULT_COMMIT_SHARD, OutboxAppend, OutboxEffect, OutboxRecord};
use odm_types::{RowWrite, Value};

use crate::{Error, Result, execute};

/// True when a physical table is Iris authority meta.
pub(crate) fn is_meta_table(name: &str) -> bool {
    name.starts_with("_iris_")
}

/// Idempotent install of commit counter + outbox tables.
pub fn ensure_schema(session: &mut Session) -> Result<()> {
    session
        .execute(
            "CREATE TABLE IF NOT EXISTS _iris_commit (
            shard TEXT PRIMARY KEY NOT NULL,
            seq BIGINT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS _iris_outbox (
            id BIGSERIAL PRIMARY KEY,
            shard TEXT NOT NULL,
            seq BIGINT NOT NULL,
            operation_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            entity_version BIGINT NOT NULL,
            effect TEXT NOT NULL,
            UNIQUE(operation_id, entity_version)
         );
         CREATE INDEX IF NOT EXISTS _iris_outbox_shard_seq
           ON _iris_outbox(shard, seq);",
        )
        .map_err(Error::Wire)?;
    let _ = DEFAULT_COMMIT_SHARD;
    Err(Error::Unsupported(
        "PostgreSQL outbox seed/query awaits gateway wire client parameter binding (third-party postgres crate removed)".into(),
    ))
}

/// Current commit token.
pub fn current_token(session: &mut Session) -> Result<CommitToken> {
    ensure_schema(session)?;
    unreachable!()
}

/// Writer handle for an authority transaction.
pub struct AuthorityTxn<'a> {
    session: &'a mut Session,
    appends: Vec<OutboxAppend>,
}

impl<'a> AuthorityTxn<'a> {
    /// Queue outbox append.
    pub fn append_outbox(&mut self, event: OutboxAppend) {
        self.appends.push(event);
    }

    /// Insert inside the authority transaction.
    pub fn insert(&mut self, write: &RowWrite) -> Result<()> {
        execute::insert_row(self.session, write)
    }

    /// Update inside the authority transaction.
    pub fn update(&mut self, write: &RowWrite) -> Result<u64> {
        execute::update_row(self.session, write)
    }

    /// Delete inside the authority transaction.
    pub fn delete(&mut self, table: &str, primary_key: &str, key: &Value) -> Result<u64> {
        execute::delete_row(self.session, table, primary_key, key)
    }
}

/// Atomic authority mutations + outbox append; returns [`CommitToken`].
pub fn authority_commit<R>(session: &mut Session, f: impl FnOnce(&mut Session) -> Result<R>) -> Result<(R, CommitToken)> {
    let _ = (session, f, OutboxEffect::Upsert);
    Err(Error::Unsupported(
        "PostgreSQL authority_commit awaits gateway wire client parameter binding (third-party postgres crate removed)".into(),
    ))
}

/// Poll outbox after sequence.
pub fn outbox_after(session: &mut Session, _after_seq: u64, _limit: usize) -> Result<Vec<OutboxRecord>> {
    let _ = session;
    Err(Error::Unsupported("PostgreSQL outbox_after awaits gateway wire client parameter binding (third-party postgres crate removed)".into()))
}

/// Outbox backlog count.
pub fn outbox_backlog(session: &mut Session) -> Result<u64> {
    let _ = session;
    Err(Error::Unsupported(
        "PostgreSQL outbox_backlog awaits gateway wire client parameter binding (third-party postgres crate removed)".into(),
    ))
}
