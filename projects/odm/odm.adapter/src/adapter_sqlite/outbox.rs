//! Authority CommitToken + transactional outbox (Phase 10-B).
//!
//! SQL for meta tables stays **private**. Public values are Iris commit/outbox shapes.
//!
//! Contract (see `决策和进度表/odm-outbox-contract.md`):
//! business rows, outbox rows and the `seq` bump share one transaction, batches are
//! split on `seq` boundaries, duplicates are compared by payload and acks gate pruning.

use database_query::ir::{CommitToken, DEFAULT_COMMIT_SHARD, OutboxAppend, OutboxBacklog, OutboxEffect, OutboxRecord};
use odm_types::{RowWrite, Value};

use crate::{Error, Result};

use super::{
    execute,
    provider::{RusqliteProvider, SqliteValue},
};

/// True when a physical table is Iris authority meta (hidden from business catalog).
pub(crate) fn is_meta_table(name: &str) -> bool {
    name.starts_with("_iris_")
}

fn shard_param() -> SqliteValue {
    SqliteValue::Text(DEFAULT_COMMIT_SHARD.as_bytes().to_vec())
}

fn text_param(value: &str) -> SqliteValue {
    SqliteValue::Text(value.as_bytes().to_vec())
}

fn integer_param(value: i64) -> SqliteValue {
    SqliteValue::Integer(value)
}

fn cell_text(cell: Option<&SqliteValue>) -> Option<String> {
    match cell? {
        SqliteValue::Text(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        _ => None,
    }
}

fn cell_integer(cell: Option<&SqliteValue>) -> Option<i64> {
    match cell? {
        SqliteValue::Integer(value) => Some(*value),
        _ => None,
    }
}

fn effect_name(effect: OutboxEffect) -> &'static str {
    match effect {
        OutboxEffect::Upsert => "upsert",
        OutboxEffect::Delete => "delete",
    }
}

fn effect_from_name(name: &str) -> OutboxEffect {
    match name {
        "delete" => OutboxEffect::Delete,
        _ => OutboxEffect::Upsert,
    }
}

/// Idempotent install of commit counter + outbox + ack tables.
pub fn ensure_schema(provider: &RusqliteProvider) -> Result<()> {
    provider.execute_batch(
        "CREATE TABLE IF NOT EXISTS _iris_commit (
            shard TEXT PRIMARY KEY NOT NULL,
            seq INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS _iris_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shard TEXT NOT NULL,
            seq INTEGER NOT NULL,
            operation_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            entity_version INTEGER NOT NULL,
            effect TEXT NOT NULL
         );
         CREATE UNIQUE INDEX IF NOT EXISTS _iris_outbox_operation
           ON _iris_outbox(operation_id, entity_version);
         CREATE INDEX IF NOT EXISTS _iris_outbox_shard_seq
           ON _iris_outbox(shard, seq);
         CREATE TABLE IF NOT EXISTS _iris_ack (
            consumer TEXT PRIMARY KEY NOT NULL,
            acked_seq INTEGER NOT NULL
         );",
    )?;
    let exists = provider.execute_one("SELECT 1 FROM _iris_commit WHERE shard = ?1", &[shard_param()])?;
    if exists.rows.is_empty() {
        provider.execute_one("INSERT INTO _iris_commit(shard, seq) VALUES (?1, 0)", &[shard_param()])?;
    }
    Ok(())
}

fn read_seq(provider: &RusqliteProvider) -> Result<u64> {
    let result = provider.execute_one("SELECT seq FROM _iris_commit WHERE shard = ?1", &[shard_param()])?;
    result
        .rows
        .first()
        .and_then(|row| cell_integer(row.first()))
        .map(|seq| seq.max(0) as u64)
        .ok_or_else(|| Error::Policy("missing authority commit row".into()))
}

fn read_min_acked(provider: &RusqliteProvider) -> Result<Option<u64>> {
    let result = provider.execute_one("SELECT acked_seq FROM _iris_ack ORDER BY acked_seq ASC LIMIT 1", &[])?;
    Ok(result.rows.first().and_then(|row| cell_integer(row.first())).map(|seq| seq.max(0) as u64))
}

fn count_rows(provider: &RusqliteProvider, sql: &str, params: &[SqliteValue]) -> Result<u64> {
    let result = provider.execute_one(sql, params)?;
    result
        .rows
        .first()
        .and_then(|row| cell_integer(row.first()))
        .map(|count| count.max(0) as u64)
        .ok_or_else(|| Error::Policy("invalid outbox count".into()))
}

/// Read the current commit token (seq of last successful authority commit).
pub fn current_token(provider: &RusqliteProvider) -> Result<CommitToken> {
    ensure_schema(provider)?;
    Ok(CommitToken::new(read_seq(provider)?))
}

/// Writer handle bound to an open authority transaction.
pub struct AuthorityTxn<'provider> {
    provider: &'provider RusqliteProvider,
    appends: Vec<OutboxAppend>,
}

impl AuthorityTxn<'_> {
    /// Queue an outbox append (flushed at commit with the new token).
    pub fn append_outbox(&mut self, event: OutboxAppend) {
        self.appends.push(event);
    }

    /// Insert a business row inside the authority transaction.
    pub fn insert(&self, write: &RowWrite) -> Result<()> {
        execute::insert_row(self.provider, write)
    }

    /// Update by primary key inside the authority transaction.
    pub fn update(&self, write: &RowWrite) -> Result<usize> {
        execute::update_row(self.provider, write)
    }

    /// Delete by primary key inside the authority transaction.
    pub fn delete(&self, table: &str, primary_key: &str, key: &Value) -> Result<usize> {
        execute::delete_row(self.provider, table, primary_key, key)
    }
}

/// Stored payload used to tell an idempotent replay from a key reuse with new content.
struct StoredEvent {
    seq: u64,
    table: String,
    entity_id: String,
    entity_version: u64,
    effect: OutboxEffect,
}

fn find_event(provider: &RusqliteProvider, event: &OutboxAppend) -> Result<Option<StoredEvent>> {
    let result = provider.execute_one(
        "SELECT seq, table_name, entity_id, entity_version, effect
         FROM _iris_outbox
         WHERE operation_id = ?1 AND entity_version = ?2",
        &[text_param(&event.operation_id), integer_param(event.entity_version as i64)],
    )?;
    let Some(row) = result.rows.first()
    else {
        return Ok(None);
    };
    let stored = StoredEvent {
        seq: cell_integer(row.first()).ok_or_else(|| Error::Policy("invalid outbox seq".into()))?.max(0) as u64,
        table: cell_text(row.get(1)).ok_or_else(|| Error::Policy("invalid outbox table".into()))?,
        entity_id: cell_text(row.get(2)).ok_or_else(|| Error::Policy("invalid outbox entity_id".into()))?,
        entity_version: cell_integer(row.get(3)).ok_or_else(|| Error::Policy("invalid outbox entity_version".into()))?.max(0) as u64,
        effect: effect_from_name(&cell_text(row.get(4)).ok_or_else(|| Error::Policy("invalid outbox effect".into()))?),
    };
    Ok(Some(stored))
}

fn payload_matches(stored: &StoredEvent, event: &OutboxAppend) -> bool {
    stored.table == event.table
        && stored.entity_id == event.entity_id
        && stored.entity_version == event.entity_version
        && stored.effect == event.effect
}

fn same_payload(left: &OutboxAppend, right: &OutboxAppend) -> bool {
    left.table == right.table && left.entity_id == right.entity_id && left.entity_version == right.entity_version && left.effect == right.effect
}

/// Run `f` inside one authority transaction: mutations + outbox appends are atomic.
///
/// On success, returns the user value and a [`CommitToken`] meaning authority committed
/// **and** durable outbox accepted propagation duty (not that projections caught up).
///
/// A replay whose appends all match durable rows keeps the existing `seq` instead of
/// consuming a new one. A key reuse with a different payload fails the whole batch.
pub fn authority_commit<R>(provider: &RusqliteProvider, f: impl FnOnce(&mut AuthorityTxn<'_>) -> Result<R>) -> Result<(R, CommitToken)> {
    ensure_schema(provider)?;
    provider.begin_immediate()?;
    let committed = (|| {
        let mut writer = AuthorityTxn { provider, appends: Vec::new() };
        let value = f(&mut writer)?;
        let appends = std::mem::take(&mut writer.appends);
        let had_appends = !appends.is_empty();

        let previous = read_seq(provider)?;
        let mut fresh: Vec<OutboxAppend> = Vec::new();
        let mut duplicates: Vec<u64> = Vec::new();
        for event in appends {
            if let Some(seen) = find_event(provider, &event)? {
                if !payload_matches(&seen, &event) {
                    return Err(Error::Policy(format!(
                        "operation `{}` version {} is already persisted with a different payload --?same idempotency key, different fact",
                        event.operation_id, event.entity_version
                    )));
                }
                duplicates.push(seen.seq);
                continue;
            }
            if let Some(batch_duplicate) =
                fresh.iter().find(|queued| queued.operation_id == event.operation_id && queued.entity_version == event.entity_version)
            {
                if !same_payload(batch_duplicate, &event) {
                    return Err(Error::Policy(format!(
                        "operation `{}` version {} appears twice in one batch with different payloads",
                        event.operation_id, event.entity_version
                    )));
                }
                continue;
            }
            fresh.push(event);
        }

        let token = if fresh.is_empty() {
            if had_appends {
                // All appends were durable already: do not consume a new seq.
                duplicates.iter().copied().max().unwrap_or(previous)
            }
            else {
                // Empty commit still consumes a seq so cursors observe the gap.
                let next = previous.saturating_add(1);
                provider.execute_one("UPDATE _iris_commit SET seq = ?1 WHERE shard = ?2", &[integer_param(next as i64), shard_param()])?;
                next
            }
        }
        else {
            let next = previous.saturating_add(1);
            provider.execute_one("UPDATE _iris_commit SET seq = ?1 WHERE shard = ?2", &[integer_param(next as i64), shard_param()])?;
            for event in &fresh {
                provider.execute_one(
                    "INSERT INTO _iris_outbox(
                        shard, seq, operation_id, table_name, entity_id, entity_version, effect
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    &[
                        shard_param(),
                        integer_param(next as i64),
                        text_param(&event.operation_id),
                        text_param(&event.table),
                        text_param(&event.entity_id),
                        integer_param(event.entity_version as i64),
                        text_param(effect_name(event.effect)),
                    ],
                )?;
            }
            next
        };

        provider.commit()?;
        Ok((value, CommitToken::new(token)))
    })();
    if committed.is_err() {
        let _ = provider.rollback();
    }
    committed
}

/// List durable outbox records with `seq > after_seq` (projector poll shape).
///
/// Batches never split one `seq` across two calls: when `limit` falls inside a group,
/// the whole group is returned so `max(seq)` stays a safe next cursor.
pub fn outbox_after(provider: &RusqliteProvider, after_seq: u64, limit: usize) -> Result<Vec<OutboxRecord>> {
    ensure_schema(provider)?;
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut rows = fetch_outbox(provider, after_seq, limit)?;
    let boundary = if rows.len() == limit { rows.last().and_then(|row| cell_integer(row.get(2))).map(|seq| seq.max(0) as u64) } else { None };
    if let Some(boundary) = boundary {
        rows = fetch_outbox_through(provider, after_seq, boundary)?;
    }
    rows.into_iter().map(row_to_record).collect()
}

fn fetch_outbox(provider: &RusqliteProvider, after_seq: u64, limit: usize) -> Result<Vec<Vec<SqliteValue>>> {
    let result = provider.execute_one(
        "SELECT id, shard, seq, operation_id, table_name, entity_id, entity_version, effect
         FROM _iris_outbox
         WHERE shard = ?1 AND seq > ?2
         ORDER BY seq ASC, id ASC
         LIMIT ?3",
        &[shard_param(), integer_param(after_seq as i64), integer_param(limit as i64)],
    )?;
    Ok(result.rows)
}

fn fetch_outbox_through(provider: &RusqliteProvider, after_seq: u64, boundary: u64) -> Result<Vec<Vec<SqliteValue>>> {
    let result = provider.execute_one(
        "SELECT id, shard, seq, operation_id, table_name, entity_id, entity_version, effect
         FROM _iris_outbox
         WHERE shard = ?1 AND seq > ?2 AND seq <= ?3
         ORDER BY seq ASC, id ASC",
        &[shard_param(), integer_param(after_seq as i64), integer_param(boundary as i64)],
    )?;
    Ok(result.rows)
}

fn row_to_record(row: Vec<SqliteValue>) -> Result<OutboxRecord> {
    let id = cell_integer(row.first()).ok_or_else(|| Error::Policy("invalid outbox id".into()))?;
    let shard = cell_text(row.get(1)).ok_or_else(|| Error::Policy("invalid outbox shard".into()))?;
    let seq = cell_integer(row.get(2)).ok_or_else(|| Error::Policy("invalid outbox seq".into()))?;
    let operation_id = cell_text(row.get(3)).ok_or_else(|| Error::Policy("invalid outbox operation_id".into()))?;
    let table = cell_text(row.get(4)).ok_or_else(|| Error::Policy("invalid outbox table".into()))?;
    let entity_id = cell_text(row.get(5)).ok_or_else(|| Error::Policy("invalid outbox entity_id".into()))?;
    let entity_version = cell_integer(row.get(6)).ok_or_else(|| Error::Policy("invalid outbox entity_version".into()))?;
    let effect = effect_from_name(&cell_text(row.get(7)).ok_or_else(|| Error::Policy("invalid outbox effect".into()))?);
    Ok(OutboxRecord {
        id: id.max(0) as u64,
        commit_token: CommitToken { shard, seq: seq.max(0) as u64 },
        operation_id,
        table,
        entity_id,
        entity_version: entity_version.max(0) as u64,
        effect,
    })
}

/// Backlog reading: rows above the lowest consumer watermark.
///
/// Without any ack row the count is cumulative and `min_acked_seq` is `None`.
pub fn outbox_backlog(provider: &RusqliteProvider) -> Result<OutboxBacklog> {
    ensure_schema(provider)?;
    let min_acked_seq = read_min_acked(provider)?;
    let pending = match min_acked_seq {
        Some(seq) => count_rows(
            provider,
            "SELECT COUNT(*) FROM _iris_outbox WHERE shard = ?1 AND seq > ?2",
            &[shard_param(), integer_param(seq as i64)],
        )?,
        None => count_rows(provider, "SELECT COUNT(*) FROM _iris_outbox WHERE shard = ?1", &[shard_param()])?,
    };
    Ok(OutboxBacklog { pending, min_acked_seq })
}

/// Record a consumer watermark. Watermarks only move forward.
pub fn outbox_ack(provider: &RusqliteProvider, consumer: &str, acked_seq: u64) -> Result<u64> {
    if consumer.trim().is_empty() {
        return Err(Error::Policy("outbox ack needs a consumer name".into()));
    }
    ensure_schema(provider)?;
    let current = read_seq(provider)?;
    if acked_seq > current {
        return Err(Error::Policy(format!("consumer `{consumer}` cannot ack seq {acked_seq} beyond current authority seq {current}")));
    }
    let result = provider.execute_one("SELECT acked_seq FROM _iris_ack WHERE consumer = ?1", &[text_param(consumer)])?;
    let previous = result.rows.first().and_then(|row| cell_integer(row.first())).map(|seq| seq.max(0) as u64);
    let Some(previous) = previous
    else {
        provider.execute_one(
            "INSERT INTO _iris_ack(consumer, acked_seq) VALUES (?1, ?2)",
            &[text_param(consumer), integer_param(acked_seq as i64)],
        )?;
        return Ok(acked_seq);
    };
    if acked_seq <= previous {
        return Ok(previous);
    }
    provider
        .execute_one("UPDATE _iris_ack SET acked_seq = ?1 WHERE consumer = ?2", &[integer_param(acked_seq as i64), text_param(consumer)])?;
    Ok(acked_seq)
}

/// Delete outbox history below `before_seq`, capped at the lowest ack watermark.
///
/// Refuses to run while no consumer acked, so nothing can be pruned silently.
pub fn prune(provider: &RusqliteProvider, before_seq: u64) -> Result<u64> {
    ensure_schema(provider)?;
    let Some(min_acked_seq) = read_min_acked(provider)?
    else {
        return Err(Error::Policy("refusing to prune outbox: no consumer has acknowledged a watermark".into()));
    };
    let upper = before_seq.min(min_acked_seq);
    let result =
        provider.execute_one("DELETE FROM _iris_outbox WHERE shard = ?1 AND seq < ?2", &[shard_param(), integer_param(upper as i64)])?;
    Ok(result.changes)
}
