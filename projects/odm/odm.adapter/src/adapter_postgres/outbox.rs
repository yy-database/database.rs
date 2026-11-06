//! Authority CommitToken + transactional outbox over the PostgreSQL wire client.
//!
//! SQL stays **private**. Public values are Iris commit/outbox shapes.
//!
//! Contract (see `决策和进度表/odm-outbox-contract.md`): business rows, outbox rows and
//! the `seq` bump share one transaction, batches are split on `seq` boundaries, duplicates
//! are compared by payload and acks gate pruning.

use database_client::pgsql::{Cell, Param, Session};
use database_query::ir::{CommitToken, DEFAULT_COMMIT_SHARD, OutboxAppend, OutboxBacklog, OutboxEffect, OutboxRecord};
use odm_types::{RowWrite, Value};

use crate::{Error, Result};

use super::execute;

/// True when a physical table is Iris authority meta.
pub(crate) fn is_meta_table(name: &str) -> bool {
    name.starts_with("_iris_")
}

fn shard_param() -> Param<'static> {
    Param::text(Some(DEFAULT_COMMIT_SHARD))
}

fn text_param(value: &str) -> Param<'_> {
    Param::text(Some(value))
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    match cell? {
        Cell::Null => None,
        Cell::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
    }
}

fn read_number(cell: Option<&Cell>, what: &str) -> Result<u64> {
    let raw = cell_text(cell).ok_or_else(|| Error::Policy(format!("missing {what}")))?;
    raw.trim().parse::<i64>().map(|value| value.max(0) as u64).map_err(|err| Error::Policy(format!("invalid {what} `{raw}`: {err}")))
}

/// Idempotent install of commit counter + outbox + ack tables.
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
           ON _iris_outbox(shard, seq);
         CREATE TABLE IF NOT EXISTS _iris_ack (
            consumer TEXT PRIMARY KEY NOT NULL,
            acked_seq BIGINT NOT NULL
         );",
        )
        .map_err(Error::Wire)?;
    let seeded = session.query_with_params("SELECT 1 FROM _iris_commit WHERE shard = $1", &[shard_param()]).map_err(Error::Wire)?;
    if seeded.rows.is_empty() {
        session
            .execute_with_params("INSERT INTO _iris_commit(shard, seq) VALUES ($1, 0) ON CONFLICT (shard) DO NOTHING", &[shard_param()])
            .map_err(Error::Wire)?;
    }
    Ok(())
}

fn read_seq(session: &mut Session, lock: bool) -> Result<u64> {
    let sql = if lock { "SELECT seq FROM _iris_commit WHERE shard = $1 FOR UPDATE" } else { "SELECT seq FROM _iris_commit WHERE shard = $1" };
    let result = session.query_with_params(sql, &[shard_param()]).map_err(Error::Wire)?;
    read_number(result.rows.first().and_then(|row| row.first()), "authority seq")
}

fn read_min_acked(session: &mut Session) -> Result<Option<u64>> {
    let result = session.query_with_params("SELECT acked_seq FROM _iris_ack ORDER BY acked_seq ASC LIMIT 1", &[]).map_err(Error::Wire)?;
    let Some(cell) = result.rows.first().and_then(|row| row.first())
    else {
        return Ok(None);
    };
    read_number(Some(cell), "acked seq").map(Some)
}

fn count_rows(session: &mut Session, sql: &str, params: &[Param<'_>]) -> Result<u64> {
    let result = session.query_with_params(sql, params).map_err(Error::Wire)?;
    read_number(result.rows.first().and_then(|row| row.first()), "outbox count")
}

/// Current commit token.
pub fn current_token(session: &mut Session) -> Result<CommitToken> {
    ensure_schema(session)?;
    Ok(CommitToken::new(read_seq(session, false)?))
}

/// Writer handle for an authority transaction.
pub struct AuthorityTxn<'a> {
    session: &'a mut Session,
    appends: Vec<OutboxAppend>,
}

impl AuthorityTxn<'_> {
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

struct StoredEvent {
    seq: u64,
    table: String,
    entity_id: String,
    entity_version: u64,
    effect: OutboxEffect,
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

fn find_event(session: &mut Session, event: &OutboxAppend) -> Result<Option<StoredEvent>> {
    let version = event.entity_version.to_string();
    let result = session
        .query_with_params(
            "SELECT seq, table_name, entity_id, entity_version, effect
             FROM _iris_outbox
             WHERE operation_id = $1 AND entity_version = $2",
            &[text_param(&event.operation_id), text_param(&version)],
        )
        .map_err(Error::Wire)?;
    let Some(row) = result.rows.first()
    else {
        return Ok(None);
    };
    let read = |index: usize| cell_text(row.get(index));
    Ok(Some(StoredEvent {
        seq: read_number(row.first(), "stored outbox seq")?,
        table: read(1).ok_or_else(|| Error::Policy("invalid stored outbox table".into()))?,
        entity_id: read(2).ok_or_else(|| Error::Policy("invalid stored outbox entity_id".into()))?,
        entity_version: read_number(row.get(3), "stored outbox entity_version")?,
        effect: effect_from_name(&read(4).ok_or_else(|| Error::Policy("invalid stored outbox effect".into()))?),
    }))
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

fn insert_event(session: &mut Session, event: &OutboxAppend, seq: u64) -> Result<()> {
    let version = event.entity_version.to_string();
    let seq = seq.to_string();
    session
        .execute_with_params(
            "INSERT INTO _iris_outbox(shard, seq, operation_id, table_name, entity_id, entity_version, effect)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (operation_id, entity_version) DO NOTHING",
            &[
                shard_param(),
                text_param(&seq),
                text_param(&event.operation_id),
                text_param(&event.table),
                text_param(&event.entity_id),
                text_param(&version),
                text_param(effect_name(event.effect)),
            ],
        )
        .map_err(Error::Wire)?;
    Ok(())
}

/// Atomic authority mutations + outbox append; returns [`CommitToken`].
///
/// A replay whose appends all match durable rows keeps the existing `seq`. A key reuse
/// with a different payload fails the whole batch.
pub fn authority_commit<R>(session: &mut Session, f: impl FnOnce(&mut AuthorityTxn<'_>) -> Result<R>) -> Result<(R, CommitToken)> {
    ensure_schema(session)?;
    session.execute("BEGIN").map_err(Error::Wire)?;
    let committed = (|| {
        let mut writer = AuthorityTxn { session: &mut *session, appends: Vec::new() };
        let value = f(&mut writer)?;
        let appends = std::mem::take(&mut writer.appends);
        let had_appends = !appends.is_empty();
        drop(writer);

        let previous = read_seq(session, true)?;
        let mut fresh: Vec<OutboxAppend> = Vec::new();
        let mut duplicates: Vec<u64> = Vec::new();
        for event in appends {
            if let Some(seen) = find_event(session, &event)? {
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
                duplicates.iter().copied().max().unwrap_or(previous)
            }
            else {
                let next = previous.saturating_add(1);
                bump(session, next)?;
                next
            }
        }
        else {
            let next = previous.saturating_add(1);
            bump(session, next)?;
            for event in &fresh {
                insert_event(session, event, next)?;
            }
            next
        };

        session.execute("COMMIT").map_err(Error::Wire)?;
        Ok((value, CommitToken::new(token)))
    })();
    if committed.is_err() {
        let _ = session.execute("ROLLBACK");
    }
    committed
}

fn bump(session: &mut Session, next: u64) -> Result<()> {
    let seq = next.to_string();
    session
        .execute_with_params("UPDATE _iris_commit SET seq = $1 WHERE shard = $2", &[text_param(&seq), shard_param()])
        .map_err(Error::Wire)?;
    Ok(())
}

/// Poll outbox after sequence.
///
/// Batches never split one `seq` across two calls: when `limit` falls inside a group,
/// the whole group is returned so `max(seq)` stays a safe next cursor.
pub fn outbox_after(session: &mut Session, after_seq: u64, limit: usize) -> Result<Vec<OutboxRecord>> {
    ensure_schema(session)?;
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut rows = fetch_outbox(session, after_seq, limit)?;
    let boundary = if rows.len() == limit {
        rows.last().and_then(|row| row.get(2)).and_then(|cell| cell_text(Some(cell))).and_then(|raw| raw.trim().parse::<i64>().ok())
    }
    else {
        None
    };
    if let Some(boundary) = boundary {
        rows = fetch_outbox_through(session, after_seq, boundary.max(0) as u64)?;
    }
    rows.into_iter().map(row_to_record).collect()
}

fn fetch_outbox(session: &mut Session, after_seq: u64, limit: usize) -> Result<Vec<Vec<Cell>>> {
    let after = after_seq.to_string();
    let limit = limit.to_string();
    let result = session
        .query_with_params(
            "SELECT id, shard, seq, operation_id, table_name, entity_id, entity_version, effect
             FROM _iris_outbox
             WHERE shard = $1 AND seq > $2
             ORDER BY seq ASC, id ASC
             LIMIT $3",
            &[shard_param(), text_param(&after), text_param(&limit)],
        )
        .map_err(Error::Wire)?;
    Ok(result.rows)
}

fn fetch_outbox_through(session: &mut Session, after_seq: u64, boundary: u64) -> Result<Vec<Vec<Cell>>> {
    let after = after_seq.to_string();
    let boundary = boundary.to_string();
    let result = session
        .query_with_params(
            "SELECT id, shard, seq, operation_id, table_name, entity_id, entity_version, effect
             FROM _iris_outbox
             WHERE shard = $1 AND seq > $2 AND seq <= $3
             ORDER BY seq ASC, id ASC",
            &[shard_param(), text_param(&after), text_param(&boundary)],
        )
        .map_err(Error::Wire)?;
    Ok(result.rows)
}

fn row_to_record(row: Vec<Cell>) -> Result<OutboxRecord> {
    let read = |index: usize| cell_text(row.get(index)).unwrap_or_default();
    Ok(OutboxRecord {
        id: read_number(row.first(), "outbox id")?,
        commit_token: CommitToken { shard: read(1), seq: read_number(row.get(2), "outbox seq")? },
        operation_id: read(3),
        table: read(4),
        entity_id: read(5),
        entity_version: read_number(row.get(6), "outbox entity_version")?,
        effect: effect_from_name(&read(7)),
    })
}

/// Backlog reading: rows above the lowest consumer watermark.
///
/// Without any ack row the count is cumulative and `min_acked_seq` is `None`.
pub fn outbox_backlog(session: &mut Session) -> Result<OutboxBacklog> {
    ensure_schema(session)?;
    let min_acked_seq = read_min_acked(session)?;
    let pending = match min_acked_seq {
        Some(seq) => {
            let after = seq.to_string();
            count_rows(session, "SELECT COUNT(*) FROM _iris_outbox WHERE shard = $1 AND seq > $2", &[shard_param(), text_param(&after)])?
        }
        None => count_rows(session, "SELECT COUNT(*) FROM _iris_outbox WHERE shard = $1", &[shard_param()])?,
    };
    Ok(OutboxBacklog { pending, min_acked_seq })
}

/// Record a consumer watermark. Watermarks only move forward.
pub fn outbox_ack(session: &mut Session, consumer: &str, acked_seq: u64) -> Result<u64> {
    if consumer.trim().is_empty() {
        return Err(Error::Policy("outbox ack needs a consumer name".into()));
    }
    ensure_schema(session)?;
    let current = read_seq(session, false)?;
    if acked_seq > current {
        return Err(Error::Policy(format!("consumer `{consumer}` cannot ack seq {acked_seq} beyond current authority seq {current}")));
    }
    let result =
        session.query_with_params("SELECT acked_seq FROM _iris_ack WHERE consumer = $1", &[text_param(consumer)]).map_err(Error::Wire)?;
    let previous = result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(|cell| cell_text(Some(cell)))
        .and_then(|raw| raw.trim().parse::<i64>().ok())
        .map(|seq| seq.max(0) as u64);
    let Some(previous) = previous
    else {
        let acked = acked_seq.to_string();
        session
            .execute_with_params("INSERT INTO _iris_ack(consumer, acked_seq) VALUES ($1, $2)", &[text_param(consumer), text_param(&acked)])
            .map_err(Error::Wire)?;
        return Ok(acked_seq);
    };
    if acked_seq <= previous {
        return Ok(previous);
    }
    let acked = acked_seq.to_string();
    session
        .execute_with_params("UPDATE _iris_ack SET acked_seq = $1 WHERE consumer = $2", &[text_param(&acked), text_param(consumer)])
        .map_err(Error::Wire)?;
    Ok(acked_seq)
}

/// Delete outbox history below `before_seq`, capped at the lowest ack watermark.
///
/// Refuses to run while no consumer acked, so nothing can be pruned silently.
pub fn prune(session: &mut Session, before_seq: u64) -> Result<u64> {
    ensure_schema(session)?;
    let Some(min_acked_seq) = read_min_acked(session)?
    else {
        return Err(Error::Policy("refusing to prune outbox: no consumer has acknowledged a watermark".into()));
    };
    let upper = before_seq.min(min_acked_seq).to_string();
    session
        .execute_with_params("DELETE FROM _iris_outbox WHERE shard = $1 AND seq < $2", &[shard_param(), text_param(&upper)])
        .map_err(Error::Wire)
}
