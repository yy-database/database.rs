//! Outbox contract parity: authority commit, cursor, backlog, ack and prune.
//!
//! Runs on in-memory SQLite; the same scenario runs live against PostgreSQL in
//! `tests/adapter_postgres/outbox_phase10b.rs`.

use database_query::ir::{CommitToken, OutboxAppend, OutboxEffect};
use odm_adapter::{Error, adapter_sqlite::SqliteSource};
use odm_types::{RowWrite, Value};
use std::collections::BTreeMap;

const SCHEMA: &str = r#"
table OutboxUser {
    @@user_id: utf8,
    @user_name: utf8,
}
"#;

fn append(operation: &str, entity: &str, version: u64) -> OutboxAppend {
    OutboxAppend {
        operation_id: operation.into(),
        table: "OutboxUser".into(),
        entity_id: entity.into(),
        entity_version: version,
        effect: OutboxEffect::Upsert,
    }
}

fn row(id: &str, name: &str) -> RowWrite {
    let mut fields = BTreeMap::new();
    fields.insert("user_id".into(), Value::Str(id.into()));
    fields.insert("user_name".into(), Value::Str(name.into()));
    RowWrite { table: "OutboxUser".into(), primary_key: "user_id".into(), fields }
}

fn source() -> SqliteSource {
    let source = SqliteSource::open_in_memory().expect("in-memory authority");
    source.managed_push(SCHEMA).expect("managed push");
    source
}

#[test]
fn commit_writes_business_row_and_outbox_in_one_batch() {
    let source = source();
    let (_, token) = source
        .authority_commit(|tx| {
            tx.insert(&row("u1", "ada"))?;
            tx.append_outbox(append("op-1", "u1", 1));
            Ok(())
        })
        .expect("authority commit");

    assert_eq!(token, CommitToken::new(1));
    let records = source.outbox_after(0, 10).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].commit_token.seq, 1);
    assert_eq!(records[0].operation_id, "op-1");
    let backlog = source.outbox_backlog().unwrap();
    assert_eq!(backlog.pending, 1);
    assert_eq!(backlog.min_acked_seq, None);
    assert!(backlog.is_cumulative());
}

#[test]
fn failed_commit_rolls_back_business_row_and_events() {
    let source = source();
    source
        .authority_commit(|tx| {
            tx.insert(&row("u1", "ada"))?;
            tx.append_outbox(append("op-1", "u1", 1));
            Ok(())
        })
        .unwrap();

    let failed = source.authority_commit(|tx| {
        tx.insert(&row("u2", "grace"))?;
        tx.append_outbox(append("op-2", "u2", 1));
        Err::<(), _>(Error::Policy("projection rejected after the write".into()))
    });
    assert!(failed.is_err());
    assert_eq!(source.current_commit_token().unwrap(), CommitToken::new(1));
    assert_eq!(source.outbox_after(0, 10).unwrap().len(), 1);

    // No PK conflict: the rolled-back insert left nothing durable.
    let (_, token) = source
        .authority_commit(|tx| {
            tx.insert(&row("u2", "grace"))?;
            tx.append_outbox(append("op-2", "u2", 1));
            Ok(())
        })
        .expect("retry after rollback");
    assert_eq!(token, CommitToken::new(2));
    assert_eq!(source.outbox_after(0, 10).unwrap().len(), 2);
}

#[test]
fn empty_commit_consumes_a_seq_so_cursors_see_a_gap() {
    let source = source();
    let (_, token) = source.authority_commit(|_tx| Ok(())).unwrap();
    assert_eq!(token, CommitToken::new(1));
    assert!(source.outbox_after(0, 10).unwrap().is_empty());
    assert_eq!(source.outbox_backlog().unwrap().pending, 0);
}

#[test]
fn batch_never_splits_one_seq_across_two_polls() {
    let source = source();
    let (_, token) = source
        .authority_commit(|tx| {
            tx.append_outbox(append("op-a", "a", 1));
            tx.append_outbox(append("op-b", "b", 1));
            tx.append_outbox(append("op-c", "c", 1));
            Ok(())
        })
        .unwrap();
    assert_eq!(token, CommitToken::new(1));

    // `limit` is a soft cap: the whole seq group is returned even with limit = 1.
    let batch = source.outbox_after(0, 1).unwrap();
    assert_eq!(batch.len(), 3);
    assert!(batch.iter().all(|event| event.commit_token.seq == 1));
    assert!(source.outbox_after(1, 10).unwrap().is_empty());
}

#[test]
fn duplicate_append_reuses_the_existing_commit_token() {
    let source = source();
    let (_, first) = source
        .authority_commit(|tx| {
            tx.append_outbox(append("op-1", "u1", 1));
            Ok(())
        })
        .unwrap();
    assert_eq!(first, CommitToken::new(1));

    let (_, replay) = source
        .authority_commit(|tx| {
            tx.append_outbox(append("op-1", "u1", 1));
            Ok(())
        })
        .expect("idempotent replay");
    assert_eq!(replay, first, "a replay does not consume a new seq");
    assert_eq!(source.outbox_after(0, 10).unwrap().len(), 1);
    assert_eq!(source.outbox_backlog().unwrap().pending, 1);
}

#[test]
fn same_idempotency_key_with_a_different_payload_is_rejected() {
    let source = source();
    source
        .authority_commit(|tx| {
            tx.append_outbox(append("op-1", "u1", 1));
            Ok(())
        })
        .unwrap();

    let err = source
        .authority_commit(|tx| {
            tx.append_outbox(append("op-1", "u2", 1));
            Ok(())
        })
        .expect_err("key reuse with a new payload");
    assert!(err.to_string().contains("different payload"), "unexpected error: {err}");
    assert_eq!(source.current_commit_token().unwrap(), CommitToken::new(1));
    assert_eq!(source.outbox_after(0, 10).unwrap().len(), 1);
}

#[test]
fn ack_moves_the_backlog_and_prune_requires_a_watermark() {
    let source = source();
    source
        .authority_commit(|tx| {
            tx.append_outbox(append("op-a", "a", 1));
            tx.append_outbox(append("op-b", "b", 1));
            tx.append_outbox(append("op-c", "c", 1));
            Ok(())
        })
        .unwrap();
    source
        .authority_commit(|tx| {
            tx.append_outbox(append("op-d", "d", 1));
            Ok(())
        })
        .unwrap();

    assert!(source.prune_outbox(10).is_err(), "prune without any ack must be refused");
    let backlog = source.outbox_backlog().unwrap();
    assert_eq!(backlog.pending, 4);
    assert!(backlog.is_cumulative());

    assert_eq!(source.outbox_ack("cache", 1).unwrap(), 1);
    let backlog = source.outbox_backlog().unwrap();
    assert_eq!(backlog.pending, 1);
    assert_eq!(backlog.min_acked_seq, Some(1));

    // Watermarks only move forward.
    assert_eq!(source.outbox_ack("cache", 0).unwrap(), 1);
    assert!(source.outbox_ack("cache", 99).is_err(), "ack beyond the authority seq must fail");

    // Capped at the watermark: acked seq 1 still keeps `seq == 1`.
    assert_eq!(source.prune_outbox(10).unwrap(), 0);
    assert_eq!(source.outbox_ack("cache", 2).unwrap(), 2);
    assert_eq!(source.prune_outbox(10).unwrap(), 3);
    let remaining = source.outbox_after(1, 10).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].commit_token.seq, 2);
    assert_eq!(source.outbox_backlog().unwrap().pending, 0);
}
