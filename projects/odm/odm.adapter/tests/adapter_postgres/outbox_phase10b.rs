//! Outbox contract parity on live PostgreSQL (`IRIS_TEST_POSTGRES_URL`).
//!
//! Mirrors `tests/adapter_sqlite/outbox_contract.rs`: one scenario, one connection,
//! so the assertions describe the same semantics the SQLite adapter must satisfy.

use database_query::ir::{OutboxAppend, OutboxEffect};
use odm_adapter::{Error, adapter_postgres::PostgresSource};
use odm_types::{RowWrite, Value};
use std::collections::BTreeMap;

const SCHEMA: &str = r#"
table OutboxUser {
    @@user_id: utf8,
    @user_name: utf8,
}
"#;

fn live_url() -> Option<String> {
    std::env::var("IRIS_TEST_POSTGRES_URL").ok().filter(|value| !value.is_empty())
}

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

fn cleanup(source: &PostgresSource) {
    source
        .with_connection(|conn| {
            for table in ["\"OutboxUser\"", "_iris_outbox", "_iris_ack", "_iris_commit"] {
                conn.execute(&format!("DROP TABLE IF EXISTS {table}")).map_err(Error::Wire)?;
            }
            Ok(())
        })
        .expect("cleanup");
}

#[test]
fn live_outbox_contract_matches_sqlite_semantics() {
    let Some(url) = live_url()
    else {
        eprintln!("skipping live postgres outbox contract: IRIS_TEST_POSTGRES_URL not set");
        return;
    };
    let source = PostgresSource::connect(&url).expect("connect");
    cleanup(&source);
    source.managed_push(SCHEMA).expect("managed push");

    // 1. Business row + event in one authority commit.
    let (_, token) = source
        .authority_commit(|tx| {
            tx.insert(&row("u1", "ada"))?;
            tx.append_outbox(append("op-1", "u1", 1));
            Ok(())
        })
        .expect("authority commit");
    assert!(token.seq >= 1, "fresh shard must start at seq 1: {token:?}");
    let records = source.outbox_after(0, 10).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].operation_id, "op-1");
    let backlog = source.outbox_backlog().unwrap();
    assert_eq!(backlog.pending, 1);
    assert!(backlog.is_cumulative());

    // 2. A replay keeps the original token and does not add a row.
    let (_, replay) = source
        .authority_commit(|tx| {
            tx.append_outbox(append("op-1", "u1", 1));
            Ok(())
        })
        .expect("idempotent replay");
    assert_eq!(replay, token);
    assert_eq!(source.outbox_after(0, 10).unwrap().len(), 1);

    // 3. A failed batch rolls back the business row and the event.
    let failed = source.authority_commit(|tx| {
        tx.insert(&row("u2", "grace"))?;
        tx.append_outbox(append("op-2", "u2", 1));
        Err::<(), _>(Error::Policy("projection rejected after the write".into()))
    });
    assert!(failed.is_err());
    assert_eq!(source.current_outbox_token().unwrap(), token);
    assert_eq!(source.outbox_after(0, 10).unwrap().len(), 1);
    let (_, retried) = source
        .authority_commit(|tx| {
            tx.insert(&row("u2", "grace"))?;
            tx.append_outbox(append("op-2", "u2", 1));
            Ok(())
        })
        .expect("retry after rollback");
    assert_eq!(retried.seq, token.seq + 1);

    // 4. One seq group is never split by `limit`.
    let (_, grouped) = source
        .authority_commit(|tx| {
            tx.append_outbox(append("op-a", "a", 1));
            tx.append_outbox(append("op-b", "b", 1));
            tx.append_outbox(append("op-c", "c", 1));
            Ok(())
        })
        .unwrap();
    assert_eq!(grouped.seq, retried.seq + 1);
    let batch = source.outbox_after(retried.seq, 1).unwrap();
    assert_eq!(batch.len(), 3, "limit must not cut a seq group in half");
    assert!(batch.iter().all(|event| event.commit_token.seq == grouped.seq));

    // 5. An empty commit consumes a seq but emits nothing.
    let (_, empty) = source.authority_commit(|_tx| Ok(())).unwrap();
    assert_eq!(empty.seq, grouped.seq + 1);
    assert!(source.outbox_after(empty.seq, 10).unwrap().is_empty());

    // 6. Key reuse with a different payload fails the batch.
    let err = source
        .authority_commit(|tx| {
            tx.append_outbox(append("op-1", "u2", 1));
            Ok(())
        })
        .expect_err("key reuse with a new payload");
    assert!(err.to_string().contains("different payload"), "unexpected error: {err}");

    // 7. Backlog counts rows above the lowest ack watermark; prune is gated by it.
    let total = source.outbox_after(0, 100).unwrap().len() as u64;
    let backlog = source.outbox_backlog().unwrap();
    assert_eq!(backlog.pending, total);
    assert!(backlog.is_cumulative());
    assert!(source.prune_outbox(total + 10).is_err(), "prune without any ack must be refused");

    assert_eq!(source.outbox_ack("cache", grouped.seq).unwrap(), grouped.seq);
    assert_eq!(source.outbox_ack("cache", 0).unwrap(), grouped.seq, "watermarks only move forward");
    assert!(source.outbox_ack("cache", empty.seq + 1).is_err(), "ack beyond authority must fail");
    let pending = source.outbox_backlog().unwrap();
    assert_eq!(pending.min_acked_seq, Some(grouped.seq));
    assert_eq!(pending.pending, 0, "everything at or below the watermark is applied");

    assert_eq!(source.prune_outbox(empty.seq).unwrap(), 2);
    assert_eq!(source.outbox_after(0, 100).unwrap().len(), 3, "the acked seq group is retained");
    assert_eq!(source.outbox_backlog().unwrap().pending, 0);

    cleanup(&source);
}
