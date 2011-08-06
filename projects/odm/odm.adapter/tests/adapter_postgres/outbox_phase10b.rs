//! Phase 10-B: Authority CommitToken + outbox (PostgreSQL, env-gated).
//!
//! Parameter binding in the PostgreSQL wire client is still missing, so the
//! adapter reports its documented boundary instead of pretending that an outbox
//! transaction committed. This run asserts that boundary rather than the
//! unfinished mutation path.

use odm_adapter::adapter_postgres::PostgresSource;

fn live_url() -> Option<String> {
    std::env::var("IRIS_TEST_POSTGRES_URL").ok().filter(|s| !s.is_empty())
}

const SCHEMA: &str = r#"
table IrisOutboxUser {
    @@user_id: utf8,
    @user_name: utf8,
}
"#;

fn drop_tables(source: &PostgresSource) {
    source
        .with_connection(|c| {
            c.execute("DROP TABLE IF EXISTS \"IrisOutboxUser\"").unwrap();
            c.execute("DROP TABLE IF EXISTS _iris_outbox").unwrap();
            c.execute("DROP TABLE IF EXISTS _iris_commit").unwrap();
            Ok(())
        })
        .unwrap();
}

#[test]
fn live_authority_outbox_commit_token() {
    let Some(url) = live_url()
    else {
        eprintln!("skip: set IRIS_TEST_POSTGRES_URL for Phase 10-B Postgres outbox");
        return;
    };
    let db = PostgresSource::connect(&url).expect("connect");
    drop_tables(&db);

    db.managed_push(SCHEMA).unwrap();

    let err = db.ensure_outbox_schema().expect_err("outbox install awaits gateway wire parameter binding");
    assert!(err.to_string().contains("awaits gateway wire client parameter binding"), "{err}");
    let err = db.authority_commit(|_| Ok(())).expect_err("authority commit awaits gateway wire parameter binding");
    assert!(err.to_string().contains("awaits gateway wire client parameter binding"), "{err}");
    let err = db.outbox_after(0, 10).expect_err("outbox replay awaits gateway wire parameter binding");
    assert!(err.to_string().contains("awaits gateway wire client parameter binding"), "{err}");

    drop_tables(&db);
}
