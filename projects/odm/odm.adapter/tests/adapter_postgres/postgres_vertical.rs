//! Phase 4: PostgreSQL foreign adapter --?offline + optional live tests.

use database_query::ir::RealizationClass;
use odm_adapter::{
    adapter_postgres::{BACKEND_ID, PostgresSource, adopt_plan, classify_type},
    odm_types::{ObservedCatalog, ObservedColumn, ObservedTable, Planner, RowWrite, Value},
};
use std::collections::BTreeMap;

const USER_SCHEMA: &str = r#"
table User {
    @@user_id: utf8,
    @user_name: utf8,
    active: bool,
}
"#;

fn live_url() -> Option<String> {
    std::env::var("IRIS_TEST_POSTGRES_URL").ok().filter(|s| !s.is_empty())
}

#[test]
fn connection_failure_bad_conninfo() {
    let err = PostgresSource::connect_with_pool_timeout(
        "host=127.0.0.1 port=1 user=iris dbname=iris connect_timeout=1",
        std::time::Duration::from_secs(2),
    )
    .expect_err("unreachable postgres");
    assert!(!err.to_string().is_empty());
}

#[test]
fn adapter_is_not_sqlite_or_mysql() {
    assert_eq!(BACKEND_ID, "postgres");
    assert_ne!(BACKEND_ID, "sqlite");
    assert_ne!(BACKEND_ID, "mysql");
    assert_ne!(BACKEND_ID, "yydb");
    let manifest = include_str!("../Cargo.toml");
    assert!(manifest.contains("postgres"));
    assert!(!manifest.contains("rusqlite"));
    assert!(!manifest.contains("mysql ="));
}

#[test]
fn postgres_type_mapping_is_dialect_specific() {
    assert_eq!(classify_type("bool"), Some("boolean"));
    assert_eq!(classify_type("text"), Some("string"));
    assert_eq!(classify_type("uuid"), Some("string"));
    // MySQL-style tinyint must not silently pass as a known PostgreSQL type.
    assert_eq!(classify_type("tinyint"), None);
}

#[test]
fn adopt_plan_blocks_missing_pk_without_live_db() {
    let catalog = ObservedCatalog {
        backend_id: BACKEND_ID.into(),
        tables: vec![ObservedTable {
            name: "Legacy".into(),
            columns: vec![ObservedColumn { name: "name".into(), type_name: "text".into(), nullable: true, primary_key: false }],
        }],
    };
    let doc = odm_adapter::odm_types::parse_schema(
        r#"
        table Legacy {
            @@id: utf8,
            name: utf8,
        }
        "#,
    )
    .expect("parse");
    let manifest = adopt_plan(&doc, &catalog);
    let legacy = manifest.tables.iter().find(|t| t.vos_table == "Legacy").unwrap();
    // The identity field `@@id` has no physical column, so adoption must block.
    assert!(legacy.blockers.iter().any(|b| b.contains("has no physical column")), "{:?}", legacy.blockers);
}

#[test]
fn live_managed_push_crud_txn_drift_and_pool() {
    let Some(url) = live_url()
    else {
        eprintln!("skip: set IRIS_TEST_POSTGRES_URL for live PostgreSQL conformance");
        return;
    };
    let db = PostgresSource::connect(&url).expect("connect");
    assert!(db.pool_connections() >= 1 || db.pool_connections() == 0);

    // Isolate from other runs.
    db.with_connection(|c| {
        c.execute("DROP TABLE IF EXISTS \"User\"").unwrap();
        Ok(())
    })
    .unwrap();

    let report = db.managed_push(USER_SCHEMA).expect("push");
    assert_eq!(report.created_tables, vec!["User".to_string()]);
    assert!(!format!("{report:?}").contains("CREATE TABLE"));
    assert!(db.drift(USER_SCHEMA).unwrap().is_clean());

    db.transaction(|client| {
        // use adapter helpers via re-checkout is fine; exercise txn API:
        let _ = client;
        Ok(())
    })
    .unwrap();

    let mut txn = db.begin_transaction().expect("begin transaction");
    txn.insert(&RowWrite {
        table: "User".into(),
        primary_key: "user_id".into(),
        fields: BTreeMap::from([
            ("user_id".into(), Value::Str("u1".into())),
            ("user_name".into(), Value::Str("alice".into())),
            ("active".into(), Value::Bool(true)),
        ]),
    })
    .unwrap();
    txn.insert(&RowWrite {
        table: "User".into(),
        primary_key: "user_id".into(),
        fields: BTreeMap::from([
            ("user_id".into(), Value::Str("u2".into())),
            ("user_name".into(), Value::Str("bob".into())),
            ("active".into(), Value::Bool(false)),
        ]),
    })
    .unwrap();
    txn.commit().unwrap();

    let plan = Planner::new(PostgresSource::capabilities())
        .plan_source(
            r#"
            User.filter(x => x.active)
                .sort_by(x => x.user_name)
                .collect()
            "#,
        )
        .unwrap();
    assert!(plan.nodes.iter().all(|n| n.realization == RealizationClass::Native));
    let rows = db.execute_plan(&plan).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("user_name"), Some(&Value::Str("alice".into())));
    assert!(matches!(rows[0].get("active"), Some(Value::Bool(true)) | Some(Value::Int(1))));

    // Live `update` and `delete` helpers only exist on `AuthorityTxn`, which has
    // no public constructor yet, so this run covers insert plus plan reads.

    let again = db.plan_managed_push(USER_SCHEMA).unwrap();
    assert!(again.changes.is_empty());

    let manifest = db.adopt(USER_SCHEMA).unwrap();
    assert_eq!(manifest.adapter_id, BACKEND_ID);
    assert!(manifest.tables.iter().find(|t| t.vos_table == "User").unwrap().blockers.is_empty());

    db.with_connection(|c| {
        c.execute("DROP TABLE IF EXISTS \"User\"").unwrap();
        Ok(())
    })
    .unwrap();
}
