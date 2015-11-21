//! Phase 4: MySQL foreign adapter — offline + optional live tests.
//!
//! Live tests speak the MySQL wire client (`Session`), never the third-party
//! `mysql` crate. They skip only when no server URL is configured or the
//! server cannot be reached.

use database_query::ir::RealizationClass;
use odm_adapter::adapter_mysql::{BACKEND_ID, MysqlSource, adopt_plan, classify_type};
use odm_types::{MappingQuality, ObservedCatalog, ObservedColumn, ObservedTable, Planner, RowWrite, Value};
use std::{collections::BTreeMap, sync::Mutex};

/// Live MySQL tests share one database and mutate the same `User` table.
static LIVE_MYSQL_LOCK: Mutex<()> = Mutex::new(());

fn live_mysql_lock() -> std::sync::MutexGuard<'static, ()> {
    LIVE_MYSQL_LOCK.lock().unwrap_or_else(|err| err.into_inner())
}

const USER_SCHEMA: &str = r#"
table User {
    @@user_id: uuid,
    @user_name: utf8,
    active: bool,
}
"#;

fn live_url() -> Option<String> {
    std::env::var("IRIS_TEST_MYSQL_URL").ok().filter(|s| !s.is_empty())
}

fn drop_user_table(db: &MysqlSource) -> odm_adapter::Result<()> {
    db.transaction(|conn| {
        conn.execute("DROP TABLE IF EXISTS `User`").map_err(odm_adapter::Error::Wire)?;
        Ok(())
    })
}

#[test]
fn adapter_is_not_sqlite_or_postgres() {
    assert_eq!(BACKEND_ID, "mysql");
    assert_ne!(BACKEND_ID, "sqlite");
    assert_ne!(BACKEND_ID, "postgres");
    assert_ne!(BACKEND_ID, "yydb");
    let manifest = include_str!("../../Cargo.toml");
    assert!(manifest.contains("database-client"));
    assert!(!manifest.contains("rusqlite"));
    assert!(!manifest.contains("mysql_async"));
}

#[test]
fn mysql_type_mapping_is_dialect_specific() {
    assert_eq!(classify_type("bool", "tinyint").0, MappingQuality::Compatible);
    assert_eq!(classify_type("utf8", "text").0, MappingQuality::Exact);
    assert_eq!(classify_type("uuid", "char").0, MappingQuality::Compatible);
    assert_eq!(classify_type("uuid", "binary").0, MappingQuality::Exact);
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
    let doc = odm_types::parse_schema(
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
    assert!(legacy.blockers.iter().any(|b| b.contains("no primary key")), "{:?}", legacy.blockers);
}

#[test]
fn plan_push_emits_add_field_for_missing_columns() {
    use odm_adapter::adapter_mysql::plan_push;
    use odm_types::LogicalChange;

    let catalog = ObservedCatalog {
        backend_id: BACKEND_ID.into(),
        tables: vec![ObservedTable {
            name: "Goods".into(),
            columns: vec![
                ObservedColumn { name: "sku_id".into(), type_name: "varchar".into(), nullable: false, primary_key: true },
                ObservedColumn { name: "name".into(), type_name: "text".into(), nullable: false, primary_key: false },
            ],
        }],
    };
    let doc = odm_types::parse_schema(
        r#"
        table Goods {
            @@sku_id: utf8,
            name: utf8,
            cover_url: utf8?,
            status: utf8,
        }
        "#,
    )
    .expect("parse");
    let plan = plan_push(&doc, &catalog).expect("plan");
    let adds: Vec<_> = plan
        .changes
        .iter()
        .filter_map(|c| match c {
            LogicalChange::AddField { vos_table, vos_field } => Some((vos_table.as_str(), vos_field.as_str())),
            _ => None,
        })
        .collect();
    assert!(adds.contains(&("Goods", "cover_url")), "{adds:?}");
    assert!(adds.contains(&("Goods", "status")), "{adds:?}");
    assert!(!plan.changes.iter().any(|c| matches!(c, LogicalChange::CreateTable { .. })));
}

#[test]
fn live_managed_push_crud_txn_drift_and_pool() {
    let _lock = live_mysql_lock();
    let Some(url) = live_url()
    else {
        eprintln!("skip: set IRIS_TEST_MYSQL_URL for live MySQL conformance");
        return;
    };
    let db = MysqlSource::connect(&url).expect("connect");
    if let Err(err) = db.ping() {
        eprintln!("skip: MySQL gateway wire client not ready ({err})");
        return;
    }
    let db = db.with_vos_schema(USER_SCHEMA).expect("uuid schema map");

    drop_user_table(&db).expect("drop");

    let report = db.managed_push(USER_SCHEMA).expect("push");
    assert_eq!(report.created_tables, vec!["User".to_string()]);
    assert!(!format!("{report:?}").contains("CREATE TABLE"));
    assert!(db.drift(USER_SCHEMA).unwrap().is_clean());

    db.insert(&RowWrite {
        table: "User".into(),
        primary_key: "user_id".into(),
        fields: BTreeMap::from([
            ("user_id".into(), Value::Str("550e8400-e29b-41d4-a716-446655440000".into())),
            ("user_name".into(), Value::Str("alice".into())),
            ("active".into(), Value::Bool(true)),
        ]),
    })
    .unwrap();
    db.insert(&RowWrite {
        table: "User".into(),
        primary_key: "user_id".into(),
        fields: BTreeMap::from([
            ("user_id".into(), Value::Str("6ba7b810-9dad-11d1-80b4-00c04fd430c8".into())),
            ("user_name".into(), Value::Str("bob".into())),
            ("active".into(), Value::Bool(false)),
        ]),
    })
    .unwrap();

    let plan = Planner::new(MysqlSource::capabilities())
        .plan_source(
            r#"
            User.filter(x => x.user_id == "550e8400-e29b-41d4-a716-446655440000")
                .collect()
            "#,
        )
        .unwrap();
    let plan_where = Planner::new(MysqlSource::capabilities())
        .plan_source(
            r#"
            User.where(user_id == "550e8400-e29b-41d4-a716-446655440000")
                .collect()
            "#,
        )
        .unwrap();
    assert_eq!(plan.nodes, plan_where.nodes);
    let rows = db.execute_plan(&plan).unwrap();
    let rows_where = db.execute_plan(&plan_where).unwrap();
    assert_eq!(rows, rows_where);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("user_id"), Some(&Value::Str("550e8400-e29b-41d4-a716-446655440000".into())));

    let plan = Planner::new(MysqlSource::capabilities())
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

    // Write plans: MySQL has no RETURNING, so Insert re-queries the new row and
    // Patch reads its target rows before the update.
    let new_id = "dddddddd-eeee-ffff-0000-111111111111";
    let insert_plan = Planner::new(MysqlSource::capabilities())
        .plan_source(&format!(r#"User::insert({{ user_id: "{new_id}", user_name: "carol", active: true }})"#))
        .unwrap();
    let rows = db.execute_plan(&insert_plan).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("user_id"), Some(&Value::Str(new_id.into())));
    assert_eq!(rows[0].get("user_name"), Some(&Value::Str("carol".into())));
    assert_eq!(rows[0].get("active"), Some(&Value::Bool(true)));

    let patch_plan = Planner::new(MysqlSource::capabilities())
        .plan_source(&format!(r#"User.filter(x => x.user_id == "{new_id}").patch({{ user_name: "carol2" }})"#))
        .unwrap();
    let rows = db.execute_plan(&patch_plan).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("user_name"), Some(&Value::Str("carol2".into())));

    let read_id =
        Planner::new(MysqlSource::capabilities()).plan_source(&format!(r#"User.filter(x => x.user_id == "{new_id}").collect()"#)).unwrap();
    assert_eq!(db.execute_plan(&read_id).unwrap()[0].get("user_name"), Some(&Value::Str("carol2".into())));

    let delete_plan =
        Planner::new(MysqlSource::capabilities()).plan_source(&format!(r#"User.filter(x => x.user_id == "{new_id}").delete()"#)).unwrap();
    assert!(db.execute_plan(&delete_plan).unwrap().is_empty());
    assert!(db.execute_plan(&read_id).unwrap().is_empty());

    let n = db
        .update(&RowWrite {
            table: "User".into(),
            primary_key: "user_id".into(),
            fields: BTreeMap::from([
                ("user_id".into(), Value::Str("6ba7b810-9dad-11d1-80b4-00c04fd430c8".into())),
                ("active".into(), Value::Bool(true)),
            ]),
        })
        .unwrap();
    assert!(n >= 1);
    let n = db.delete("User", "user_id", &Value::Str("550e8400-e29b-41d4-a716-446655440000".into())).unwrap();
    assert!(n >= 1);

    let again = db.plan_managed_push(USER_SCHEMA).unwrap();
    assert!(again.changes.is_empty());

    let manifest = db.adopt(USER_SCHEMA).unwrap();
    assert_eq!(manifest.adapter_id, BACKEND_ID);

    drop_user_table(&db).expect("drop");
}

#[test]
fn live_same_connection_txn_visible_and_rolls_back_on_err() {
    let _lock = live_mysql_lock();
    let Some(url) = live_url()
    else {
        eprintln!("skip: set IRIS_TEST_MYSQL_URL for live MySQL conformance");
        return;
    };
    let db = MysqlSource::connect(&url).expect("connect");
    if let Err(err) = db.ping() {
        eprintln!("skip: MySQL gateway wire client not ready ({err})");
        return;
    }
    let db = db.with_vos_schema(USER_SCHEMA).expect("uuid schema map");

    drop_user_table(&db).expect("drop");
    db.managed_push(USER_SCHEMA).expect("push");

    let fixture_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let write = RowWrite {
        table: "User".into(),
        primary_key: "user_id".into(),
        fields: BTreeMap::from([
            ("user_id".into(), Value::Str(fixture_id.into())),
            ("user_name".into(), Value::Str("txn_visible".into())),
            ("active".into(), Value::Bool(true)),
        ]),
    };
    let plan =
        Planner::new(MysqlSource::capabilities()).plan_source(&format!(r#"User.filter(x => x.user_id == "{fixture_id}").collect()"#)).unwrap();

    db.transaction(|conn| {
        db.insert_on(conn, &write)?;
        let rows = db.execute_plan_on(conn, &plan)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("user_name"), Some(&Value::Str("txn_visible".into())));
        Ok(())
    })
    .unwrap();

    assert_eq!(db.execute_plan(&plan).unwrap().len(), 1);

    let ghost_id = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff";
    let ghost = RowWrite {
        table: "User".into(),
        primary_key: "user_id".into(),
        fields: BTreeMap::from([
            ("user_id".into(), Value::Str(ghost_id.into())),
            ("user_name".into(), Value::Str("ghost".into())),
            ("active".into(), Value::Bool(false)),
        ]),
    };
    let ghost_plan =
        Planner::new(MysqlSource::capabilities()).plan_source(&format!(r#"User.filter(x => x.user_id == "{ghost_id}").collect()"#)).unwrap();
    let err = db
        .transaction(|conn| -> odm_adapter::Result<()> {
            db.insert_on(conn, &ghost)?;
            Err(odm_adapter::Error::Policy("force rollback".into()))
        })
        .expect_err("must roll back");
    assert!(err.to_string().contains("force rollback"));
    assert!(db.execute_plan(&ghost_plan).unwrap().is_empty());

    drop_user_table(&db).expect("drop");
}

#[test]
fn live_with_rollback_leaves_no_residue_on_success() {
    let _lock = live_mysql_lock();
    let Some(url) = live_url()
    else {
        eprintln!("skip: set IRIS_TEST_MYSQL_URL for live MySQL conformance");
        return;
    };
    let db = MysqlSource::connect(&url).expect("connect");
    if let Err(err) = db.ping() {
        eprintln!("skip: MySQL gateway wire client not ready ({err})");
        return;
    }
    let db = db.with_vos_schema(USER_SCHEMA).expect("uuid schema map");

    drop_user_table(&db).expect("drop");
    db.managed_push(USER_SCHEMA).expect("push");

    let fixture_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let write = RowWrite {
        table: "User".into(),
        primary_key: "user_id".into(),
        fields: BTreeMap::from([
            ("user_id".into(), Value::Str(fixture_id.into())),
            ("user_name".into(), Value::Str("rollback_ok".into())),
            ("active".into(), Value::Bool(true)),
        ]),
    };
    let plan =
        Planner::new(MysqlSource::capabilities()).plan_source(&format!(r#"User.filter(x => x.user_id == "{fixture_id}").collect()"#)).unwrap();

    db.with_rollback(|conn| {
        db.insert_on(conn, &write)?;
        let rows = db.execute_plan_on(conn, &plan)?;
        assert_eq!(rows.len(), 1);
        Ok(())
    })
    .unwrap();

    assert!(db.execute_plan(&plan).unwrap().is_empty());

    drop_user_table(&db).expect("drop");
}
