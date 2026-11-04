use yyds_gateway::{
    bind_sql, GatewayConfig, GatewayKind, SqlBoundExpression, SqlBoundLiteral, SqlBoundProjection, SqlBoundStatement,
    SqlDialect, ALL_GATEWAY_KINDS, FORBIDDEN_LEGACY_SURFACES,
};

/// 提取数值列的 `f64` 视图。整数列现在是精确的 `Integer`，浮点结果仍是 `Number`。
fn numeric(value: &yyds_gateway::SqlValue) -> f64 {
    match value {
        yyds_gateway::SqlValue::Integer(n) => *n as f64,
        yyds_gateway::SqlValue::Number(n) => *n,
        other => panic!("expected numeric value, got {other:?}"),
    }
}

fn catalog(source: &str) -> yyds_gateway::SqlCatalog {
    let projection = vos::parse_oak(source).expect("Oak VOS source").project_schema().expect("projection");
    let mut next_field_id = 1u64;
    let types = projection
        .types
        .iter()
        .enumerate()
        .map(|(type_index, entry)| vos::types::TypeIdentity {
            canonical_path: entry.canonical_path.clone(),
            type_id: type_index as u64 + 1,
            kind: entry.kind,
            fields: entry
                .fields
                .iter()
                .enumerate()
                .map(|(field_index, field)| {
                    let field_id = next_field_id;
                    next_field_id += 1;
                    vos::types::FieldIdentity {
                        canonical_name: field.canonical_name.clone(),
                        field_id,
                        virtual_field_index: Some(field_index as u32),
                    }
                })
                .collect(),
        })
        .collect();
    let manifest = vos::types::IdentityManifest { format_version: vos::types::IDENTITY_MANIFEST_VERSION.into(), types };
    let contract = vos::resolve_contract(&projection, &manifest).expect("resolved contract");
    yyds_gateway::SqlCatalog::from_resolved_contract(7, &contract).expect("gateway catalog")
}

/// 由 DDL 建出的 catalog，会带上 `UNIQUE` 约束与序列列。
fn ddl_catalog(source: &str) -> yyds_gateway::SqlCatalog {
    let registry = yyds_gateway::SqlSchemaRegistry::new_in_memory();
    let statement = yyds_gateway::parse_ddl(source, SqlDialect::Postgres).expect("DDL parses").expect("DDL statement");
    registry.apply_ddl(&statement).expect("DDL applies");
    registry.catalog()
}

/// 用 catalog 自带的 schema version 绑定，避免固定版本号。
fn run_catalog_sql(
    executor: &dyn yyds_execution::KeyValueExecutor,
    catalog: &yyds_gateway::SqlCatalog,
    sql: &str,
) -> Result<yyds_gateway::SqlResult, yyds_gateway::SqlFrontendError> {
    let bound = yyds_gateway::bind_sql_catalog(sql, catalog, catalog.schema_version(), SqlDialect::Postgres)?;
    let plan = yyds_gateway::plan_statement(bound, catalog)?;
    yyds_gateway::execute_plan(executor, None, &plan, SqlDialect::Postgres)
}

#[test]
fn sql_catalog_binding_consumes_published_yyds_identity() {
    let mut yyds_catalog = yyds_catalog::Catalog::open_memory();
    yyds_catalog.ensure_schema(7, "table users { @@id: i64 }\n").expect("publish VOS schema");
    let catalog = yyds_gateway::SqlCatalog::from_yyds_catalog(&yyds_catalog).expect("published identity");
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT id FROM users", &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    assert!(matches!(bound, yyds_gateway::SqlCatalogBoundStatement::Select(_)));
}

#[test]
fn sql_catalog_binding_prefers_published_resolved_contract() {
    let source = "table users { @@id: i64 }\n";
    let projection = vos::parse_oak(source).expect("Oak parses").project_schema().expect("projection");
    let manifest = vos::types::IdentityManifest {
        format_version: vos::types::IDENTITY_MANIFEST_VERSION.into(),
        types: vec![vos::types::TypeIdentity {
            canonical_path: vec!["users".into()],
            type_id: 1,
            kind: vos::types::TypeContractKind::Table,
            fields: vec![vos::types::FieldIdentity { canonical_name: "id".into(), field_id: 1, virtual_field_index: Some(0) }],
        }],
    };
    let contract = vos::resolve_contract(&projection, &manifest).expect("resolved contract");
    let mut yyds_catalog = yyds_catalog::Catalog::open_memory();
    yyds_catalog.ensure_schema(7, source).expect("schema");
    yyds_catalog.publish_resolved_contract(contract).expect("contract");
    let catalog = yyds_gateway::SqlCatalog::from_yyds_catalog(&yyds_catalog).expect("gateway catalog");
    assert_eq!(catalog.tables()[0].type_id, 1);
    assert_eq!(catalog.tables()[0].fields[0].field_id, 1);
}

#[test]
fn all_gateway_kinds_have_distinct_ids() {
    let ids: Vec<_> = ALL_GATEWAY_KINDS.iter().map(|kind| kind.id()).collect();
    let mut unique = ids.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(ids.len(), unique.len());
}

#[test]
fn startup_banner_uses_crate_name_and_port() {
    let banner = yyds_gateway::startup_banner(&GatewayConfig::new(GatewayKind::Redis).with_port(6380));
    assert!(banner.contains("yyds-gateway-redis"));
    assert!(banner.contains("6380"));
}

#[test]
fn forbidden_legacy_surfaces_include_sql_gateway() {
    assert!(FORBIDDEN_LEGACY_SURFACES.contains(&"yyds-gateway/src/sql"));
    assert!(yyds_gateway::is_forbidden_legacy_surface("yyds-gateway/src/sql"));
    assert!(!FORBIDDEN_LEGACY_SURFACES.contains(&"oak-sql"));
    assert!(!yyds_gateway::is_forbidden_legacy_surface("oak-sql"));
}

#[test]
fn sql_surface_is_parsed_by_oak_without_creating_a_gateway_planner() {
    yyds_gateway::parse_sql("SELECT 1", SqlDialect::Postgres).expect("Oak parses the SQL compatibility surface");
}

#[test]
fn sql_binding_preserves_surface_shape_for_a_yyds_catalog_binder() {
    let bound = bind_sql("SELECT id, 1 AS constant FROM users WHERE id = 7", SqlDialect::Postgres).expect("bind");
    let select = match bound {
        SqlBoundStatement::Select(s) => s,
        other => panic!("expected SqlBoundStatement::Select, got {other:?}"),
    };
    assert!(matches!(
        select.source.as_ref(),
        Some(yyds_gateway::SqlBoundSource::Table { name, alias: None }) if name == "users"
    ));
    assert_eq!(select.projections.len(), 2);
    assert!(matches!(
        &select.projections[0],
        SqlBoundProjection::Expression {
            expression: SqlBoundExpression::Identifier(name),
            alias: None,
        } if name == "id"
    ));
    assert!(matches!(
        &select.projections[1],
        SqlBoundProjection::Expression {
            expression: SqlBoundExpression::Literal(SqlBoundLiteral::Integer(value)),
            alias: Some(alias),
        } if *value == 1 && alias == "constant"
    ));
    assert!(select.predicate.is_some());
}

#[test]
fn sql_binding_rejects_unimplemented_clauses_and_dml() {
    assert!(bind_sql("CREATE TABLE users (id i64)", SqlDialect::Postgres).is_err());
    assert!(bind_sql("SELECT * FROM a JOIN b", SqlDialect::Postgres).is_err()); // JOIN without ON
}

#[test]
fn sql_catalog_binding_resolves_vos_type_and_field_identity() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\ntable admins { @@id: i64 }\n");
    let bound = yyds_gateway::bind_sql_catalog("SELECT id, name FROM users WHERE id = 7", &catalog, 7, SqlDialect::Postgres)
        .expect("catalog bind");
    let select = match bound {
        yyds_gateway::SqlCatalogBoundStatement::Select(s) => s,
        other => panic!("expected SqlCatalogBoundStatement::Select, got {other:?}"),
    };
    let source = select.source.expect("source");
    assert_eq!(source.name, "users");
    assert_eq!(source.type_id, 1);
    assert_eq!(select.projections.len(), 2);
    assert!(matches!(
        &select.projections[0],
        yyds_gateway::SqlCatalogBoundProjection::Expression {
            expression: yyds_gateway::SqlCatalogBoundExpression::Column(column),
            alias: None,
        } if column.table_name == "users" && column.field_name == "id"
    ));
}

#[test]
fn sql_catalog_binding_rejects_unknown_names_and_schema_versions() {
    let catalog = catalog("table users { @@id: i64 }\n");
    let unknown_table =
        yyds_gateway::bind_sql_catalog("SELECT id FROM missing", &catalog, 7, SqlDialect::Postgres).expect_err("unknown table");
    assert!(unknown_table.message.contains("unknown SQL table"));
    let unknown_column = yyds_gateway::bind_sql_catalog("SELECT missing FROM users", &catalog, 7, SqlDialect::Postgres)
        .expect_err("unknown column");
    assert!(unknown_column.message.contains("unknown SQL column"));
    let version = yyds_gateway::bind_sql_catalog("SELECT id FROM users", &catalog, 8, SqlDialect::Postgres)
        .expect_err("schema version mismatch");
    assert!(version.message.contains("schema version mismatch"));
}

// --- SQL → KV 执行层适配器 ---

fn make_executor_with_row(
    namespace: &str,
    rowid: &[u8],
    row_fields: &[(u64, &[u8])],
) -> yyds_execution::LocalExecutor<yyds_kv::MemoryShard> {
    use yyds_kv::{InlineValue, Key, KvStore, MemoryShard, StoredValue};
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let epoch = ShardEpoch(1);
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(epoch, vec![shard_id.clone()]).expect("routing");

    let mut shard = MemoryShard::new();
    let mut values = yyds_gateway::RowFieldValues::new();
    for (field_id, bytes) in row_fields {
        values.insert(*field_id, bytes.to_vec());
    }
    let row_bytes = yyds_gateway::encode_row(&values);
    shard.put(Key::new(namespace, rowid.to_vec()), StoredValue::Inline(InlineValue(row_bytes))).expect("put row");

    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    yyds_execution::LocalExecutor::new(routing, shards, Vec::new()).expect("executor")
}

#[test]
fn sql_select_pk_equality_executes_against_kv_adapter() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = make_executor_with_row("table:1", b"42", &[(1, b"42"), (2, b"alice"), (3, b"30")]);

    let bound = yyds_gateway::bind_sql_catalog("SELECT id, name FROM users WHERE id = 42", &catalog, 7, SqlDialect::Postgres)
        .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");

    assert_eq!(result.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(42), yyds_gateway::SqlValue::String("alice".to_string()),]);
}

#[test]
fn sql_select_star_expands_all_fields() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_row("table:1", b"1", &[(1, b"1"), (2, b"bob")]);

    let bound = yyds_gateway::bind_sql_catalog("SELECT * FROM users WHERE id = 1", &catalog, 7, SqlDialect::Postgres)
        .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");

    assert_eq!(result.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(result.rows[0].len(), 2);
}

#[test]
fn sql_select_pk_string_literal_uses_string_rowid() {
    let catalog = catalog("table users { @@id: utf8, score: i64 }\n");
    let executor = make_executor_with_row("table:1", b"alice", &[(1, b"alice"), (2, b"100")]);

    let bound = yyds_gateway::bind_sql_catalog("SELECT score FROM users WHERE id = 'alice'", &catalog, 7, SqlDialect::Postgres)
        .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");

    assert_eq!(result.rows[0][0], yyds_gateway::SqlValue::Integer(100));
}

#[test]
fn sql_select_missing_row_returns_empty_result() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    // namespace "table:1" 存在但 key "999" 没有
    let executor = make_executor_with_row("table:1", b"1", &[(1, b"1"), (2, b"exist")]);

    let bound = yyds_gateway::bind_sql_catalog("SELECT name FROM users WHERE id = 999", &catalog, 7, SqlDialect::Postgres)
        .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");

    assert!(result.rows.is_empty());
}

#[test]
fn sql_select_without_pk_generates_scan_plan() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users", &catalog, 7, SqlDialect::Postgres)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("pk-less query should now produce a scan plan");
    match plan {
        yyds_gateway::SqlExecutionPlan::Scan(scan) => {
            assert_eq!(scan.namespace, "table:1");
            assert!(scan.rowid_prefix.is_empty());
            assert!(scan.rowid_after.is_none());
            assert_eq!(scan.limit, 0);
            assert!(scan.remaining_predicate.is_none());
        }
        other => panic!("expected Scan plan, got {other:?}"),
    }
}

#[test]
fn sql_select_non_pk_equality_becomes_scan_with_remaining_predicate() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE name = 'foo'", &catalog, 7, SqlDialect::Postgres)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("non-pk equality should produce scan plan with remaining predicate");
    match plan {
        yyds_gateway::SqlExecutionPlan::Scan(scan) => {
            assert_eq!(scan.namespace, "table:1");
            assert!(scan.remaining_predicate.is_some()); // name = 'foo' stays as row-level filter
        }
        other => panic!("expected Scan plan, got {other:?}"),
    }
}

#[test]
fn sql_select_and_extracts_pk_and_filters_on_remaining() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = make_executor_with_row("table:1", b"5", &[(1, b"5"), (2, b"carol"), (3, b"25")]);

    // AND predicate 包含 pk 等值 + 额外条件；plan 应只提 pk，额外条件作为 remaining
    let bound = yyds_gateway::bind_sql_catalog(
        "SELECT name, age FROM users WHERE id = 5 AND age = 25",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let read_plan = match &plan {
        yyds_gateway::SqlExecutionPlan::Read(p) => p,
        other => panic!("expected Read plan, got {other:?}"),
    };
    assert!(read_plan.remaining_predicate.is_some(), "AND 的右侧应留在 remaining");

    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn sql_select_projects_binary_expression() {
    let catalog = catalog("table users { @@id: i64, x: i64, y: i64 }\n");
    let executor = make_executor_with_row("table:1", b"1", &[(1, b"1"), (2, b"10"), (3, b"3")]);

    // 先确认 Oak 支持哪些 projection 表达式
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT id + 1 AS inc FROM users WHERE id = 1", &catalog, 7, SqlDialect::Postgres)
            .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");

    assert_eq!(result.rows[0][0], yyds_gateway::SqlValue::Integer(2));
}

#[test]
fn row_field_values_round_trip_tlv() {
    let mut row = yyds_gateway::RowFieldValues::new();
    row.insert(1, b"42".to_vec());
    row.insert(7, b"hello".to_vec());
    let bytes = yyds_gateway::encode_row(&row);
    let decoded = yyds_gateway::decode_row(&bytes).expect("decode");
    assert_eq!(decoded.get(1), Some(b"42".as_slice()));
    assert_eq!(decoded.get(7), Some(b"hello".as_slice()));
    assert_eq!(decoded.get(99), None);
}

// --- DML round-trip (INSERT / UPDATE / DELETE) ---

/// 构造一个**空** executor — 用于测试 INSERT 后 SELECT / UPDATE / DELETE。
fn empty_executor() -> yyds_execution::LocalExecutor<yyds_kv::MemoryShard> {
    use yyds_kv::MemoryShard;
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let epoch = ShardEpoch(1);
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(epoch, vec![shard_id.clone()]).expect("routing");
    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, MemoryShard::new());
    yyds_execution::LocalExecutor::new(routing, shards, Vec::new()).expect("executor")
}

/// 三行可空数据：id=1 全 NULL，id=2 空串与 0，id=3 正常字符串与 10。
fn nullable_users() -> (yyds_gateway::SqlCatalog, yyds_execution::LocalExecutor<yyds_kv::MemoryShard>) {
    let catalog = catalog("table users { @@id: i64, name: utf8?, age: i64? }\n");
    let executor = empty_executor();
    for sql in [
        "INSERT INTO users VALUES (1, NULL, NULL)",
        "INSERT INTO users VALUES (2, '', 0)",
        "INSERT INTO users VALUES (3, 'x', 10)",
    ] {
        let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("insert bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("insert plan");
        yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("insert nullable row");
    }
    (catalog, executor)
}

/// `empty_executor` 的 `FileShard` 版本：真正走 commit 的事务测试要求存储具备
/// durable atomic commit 能力，易失的 `MemoryShard` 会在提交边界被显式拒绝。
fn file_empty_executor(directory: &std::path::Path) -> yyds_execution::LocalExecutor<yyds_kv::FileShard> {
    use yyds_kv::FileShard;
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(ShardEpoch(1), vec![shard_id.clone()]).expect("routing");
    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, FileShard::open(directory.join("s1.yykv")).expect("open file shard"));
    yyds_execution::LocalExecutor::new(routing, shards, Vec::new()).expect("executor")
}

/// `nullable_users` 的 `FileShard` 版本，附带必须与 executor 一同存活的 `TempFolder`。
fn file_nullable_users(
) -> (yyds_gateway::SqlCatalog, yyds_execution::LocalExecutor<yyds_kv::FileShard>, boost_fs::temp::TempFolder) {
    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let catalog = catalog("table users { @@id: i64, name: utf8?, age: i64? }\n");
    let executor = file_empty_executor(directory.path());
    for sql in [
        "INSERT INTO users VALUES (1, NULL, NULL)",
        "INSERT INTO users VALUES (2, '', 0)",
        "INSERT INTO users VALUES (3, 'x', 10)",
    ] {
        let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("insert bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("insert plan");
        yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("insert nullable row");
    }
    (catalog, executor, directory)
}

/// `make_executor_with_rows` 的 `FileShard` 版本。
fn make_file_executor_with_rows(
    directory: &std::path::Path,
    namespace: &str,
    rows: &[(&[u8], &[(u64, &[u8])])],
) -> yyds_execution::LocalExecutor<yyds_kv::FileShard> {
    use yyds_kv::{FileShard, InlineValue, Key, KvStore, StoredValue};
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(ShardEpoch(1), vec![shard_id.clone()]).expect("routing");

    let mut shard = FileShard::open(directory.join("s1.yykv")).expect("open file shard");
    for (rowid, fields) in rows {
        let mut values = yyds_gateway::RowFieldValues::new();
        for (field_id, bytes) in *fields {
            values.insert(*field_id, bytes.to_vec());
        }
        let row_bytes = yyds_gateway::encode_row(&values);
        shard.put(Key::new(namespace, rowid.to_vec()), StoredValue::Inline(InlineValue(row_bytes))).expect("put row");
    }

    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    yyds_execution::LocalExecutor::new(routing, shards, Vec::new()).expect("executor")
}

fn make_executor_with_rows(
    namespace: &str,
    rows: &[(&[u8], &[(u64, &[u8])])],
) -> yyds_execution::LocalExecutor<yyds_kv::MemoryShard> {
    use yyds_kv::{InlineValue, Key, KvStore, MemoryShard, StoredValue};
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let epoch = ShardEpoch(1);
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(epoch, vec![shard_id.clone()]).expect("routing");

    let mut shard = MemoryShard::new();
    for (rowid, fields) in rows {
        let mut values = yyds_gateway::RowFieldValues::new();
        for (field_id, bytes) in *fields {
            values.insert(*field_id, bytes.to_vec());
        }
        let row_bytes = yyds_gateway::encode_row(&values);
        shard.put(Key::new(namespace, rowid.to_vec()), StoredValue::Inline(InlineValue(row_bytes))).expect("put row");
    }

    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    yyds_execution::LocalExecutor::new(routing, shards, Vec::new()).expect("executor")
}

#[test]
fn sql_insert_then_select_round_trip() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = empty_executor();

    // INSERT INTO users VALUES (7, 'alice', 30)
    // Oak 解析后 INSERT VALUES 第一个值是 pk literal = 7
    let bound = yyds_gateway::bind_sql_catalog("INSERT INTO users VALUES (7, 'alice', 30)", &catalog, 7, SqlDialect::Postgres)
        .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("insert executes");

    // SELECT 出来验证
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT id, name, age FROM users WHERE id = 7", &catalog, 7, SqlDialect::Postgres)
            .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select after insert");

    assert_eq!(result.columns, vec!["id".to_string(), "name".to_string(), "age".to_string()]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0],
        vec![
            yyds_gateway::SqlValue::Integer(7),
            yyds_gateway::SqlValue::String("alice".into()),
            yyds_gateway::SqlValue::Integer(30),
        ]
    );
}

#[test]
fn sql_int64_round_trip_preserves_precision_beyond_f64() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = empty_executor();
    // 2^53 + 1 无法用 f64 精确表示；旧实现会把它读成 2^53。
    let id: i64 = 9_007_199_254_740_993;
    let age: i64 = i64::MAX - 1;

    let bound = yyds_gateway::bind_sql_catalog(
        &format!("INSERT INTO users VALUES ({id}, 'big', {age})"),
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("insert bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("insert plan");
    yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("insert executes");

    let bound = yyds_gateway::bind_sql_catalog(
        &format!("SELECT id, age FROM users WHERE id = {id}"),
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("select bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("select plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select exact int64");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(id), yyds_gateway::SqlValue::Integer(age)]);

    // 扫描路径的整数等值比较也必须精确，不能先转成 f64。
    let bound = yyds_gateway::bind_sql_catalog(
        &format!("SELECT COUNT(*) FROM users WHERE age = {age}"),
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("count bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("count plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("count exact int64 predicate");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(1)]);

    // SUM 走 i128 精确累加。
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT SUM(age) FROM users", &catalog, 7, SqlDialect::Postgres).expect("sum bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("sum plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("sum exact int64");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(age)]);

    // 整数溢出必须显式失败，而不是静默转成 f64。
    let bound = yyds_gateway::bind_sql_catalog(
        &format!("SELECT age + 2 FROM users WHERE id = {id}"),
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("overflow select bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("overflow select plan");
    let error =
        yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect_err("integer overflow must fail");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::NumericOverflow, "{}", error.message);
}

#[test]
fn sql_contract_catalog_projects_integer_types_for_exact_decode() {
    let catalog = catalog("table users { @@id: i64, age: i64 }\n");
    assert_eq!(catalog.tables()[0].fields[0].field_type, yyds_gateway::SqlFieldType::Int64);
    assert_eq!(catalog.tables()[0].fields[1].field_type, yyds_gateway::SqlFieldType::Int64);

    let executor = make_executor_with_row("table:1", b"1", &[(1, b"1"), (2, b"9007199254740993")]);
    let bound = yyds_gateway::bind_sql_catalog("SELECT age FROM users", &catalog, 7, SqlDialect::Postgres).expect("scan bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("scan plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("contract typed decode");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(9_007_199_254_740_993)]);
}

#[test]
fn sql_aggregate_functions_count_sum_avg_max_min() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = empty_executor();

    // Insert three rows: (7, alice, 30), (11, bob, 25), (13, carol, 35)
    for (id, name, age) in [(7i64, "alice", 30i64), (11, "bob", 25), (13, "carol", 35)] {
        let sql = format!("INSERT INTO users VALUES ({id}, '{name}', {age})");
        let bound = yyds_gateway::bind_sql_catalog(&sql, &catalog, 7, SqlDialect::Postgres).expect("bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
        yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("insert");
    }

    // COUNT(*) → 3
    let bound = yyds_gateway::bind_sql_catalog("SELECT COUNT(*) FROM users", &catalog, 7, SqlDialect::Postgres).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec COUNT(*)");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(3)]);

    // COUNT(age) WHERE age > 28 → alice(30) + carol(35) = 2
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT COUNT(age) FROM users WHERE age > 28", &catalog, 7, SqlDialect::Postgres)
            .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec COUNT(col) WHERE");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(2)]);

    // SUM(age) → 30 + 25 + 35 = 90
    let bound = yyds_gateway::bind_sql_catalog("SELECT SUM(age) FROM users", &catalog, 7, SqlDialect::Postgres).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec SUM");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(90)]);

    // AVG(age) → 90 / 3 = 30
    let bound = yyds_gateway::bind_sql_catalog("SELECT AVG(age) FROM users", &catalog, 7, SqlDialect::Postgres).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec AVG");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Number(30.0)]);

    // MAX(age) → 35
    let bound = yyds_gateway::bind_sql_catalog("SELECT MAX(age) FROM users", &catalog, 7, SqlDialect::Postgres).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec MAX");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(35)]);

    // MIN(age) → 25
    let bound = yyds_gateway::bind_sql_catalog("SELECT MIN(age) FROM users", &catalog, 7, SqlDialect::Postgres).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec MIN");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(25)]);
}

#[test]
fn sql_count_distinct_deduplicates_non_null_values() {
    // nullable_users() seed: (1, NULL, NULL), (2, '', 0), (3, 'x', 10)
    let (catalog, executor) = nullable_users();

    // Verify fixture has the expected seed (3 rows)
    let bound = yyds_gateway::bind_sql_catalog("SELECT COUNT(*) FROM users", &catalog, 7, SqlDialect::Postgres).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(3)]);

    // COUNT(age) — non-NULL ages = 2 rows (age=0 and age=10)
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT COUNT(age) FROM users", &catalog, 7, SqlDialect::Postgres).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec COUNT(col)");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(2)]);

    // COUNT(DISTINCT age) — unique non-NULL ages = 2 distinct values (0 and 10)
    let bound = yyds_gateway::bind_sql_catalog("SELECT COUNT(DISTINCT age) FROM users", &catalog, 7, SqlDialect::Postgres)
        .expect("bind COUNT DISTINCT");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan COUNT DISTINCT");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec COUNT DISTINCT");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(2)], "COUNT(DISTINCT age) on default fixture");

    // Now insert duplicates: age=10 twice and age=5 once
    for (id, name, age) in [(17i64, "d", 10i64), (19, "e", 10), (23, "f", 5)] {
        let sql = format!("INSERT INTO users VALUES ({id}, '{name}', {age})");
        let bound = yyds_gateway::bind_sql_catalog(&sql, &catalog, 7, SqlDialect::Postgres).expect("bind insert");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan insert");
        yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("insert");
    }

    // COUNT(*) = 6 now
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT COUNT(*) FROM users", &catalog, 7, SqlDialect::Postgres).expect("bind COUNT*");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan COUNT*");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec COUNT*");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(6)]);

    // COUNT(age) = 5 (original 2 non-NULL + 3 inserted)
    let bound = yyds_gateway::bind_sql_catalog("SELECT COUNT(age) FROM users", &catalog, 7, SqlDialect::Postgres)
        .expect("bind COUNT col after insert");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan COUNT col");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec COUNT col after insert");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(5)]);

    // COUNT(DISTINCT age) = 3 (unique non-NULL: 0, 10, 5)
    let bound = yyds_gateway::bind_sql_catalog("SELECT COUNT(DISTINCT age) FROM users", &catalog, 7, SqlDialect::Postgres)
        .expect("bind COUNT DISTINCT after insert");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan COUNT DISTINCT");
    let result =
        yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec COUNT DISTINCT after insert");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Integer(3)], "COUNT(DISTINCT age) must dedup 10s and ignore NULL");

    // COUNT(DISTINCT *) is not semantically different from COUNT(*) — still 6 rows
    let bound = yyds_gateway::bind_sql_catalog("SELECT COUNT(DISTINCT *) FROM users", &catalog, 7, SqlDialect::Postgres)
        .expect("bind COUNT DISTINCT star");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan COUNT DISTINCT star");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec COUNT DISTINCT star");
    assert_eq!(
        result.rows[0],
        vec![yyds_gateway::SqlValue::Integer(6)],
        "COUNT(DISTINCT *) should behave as COUNT(*) for row count"
    );
}

#[test]
fn sql_aggregate_rejects_wildcard_for_non_count_functions() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = empty_executor();
    let bound = yyds_gateway::bind_sql_catalog("INSERT INTO users VALUES (7, 'alice', 30)", &catalog, 7, SqlDialect::Postgres)
        .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("insert");

    for sql in ["SELECT SUM(*) FROM users", "SELECT AVG(*) FROM users", "SELECT MIN(*) FROM users", "SELECT MAX(*) FROM users"]
    {
        let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
        let err =
            yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect_err("`*` is only valid for COUNT");
        assert!(format!("{err:?}").contains("does not accept `*`"), "{sql} error: {err:?}");
    }
}

#[test]
fn sql_null_tests_and_not_predicate_use_three_valued_logic() {
    let (catalog, executor) = nullable_users();

    // name IS NULL / IS NOT NULL 是空值存在性判断，不是与 NULL 比较。
    for (sql, expected) in [
        ("SELECT id FROM users WHERE name IS NULL", vec![1i64]),
        ("SELECT id FROM users WHERE name IS NOT NULL ORDER BY id", vec![2, 3]),
    ] {
        let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
        let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute null test");
        let ids: Vec<i64> = result
            .rows
            .iter()
            .map(|row| match row.first() {
                Some(yyds_gateway::SqlValue::Integer(value)) => *value,
                other => panic!("{sql}: unexpected row {other:?}"),
            })
            .collect();
        assert_eq!(ids, expected, "{sql}");
    }

    // `NOT (age = 0)` 对 age = NULL 的行仍是 NULL，因此只返回 id=3。
    let bound = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE NOT (age = 0)", &catalog, 7, SqlDialect::Postgres)
        .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute NOT predicate");
    assert_eq!(result.rows, vec![vec![yyds_gateway::SqlValue::Integer(3)]]);
}

#[test]
fn sql_scan_update_and_delete_rewrite_matching_rows() {
    let (catalog, executor, _directory) = file_nullable_users();

    // name IS NULL 只在 id=1 成立，扫描路径必须只改这一行。
    let updated = run_sql(&executor, None, &catalog, "UPDATE users SET age = 5 WHERE name IS NULL");
    assert_eq!(updated.affected_rows, 1);
    let ids = select_integers(&executor, &catalog, "SELECT id FROM users WHERE age = 5");
    assert_eq!(ids, vec![1]);

    // 主键范围条件走 rowid 下推 + 扫描，两条匹配行都要改。
    let updated = run_sql(&executor, None, &catalog, "UPDATE users SET name = 'u' WHERE id > 1");
    assert_eq!(updated.affected_rows, 2);
    let ids = select_integers(&executor, &catalog, "SELECT id FROM users WHERE name = 'u' ORDER BY id");
    assert_eq!(ids, vec![2, 3]);

    // 无 WHERE 的 UPDATE 覆盖整个 namespace。
    let updated = run_sql(&executor, None, &catalog, "UPDATE users SET age = 7");
    assert_eq!(updated.affected_rows, 3);

    // 扫描 DELETE 返回真实影响行数。
    let deleted = run_sql(&executor, None, &catalog, "DELETE FROM users WHERE age = 7");
    assert_eq!(deleted.affected_rows, 3);
    let ids = select_integers(&executor, &catalog, "SELECT id FROM users");
    assert!(ids.is_empty(), "scan delete should remove every matching row");
}

#[test]
fn sql_scan_update_and_delete_inside_transaction_roll_back() {
    use std::sync::Arc;
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let (catalog, executor) = nullable_users();
    let executor: Arc<dyn yyds_execution::KeyValueExecutor> = Arc::new(executor);
    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(Arc::clone(&executor), None));

    let updated =
        run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, "UPDATE users SET age = 99 WHERE name IS NOT NULL");
    assert_eq!(updated.affected_rows, 2);
    let deleted = run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, "DELETE FROM users WHERE age = 99");
    assert_eq!(deleted.affected_rows, 2);
    let inside = run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, "SELECT id FROM users");
    assert_eq!(inside.rows, vec![vec![yyds_gateway::SqlValue::Integer(1)]]);

    handle.rollback();

    // 回滚后 base 完全恢复，扫描写入没有留下部分提交。
    let ids = select_integers(executor.as_ref(), &catalog, "SELECT id FROM users ORDER BY id");
    assert_eq!(ids, vec![1, 2, 3]);
    let ages = select_integers(executor.as_ref(), &catalog, "SELECT age FROM users WHERE age IS NOT NULL ORDER BY id");
    assert_eq!(ages, vec![0, 10]);
}

#[test]
fn sql_scan_update_conflict_is_rejected_at_commit() {
    use std::sync::Arc;
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let (catalog, executor, _directory) = file_nullable_users();
    let executor: Arc<dyn yyds_execution::KeyValueExecutor> = Arc::new(executor);
    let t1: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(Arc::clone(&executor), None));
    let t2: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(Arc::clone(&executor), None));

    let first = run_sql(executor.as_ref(), Some(t1.as_ref()), &catalog, "UPDATE users SET age = 1 WHERE age IS NOT NULL");
    assert_eq!(first.affected_rows, 2);
    let second = run_sql(executor.as_ref(), Some(t2.as_ref()), &catalog, "UPDATE users SET age = 2 WHERE age IS NOT NULL");
    assert_eq!(second.affected_rows, 2);

    t1.commit().expect("first transaction commits");
    let error = t2.commit().expect_err("scan update must still be validated by OCC");
    assert!(
        matches!(error, yyds_types::Error::CasConflict { .. }),
        "expected CasConflict for the stale scan pre-images, got: {error:?}"
    );
}

/// 执行一条返回单列的 SELECT，只保留 INTEGER 值。
fn select_integers(executor: &dyn yyds_execution::KeyValueExecutor, catalog: &yyds_gateway::SqlCatalog, sql: &str) -> Vec<i64> {
    let result = run_sql(executor, None, catalog, sql);
    result
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(yyds_gateway::SqlValue::Integer(value)) => *value,
            other => panic!("{sql}: unexpected row {other:?}"),
        })
        .collect()
}

#[test]
fn sql_in_subquery_evaluates_membership_with_three_valued_logic() {
    let (catalog, executor) = nullable_users();

    let cases = [
        // IN (SELECT ...) — 只保留 age = 0 的 id=2。
        ("SELECT id FROM users WHERE id IN (SELECT id FROM users WHERE age = 0)", vec![2i64]),
        // NOT IN — 同一条子查询的补集。
        ("SELECT id FROM users WHERE id NOT IN (SELECT id FROM users WHERE age = 0)", vec![1, 3]),
        // 子查询返回 NULL 时 NOT IN 结果未知，整条 predicate 过滤掉所有行。
        ("SELECT id FROM users WHERE id NOT IN (SELECT age FROM users WHERE id = 1)", Vec::new()),
    ];
    for (sql, expected) in cases {
        let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
        let mut result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute IN subquery");
        result.rows.sort_by_key(|row| match row.first() {
            Some(yyds_gateway::SqlValue::Integer(value)) => *value,
            _ => i64::MAX,
        });
        let ids: Vec<i64> = result
            .rows
            .iter()
            .map(|row| match row.first() {
                Some(yyds_gateway::SqlValue::Integer(value)) => *value,
                other => panic!("{sql}: unexpected row {other:?}"),
            })
            .collect();
        assert_eq!(ids, expected, "{sql}");
    }
}

#[test]
fn sql_aggregate_rejects_mixed_aggregate_nonaggregate() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let result = yyds_gateway::bind_sql_catalog("SELECT id, COUNT(*) FROM users", &catalog, 7, SqlDialect::Postgres);
    assert!(result.is_ok());
    let bound = result.unwrap();
    let err = yyds_gateway::plan_statement(bound, &catalog).expect_err("planner should reject mixed");
    let err_debug = format!("{err:?}");
    assert!(err_debug.contains("cannot mix aggregate and non-aggregate"));
}

#[test]
fn sql_limit_with_where_scans_all_then_returns_limited() {
    // Regression: kv_limit 不能下推到 KV-scan 当有 WHERE（非 pk 条件）时。
    // 否则先 scan N 行再 filter 会返回不足 N 行的结果。
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = empty_executor();

    // 按 rowid 顺序（= id 升序）：(7,30), (11,20), (13,35), (17,22)
    for (id, _name, age) in [(7i64, "alice", 30i64), (11, "bob", 20), (13, "carol", 35), (17, "dan", 22)] {
        let sql = format!("INSERT INTO users VALUES ({id}, '', {age})");
        let bound = yyds_gateway::bind_sql_catalog(&sql, &catalog, 7, SqlDialect::Postgres).expect("bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
        yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("insert");
    }

    // WHERE age > 22 LIMIT 2 → 预期：2 行 alice(30), carol(35)
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT id, age FROM users WHERE age > 22 LIMIT 2", &catalog, 7, SqlDialect::Postgres)
            .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec");

    // 顺序由 KV 实现决定（未指定 ORDER BY），只检查数量和值集合。
    assert_eq!(result.rows.len(), 2, "expected 2 rows, got {} — kv_limit pushdown bug", result.rows.len());
    let ages: Vec<f64> = result.rows.iter().map(|r| numeric(&r[1])).collect();
    assert!(ages.contains(&30.0), "missing alice(30)");
    assert!(ages.contains(&35.0), "missing carol(35)");
}

#[test]
fn sql_order_by_with_limit_offset_uses_top_n_heap() {
    // ORDER BY + (LIMIT 或 OFFSET) 必须正确排序后再应用分页。
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = empty_executor();

    // 按 age: bob=20, dan=22, alice=30, carol=35
    for (id, _name, age) in [(7i64, "alice", 30i64), (11, "bob", 20), (13, "carol", 35), (17, "dan", 22)] {
        let sql = format!("INSERT INTO users VALUES ({id}, '', {age})");
        let bound = yyds_gateway::bind_sql_catalog(&sql, &catalog, 7, SqlDialect::Postgres).expect("bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
        yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("insert");
    }

    // ORDER BY age ASC LIMIT 2 → bob(20), dan(22)
    let bound = yyds_gateway::bind_sql_catalog(
        "SELECT name, age FROM users ORDER BY age ASC LIMIT 2",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec ASC+LIMIT");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][1], yyds_gateway::SqlValue::Integer(20), "ASC first should be bob(20)");
    assert_eq!(result.rows[1][1], yyds_gateway::SqlValue::Integer(22), "ASC second should be dan(22)");

    // ORDER BY age DESC LIMIT 2 → carol(35), alice(30)
    let bound = yyds_gateway::bind_sql_catalog(
        "SELECT name, age FROM users ORDER BY age DESC LIMIT 2",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec DESC+LIMIT");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][1], yyds_gateway::SqlValue::Integer(35), "DESC first should be carol(35)");
    assert_eq!(result.rows[1][1], yyds_gateway::SqlValue::Integer(30), "DESC second should be alice(30)");

    // ORDER BY age ASC LIMIT 1 OFFSET 1 → dan(22) — 跳过 bob(20)
    let bound = yyds_gateway::bind_sql_catalog(
        "SELECT name, age FROM users ORDER BY age ASC LIMIT 1 OFFSET 1",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec ASC+OFFSET+LIMIT");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], yyds_gateway::SqlValue::Integer(22), "OFFSET 1 should be dan(22)");
}

#[test]
fn sql_update_then_select_round_trip() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    // 预置 row: id=9, name=original, age=20
    let executor = make_executor_with_row("table:1", b"9", &[(1, b"9"), (2, b"original"), (3, b"20")]);

    // UPDATE users SET name='updated', age=99 WHERE id = 9
    let bound = yyds_gateway::bind_sql_catalog(
        "UPDATE users SET name = 'updated', age = 99 WHERE id = 9",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("update executes");

    // SELECT 验证
    let bound = yyds_gateway::bind_sql_catalog("SELECT name, age FROM users WHERE id = 9", &catalog, 7, SqlDialect::Postgres)
        .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select after update");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::String("updated".into()), yyds_gateway::SqlValue::Integer(99),]);
}

#[test]
fn sql_delete_removes_row() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_row("table:1", b"55", &[(1, b"55"), (2, b"victim")]);

    // DELETE FROM users WHERE id = 55
    let bound = yyds_gateway::bind_sql_catalog("DELETE FROM users WHERE id = 55", &catalog, 7, SqlDialect::Postgres)
        .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("delete executes");

    // SELECT 验证空
    let bound = yyds_gateway::bind_sql_catalog("SELECT name FROM users WHERE id = 55", &catalog, 7, SqlDialect::Postgres)
        .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select after delete");
    assert!(result.rows.is_empty());
}

#[test]
fn sql_insert_column_list_must_name_catalog_columns() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    // `@@id` 是 catalog 里的主键注解，SQL 列名是 `id`；`@@` 前缀属于系统变量，不能当列名。
    let error = yyds_gateway::bind_sql_catalog(
        "INSERT INTO users (age, @@id, name) VALUES (25, 11, 'bob')",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect_err("system variable prefix is not a column");
    assert!(error.message.contains("unknown SQL column"), "{}", error.message);
}

#[test]
fn sql_insert_pk_must_be_first_value() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    // VALUES 第一个必须是 pk literal — 如果 Oak 把 "1" 当作 name 值也会在 planner 层报错
    // 这里只是验证 planner 接受正确的形式
    let _bound = yyds_gateway::bind_sql_catalog("INSERT INTO users VALUES (1, 'one')", &catalog, 7, SqlDialect::Postgres)
        .expect("insert bind");
}

// --- INSERT ... SELECT ---

#[test]
fn sql_insert_select_from_constants_writes_rows() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = empty_executor();

    let inserted = run_sql(&executor, None, &catalog, "INSERT INTO users (id, name) SELECT 7, 'alice'");
    assert_eq!(inserted.affected_rows, 1);
    assert!(inserted.rows.is_empty());

    let selected = run_sql(&executor, None, &catalog, "SELECT id, name FROM users WHERE id = 7");
    assert_eq!(selected.rows, vec![vec![yyds_gateway::SqlValue::Integer(7), yyds_gateway::SqlValue::String("alice".into())]]);
}

#[test]
fn sql_insert_select_copies_filtered_rows_from_a_table() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\ntable admins { @@id: i64, name: utf8 }\n");
    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let executor = make_file_executor_with_rows(
        directory.path(),
        "table:1",
        &[(b"1", &[(1, b"1"), (2, b"alice")]), (b"2", &[(1, b"2"), (2, b"bob")]), (b"3", &[(1, b"3"), (2, b"carol")])],
    );

    // 源查询走既有 SELECT 读路径，命中 WHERE 的两行写入目标表。
    let inserted = run_sql(&executor, None, &catalog, "INSERT INTO admins (id, name) SELECT id, name FROM users WHERE id > 1");
    assert_eq!(inserted.affected_rows, 2);

    let copied = run_sql(&executor, None, &catalog, "SELECT id, name FROM admins ORDER BY id");
    assert_eq!(
        copied.rows,
        vec![
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::String("bob".into())],
            vec![yyds_gateway::SqlValue::Integer(3), yyds_gateway::SqlValue::String("carol".into())],
        ]
    );
}

#[test]
fn sql_insert_select_requires_matching_column_counts() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let bound =
        yyds_gateway::bind_sql_catalog("INSERT INTO users (id, name) SELECT id FROM users", &catalog, 7, SqlDialect::Postgres)
            .expect("bind");
    let error = yyds_gateway::plan_statement(bound, &catalog).expect_err("column count mismatch");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Syntax);
    assert!(error.message.contains("column count mismatch"), "{}", error.message);
}

#[test]
fn sql_insert_select_rejects_incompatible_column_types() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let bound = yyds_gateway::bind_sql_catalog(
        "INSERT INTO users (id, name) SELECT name, name FROM users",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let error = yyds_gateway::plan_statement(bound, &catalog).expect_err("type mismatch");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::TypeMismatch);
}

#[test]
fn sql_unique_constraint_rejects_duplicate_insert_and_update() {
    let catalog = ddl_catalog(
        "CREATE TABLE \"_iris_outbox\" (\"id\" BIGSERIAL PRIMARY KEY, \"operation_id\" TEXT NOT NULL, \
         \"entity_version\" BIGINT NOT NULL, \"payload\" TEXT, UNIQUE (\"operation_id\", \"entity_version\"))",
    );
    let executor = empty_executor();
    run_catalog_sql(
        &executor,
        &catalog,
        "INSERT INTO \"_iris_outbox\" (\"id\", \"operation_id\", \"entity_version\", \"payload\") VALUES (1, 'op-1', 1, 'first')",
    )
    .expect("first insert");

    // 相同 (operation_id, entity_version) 必须报 DuplicateKey，而不是静默写入第二行。
    let error = run_catalog_sql(
        &executor,
        &catalog,
        "INSERT INTO \"_iris_outbox\" (\"id\", \"operation_id\", \"entity_version\", \"payload\") VALUES (2, 'op-1', 1, 'duplicate')",
    )
    .expect_err("duplicate composite unique key");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::DuplicateKey);
    assert!(error.message.contains("UNIQUE (operation_id, entity_version)"), "unexpected message: {}", error.message);

    // Iris 幂等去重：失败插入不改变行集，仍只有第一条 payload。
    let rows =
        run_catalog_sql(&executor, &catalog, "SELECT \"operation_id\", \"entity_version\", \"payload\" FROM \"_iris_outbox\"")
            .expect("select");
    assert_eq!(
        rows.rows,
        vec![vec![
            yyds_gateway::SqlValue::String("op-1".into()),
            yyds_gateway::SqlValue::Integer(1),
            yyds_gateway::SqlValue::String("first".into()),
        ]]
    );

    run_catalog_sql(
        &executor,
        &catalog,
        "INSERT INTO \"_iris_outbox\" (\"id\", \"operation_id\", \"entity_version\", \"payload\") VALUES (3, 'op-2', 1, 'second')",
    )
    .expect("second insert");
    // UPDATE 把 op-2 的组合改成与 op-1 相同，也必须被拒绝且不落盘。
    let error = run_catalog_sql(
        &executor,
        &catalog,
        "UPDATE \"_iris_outbox\" SET \"operation_id\" = 'op-1' WHERE \"operation_id\" = 'op-2'",
    )
    .expect_err("duplicate composite unique key on update");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::DuplicateKey);
    let rows = run_catalog_sql(&executor, &catalog, "SELECT \"operation_id\" FROM \"_iris_outbox\" ORDER BY \"id\"")
        .expect("select after rejected update");
    assert_eq!(
        rows.rows,
        vec![vec![yyds_gateway::SqlValue::String("op-1".into())], vec![yyds_gateway::SqlValue::String("op-2".into())],]
    );
}

#[test]
fn sql_insert_select_unique_conflict_publishes_nothing() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\ntable admins { @@id: i64, name: utf8 }\n");
    let executor = empty_executor();
    run_sql(&executor, None, &catalog, "INSERT INTO admins VALUES (1, 'existing')");
    run_sql(&executor, None, &catalog, "INSERT INTO users VALUES (1, 'alice')");
    run_sql(&executor, None, &catalog, "INSERT INTO users VALUES (2, 'bob')");

    let bound = yyds_gateway::bind_sql_catalog(
        "INSERT INTO admins (id, name) SELECT id, name FROM users",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let error = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect_err("duplicate primary key");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::DuplicateKey);

    // 第一条冲突时整条语句不发布，id=2 不能落到目标表。
    let admins = run_sql(&executor, None, &catalog, "SELECT id FROM admins");
    assert_eq!(admins.rows, vec![vec![yyds_gateway::SqlValue::Integer(1)]]);
}

#[test]
fn sql_insert_select_inside_transaction_rolls_back() {
    use std::sync::Arc;
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let catalog = catalog("table users { @@id: i64, name: utf8 }\ntable admins { @@id: i64, name: utf8 }\n");
    let executor: Arc<dyn yyds_execution::KeyValueExecutor> = Arc::new(empty_executor());
    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(Arc::clone(&executor), None));

    run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, "INSERT INTO users VALUES (1, 'alice')");
    let inserted =
        run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, "INSERT INTO admins (id, name) SELECT id, name FROM users");
    assert_eq!(inserted.affected_rows, 1);

    // 源查询读到本事务未提交的行，写入也留在事务缓冲里。
    let inside = run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, "SELECT id, name FROM admins");
    assert_eq!(inside.rows, vec![vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("alice".into())]]);
    let outside = run_sql(executor.as_ref(), None, &catalog, "SELECT id FROM admins");
    assert!(outside.rows.is_empty());

    handle.rollback();
}

// --- ON CONFLICT DO NOTHING ---

#[test]
fn sql_on_conflict_do_nothing_skips_existing_rows() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = empty_executor();
    run_sql(&executor, None, &catalog, "INSERT INTO users VALUES (1, 'alice')");

    // 冲突行被跳过，affected rows 只统计真正写入的行。
    let skipped =
        run_sql(&executor, None, &catalog, "INSERT INTO users (id, name) VALUES (1, 'other') ON CONFLICT (id) DO NOTHING");
    assert_eq!(skipped.affected_rows, 0);
    let inserted = run_sql(&executor, None, &catalog, "INSERT INTO users (id, name) VALUES (2, 'bob') ON CONFLICT DO NOTHING");
    assert_eq!(inserted.affected_rows, 1);

    let rows = run_sql(&executor, None, &catalog, "SELECT id, name FROM users ORDER BY id");
    assert_eq!(
        rows.rows,
        vec![
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("alice".into())],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::String("bob".into())],
        ]
    );
}

#[test]
fn sql_on_conflict_do_nothing_skips_conflicting_insert_select_rows() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\ntable admins { @@id: i64, name: utf8 }\n");
    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let executor = make_file_executor_with_rows(
        directory.path(),
        "table:1",
        &[(b"1", &[(1, b"1"), (2, b"alice")]), (b"2", &[(1, b"2"), (2, b"bob")]), (b"3", &[(1, b"3"), (2, b"carol")])],
    );
    run_sql(&executor, None, &catalog, "INSERT INTO admins VALUES (2, 'stale')");

    // 只有 id=1 与 id=3 落库，已存在的 id=2 保持原值。
    let inserted = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO admins (id, name) SELECT id, name FROM users ON CONFLICT (id) DO NOTHING",
    );
    assert_eq!(inserted.affected_rows, 2);

    let copied = run_sql(&executor, None, &catalog, "SELECT id, name FROM admins ORDER BY id");
    assert_eq!(
        copied.rows,
        vec![
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("alice".into())],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::String("stale".into())],
            vec![yyds_gateway::SqlValue::Integer(3), yyds_gateway::SqlValue::String("carol".into())],
        ]
    );
}

#[test]
fn sql_on_conflict_rejects_non_unique_targets_and_mysql() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");

    // 唯一约束只有主键，指向其他列的目标在 planner 层显式拒绝。
    let bound = yyds_gateway::bind_sql_catalog(
        "INSERT INTO users (id, name) VALUES (1, 'alice') ON CONFLICT (name) DO NOTHING",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let error = yyds_gateway::plan_statement(bound, &catalog).expect_err("target is not unique");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Unsupported);
    assert!(error.message.contains("unique constraint"), "{}", error.message);

    // MySQL 没有 ON CONFLICT 语法。
    let bound = yyds_gateway::bind_sql_catalog(
        "INSERT INTO users (id, name) VALUES (1, 'alice') ON CONFLICT DO NOTHING",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let error =
        yyds_gateway::plan_statement_for_dialect(bound, &catalog, SqlDialect::Mysql).expect_err("MySQL has no ON CONFLICT");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Syntax);
}

#[test]
fn sql_on_conflict_do_update_rewrites_existing_rows() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = empty_executor();
    run_sql(&executor, None, &catalog, "INSERT INTO users VALUES (1, 'alice')");

    // 命中既有行：改写而不是新增。
    let updated = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO UPDATE SET name = 'bob'",
    );
    assert_eq!(updated.affected_rows, 1);

    // 未命中：按普通插入落库。
    let inserted = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users (id, name) VALUES (2, 'carol') ON CONFLICT (id) DO UPDATE SET name = 'x'",
    );
    assert_eq!(inserted.affected_rows, 1);

    let rows = run_sql(&executor, None, &catalog, "SELECT id, name FROM users ORDER BY id");
    assert_eq!(
        rows.rows,
        vec![
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("bob".into())],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::String("carol".into())],
        ]
    );
}

#[test]
fn sql_on_conflict_do_update_where_false_leaves_the_row_untouched() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = empty_executor();
    run_sql(&executor, None, &catalog, "INSERT INTO users VALUES (1, 'alice')");

    let skipped = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO UPDATE SET name = 'bob' WHERE name = 'carol'",
    );
    assert_eq!(skipped.affected_rows, 0);

    let matched = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO UPDATE SET name = 'bob' WHERE name = 'alice'",
    );
    assert_eq!(matched.affected_rows, 1);

    let rows = run_sql(&executor, None, &catalog, "SELECT id, name FROM users");
    assert_eq!(rows.rows, vec![vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("bob".into())]]);
}

#[test]
fn sql_on_conflict_do_update_applies_to_every_values_row() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let executor = file_empty_executor(directory.path());
    run_sql(&executor, None, &catalog, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')");

    // 第一行命中改写、第二行未命中照常插入，影响行数统计两行。
    let upserted = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users (id, name) VALUES (1, 'zed'), (3, 'carol') ON CONFLICT (id) DO UPDATE SET name = 'rewritten'",
    );
    assert_eq!(upserted.affected_rows, 2);

    let rows = run_sql(&executor, None, &catalog, "SELECT id, name FROM users ORDER BY id");
    assert_eq!(
        rows.rows,
        vec![
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("rewritten".into())],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::String("bob".into())],
            vec![yyds_gateway::SqlValue::Integer(3), yyds_gateway::SqlValue::String("carol".into())],
        ]
    );
}

#[test]
fn sql_on_conflict_do_update_applies_to_insert_select_rows() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\ntable admins { @@id: i64, name: utf8 }\n");
    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let executor = make_file_executor_with_rows(
        directory.path(),
        "table:1",
        &[(b"1", &[(1, b"1"), (2, b"alice")]), (b"2", &[(1, b"2"), (2, b"bob")])],
    );
    run_sql(&executor, None, &catalog, "INSERT INTO admins VALUES (2, 'stale')");

    let upserted = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO admins (id, name) SELECT id, name FROM users ON CONFLICT (id) DO UPDATE SET name = 'copied'",
    );
    assert_eq!(upserted.affected_rows, 2);

    let rows = run_sql(&executor, None, &catalog, "SELECT id, name FROM admins ORDER BY id");
    assert_eq!(
        rows.rows,
        vec![
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("alice".into())],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::String("copied".into())],
        ]
    );
}

#[test]
fn sql_on_conflict_do_update_rejects_invalid_shapes() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");

    // 缺少冲突目标：PostgreSQL 只允许 DO NOTHING 省略目标。
    let bound = yyds_gateway::bind_sql_catalog(
        "INSERT INTO users (id, name) VALUES (1, 'alice') ON CONFLICT DO UPDATE SET name = 'x'",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let error = yyds_gateway::plan_statement(bound, &catalog).expect_err("DO UPDATE requires a target");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Syntax);
    assert!(error.message.contains("conflict target"), "{}", error.message);

    // 改写主键等于删除加插入，网关显式拒绝而不是近似执行。
    let bound = yyds_gateway::bind_sql_catalog(
        "INSERT INTO users (id, name) VALUES (1, 'alice') ON CONFLICT (id) DO UPDATE SET id = 2",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let error = yyds_gateway::plan_statement(bound, &catalog).expect_err("primary key cannot be reassigned");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Unsupported);
    assert!(error.message.contains("primary key"), "{}", error.message);

    // 结构不完整（`ON CONFLICT` 没有动作）由 Oak 拒绝。
    let error =
        yyds_gateway::bind_sql("INSERT INTO users (id, name) VALUES (1, 'alice') ON CONFLICT (id)", SqlDialect::Postgres)
            .expect_err("ON CONFLICT requires an action");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Syntax);

    // MySQL 方言显式拒绝，不退化成 ON DUPLICATE KEY UPDATE。
    let bound = yyds_gateway::bind_sql_catalog(
        "INSERT INTO users (id, name) VALUES (1, 'alice') ON CONFLICT (id) DO UPDATE SET name = 'x'",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let error =
        yyds_gateway::plan_statement_for_dialect(bound, &catalog, SqlDialect::Mysql).expect_err("MySQL has no ON CONFLICT");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Syntax);
}

#[test]
fn sql_on_conflict_do_update_reads_the_excluded_and_the_stored_row() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = empty_executor();
    run_sql(&executor, None, &catalog, "INSERT INTO users VALUES (1, 'alice', 30)");

    // `EXCLUDED` 指向提议写入的那一行。PostgreSQL 会把未加引号的标识符折叠成小写，
    // 因此两种拼写都接受。
    let copied = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users VALUES (1, 'bob', 99) ON CONFLICT (id) DO UPDATE SET name = excluded.name, age = EXCLUDED.age",
    );
    assert_eq!(copied.affected_rows, 1);
    assert_eq!(
        run_sql(&executor, None, &catalog, "SELECT name, age FROM users").rows,
        vec![vec![yyds_gateway::SqlValue::String("bob".into()), yyds_gateway::SqlValue::Integer(99)]]
    );

    // 既有行以表名限定后可以做 `column + literal`。
    run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users VALUES (1, 'ignored', 100) ON CONFLICT (id) DO UPDATE SET age = users.age + 1",
    );
    assert_eq!(
        run_sql(&executor, None, &catalog, "SELECT name, age FROM users").rows,
        vec![vec![yyds_gateway::SqlValue::String("bob".into()), yyds_gateway::SqlValue::Integer(100)]]
    );
}

/// `DO UPDATE SET` 的右值走普通数值表达式路径：一元负号与列之间的 `+` / `-` 都直接
/// 对既有行与 `EXCLUDED` 求值，再按目标列类型校验后落库。
#[test]
fn sql_on_conflict_do_update_accepts_numeric_arithmetic_over_columns() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = empty_executor();
    run_sql(&executor, None, &catalog, "INSERT INTO users VALUES (1, 'alice', 30)");

    let negated = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users VALUES (1, 'ignored', 99) ON CONFLICT (id) DO UPDATE SET age = -users.age",
    );
    assert_eq!(negated.affected_rows, 1);
    assert_eq!(
        run_sql(&executor, None, &catalog, "SELECT age FROM users").rows,
        vec![vec![yyds_gateway::SqlValue::Integer(-30)]]
    );

    let summed = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users VALUES (1, 'ignored', 99) ON CONFLICT (id) DO UPDATE SET age = users.age + EXCLUDED.age",
    );
    assert_eq!(summed.affected_rows, 1);
    assert_eq!(
        run_sql(&executor, None, &catalog, "SELECT age FROM users").rows,
        vec![vec![yyds_gateway::SqlValue::Integer(69)]]
    );

    let subtracted = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users VALUES (1, 'ignored', 99) ON CONFLICT (id) DO UPDATE SET age = EXCLUDED.age - users.age",
    );
    assert_eq!(subtracted.affected_rows, 1);
    assert_eq!(
        run_sql(&executor, None, &catalog, "SELECT age FROM users").rows,
        vec![vec![yyds_gateway::SqlValue::Integer(30)]]
    );

    let multiplied = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users VALUES (1, 'ignored', 99) ON CONFLICT (id) DO UPDATE SET age = users.age * 3",
    );
    assert_eq!(multiplied.affected_rows, 1);
    assert_eq!(
        run_sql(&executor, None, &catalog, "SELECT age FROM users").rows,
        vec![vec![yyds_gateway::SqlValue::Integer(90)]]
    );

    // PostgreSQL 的整数除法向零截断，与 `SELECT` 路径共用同一份规则。
    let divided = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users VALUES (1, 'ignored', 99) ON CONFLICT (id) DO UPDATE SET age = users.age / 7",
    );
    assert_eq!(divided.affected_rows, 1);
    assert_eq!(
        run_sql(&executor, None, &catalog, "SELECT age FROM users").rows,
        vec![vec![yyds_gateway::SqlValue::Integer(12)]]
    );
}

#[test]
fn sql_on_conflict_do_update_assigns_columns_simultaneously() {
    let catalog = catalog("table pairs { @@id: i64, a: i64, b: i64 }\n");
    let executor = empty_executor();
    run_sql(&executor, None, &catalog, "INSERT INTO pairs VALUES (1, 10, 20)");

    // 两个右值都读取改写前的既有行，交换列不会因为写入顺序而丢失原值。
    run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO pairs VALUES (1, 0, 0) ON CONFLICT (id) DO UPDATE SET a = pairs.b, b = pairs.a",
    );
    assert_eq!(
        run_sql(&executor, None, &catalog, "SELECT a, b FROM pairs").rows,
        vec![vec![yyds_gateway::SqlValue::Integer(20), yyds_gateway::SqlValue::Integer(10)]]
    );
}

#[test]
fn sql_on_conflict_do_update_rejects_expressions_beyond_column_arithmetic() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");

    // 只有字面量、列引用与 `+` / `-` / `*` / `/` 组成的数值表达式能直接在行对上求值，
    // 其余形态显式拒绝。
    for sql in [
        "INSERT INTO users VALUES (1, 'alice', 30) ON CONFLICT (id) DO UPDATE SET age = users.age % 2",
        "INSERT INTO users VALUES (1, 'alice', 30) ON CONFLICT (id) DO UPDATE SET age = CASE WHEN users.age > 0 THEN 1 ELSE 2 END",
        "INSERT INTO users VALUES (1, 'alice', 30) ON CONFLICT (id) DO UPDATE SET age = (SELECT MAX(age) FROM users)",
    ] {
        let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres)
            .unwrap_or_else(|error| panic!("bind `{sql}`: {}", error.message));
        let error = yyds_gateway::plan_statement(bound, &catalog).expect_err("shape is not executable");
        assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Unsupported, "{sql}");
    }

    // 函数调用连上游 `SET` 赋值语法都不接受，在绑定阶段就报错，同样不会被近似执行。
    let function_error = yyds_gateway::bind_sql_catalog(
        "INSERT INTO users VALUES (1, 'alice', 30) ON CONFLICT (id) DO UPDATE SET age = ABS(users.age)",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect_err("a function call is not a supported assignment value");
    assert!(!function_error.message.is_empty(), "function assignment must report a reason");

    // 未限定的列名在既有行与 `EXCLUDED` 之间二义，必须显式限定。
    let error = yyds_gateway::bind_sql_catalog(
        "INSERT INTO users VALUES (1, 'alice', 30) ON CONFLICT (id) DO UPDATE SET age = age + 1",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect_err("unqualified column is ambiguous");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::AmbiguousColumn);

    // `WHERE` 只对既有行求值，`EXCLUDED` 在那里不可见。
    let error = yyds_gateway::bind_sql_catalog(
        "INSERT INTO users VALUES (1, 'alice', 30) ON CONFLICT (id) DO UPDATE SET age = EXCLUDED.age WHERE EXCLUDED.age > 0",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect_err("excluded is not visible in the conflict predicate");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::UnknownTableQualifier);

    // MySQL 方言仍然拒绝整条 UPSERT，不退化成 ON DUPLICATE KEY UPDATE。
    let bound = yyds_gateway::bind_sql_catalog(
        "INSERT INTO users VALUES (1, 'alice', 30) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let error =
        yyds_gateway::plan_statement_for_dialect(bound, &catalog, SqlDialect::Mysql).expect_err("MySQL has no ON CONFLICT");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Syntax);
}

// --- INSERT ... RETURNING ---

#[test]
fn sql_insert_returning_reports_the_written_row() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = empty_executor();

    let inserted = run_sql(&executor, None, &catalog, "INSERT INTO users (id, name) VALUES (1, 'alice') RETURNING id, name");
    assert_eq!(inserted.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(inserted.rows, vec![vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("alice".into())]]);
    assert_eq!(inserted.affected_rows, 1);

    // `RETURNING *` 按 catalog 字段顺序展开。
    let star = run_sql(&executor, None, &catalog, "INSERT INTO users (id, name) VALUES (2, 'bob') RETURNING *");
    assert_eq!(star.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(star.rows, vec![vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::String("bob".into())]]);

    // 别名覆盖默认列名。
    let aliased = run_sql(&executor, None, &catalog, "INSERT INTO users (id, name) VALUES (3, 'carol') RETURNING name AS who");
    assert_eq!(aliased.columns, vec!["who".to_string()]);
    assert_eq!(aliased.rows, vec![vec![yyds_gateway::SqlValue::String("carol".into())]]);
}

#[test]
fn sql_multi_row_insert_returning_reports_every_row() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let executor = file_empty_executor(directory.path());

    let inserted =
        run_sql(&executor, None, &catalog, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob') RETURNING id");
    assert_eq!(inserted.columns, vec!["id".to_string()]);
    assert_eq!(inserted.rows, vec![vec![yyds_gateway::SqlValue::Integer(1)], vec![yyds_gateway::SqlValue::Integer(2)]]);
    assert_eq!(inserted.affected_rows, 2);
}

fn try_run_sql(
    executor: &dyn yyds_execution::KeyValueExecutor,
    catalog: &yyds_gateway::SqlCatalog,
    sql: &str,
) -> Result<yyds_gateway::SqlResult, yyds_gateway::SqlFrontendError> {
    let bound = yyds_gateway::bind_sql_catalog(sql, catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, catalog).expect("plan");
    yyds_gateway::execute_plan(executor, None, &plan, SqlDialect::Postgres)
}

#[test]
fn sql_multi_row_insert_publishes_every_row_through_one_durable_boundary() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let executor = file_empty_executor(directory.path());
    run_sql(&executor, None, &catalog, "INSERT INTO users VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    drop(executor);

    // 重新打开落盘快照：整批三行必须一起可见，不存在半语句。
    let reopened = file_empty_executor(directory.path());
    let rows = run_sql(&reopened, None, &catalog, "SELECT id, name FROM users ORDER BY id");
    assert_eq!(
        rows.rows,
        vec![
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("a".into())],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::String("b".into())],
            vec![yyds_gateway::SqlValue::Integer(3), yyds_gateway::SqlValue::String("c".into())],
        ]
    );
}

#[test]
fn sql_multi_row_insert_rejects_intra_statement_duplicate_rowid_without_publishing_any_row() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let executor = file_empty_executor(directory.path());

    let error = try_run_sql(&executor, &catalog, "INSERT INTO users VALUES (1, 'first'), (1, 'second')")
        .expect_err("duplicate primary key inside one statement");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::DuplicateKey);

    // 旧逐条路径会把第一行留在基存里；原子边界必须让整条语句一行都不发布。
    drop(executor);
    let reopened = file_empty_executor(directory.path());
    let rows = run_sql(&reopened, None, &catalog, "SELECT id, name FROM users ORDER BY id");
    assert!(rows.rows.is_empty(), "half-statement rows leaked: {:?}", rows.rows);
}

#[test]
fn sql_multi_row_insert_rejects_existing_row_without_publishing_later_rows() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let executor = file_empty_executor(directory.path());
    run_sql(&executor, None, &catalog, "INSERT INTO users VALUES (1, 'alice')");

    let error = try_run_sql(&executor, &catalog, "INSERT INTO users VALUES (2, 'bob'), (1, 'zed')")
        .expect_err("second row collides with the stored row");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::DuplicateKey);

    drop(executor);
    let reopened = file_empty_executor(directory.path());
    let rows = run_sql(&reopened, None, &catalog, "SELECT id, name FROM users ORDER BY id");
    assert_eq!(
        rows.rows,
        vec![vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("alice".into())]],
        "the failed statement must not publish its first row"
    );
}

#[test]
fn sql_multi_row_insert_rejects_a_store_without_durable_atomic_commit() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = empty_executor();

    // 易失的 MemoryShard 没有 durable 提交边界，多行语句必须显式拒绝而不是退回逐条发布。
    let error = try_run_sql(&executor, &catalog, "INSERT INTO users VALUES (1, 'a'), (2, 'b')").expect_err("volatile store");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Internal);
    assert!(error.message.contains("durable atomic commit"), "unexpected message: {}", error.message);
}

#[test]
fn sql_multi_row_insert_on_conflict_do_nothing_skips_intra_statement_duplicates() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let executor = file_empty_executor(directory.path());

    let inserted = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users (id, name) VALUES (1, 'first'), (1, 'second'), (2, 'other') ON CONFLICT (id) DO NOTHING",
    );
    assert_eq!(inserted.affected_rows, 2, "first occurrence wins, the duplicate is skipped");

    let rows = run_sql(&executor, None, &catalog, "SELECT id, name FROM users ORDER BY id");
    assert_eq!(
        rows.rows,
        vec![
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("first".into())],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::String("other".into())],
        ]
    );
}

#[test]
fn sql_multi_row_insert_on_conflict_do_update_folds_duplicate_rowids_sequentially() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let executor = file_empty_executor(directory.path());

    // 语句内重复主键按出现顺序折叠：先插入，再基于折叠后的行改写，与逐行语义一致。
    let upserted = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users (id, name) VALUES (1, 'first'), (1, 'second') ON CONFLICT (id) DO UPDATE SET name = excluded.name",
    );
    assert_eq!(upserted.affected_rows, 1);

    let rows = run_sql(&executor, None, &catalog, "SELECT id, name FROM users ORDER BY id");
    assert_eq!(rows.rows, vec![vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("second".into())]]);
}

#[test]
fn sql_insert_returning_reports_the_skipped_and_updated_rows() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = empty_executor();
    run_sql(&executor, None, &catalog, "INSERT INTO users VALUES (1, 'alice')");

    // `DO NOTHING` 跳过冲突行，结果集为空，但列定义仍按 RETURNING 上报。
    let skipped = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO NOTHING RETURNING id, name",
    );
    assert_eq!(skipped.columns, vec!["id".to_string(), "name".to_string()]);
    assert!(skipped.rows.is_empty());
    assert_eq!(skipped.affected_rows, 0);

    // `DO UPDATE` 返回改写后的行。
    let updated = run_sql(
        &executor,
        None,
        &catalog,
        "INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO UPDATE SET name = 'bob' RETURNING id, name",
    );
    assert_eq!(updated.rows, vec![vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("bob".into())]]);
    assert_eq!(updated.affected_rows, 1);
}

#[test]
fn sql_insert_returning_rejects_unknown_columns_and_mysql() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = empty_executor();

    // 未知列在 catalog 绑定层拒绝，语句不执行。
    let bound = yyds_gateway::bind_sql_catalog(
        "INSERT INTO users (id, name) VALUES (1, 'alice') RETURNING missing",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect_err("unknown RETURNING column");
    assert_eq!(bound.kind, yyds_gateway::SqlErrorKind::UnknownColumn);
    let rows = run_sql(&executor, None, &catalog, "SELECT id FROM users");
    assert!(rows.rows.is_empty(), "the rejected statement must not write");

    // MySQL 的 INSERT 没有 RETURNING。
    let bound = yyds_gateway::bind_sql_catalog(
        "INSERT INTO users (id, name) VALUES (1, 'alice') RETURNING id",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let error = yyds_gateway::plan_statement_for_dialect(bound, &catalog, SqlDialect::Mysql)
        .expect_err("MySQL has no INSERT ... RETURNING");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Syntax);
    assert!(error.message.contains("RETURNING"), "{}", error.message);
}

#[test]
fn sql_select_star_from_table_scans_all_rows() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    // 预置三行 — id=1/alice, id=2/bob, id=3/carol
    let executor = make_executor_with_rows(
        "table:1",
        &[(b"1", &[(1, b"1"), (2, b"alice")]), (b"2", &[(1, b"2"), (2, b"bob")]), (b"3", &[(1, b"3"), (2, b"carol")])],
    );

    let bound = yyds_gateway::bind_sql_catalog("SELECT * FROM users", &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    // 确认走 Scan 路径
    assert!(matches!(plan, yyds_gateway::SqlExecutionPlan::Scan(_)));
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");

    assert_eq!(result.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(result.rows.len(), 3);
    let ids: Vec<f64> = result.rows.iter().map(|row| numeric(&row[0])).collect();
    assert_eq!(ids, vec![1.0, 2.0, 3.0]);
}

#[test]
fn sql_scan_applies_remaining_predicate_from_non_pk_equality() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[(b"1", &[(1, b"1"), (2, b"alice")]), (b"2", &[(1, b"2"), (2, b"bob")]), (b"3", &[(1, b"3"), (2, b"alice")])],
    );

    // WHERE name = 'alice' — 不是 pk 等值，planner 输出 ScanPlan 带 remaining_predicate
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT id, name FROM users WHERE name = 'alice'", &catalog, 7, SqlDialect::Postgres)
            .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    assert!(matches!(plan, yyds_gateway::SqlExecutionPlan::Scan(_)));
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");

    assert_eq!(result.rows.len(), 2);
    // alice 在 id=1 和 id=3 两行
    let ids: Vec<f64> = result.rows.iter().map(|row| numeric(&row[0])).collect();
    assert!(ids.contains(&1.0));
    assert!(ids.contains(&3.0));
    assert_eq!(ids.len(), 2);
}

#[test]
fn sql_insert_multiple_then_select_star_returns_all() {
    let catalog = catalog("table items { @@id: i64, sku: utf8 }\n");
    let executor = empty_executor();

    // 连续 INSERT 三行 — 全部写入同一个 executor
    for sql in
        &["INSERT INTO items VALUES (1, 'A1')", "INSERT INTO items VALUES (2, 'B2')", "INSERT INTO items VALUES (3, 'C3')"]
    {
        let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
        yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("insert");
    }

    // SELECT * 验证 scan 路径返回所有三行
    let bound = yyds_gateway::bind_sql_catalog("SELECT * FROM items", &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    assert!(matches!(plan, yyds_gateway::SqlExecutionPlan::Scan(_)));
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("scan");

    assert_eq!(result.rows.len(), 3);
    let ids: Vec<f64> = result.rows.iter().map(|row| numeric(&row[0])).collect();
    assert_eq!(ids, vec![1.0, 2.0, 3.0]);
}

#[test]
fn sql_select_pk_greater_pushes_after_cursor() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[(b"1", &[(1, b"1"), (2, b"alice")]), (b"2", &[(1, b"2"), (2, b"bob")]), (b"3", &[(1, b"3"), (2, b"carol")])],
    );

    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id > 1", &catalog, 7, SqlDialect::Postgres)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(s) => {
            assert_eq!(s.rowid_after.as_deref(), Some(b"1".as_ref()));
            assert!(s.rowid_before.is_none());
            assert_eq!(s.limit, 0);
        }
        other => panic!("expected Scan, got {other:?}"),
    }
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec");
    assert_eq!(result.rows.len(), 2);
    let ids: Vec<f64> = result.rows.iter().map(|r| numeric(&r[0])).collect();
    assert_eq!(ids, vec![2.0, 3.0]);
}

#[test]
fn sql_select_pk_less_pushes_before_cursor() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[(b"1", &[(1, b"1"), (2, b"alice")]), (b"2", &[(1, b"2"), (2, b"bob")]), (b"3", &[(1, b"3"), (2, b"carol")])],
    );

    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id < 3", &catalog, 7, SqlDialect::Postgres)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(s) => {
            assert!(s.rowid_after.is_none());
            assert_eq!(s.rowid_before.as_deref(), Some(b"3".as_ref()));
        }
        other => panic!("expected Scan, got {other:?}"),
    }
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec");
    assert_eq!(result.rows.len(), 2);
    let ids: Vec<f64> = result.rows.iter().map(|r| numeric(&r[0])).collect();
    assert_eq!(ids, vec![1.0, 2.0]);
}

#[test]
fn sql_select_pk_range_after_and_before_together() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[
            (b"1", &[(1, b"1"), (2, b"alice")]),
            (b"2", &[(1, b"2"), (2, b"bob")]),
            (b"3", &[(1, b"3"), (2, b"carol")]),
            (b"4", &[(1, b"4"), (2, b"dan")]),
        ],
    );

    let plan =
        yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id > 1 AND id < 4", &catalog, 7, SqlDialect::Postgres)
            .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
            .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(s) => {
            assert_eq!(s.rowid_after.as_deref(), Some(b"1".as_ref()));
            assert_eq!(s.rowid_before.as_deref(), Some(b"4".as_ref()));
        }
        other => panic!("expected Scan, got {other:?}"),
    }
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec");
    assert_eq!(result.rows.len(), 2);
    let ids: Vec<f64> = result.rows.iter().map(|r| numeric(&r[0])).collect();
    assert_eq!(ids, vec![2.0, 3.0]);
}

#[test]
fn sql_limit_pushes_down_to_scan() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[
            (b"1", &[(1, b"1"), (2, b"alice")]),
            (b"2", &[(1, b"2"), (2, b"bob")]),
            (b"3", &[(1, b"3"), (2, b"carol")]),
            (b"4", &[(1, b"4"), (2, b"dan")]),
            (b"5", &[(1, b"5"), (2, b"eve")]),
        ],
    );

    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users LIMIT 3", &catalog, 7, SqlDialect::Postgres)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(s) => {
            assert_eq!(s.limit, 3);
        }
        other => panic!("expected Scan, got {other:?}"),
    }
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec");
    assert_eq!(result.rows.len(), 3);
    let ids: Vec<f64> = result.rows.iter().map(|r| numeric(&r[0])).collect();
    assert_eq!(ids, vec![1.0, 2.0, 3.0]);
}

#[test]
fn sql_select_pk_like_prefix_pushes_rowid_prefix() {
    let catalog = catalog("table users { @@id: utf8, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[
            (b"alice", &[(1, b"alice"), (2, b"alice wang")]),
            (b"bob", &[(1, b"bob"), (2, b"bob smith")]),
            (b"carol", &[(1, b"carol"), (2, b"carol lee")]),
            (b"apple", &[(1, b"apple"), (2, b"apple pie")]),
            (b"apply", &[(1, b"apply"), (2, b"apply patch")]),
        ],
    );

    // LIKE 'app%' on pk → rowid_prefix = b"app" → scan pushes prefix
    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id LIKE 'app%'", &catalog, 7, SqlDialect::Postgres)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(s) => {
            assert_eq!(&s.rowid_prefix, b"app", "LIKE prefix should push rowid_prefix");
        }
        other => panic!("expected Scan for LIKE, got {other:?}"),
    }
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec");
    let ids: Vec<String> = result
        .rows
        .iter()
        .map(|r| {
            if let yyds_gateway::SqlValue::String(s) = &r[0] {
                s.to_string()
            }
            else {
                panic!()
            }
        })
        .collect();
    assert_eq!(ids, vec!["apple".to_string(), "apply".to_string()]);
}

#[test]
fn sql_select_pk_between_extracts_range() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[
            (b"1", &[(1, b"1"), (2, b"alice")]),
            (b"2", &[(1, b"2"), (2, b"bob")]),
            (b"3", &[(1, b"3"), (2, b"carol")]),
            (b"4", &[(1, b"4"), (2, b"dan")]),
            (b"5", &[(1, b"5"), (2, b"eve")]),
        ],
    );

    // 显式 AND：id >= 2 AND id <= 4
    // GreaterEqual(2) → after 必须 exclusive：key > (2-1) = key > 1 → after=b"1"
    // LessEqual(4) → before 必须 exclusive：key < (4+1) = key < 5 → before=b"5"
    let plan =
        yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id >= 2 AND id <= 4", &catalog, 7, SqlDialect::Postgres)
            .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
            .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(s) => {
            assert_eq!(s.rowid_after.as_deref(), Some(b"1".as_ref()));
            assert_eq!(s.rowid_before.as_deref(), Some(b"5".as_ref()));
        }
        other => panic!("expected Scan, got {other:?}"),
    }

    // BETWEEN 2 AND 4 → 展开同上
    let plan =
        yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id BETWEEN 2 AND 4", &catalog, 7, SqlDialect::Postgres)
            .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
            .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(s) => {
            assert_eq!(s.rowid_after.as_deref(), Some(b"1".as_ref()));
            assert_eq!(s.rowid_before.as_deref(), Some(b"5".as_ref()));
        }
        other => panic!("expected Scan for BETWEEN, got {other:?}"),
    }
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec");
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn sql_select_pk_in_list_filters_rows() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[
            (b"1", &[(1, b"1"), (2, b"alice")]),
            (b"2", &[(1, b"2"), (2, b"bob")]),
            (b"3", &[(1, b"3"), (2, b"carol")]),
            (b"4", &[(1, b"4"), (2, b"dan")]),
            (b"5", &[(1, b"5"), (2, b"eve")]),
        ],
    );

    // id IN (2, 4, 5) — 展开成 OR 树，走 Scan + remaining_predicate
    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id IN (2, 4, 5)", &catalog, 7, SqlDialect::Postgres)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(_) => { /* IN 不做 pushdown，走 Scan */ }
        other => panic!("expected Scan for IN, got {other:?}"),
    }
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec");
    assert_eq!(result.rows.len(), 3);
    let ids: Vec<f64> = result.rows.iter().map(|r| numeric(&r[0])).collect();
    assert!(ids.contains(&2.0) && ids.contains(&4.0) && ids.contains(&5.0));
}

/// 直接用 yyds_gateway::parse_sql 看 AST，诊断 oak_sql parser 对各 WHERE 语法到底产啥。
fn probe_ast(sql: &str) -> String {
    let root = yyds_gateway::parse_sql(sql, SqlDialect::Postgres).unwrap_or_else(|e| panic!("parse fail {sql}: {e:?}"));
    let sel = match &root.statements[0] {
        oak_sql::ast::SqlStatement::Select(s) => s,
        _ => panic!("not select"),
    };
    format!("{:?}", sel.expr)
}

#[test]
fn probe_parser_basic_binary_ops() {
    let bin = "Some(Binary";
    assert!(probe_ast("SELECT * FROM t WHERE id = 1").starts_with(bin));
    assert!(probe_ast("SELECT * FROM t WHERE id > 1").starts_with(bin));
    assert!(probe_ast("SELECT * FROM t WHERE id >= 1").starts_with(bin));
    assert!(probe_ast("SELECT * FROM t WHERE id < 1").starts_with(bin));
    assert!(probe_ast("SELECT * FROM t WHERE id <= 1").starts_with(bin));
    assert!(probe_ast("SELECT * FROM t WHERE id != 1").starts_with(bin));
    assert!(probe_ast("SELECT * FROM t WHERE a = 1 AND b = 2").starts_with(bin));
    assert!(probe_ast("SELECT * FROM t WHERE a = 1 OR b = 2").starts_with(bin));
}

#[test]
fn probe_parser_between_in_like_is_null() {
    let v = probe_ast("SELECT * FROM t WHERE id BETWEEN 1 AND 10");
    eprintln!("BETWEEN → {v}");

    let v = probe_ast("SELECT * FROM t WHERE id IN (1, 2, 3)");
    eprintln!("IN → {v}");

    let v = probe_ast("SELECT * FROM t WHERE name LIKE 'apple-%'");
    eprintln!("LIKE → {v}");

    let v = probe_ast("SELECT * FROM t WHERE name IS NULL");
    eprintln!("IS NULL → {v}");

    eprintln!("---");
    // 用纯 infix 验证 LIKE/IN/IS token 是否被 lexer 正确识别
    // 如果 = 能产 Binary 但 LIKE 只产 Identifier，说明 infix 层没接上
    let v_eq = probe_ast("SELECT * FROM t WHERE id = 1");
    eprintln!("EQ → {v_eq}");
    let v_like = probe_ast("SELECT * FROM t WHERE name LIKE 'x'");
    eprintln!("LIKE2 → {v_like}");
}

// raw_children probe 已删——lexer bug 定位完毕，补 keyword 表后重跑 BETWEEN/IN/LIKE/IS NULL probe 验证

// ===== planner capability probes =====

#[test]
fn probe_planner_supports_order_by() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let result = yyds_gateway::bind_sql_catalog("SELECT id FROM users ORDER BY id", &catalog, 7, SqlDialect::Postgres)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog));
    match result {
        Ok(_) => eprintln!("ORDER BY: supported ✅"),
        Err(e) => eprintln!("ORDER BY: blocked ❌ — {e:?}"),
    }
}

#[test]
fn probe_planner_supports_group_by() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let result = yyds_gateway::bind_sql_catalog("SELECT name FROM users GROUP BY name", &catalog, 7, SqlDialect::Postgres)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog));
    match result {
        Ok(_) => eprintln!("GROUP BY: supported ✅"),
        Err(e) => eprintln!("GROUP BY: blocked ❌ — {e:?}"),
    }
}

#[test]
fn probe_planner_supports_distinct() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let result = yyds_gateway::bind_sql_catalog("SELECT DISTINCT name FROM users", &catalog, 7, SqlDialect::Postgres)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog));
    match result {
        Ok(_) => eprintln!("DISTINCT: supported ✅"),
        Err(e) => eprintln!("DISTINCT: blocked ❌ — {e:?}"),
    }
}

#[test]
fn probe_planner_supports_aggregates() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let queries =
        ["SELECT COUNT(*) FROM users", "SELECT MAX(id) FROM users", "SELECT SUM(id) FROM users", "SELECT AVG(id) FROM users"];
    for q in &queries {
        let r = yyds_gateway::bind_sql_catalog(q, &catalog, 7, SqlDialect::Postgres)
            .and_then(|b| yyds_gateway::plan_statement(b, &catalog));
        match r {
            Ok(_) => eprintln!("{q}: supported ✅"),
            Err(e) => eprintln!("{q}: blocked ❌ — {e:?}"),
        }
    }
}

#[test]
fn probe_planner_supports_or_in_where() {
    // WHERE a = 1 OR b = 2  — OR 树能否正确走 remaining_predicate
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let plan = yyds_gateway::bind_sql_catalog(
        "SELECT id FROM users WHERE name = 'alice' OR age > 20",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
    .expect("plan");
    match plan {
        yyds_gateway::SqlExecutionPlan::Scan(_) => eprintln!("OR in WHERE: Scan with remaining_predicate ✅"),
        other => panic!("expected Scan, got {other:?}"),
    }
}

#[test]
fn sql_update_rejects_when_remaining_predicate_fails() {
    // 预置 row: id=9, name='original', age=20
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = make_executor_with_row("table:1", b"9", &[(1, b"9"), (2, b"original"), (3, b"20")]);

    // UPDATE users SET name='updated' WHERE id = 9 AND age = 99
    // age=99 与实际 age=20 不匹配 — 应该影响 0 行且数据不变
    let bound = yyds_gateway::bind_sql_catalog(
        "UPDATE users SET name = 'updated' WHERE id = 9 AND age = 99",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("update");

    assert_eq!(result.affected_rows, 0, "remaining predicate mismatch should affect 0 rows");

    // SELECT 验证：数据保持原样
    let bound = yyds_gateway::bind_sql_catalog("SELECT name, age FROM users WHERE id = 9", &catalog, 7, SqlDialect::Postgres)
        .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select");

    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::String("original".into()), yyds_gateway::SqlValue::Integer(20),]);
}

#[test]
fn sql_update_succeeds_when_remaining_predicate_matches() {
    // 预置 row: id=9, name='original', age=20
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = make_executor_with_row("table:1", b"9", &[(1, b"9"), (2, b"original"), (3, b"20")]);

    // UPDATE users SET name='updated' WHERE id = 9 AND age = 20 — 应该成功
    let bound = yyds_gateway::bind_sql_catalog(
        "UPDATE users SET name = 'updated' WHERE id = 9 AND age = 20",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("update");

    assert_eq!(result.affected_rows, 1, "matching remaining predicate should affect 1 row");

    // SELECT 验证：name 已更新
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT name FROM users WHERE id = 9", &catalog, 7, SqlDialect::Postgres).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select");

    assert_eq!(result.rows[0][0], yyds_gateway::SqlValue::String("updated".into()));
}

#[test]
fn sql_update_applies_range_predicate_beyond_pk() {
    // 预置 row: id=9, name='original', age=20
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = make_executor_with_row("table:1", b"9", &[(1, b"9"), (2, b"original"), (3, b"20")]);

    // pk 等值命中但 age 范围不命中 —— 不允许只按主键就写。
    let bound = yyds_gateway::bind_sql_catalog(
        "UPDATE users SET name = 'updated' WHERE id = 9 AND age > 25",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("update");
    assert_eq!(result.affected_rows, 0, "range predicate mismatch should affect 0 rows");

    let bound = yyds_gateway::bind_sql_catalog("SELECT name, age FROM users WHERE id = 9", &catalog, 7, SqlDialect::Postgres)
        .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::String("original".into()), yyds_gateway::SqlValue::Integer(20)]);

    // 范围命中 —— 应该写入。
    let bound = yyds_gateway::bind_sql_catalog(
        "UPDATE users SET name = 'updated' WHERE id = 9 AND age > 10",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("update");
    assert_eq!(result.affected_rows, 1, "range predicate match should affect 1 row");

    let bound =
        yyds_gateway::bind_sql_catalog("SELECT name FROM users WHERE id = 9", &catalog, 7, SqlDialect::Postgres).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select");
    assert_eq!(result.rows[0][0], yyds_gateway::SqlValue::String("updated".into()));
}

#[test]
fn sql_delete_applies_like_predicate_beyond_pk() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor =
        make_executor_with_rows("table:1", &[(b"55", &[(1, b"55"), (2, b"victim")]), (b"56", &[(1, b"56"), (2, b"other")])]);

    // pk 等值命中但 LIKE 不命中 —— 不允许只按主键就删除。
    let bound =
        yyds_gateway::bind_sql_catalog("DELETE FROM users WHERE id = 56 AND name LIKE 'v%'", &catalog, 7, SqlDialect::Postgres)
            .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("delete");
    assert_eq!(result.affected_rows, 0, "LIKE predicate mismatch should affect 0 rows");

    let bound =
        yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id = 56", &catalog, 7, SqlDialect::Postgres).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select");
    assert_eq!(result.rows.len(), 1, "row 56 must survive a failed LIKE predicate");

    // LIKE 命中 —— 应该删除，且只删命中的那一行。
    let bound =
        yyds_gateway::bind_sql_catalog("DELETE FROM users WHERE id = 55 AND name LIKE 'v%'", &catalog, 7, SqlDialect::Postgres)
            .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("delete");
    assert_eq!(result.affected_rows, 1, "LIKE predicate match should affect 1 row");

    let bound =
        yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id = 55", &catalog, 7, SqlDialect::Postgres).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select");
    assert!(result.rows.is_empty(), "row 55 must be deleted");

    let bound =
        yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id = 56", &catalog, 7, SqlDialect::Postgres).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select");
    assert_eq!(result.rows.len(), 1, "row 56 must not be affected");
}

#[test]
fn sql_delete_rejects_when_remaining_predicate_fails() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_row("table:1", b"55", &[(1, b"55"), (2, b"victim")]);

    // DELETE FROM users WHERE id = 55 AND name = 'other' — name 不匹配
    let bound =
        yyds_gateway::bind_sql_catalog("DELETE FROM users WHERE id = 55 AND name = 'other'", &catalog, 7, SqlDialect::Postgres)
            .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("delete");

    assert_eq!(result.affected_rows, 0, "remaining predicate mismatch should affect 0 rows");

    // SELECT 验证：数据仍然存在
    let bound = yyds_gateway::bind_sql_catalog("SELECT name FROM users WHERE id = 55", &catalog, 7, SqlDialect::Postgres)
        .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select");

    assert!(!result.rows.is_empty(), "row should still exist when predicate fails");
}

#[test]
fn utf8_field_preserves_leading_zeros_through_decode() {
    // utf8 字段存 "001" — 不应该被误解析成 Number(1.0)
    let catalog = catalog("table users { @@id: i64, code: utf8 }\n");
    let executor = make_executor_with_row("table:1", b"9", &[(1, b"9"), (2, b"001")]);

    let bound =
        yyds_gateway::bind_sql_catalog("SELECT code FROM users WHERE id = 9", &catalog, 7, SqlDialect::Postgres).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select");

    assert_eq!(result.rows.len(), 1);
    // 关键字段：必须是 String 不是 Number！
    assert_eq!(result.rows[0][0], yyds_gateway::SqlValue::String("001".into()));
}

#[test]
fn utf8_field_preserves_empty_string() {
    // utf8 字段存空字符串 — 不能被当成 NULL 或 Number
    let catalog = catalog("table users { @@id: i64, label: utf8 }\n");
    let executor = make_executor_with_row("table:1", b"1", &[(1, b"1"), (2, b"")]);

    let bound = yyds_gateway::bind_sql_catalog("SELECT label FROM users WHERE id = 1", &catalog, 7, SqlDialect::Postgres)
        .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], yyds_gateway::SqlValue::String(String::new()));
}

#[test]
fn sql_null_is_distinct_from_empty_string() {
    let (catalog, executor) = nullable_users();

    let bound =
        yyds_gateway::bind_sql_catalog("SELECT name, age FROM users", &catalog, 7, SqlDialect::Postgres).expect("scan bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("scan plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("scan nullable rows");

    assert_eq!(
        result.rows,
        vec![
            vec![yyds_gateway::SqlValue::Null, yyds_gateway::SqlValue::Null],
            vec![yyds_gateway::SqlValue::String(String::new()), yyds_gateway::SqlValue::Integer(0)],
            vec![yyds_gateway::SqlValue::String("x".into()), yyds_gateway::SqlValue::Integer(10)],
        ]
    );
}

#[test]
fn sql_order_by_nulls_first_last_overrides_dialect_default() {
    use yyds_gateway::SqlDialect;

    let (catalog, executor) = nullable_users();
    let ids = |dialect: SqlDialect, sql: &str| -> Vec<yyds_gateway::SqlValue> {
        let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("order bind");
        let plan = yyds_gateway::plan_statement_for_dialect(bound, &catalog, dialect).expect("order plan");
        yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres)
            .expect("order exec")
            .rows
            .into_iter()
            .map(|row| row[0].clone())
            .collect()
    };
    let integer = yyds_gateway::SqlValue::Integer;

    // PostgreSQL 视 NULL 为最大值：ASC 置后，DESC 置前。
    assert_eq!(ids(SqlDialect::Postgres, "SELECT id FROM users ORDER BY age ASC"), vec![integer(2), integer(3), integer(1)]);
    assert_eq!(ids(SqlDialect::Postgres, "SELECT id FROM users ORDER BY age DESC"), vec![integer(1), integer(3), integer(2)]);
    // MySQL 视 NULL 为最小值：ASC 置前，DESC 置后。
    assert_eq!(ids(SqlDialect::Mysql, "SELECT id FROM users ORDER BY age ASC"), vec![integer(1), integer(2), integer(3)]);
    assert_eq!(ids(SqlDialect::Mysql, "SELECT id FROM users ORDER BY age DESC"), vec![integer(3), integer(2), integer(1)]);

    // 显式 NULLS FIRST/LAST 覆盖方言默认。
    assert_eq!(
        ids(SqlDialect::Postgres, "SELECT id FROM users ORDER BY age ASC NULLS FIRST"),
        vec![integer(1), integer(2), integer(3)]
    );
    assert_eq!(
        ids(SqlDialect::Postgres, "SELECT id FROM users ORDER BY age DESC NULLS LAST"),
        vec![integer(3), integer(2), integer(1)]
    );
    assert_eq!(
        ids(SqlDialect::Mysql, "SELECT id FROM users ORDER BY age ASC NULLS LAST"),
        vec![integer(2), integer(3), integer(1)]
    );
    assert_eq!(
        ids(SqlDialect::Mysql, "SELECT id FROM users ORDER BY age DESC NULLS FIRST"),
        vec![integer(1), integer(3), integer(2)]
    );
}

#[test]
fn sql_group_by_ordinal_and_unqualified_having() {
    let (catalog, executor) = nullable_users();
    let rows = |sql: &str| -> Vec<Vec<yyds_gateway::SqlValue>> {
        let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
        yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("exec").rows
    };
    let integer = yyds_gateway::SqlValue::Integer;

    // `GROUP BY 1` resolves to the first projection expression (`age`).
    assert_eq!(
        rows("SELECT age, COUNT(*) FROM users GROUP BY 1 ORDER BY age ASC"),
        vec![vec![integer(0), integer(1)], vec![integer(10), integer(1)], vec![yyds_gateway::SqlValue::Null, integer(1)],]
    );
    // HAVING binds without GROUP BY and filters the global aggregate row.
    assert_eq!(rows("SELECT COUNT(*) FROM users HAVING COUNT(*) > 2"), vec![vec![integer(3)]]);
    assert!(rows("SELECT COUNT(*) FROM users HAVING COUNT(*) > 3").is_empty());

    // Out-of-range and star positions are rejected explicitly.
    assert!(yyds_gateway::bind_sql_catalog("SELECT age FROM users GROUP BY 2", &catalog, 7, SqlDialect::Postgres).is_err());
    assert!(yyds_gateway::bind_sql_catalog("SELECT * FROM users GROUP BY 1", &catalog, 7, SqlDialect::Postgres).is_err());
}

#[test]
fn sql_null_predicates_and_order_by_follow_three_valued_logic() {
    let (catalog, executor) = nullable_users();

    // 扫描路径：NULL > 5 求值为 NULL，不进入结果，也不报错。
    let bound = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE age > 5", &catalog, 7, SqlDialect::Postgres)
        .expect("scan bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("scan plan");
    let result =
        yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("null predicate should not error");
    assert_eq!(result.rows, vec![vec![yyds_gateway::SqlValue::Integer(3)]]);

    // 主键 Read 路径的剩余谓词同样按三值逻辑处理：NULL 条件为假，影响 0 行。
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id = 1 AND age > 5", &catalog, 7, SqlDialect::Postgres)
            .expect("read bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("read plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("null remaining predicate");
    assert!(result.rows.is_empty());

    // 投影里的比较结果本身是 NULL。
    let bound = yyds_gateway::bind_sql_catalog("SELECT age > 5 FROM users WHERE id = 1", &catalog, 7, SqlDialect::Postgres)
        .expect("projection bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("projection plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("null comparison projection");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Null]);

    // ASC 时 NULL 最后，DESC 时 NULL 最前。
    let ids = |sql: &str| -> Vec<yyds_gateway::SqlValue> {
        let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("order bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("order plan");
        yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres)
            .expect("order exec")
            .rows
            .into_iter()
            .map(|r| r[0].clone())
            .collect()
    };
    assert_eq!(
        ids("SELECT id FROM users ORDER BY age ASC"),
        vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::Integer(3), yyds_gateway::SqlValue::Integer(1),]
    );
    assert_eq!(
        ids("SELECT id FROM users ORDER BY age DESC"),
        vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::Integer(3), yyds_gateway::SqlValue::Integer(2),]
    );

    // 聚合忽略 NULL：COUNT(age)=2，SUM=10，AVG=5，MAX=10，MIN=0。
    let bound = yyds_gateway::bind_sql_catalog(
        "SELECT COUNT(age), SUM(age), AVG(age), MAX(age), MIN(age) FROM users",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("aggregate bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("aggregate plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("aggregate exec");
    assert_eq!(
        result.rows[0],
        vec![
            yyds_gateway::SqlValue::Integer(2),
            yyds_gateway::SqlValue::Integer(10),
            yyds_gateway::SqlValue::Number(5.0),
            yyds_gateway::SqlValue::Integer(10),
            yyds_gateway::SqlValue::Integer(0),
        ]
    );
}

#[test]
fn sql_null_three_valued_predicate_logic() {
    let (catalog, executor) = nullable_users();
    let count = |predicate: &str| -> yyds_gateway::SqlValue {
        let sql = format!("SELECT COUNT(*) FROM users WHERE {predicate}");
        let bound = yyds_gateway::bind_sql_catalog(&sql, &catalog, 7, SqlDialect::Postgres).expect("predicate bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("predicate plan");
        yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("predicate exec").rows[0][0].clone()
    };

    // NULL AND FALSE = FALSE，NULL OR TRUE = TRUE。
    assert_eq!(count("NULL AND FALSE"), yyds_gateway::SqlValue::Integer(0));
    assert_eq!(count("NULL OR TRUE"), yyds_gateway::SqlValue::Integer(3));
    // NULL 参与比较后仍是 NULL，作为 WHERE 条件不命中。
    assert_eq!(count("NULL = NULL"), yyds_gateway::SqlValue::Integer(0));
}

#[test]
fn sql_update_set_null_removes_field() {
    let (catalog, executor) = nullable_users();

    let bound = yyds_gateway::bind_sql_catalog("UPDATE users SET age = NULL WHERE id = 3", &catalog, 7, SqlDialect::Postgres)
        .expect("update bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("update plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("update set null");
    assert_eq!(result.affected_rows, 1);

    let bound = yyds_gateway::bind_sql_catalog("SELECT age FROM users WHERE id = 3", &catalog, 7, SqlDialect::Postgres)
        .expect("select bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("select plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select null after update");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Null]);
}

#[test]
fn predicate_on_utf8_field_does_not_numeric_coerce() {
    // code='001' 但我们用 code=1 去查 — 应该返回空（类型不同不匹配）
    let catalog = catalog("table users { @@id: i64, code: utf8 }\n");
    let executor = make_executor_with_row("table:1", b"5", &[(1, b"5"), (2, b"001")]);

    // 类型正确的匹配
    let bound = yyds_gateway::bind_sql_catalog(
        "SELECT code FROM users WHERE id = 5 AND code = '001'",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select");
    assert_eq!(result.rows.len(), 1, "string literal match should find row");

    // 类型错误：用数字 literal 查字符串字段 — 应该不匹配（宽松比较不做自动类型转换）
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT code FROM users WHERE id = 5 AND code = 1", &catalog, 7, SqlDialect::Postgres)
            .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("select");
    assert_eq!(result.rows.len(), 0, "number literal should not coerce match against string field");
}

// --- 无 FROM 的 SELECT（驱动/ORM 连接探测）与标量内建函数 ---

/// 驱动探测链路上的常量 SELECT：不触碰任何 namespace。
fn evaluate_source_less(sql: &str) -> yyds_gateway::SqlResult {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = empty_executor();
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    assert!(
        matches!(plan, yyds_gateway::SqlExecutionPlan::Scalar(_)),
        "source-less SELECT should plan as Scalar, got {plan:?}"
    );
    yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute")
}

#[test]
fn sql_source_less_select_returns_constant_row() {
    let result = evaluate_source_less("SELECT 1");
    assert_eq!(result.columns, vec!["1".to_string()]);
    assert_eq!(result.rows, vec![vec![yyds_gateway::SqlValue::Integer(1)]]);
    assert_eq!(result.affected_rows, 0);
}

#[test]
fn sql_system_variable_requires_a_dialect_resolver() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    // 没有变量表方言（PostgreSQL 等）必须明确拒绝 `@@`，不能退化成列名查找。
    let error =
        yyds_gateway::bind_sql_catalog("SELECT @@version", &catalog, 7, SqlDialect::Postgres).expect_err("no system variables");
    assert!(error.message.contains("system variable"), "{}", error.message);
}

#[test]
fn sql_system_variable_binds_through_injected_resolver() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = empty_executor();
    // 方言解析器拿到的是客户端原本拼写（含 `@@` 与作用域前缀）。
    let resolver = |name: &str| -> Result<yyds_gateway::SqlBoundLiteral, yyds_gateway::SqlFrontendError> {
        Ok(yyds_gateway::SqlBoundLiteral::String(name.trim_start_matches("@@").to_string()))
    };
    let bound = yyds_gateway::bind_sql_catalog_with_variables(
        "SELECT @@version, @@session.sql_mode",
        &catalog,
        7,
        &resolver,
        SqlDialect::Postgres,
    )
    .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    assert!(matches!(plan, yyds_gateway::SqlExecutionPlan::Scalar(_)));
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0],
        vec![
            yyds_gateway::SqlValue::String("version".to_string()),
            yyds_gateway::SqlValue::String("session.sql_mode".to_string()),
        ]
    );
}

#[test]
fn sql_source_less_select_projects_literals_with_default_labels() {
    let result = evaluate_source_less("SELECT 1 AS one, 'x' AS label, TRUE AS flag, NULL AS missing");
    assert_eq!(result.columns, vec!["one", "label", "flag", "missing"]);
    assert_eq!(
        result.rows[0],
        vec![
            yyds_gateway::SqlValue::Integer(1),
            yyds_gateway::SqlValue::String("x".into()),
            yyds_gateway::SqlValue::Boolean(true),
            yyds_gateway::SqlValue::Null,
        ]
    );
}

#[test]
fn sql_source_less_select_evaluates_version_probe() {
    let result = evaluate_source_less("SELECT VERSION()");
    assert_eq!(result.columns, vec!["VERSION".to_string()]);
    match &result.rows[0][0] {
        yyds_gateway::SqlValue::String(banner) => assert!(banner.contains("yyds"), "unexpected banner: {banner}"),
        other => panic!("VERSION() should return text, got {other:?}"),
    }
}

#[test]
fn sql_source_less_select_evaluates_now_probe() {
    let result = evaluate_source_less("SELECT NOW()");
    match &result.rows[0][0] {
        yyds_gateway::SqlValue::String(timestamp) => {
            assert_eq!(timestamp.len(), 19, "expected `YYYY-MM-DD HH:MM:SS`, got {timestamp}");
            assert!(timestamp.as_bytes()[4] == b'-' && timestamp.as_bytes()[10] == b' ', "unexpected shape: {timestamp}");
        }
        other => panic!("NOW() should return text, got {other:?}"),
    }
}

#[test]
fn sql_source_less_select_evaluates_string_and_numeric_scalars() {
    let result = evaluate_source_less(
        "SELECT LOWER('ABC'), UPPER('abc'), LENGTH('hello'), COALESCE(NULL, 'fallback'), ABS(-3), ROUND(2.567, 2)",
    );
    assert_eq!(
        result.rows[0],
        vec![
            yyds_gateway::SqlValue::String("abc".into()),
            yyds_gateway::SqlValue::String("ABC".into()),
            yyds_gateway::SqlValue::Integer(5),
            yyds_gateway::SqlValue::String("fallback".into()),
            yyds_gateway::SqlValue::Integer(3),
            yyds_gateway::SqlValue::Number(2.57),
        ]
    );
}

#[test]
fn sql_source_less_select_honours_limit_zero() {
    let result = evaluate_source_less("SELECT 1 LIMIT 0");
    assert_eq!(result.columns, vec!["1".to_string()]);
    assert!(result.rows.is_empty(), "LIMIT 0 must suppress the constant row");
}

#[test]
fn sql_source_less_select_rejects_where_clause() {
    let catalog = catalog("table users { @@id: i64 }\n");
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT 1 WHERE 1 = 1", &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let error = yyds_gateway::plan_statement(bound, &catalog).expect_err("WHERE without FROM must be rejected");
    assert!(error.message.contains("requires a FROM source"), "unexpected message: {}", error.message);
}

#[test]
fn sql_source_less_select_keeps_unary_minus_sign() {
    // 回归：Oak 曾把前导 `-` 当成缺失左操作数的二元减号，`SELECT -3` 返回 3。
    let result = evaluate_source_less("SELECT -3, ABS(-3), 1 - -2");
    assert_eq!(
        result.rows[0],
        vec![yyds_gateway::SqlValue::Integer(-3), yyds_gateway::SqlValue::Integer(3), yyds_gateway::SqlValue::Integer(3),]
    );
}

// --- JOIN（内连接） ---

/// 多 namespace 的 executor：JOIN 需要同时扫描基表与连接表。
fn make_executor_with_tables(
    tables: &[(&str, &[(&[u8], &[(u64, &[u8])])])],
) -> yyds_execution::LocalExecutor<yyds_kv::MemoryShard> {
    use yyds_kv::{InlineValue, Key, KvStore, MemoryShard, StoredValue};
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let epoch = ShardEpoch(1);
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(epoch, vec![shard_id.clone()]).expect("routing");

    let mut shard = MemoryShard::new();
    for (namespace, rows) in tables {
        for (rowid, fields) in *rows {
            let mut values = yyds_gateway::RowFieldValues::new();
            for (field_id, bytes) in *fields {
                values.insert(*field_id, bytes.to_vec());
            }
            let row_bytes = yyds_gateway::encode_row(&values);
            shard.put(Key::new(*namespace, rowid.to_vec()), StoredValue::Inline(InlineValue(row_bytes))).expect("put row");
        }
    }

    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    yyds_execution::LocalExecutor::new(routing, shards, Vec::new()).expect("executor")
}

/// `users`(type 1: id=1, name=2) 与 `orders`(type 2: id=3, user_id=4, total=5)。
fn join_fixture() -> (yyds_gateway::SqlCatalog, yyds_execution::LocalExecutor<yyds_kv::MemoryShard>) {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\ntable orders { @@id: i64, user_id: i64, total: i64 }\n");
    let executor = make_executor_with_tables(&[
        ("table:1", &[(b"1", &[(1, b"1"), (2, b"alice")]), (b"2", &[(1, b"2"), (2, b"bob")])]),
        (
            "table:2",
            &[
                (b"10", &[(3, b"10"), (4, b"1"), (5, b"100")]),
                (b"11", &[(3, b"11"), (4, b"1"), (5, b"50")]),
                (b"12", &[(3, b"12"), (4, b"2"), (5, b"70")]),
                (b"13", &[(3, b"13"), (4, b"9"), (5, b"5")]),
            ],
        ),
    ]);
    (catalog, executor)
}

/// `join_fixture` 的 `FileShard` 版本：写入路径的语句级提交要求存储具备 durable
/// atomic commit 能力，易失的 `MemoryShard` 会在提交边界被显式拒绝。
fn file_join_fixture(
) -> (yyds_gateway::SqlCatalog, yyds_execution::LocalExecutor<yyds_kv::FileShard>, boost_fs::temp::TempFolder) {
    use yyds_kv::{FileShard, InlineValue, Key, KvStore, StoredValue};
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let catalog = catalog("table users { @@id: i64, name: utf8 }\ntable orders { @@id: i64, user_id: i64, total: i64 }\n");
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(ShardEpoch(1), vec![shard_id.clone()]).expect("routing");
    let mut shard = FileShard::open(directory.path().join("s1.yykv")).expect("open file shard");
    let users: &[(&[u8], &[(u64, &[u8])])] = &[(b"1", &[(1, b"1"), (2, b"alice")]), (b"2", &[(1, b"2"), (2, b"bob")])];
    let orders: &[(&[u8], &[(u64, &[u8])])] = &[
        (b"10", &[(3, b"10"), (4, b"1"), (5, b"100")]),
        (b"11", &[(3, b"11"), (4, b"1"), (5, b"50")]),
        (b"12", &[(3, b"12"), (4, b"2"), (5, b"70")]),
        (b"13", &[(3, b"13"), (4, b"9"), (5, b"5")]),
    ];
    for (namespace, rows) in [("table:1", users), ("table:2", orders)] {
        for (rowid, fields) in rows {
            let mut values = yyds_gateway::RowFieldValues::new();
            for (field_id, bytes) in *fields {
                values.insert(*field_id, bytes.to_vec());
            }
            let row_bytes = yyds_gateway::encode_row(&values);
            shard.put(Key::new(namespace, rowid.to_vec()), StoredValue::Inline(InlineValue(row_bytes))).expect("put row");
        }
    }
    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    let executor = yyds_execution::LocalExecutor::new(routing, shards, Vec::new()).expect("executor");
    (catalog, executor, directory)
}

#[test]
fn sql_inner_join_projects_both_sides_with_where_and_order_by() {
    let (catalog, executor) = join_fixture();
    let sql = "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id \
               WHERE orders.total > 60 ORDER BY orders.total";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    assert!(matches!(plan, yyds_gateway::SqlExecutionPlan::Join(_)), "got {plan:?}");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");

    assert_eq!(result.columns, vec!["name".to_string(), "total".to_string()]);
    assert_eq!(
        result.rows,
        vec![
            vec![yyds_gateway::SqlValue::String("bob".into()), yyds_gateway::SqlValue::Integer(70)],
            vec![yyds_gateway::SqlValue::String("alice".into()), yyds_gateway::SqlValue::Integer(100)],
        ]
    );
}

#[test]
fn sql_inner_join_star_expands_every_joined_table() {
    let (catalog, executor) = join_fixture();
    let sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.total";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");

    assert_eq!(
        result.columns,
        vec!["id".to_string(), "name".to_string(), "id".to_string(), "user_id".to_string(), "total".to_string()]
    );
    assert_eq!(result.rows.len(), 3, "orders.user_id = 9 has no users match");
    let totals: Vec<f64> = result.rows.iter().map(|row| numeric(&row[4])).collect();
    assert_eq!(totals, vec![50.0, 70.0, 100.0]);
    assert_eq!(result.rows[0][1], yyds_gateway::SqlValue::String("alice".into()));
}

#[test]
fn sql_inner_join_without_matching_rows_returns_empty() {
    let (catalog, executor) = join_fixture();

    // ON 条件本身不命中。
    let sql = "SELECT users.name FROM users JOIN orders ON users.id = orders.user_id AND orders.total > 1000";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert!(result.rows.is_empty(), "ON filter should drop every pair");

    // JOIN 命中但 WHERE 全部过滤掉。
    let sql = "SELECT users.name FROM users JOIN orders ON users.id = orders.user_id WHERE orders.total > 1000";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert!(result.rows.is_empty(), "WHERE filter should drop every joined row");
}

#[test]
fn sql_left_join_preserves_unmatched_left_rows() {
    let (catalog, executor) = join_fixture();

    // 所有左行都匹配时，LEFT JOIN 与 INNER JOIN 等价。
    let sql = "SELECT users.name, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY orders.total";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    assert!(matches!(plan, yyds_gateway::SqlExecutionPlan::Join(_)));
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert_eq!(result.rows.len(), 3, "two users match three orders");

    // ON 条件永远不命中：每个左行保留，右列全部 NULL。
    let sql = "SELECT users.name, orders.total FROM users LEFT JOIN orders ON 1 = 0";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert_eq!(result.rows.len(), 2, "both users must appear even with zero right matches");
    for row in &result.rows {
        assert_eq!(row[1], yyds_gateway::SqlValue::Null, "right side should be NULL on unmatched left row");
    }

    // 与 INNER JOIN 对比：同 ON 条件下 INNER JOIN 返回空集。
    let sql_inner = "SELECT users.name FROM users JOIN orders ON 1 = 0";
    let bound = yyds_gateway::bind_sql_catalog(sql_inner, &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let inner_result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert!(inner_result.rows.is_empty(), "inner join with never-match ON must be empty");
}

#[test]
fn sql_right_join_preserves_unmatched_right_rows() {
    let (catalog, executor) = join_fixture();

    // orders.user_id = 9 没有 users 匹配：该右行保留，左表字段全 NULL。
    let sql = "SELECT users.name, orders.total FROM users RIGHT JOIN orders ON users.id = orders.user_id \
               ORDER BY orders.total";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    assert!(matches!(plan, yyds_gateway::SqlExecutionPlan::Join(_)));
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");

    assert_eq!(result.columns, vec!["name".to_string(), "total".to_string()], "基表列在前、JOIN 表列在后");
    assert_eq!(
        result.rows,
        vec![
            vec![yyds_gateway::SqlValue::Null, yyds_gateway::SqlValue::Integer(5)],
            vec![yyds_gateway::SqlValue::String("alice".into()), yyds_gateway::SqlValue::Integer(50)],
            vec![yyds_gateway::SqlValue::String("bob".into()), yyds_gateway::SqlValue::Integer(70)],
            vec![yyds_gateway::SqlValue::String("alice".into()), yyds_gateway::SqlValue::Integer(100)],
        ]
    );

    // ON 永不命中：左行全部丢弃，每个右行以 NULL 左字段保留。
    let sql = "SELECT users.name, orders.total FROM users RIGHT JOIN orders ON 1 = 0";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert_eq!(result.rows.len(), 4, "every right row must survive with no left match");
    for row in &result.rows {
        assert_eq!(row[0], yyds_gateway::SqlValue::Null, "left side should be NULL on unmatched right row");
    }
}

#[test]
fn sql_full_join_preserves_both_unmatched_sides() {
    let (catalog, executor) = join_fixture();

    // ON 只让 alice 的订单匹配：bob 成为未匹配左行，user_id = 2 / 9 的订单成为未匹配右行。
    let sql = "SELECT users.name, orders.total FROM users FULL JOIN orders \
               ON users.id = orders.user_id AND users.id = 1 ORDER BY orders.total";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");

    assert_eq!(result.rows.len(), 5, "two matches plus one left-only and two right-only rows");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Null, yyds_gateway::SqlValue::Integer(5)]);
    assert_eq!(result.rows[1], vec![yyds_gateway::SqlValue::String("alice".into()), yyds_gateway::SqlValue::Integer(50)]);
    assert_eq!(result.rows[2], vec![yyds_gateway::SqlValue::Null, yyds_gateway::SqlValue::Integer(70)]);
    assert_eq!(result.rows[3], vec![yyds_gateway::SqlValue::String("alice".into()), yyds_gateway::SqlValue::Integer(100)]);
    assert_eq!(
        result.rows[4],
        vec![yyds_gateway::SqlValue::String("bob".into()), yyds_gateway::SqlValue::Null],
        "unmatched left row keeps NULL right columns"
    );
}

#[test]
fn sql_outer_join_keeps_present_side_when_other_table_is_empty() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\ntable orders { @@id: i64, user_id: i64, total: i64 }\n");
    let query = |executor: &dyn yyds_execution::KeyValueExecutor, sql: &str| {
        let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
        yyds_gateway::execute_plan(executor, None, &plan, SqlDialect::Postgres).expect("execute")
    };

    // 左表为空：RIGHT / FULL JOIN 仍返回全部右行，左字段 NULL；INNER JOIN 为空。
    let orders_only = make_executor_with_tables(&[("table:2", &[(b"10", &[(3, b"10"), (4, b"1"), (5, b"100")])])]);
    let expected = vec![vec![yyds_gateway::SqlValue::Null, yyds_gateway::SqlValue::Integer(100)]];
    let right =
        query(&orders_only, "SELECT users.name, orders.total FROM users RIGHT JOIN orders ON users.id = orders.user_id");
    assert_eq!(right.rows, expected);
    let full = query(&orders_only, "SELECT users.name, orders.total FROM users FULL JOIN orders ON users.id = orders.user_id");
    assert_eq!(full.rows, expected);
    let inner = query(&orders_only, "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id");
    assert!(inner.rows.is_empty(), "inner join with an empty left table must be empty");

    // 右表为空：LEFT / FULL JOIN 仍返回全部左行，右字段 NULL；RIGHT JOIN 为空。
    let users_only = make_executor_with_tables(&[("table:1", &[(b"1", &[(1, b"1"), (2, b"alice")])])]);
    let expected = vec![vec![yyds_gateway::SqlValue::String("alice".into()), yyds_gateway::SqlValue::Null]];
    let left = query(&users_only, "SELECT users.name, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id");
    assert_eq!(left.rows, expected);
    let full = query(&users_only, "SELECT users.name, orders.total FROM users FULL JOIN orders ON users.id = orders.user_id");
    assert_eq!(full.rows, expected);
    let right = query(&users_only, "SELECT users.name, orders.total FROM users RIGHT JOIN orders ON users.id = orders.user_id");
    assert!(right.rows.is_empty(), "right join cannot invent right rows");
}

#[test]
fn sql_outer_join_on_and_where_differ_for_unmatched_rows() {
    let (catalog, executor) = join_fixture();

    // 过滤写在 ON 里：只决定「是否算匹配」，未匹配右行仍以 NULL 左字段保留。
    let sql = "SELECT users.name, orders.total FROM users RIGHT JOIN orders \
               ON users.id = orders.user_id AND orders.total > 60 ORDER BY orders.total";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let on_filter = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert_eq!(
        on_filter.rows,
        vec![
            vec![yyds_gateway::SqlValue::Null, yyds_gateway::SqlValue::Integer(5)],
            vec![yyds_gateway::SqlValue::Null, yyds_gateway::SqlValue::Integer(50)],
            vec![yyds_gateway::SqlValue::String("bob".into()), yyds_gateway::SqlValue::Integer(70)],
            vec![yyds_gateway::SqlValue::String("alice".into()), yyds_gateway::SqlValue::Integer(100)],
        ]
    );

    // 同样的过滤写在 WHERE 里：未匹配行同样被 total 过滤掉，结果只剩真正命中的组合。
    let sql = "SELECT users.name, orders.total FROM users RIGHT JOIN orders \
               ON users.id = orders.user_id WHERE orders.total > 60 ORDER BY orders.total";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let where_filter = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert_eq!(
        where_filter.rows,
        vec![
            vec![yyds_gateway::SqlValue::String("bob".into()), yyds_gateway::SqlValue::Integer(70)],
            vec![yyds_gateway::SqlValue::String("alice".into()), yyds_gateway::SqlValue::Integer(100)],
        ]
    );
}

/// `CROSS JOIN` 必须是完整笛卡尔积：binder 左侧的恒真 `ON` 占位不能变成过滤器，
/// 也不能按某一侧的扫描行数截断另一侧的组合。
#[test]
fn sql_cross_join_is_a_full_cartesian_product() {
    let (catalog, executor) = join_fixture();

    // 2 users × 4 orders = 8 行，排序只改变呈现顺序，不改变组合数量。
    let product = run_sql(
        &executor,
        None,
        &catalog,
        "SELECT users.id, orders.id FROM users CROSS JOIN orders ORDER BY users.id, orders.id",
    );
    assert_eq!(product.columns, vec!["id".to_string(), "id".to_string()]);
    assert_eq!(
        product.rows,
        vec![
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::Integer(10)],
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::Integer(11)],
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::Integer(12)],
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::Integer(13)],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::Integer(10)],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::Integer(11)],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::Integer(12)],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::Integer(13)],
        ]
    );

    // 同表自连接：两侧都出现重复值，每个组合仍然各出现一次。
    let self_product =
        run_sql(&executor, None, &catalog, "SELECT a.id, b.id FROM users a CROSS JOIN users b ORDER BY a.id, b.id");
    assert_eq!(
        self_product.rows,
        vec![
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::Integer(1)],
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::Integer(2)],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::Integer(1)],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::Integer(2)],
        ]
    );

    // 三表连续 CROSS JOIN 同样是逐层嵌套循环，没有固定连接数上限。
    let three_way =
        run_sql(&executor, None, &catalog, "SELECT users.id, orders.id, c.id FROM users CROSS JOIN orders CROSS JOIN users c");
    assert_eq!(three_way.rows.len(), 2 * 4 * 2, "every three-way combination must be produced once");

    // 分页只裁剪最终组合，不改变笛卡尔积本身。
    let paged = run_sql(&executor, None, &catalog, "SELECT users.id, orders.id FROM users CROSS JOIN orders LIMIT 3");
    assert_eq!(paged.rows.len(), 3);

    // binder 侧：CROSS JOIN 映射为 `Cross`，`ON` 只是恒真占位。
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT users.id FROM users CROSS JOIN orders", &catalog, 7, SqlDialect::Postgres)
            .expect("cross join bind");
    let yyds_gateway::SqlCatalogBoundStatement::Select(select) = bound
    else {
        panic!("expected SELECT")
    };
    assert_eq!(select.joins.len(), 1);
    assert!(matches!(select.joins[0].kind, yyds_gateway::SqlBoundJoinKind::Cross));
}

/// CROSS JOIN 不保留任何一侧：任意一侧为空时笛卡尔积为空集。
#[test]
fn sql_cross_join_with_an_empty_side_is_empty() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\ntable orders { @@id: i64, user_id: i64, total: i64 }\n");
    let query = |executor: &dyn yyds_execution::KeyValueExecutor, sql: &str| run_sql(executor, None, &catalog, sql);

    // 右表为空。
    let users_only = make_executor_with_tables(&[("table:1", &[(b"1", &[(1, b"1"), (2, b"alice")])])]);
    assert!(
        query(&users_only, "SELECT users.id, orders.id FROM users CROSS JOIN orders").rows.is_empty(),
        "an empty right side yields no combinations"
    );
    assert!(
        query(&users_only, "SELECT orders.id, users.id FROM orders CROSS JOIN users").rows.is_empty(),
        "an empty right side yields no combinations regardless of which side leads"
    );

    // 左表为空。
    let orders_only = make_executor_with_tables(&[("table:2", &[(b"10", &[(3, b"10"), (4, b"1"), (5, b"100")])])]);
    assert!(
        query(&orders_only, "SELECT users.id, orders.id FROM users CROSS JOIN orders").rows.is_empty(),
        "an empty left side cannot invent combinations"
    );
    assert!(
        query(&orders_only, "SELECT orders.id, users.id FROM orders CROSS JOIN users").rows.is_empty(),
        "an empty right side cannot be filled from the visible left rows"
    );
}

/// CROSS JOIN 之后列名作用域包含两侧全部表，未限定且重名的列必须报歧义。
#[test]
fn sql_cross_join_keeps_both_column_scopes() {
    let (catalog, executor) = join_fixture();

    // `users.id` 与 `orders.id` 都可见，未限定的 `id` 必须报歧义。
    let ambiguous = yyds_gateway::bind_sql_catalog("SELECT id FROM users CROSS JOIN orders", &catalog, 7, SqlDialect::Postgres)
        .expect_err("ambiguous cross join column");
    assert_eq!(ambiguous.kind, yyds_gateway::SqlErrorKind::AmbiguousColumn, "{}", ambiguous.message);
    assert!(ambiguous.message.contains("ambiguous SQL column: id"), "{}", ambiguous.message);

    // 限定名可正常读取，证明两侧作用域都保留而非只留基表。
    let qualified = run_sql(&executor, None, &catalog, "SELECT users.name, orders.total FROM users CROSS JOIN orders LIMIT 1");
    assert_eq!(qualified.columns, vec!["name".to_string(), "total".to_string()]);
    assert_eq!(qualified.rows.len(), 1);
}

// --- 链式集合运算 ---

/// 三张 `value: i64` 表：`a` 含重复行（1,2,2），`b` 为 2,3，`c` 为 2,4，
/// 外加一张 `name: utf8` 表供类型不匹配用例使用。
fn set_operation_fixture() -> (yyds_gateway::SqlCatalog, yyds_execution::LocalExecutor<yyds_kv::MemoryShard>) {
    let catalog =
        catalog("table a { @@id: i64, value: i64 }\ntable b { @@id: i64, value: i64 }\ntable c { @@id: i64, value: i64 }\ntable s { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_tables(&[
        ("table:1", &[(b"1", &[(1, b"1"), (2, b"1")]), (b"2", &[(1, b"2"), (2, b"2")]), (b"3", &[(1, b"3"), (2, b"2")])]),
        ("table:2", &[(b"10", &[(3, b"10"), (4, b"2")]), (b"11", &[(3, b"11"), (4, b"3")])]),
        ("table:3", &[(b"20", &[(5, b"20"), (6, b"2")]), (b"21", &[(5, b"21"), (6, b"4")])]),
    ]);
    (catalog, executor)
}

fn integer_rows(values: &[i64]) -> Vec<Vec<yyds_gateway::SqlValue>> {
    values.iter().map(|value| vec![yyds_gateway::SqlValue::Integer(*value)]).collect()
}

/// `INTERSECT` 比 `UNION ALL` 结合更紧：`A UNION ALL B INTERSECT C` 必须是
/// `A UNION ALL (B INTERSECT C)`，而不是左结合先并再交。
#[test]
fn sql_set_operation_intersect_binds_tighter_than_union_all() {
    let (catalog, executor) = set_operation_fixture();

    let chained = run_sql(
        &executor,
        None,
        &catalog,
        "SELECT value FROM a UNION ALL SELECT value FROM b INTERSECT SELECT value FROM c ORDER BY 1",
    );
    // `b INTERSECT c` = {2}，再并上 `a` 的全部行（含重复）。
    assert_eq!(chained.columns, vec!["value".to_string()]);
    assert_eq!(chained.rows, integer_rows(&[1, 2, 2, 2]));

    // 左结合读法 `(a UNION ALL b) INTERSECT c` 只会剩一行 2，用来证明折中确有优先级。
    assert_ne!(chained.rows.len(), 1, "the chain must not be evaluated left-associatively");
}

/// 同级运算符左结合：`A UNION B EXCEPT C` = `(A UNION B) EXCEPT C`。
#[test]
fn sql_set_operation_union_then_except_is_left_associative() {
    let (catalog, executor) = set_operation_fixture();

    let result = run_sql(
        &executor,
        None,
        &catalog,
        "SELECT value FROM a UNION SELECT value FROM b EXCEPT SELECT value FROM c ORDER BY 1",
    );
    // `a UNION b` 去重后为 {1,2,3}，再减去 `c` 的 {2,4} 得 {1,3}。
    assert_eq!(result.rows, integer_rows(&[1, 3]));
}

/// 整体 `ORDER BY` / `LIMIT` 只在全部层级合并之后施加一次。
#[test]
fn sql_set_operation_applies_order_and_limit_once_after_the_merge() {
    let (catalog, executor) = set_operation_fixture();

    let result = run_sql(
        &executor,
        None,
        &catalog,
        "SELECT value FROM a UNION ALL SELECT value FROM b UNION ALL SELECT value FROM c ORDER BY 1 DESC LIMIT 2",
    );
    // 三个操作数共 8 行全部进入合并，排序后才截断到最大的两行。
    assert_eq!(result.rows, integer_rows(&[4, 3]));
}

/// `UNION` 去重、`UNION ALL` 保留重复，且 `INTERSECT` / `EXCEPT` 保持左侧集合语义。
#[test]
fn sql_set_operation_deduplicates_unless_all_is_used() {
    let (catalog, executor) = set_operation_fixture();

    let distinct = run_sql(&executor, None, &catalog, "SELECT value FROM a UNION SELECT value FROM a ORDER BY 1");
    assert_eq!(distinct.rows, integer_rows(&[1, 2]));

    let all = run_sql(&executor, None, &catalog, "SELECT value FROM a UNION ALL SELECT value FROM a ORDER BY 1");
    assert_eq!(all.rows, integer_rows(&[1, 1, 2, 2, 2, 2]));

    let intersect = run_sql(&executor, None, &catalog, "SELECT value FROM a INTERSECT SELECT value FROM b ORDER BY 1");
    assert_eq!(intersect.rows, integer_rows(&[2]), "INTERSECT 取去重后的交集");
}

/// 集合运算按 SQL 语义把 NULL 视为同一个值：既可比对去重，也可作交集成员。
#[test]
fn sql_set_operation_treats_nulls_as_equal() {
    let catalog = catalog("table na { @@id: i64, value: i64? }\ntable nb { @@id: i64, value: i64? }\n");
    let executor = empty_executor();
    for sql in ["INSERT INTO na VALUES (1, NULL)", "INSERT INTO na VALUES (2, 5)", "INSERT INTO nb VALUES (1, NULL)"] {
        run_sql(&executor, None, &catalog, sql);
    }

    let union = run_sql(&executor, None, &catalog, "SELECT value FROM na UNION SELECT value FROM nb ORDER BY 1");
    // PostgreSQL 默认 `ASC` 把 NULL 排在最后；两行 NULL 合并成一行的关键在行数。
    assert_eq!(
        union.rows,
        vec![vec![yyds_gateway::SqlValue::Integer(5)], vec![yyds_gateway::SqlValue::Null]],
        "two NULL rows collapse into one"
    );

    let intersect = run_sql(&executor, None, &catalog, "SELECT value FROM na INTERSECT SELECT value FROM nb ORDER BY 1");
    assert_eq!(intersect.rows, vec![vec![yyds_gateway::SqlValue::Null]], "NULL 与 NULL 在集合运算里相等");
}

/// 交集/差集为空时返回空结果集而不是错误。
#[test]
fn sql_set_operation_returns_an_empty_result_when_nothing_matches() {
    let (catalog, executor) = set_operation_fixture();

    let empty_except = run_sql(&executor, None, &catalog, "SELECT value FROM a EXCEPT SELECT value FROM a");
    assert!(empty_except.rows.is_empty());
    assert_eq!(empty_except.columns, vec!["value".to_string()]);

    let empty_intersect =
        run_sql(&executor, None, &catalog, "SELECT value FROM a INTERSECT SELECT value FROM b WHERE value = 9");
    assert!(empty_intersect.rows.is_empty());
}

/// 任意相邻两操作数的列数或类型不兼容都必须显式报错，而不是悄悄对齐。
#[test]
fn sql_set_operation_rejects_mismatched_operands_at_any_boundary() {
    let (catalog, _executor) = set_operation_fixture();

    let bound =
        yyds_gateway::bind_sql_catalog("SELECT id, value FROM a UNION SELECT value FROM b", &catalog, 7, SqlDialect::Postgres)
            .expect("bind");
    let error = yyds_gateway::plan_statement(bound, &catalog).expect_err("column count mismatch");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Syntax, "{}", error.message);
    assert!(error.message.contains("column count mismatch"), "{}", error.message);

    // 不匹配发生在第三个操作数上：逐对校验要指出 2/3 边界，而不只是最左一对。
    let bound = yyds_gateway::bind_sql_catalog(
        "SELECT value FROM a UNION SELECT value FROM b UNION SELECT name FROM s",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let error = yyds_gateway::plan_statement(bound, &catalog).expect_err("type mismatch");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::TypeMismatch, "{}", error.message);
    assert!(error.message.contains("operands 2/3"), "{}", error.message);
}

/// 整条链只有 `INTERSECT` 时折叠出的层级仍要能执行，且共享一次尾部裁剪。
#[test]
fn sql_set_operation_executes_a_pure_intersect_chain() {
    let (catalog, executor) = set_operation_fixture();

    let result = run_sql(
        &executor,
        None,
        &catalog,
        "SELECT value FROM a INTERSECT SELECT value FROM b INTERSECT SELECT value FROM c ORDER BY 1",
    );
    // a{1,2} ∩ b{2,3} ∩ c{2,4} = {2}
    assert_eq!(result.rows, integer_rows(&[2]));
}

// --- LIMIT / OFFSET 语义 ---

/// 四行 `users`：`id` 与 `value` 都按 1..4 递增，便于验证跳过与截断。
fn limit_fixture() -> (yyds_gateway::SqlCatalog, yyds_execution::LocalExecutor<yyds_kv::MemoryShard>) {
    let catalog = catalog("table users { @@id: i64, value: i64 }\n");
    let rows: &[(&[u8], &[(u64, &[u8])])] = &[
        (b"1", &[(1, b"1"), (2, b"10")]),
        (b"2", &[(1, b"2"), (2, b"20")]),
        (b"3", &[(1, b"3"), (2, b"30")]),
        (b"4", &[(1, b"4"), (2, b"40")]),
    ];
    let executor = make_executor_with_tables(&[("table:1", rows)]);
    (catalog, executor)
}

/// `LIMIT` / `OFFSET` 是常量表达式：计划阶段折叠，参数占位符按既有参数机制渲染后同样是常量。
#[test]
fn sql_limit_and_offset_fold_constant_expressions() {
    let (catalog, executor) = limit_fixture();

    let arithmetic = run_sql(&executor, None, &catalog, "SELECT id FROM users LIMIT 2 + 2");
    assert_eq!(arithmetic.rows, integer_rows(&[1, 2, 3, 4]));

    let expression_offset = run_sql(&executor, None, &catalog, "SELECT id FROM users LIMIT 1 OFFSET 4 / 2");
    assert_eq!(expression_offset.rows, integer_rows(&[3]), "OFFSET 4/2 = 2 must skip two rows");

    let rendered = yyds_gateway::substitute_sql_parameters(
        "SELECT id FROM users LIMIT ?",
        &[yyds_gateway::SqlValue::Integer(2)],
        yyds_gateway::SqlParameterStyle::QuestionMark,
    )
    .expect("render parameter");
    let bound = run_sql(&executor, None, &catalog, &rendered);
    assert_eq!(bound.rows, integer_rows(&[1, 2]));
}

/// `OFFSET` 在无 `ORDER BY` 与主键单点读路径上都不能被丢弃。
#[test]
fn sql_offset_skips_rows_without_order_by_or_scan_pushdown() {
    let (catalog, executor) = limit_fixture();

    let paged = run_sql(&executor, None, &catalog, "SELECT id FROM users LIMIT 2 OFFSET 1");
    assert_eq!(paged.rows, integer_rows(&[2, 3]), "OFFSET must skip rows even when kv-scan could push LIMIT down");

    // 单点读最多一行，任何正的 OFFSET 都让结果为空集。
    let read = run_sql(&executor, None, &catalog, "SELECT id FROM users WHERE id = 1 LIMIT 1 OFFSET 1");
    assert!(read.rows.is_empty(), "OFFSET 1 must drop the only row of a primary-key read");

    // 无 FROM 的常量 SELECT 也遵守同一套分页语义。
    let source_less = run_sql(&executor, None, &catalog, "SELECT 7 LIMIT 1 OFFSET 1");
    assert!(source_less.rows.is_empty());
}

/// standalone `OFFSET`（无 `LIMIT`）：跳过前 n 行并返回其余全部，
/// 跳过的行数不得被当成结果集的行数上限。
#[test]
fn sql_standalone_offset_skips_rows_without_capping() {
    let (catalog, executor) = limit_fixture();

    let skipped = run_sql(&executor, None, &catalog, "SELECT id FROM users OFFSET 1");
    assert_eq!(skipped.rows, integer_rows(&[2, 3, 4]), "OFFSET 1 without LIMIT must return every remaining row");

    let ordered = run_sql(&executor, None, &catalog, "SELECT id FROM users ORDER BY id DESC OFFSET 2");
    assert_eq!(ordered.rows, integer_rows(&[2, 1]), "OFFSET without LIMIT keeps the sorted tail after skipping");

    // 主键单点读最多一行，正的 OFFSET 同样只跳过、不截断。
    let read = run_sql(&executor, None, &catalog, "SELECT id FROM users WHERE id = 1 OFFSET 1");
    assert!(read.rows.is_empty(), "OFFSET 1 must drop the only row of a primary-key read");

    // 跳过超过行数得到空集，而不是错误。
    let beyond = run_sql(&executor, None, &catalog, "SELECT id FROM users OFFSET 10");
    assert!(beyond.rows.is_empty(), "an OFFSET beyond the row count returns an empty result");
}

/// `LIMIT` / `OFFSET` 必须是常量非负整数：NULL、负数、非整数、溢出与除零都显式报错。
#[test]
fn sql_limit_and_offset_reject_invalid_counts() {
    let (catalog, _executor) = limit_fixture();

    // 超出 `i64` 字面量在绑定阶段就被拒绝，其余语义在计划阶段拒绝，两者都是稳定错误。
    let clause_error = |sql: &str| -> yyds_gateway::SqlFrontendError {
        let bound = match yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres) {
            Ok(bound) => bound,
            Err(error) => return error,
        };
        yyds_gateway::plan_statement(bound, &catalog).expect_err("plan must reject the clause")
    };

    let null = clause_error("SELECT id FROM users LIMIT NULL");
    assert_eq!(null.kind, yyds_gateway::SqlErrorKind::TypeMismatch, "{}", null.message);
    assert!(null.message.contains("NULL is not a row count"), "{}", null.message);

    let negative = clause_error("SELECT id FROM users LIMIT -1");
    assert_eq!(negative.kind, yyds_gateway::SqlErrorKind::NumericOverflow, "{}", negative.message);
    assert!(negative.message.contains("must not be negative"), "{}", negative.message);

    let negative_offset = clause_error("SELECT id FROM users LIMIT 1 OFFSET -2");
    assert_eq!(negative_offset.kind, yyds_gateway::SqlErrorKind::NumericOverflow, "{}", negative_offset.message);
    assert!(negative_offset.message.contains("must not be negative"), "{}", negative_offset.message);

    let fractional = clause_error("SELECT id FROM users LIMIT 1.5");
    assert_eq!(fractional.kind, yyds_gateway::SqlErrorKind::TypeMismatch, "{}", fractional.message);
    assert!(fractional.message.contains("exact non-negative integer"), "{}", fractional.message);

    let text = clause_error("SELECT id FROM users LIMIT 'many'");
    assert_eq!(text.kind, yyds_gateway::SqlErrorKind::TypeMismatch, "{}", text.message);
    assert!(text.message.contains("got a text value"), "{}", text.message);

    // 超过 `i64` 的字面量在绑定期就被拒绝，不会被折成 0 或某个近似值。
    let oversized = clause_error("SELECT id FROM users LIMIT 9223372036854775808");
    assert_eq!(oversized.kind, yyds_gateway::SqlErrorKind::Syntax, "{}", oversized.message);
    assert!(oversized.message.contains("out of range"), "{}", oversized.message);

    // 折叠走执行层同一个求值器，因此除零保留自己的错误类别。
    let division = clause_error("SELECT id FROM users LIMIT 1 / 0");
    assert_eq!(division.kind, yyds_gateway::SqlErrorKind::DivisionByZero, "{}", division.message);
}

#[test]
fn sql_join_rejects_ambiguous_unqualified_columns() {
    let (catalog, _executor) = join_fixture();

    // `id` 在两张表里都有，未限定必须报歧义而不是随便选一张。
    let error = yyds_gateway::bind_sql_catalog(
        "SELECT id FROM users JOIN orders ON users.id = orders.user_id",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect_err("ambiguous column");
    assert!(error.message.contains("ambiguous SQL column: id"), "{}", error.message);

    // 引用不存在表的限定名。
    let error = yyds_gateway::bind_sql_catalog(
        "SELECT admins.id FROM users JOIN orders ON users.id = orders.user_id",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect_err("unknown qualifier");
    assert!(error.message.contains("unknown SQL table qualifier"), "{}", error.message);
}

#[test]
fn sql_join_rejects_aggregate_and_duplicate_unaliased_source() {
    let (catalog, _executor) = join_fixture();

    // RIGHT JOIN / FULL JOIN 现已支持，bind 层保留原样 kind。
    let right = yyds_gateway::bind_sql_catalog(
        "SELECT users.name FROM users RIGHT JOIN orders ON users.id = orders.user_id",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("right join should bind");
    if let yyds_gateway::SqlCatalogBoundStatement::Select(sel) = &right {
        assert!(matches!(sel.joins[0].kind, yyds_gateway::SqlBoundJoinKind::Right));
    }
    else {
        panic!("expected Select statement");
    }

    let full = yyds_gateway::bind_sql_catalog(
        "SELECT users.name FROM users FULL JOIN orders ON users.id = orders.user_id",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("full join should bind");
    if let yyds_gateway::SqlCatalogBoundStatement::Select(sel) = &full {
        assert!(matches!(sel.joins[0].kind, yyds_gateway::SqlBoundJoinKind::Full));
    }
    else {
        panic!("expected Select statement");
    }

    // LEFT JOIN 同样在 bind 层通过。
    let left = yyds_gateway::bind_sql_catalog(
        "SELECT users.name FROM users LEFT JOIN orders ON users.id = orders.user_id",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("left join should bind");
    if let yyds_gateway::SqlCatalogBoundStatement::Select(sel) = &left {
        assert!(matches!(sel.joins[0].kind, yyds_gateway::SqlBoundJoinKind::Left));
    }
    else {
        panic!("expected Select statement");
    }

    // 同一张表出现两次又都没写别名：两个 `users` 争同一个限定名，按歧义表拒绝。
    let duplicate = yyds_gateway::bind_sql_catalog(
        "SELECT users.name FROM users JOIN users ON users.id = users.id",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect_err("duplicate table without aliases");
    assert_eq!(duplicate.kind, yyds_gateway::SqlErrorKind::AmbiguousTable, "{}", duplicate.message);
    assert!(duplicate.message.contains("more than once"), "{}", duplicate.message);

    let aggregate = yyds_gateway::bind_sql_catalog(
        "SELECT SUM(orders.total) FROM users JOIN orders ON users.id = orders.user_id",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("catalog bind");
    let error = yyds_gateway::plan_statement(aggregate, &catalog).expect_err("aggregate over join");
    assert!(error.message.contains("aggregate projections"), "{}", error.message);
}

#[test]
fn sql_table_alias_replaces_the_table_qualifier() {
    let (catalog, executor) = join_fixture();

    // 别名接管限定名：`u.id` 可用，主键等值仍然下推成单点读。
    let bound = yyds_gateway::bind_sql_catalog("SELECT u.name FROM users u WHERE u.id = 2", &catalog, 7, SqlDialect::Postgres)
        .expect("aliased bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    assert!(matches!(plan, yyds_gateway::SqlExecutionPlan::Read(_)), "got {plan:?}");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert_eq!(result.rows, vec![vec![yyds_gateway::SqlValue::String("bob".into())]]);

    // `AS` 拼写等价。
    let bound = yyds_gateway::bind_sql_catalog("SELECT u.id FROM users AS u ORDER BY u.id", &catalog, 7, SqlDialect::Postgres)
        .expect("AS alias bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert_eq!(result.rows.len(), 2);

    // 别名存在后，原表名不再是可见限定名。
    let error = yyds_gateway::bind_sql_catalog("SELECT users.name FROM users u", &catalog, 7, SqlDialect::Postgres)
        .expect_err("table name is hidden by its alias");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::UnknownTableQualifier, "{}", error.message);

    // JOIN 两侧都可以用别名。
    let bound = yyds_gateway::bind_sql_catalog(
        "SELECT u.name, o.total FROM users AS u JOIN orders AS o ON u.id = o.user_id ORDER BY o.total",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("aliased join bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert_eq!(
        result.rows,
        vec![
            vec![yyds_gateway::SqlValue::String("alice".into()), yyds_gateway::SqlValue::Integer(50)],
            vec![yyds_gateway::SqlValue::String("bob".into()), yyds_gateway::SqlValue::Integer(70)],
            vec![yyds_gateway::SqlValue::String("alice".into()), yyds_gateway::SqlValue::Integer(100)],
        ]
    );

    // 别名只改限定名，不改 catalog 列名：拼错的列照常报未知列。
    let error = yyds_gateway::bind_sql_catalog("SELECT u.nope FROM users u", &catalog, 7, SqlDialect::Postgres)
        .expect_err("unknown column");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::UnknownColumn, "{}", error.message);
}

#[test]
fn sql_self_join_keeps_both_occurrences_of_a_shared_column() {
    let (catalog, executor) = join_fixture();

    // 同一张表出现两次，两个别名各自保留一份 `id` / `name`。
    let bound = yyds_gateway::bind_sql_catalog(
        "SELECT a.id, a.name, b.name FROM users a JOIN users b ON a.id = b.id + 1",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("self join bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert_eq!(
        result.rows,
        vec![vec![
            yyds_gateway::SqlValue::Integer(2),
            yyds_gateway::SqlValue::String("bob".into()),
            yyds_gateway::SqlValue::String("alice".into()),
        ]]
    );
    assert_eq!(result.columns, vec!["id".to_string(), "name".to_string(), "name".to_string()]);

    // 未限定同名列必须报歧义，而不是随便挑一侧。
    let ambiguous = yyds_gateway::bind_sql_catalog(
        "SELECT name FROM users a JOIN users b ON a.id = b.id",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect_err("ambiguous shared column");
    assert_eq!(ambiguous.kind, yyds_gateway::SqlErrorKind::AmbiguousColumn, "{}", ambiguous.message);
}

#[test]
fn sql_derived_table_flattens_when_it_is_the_same_table_under_a_new_name() {
    let (catalog, executor) = join_fixture();

    // 内层过滤并入外层 `WHERE`，外层只看得见派生别名 `x`。
    let bound = yyds_gateway::bind_sql_catalog(
        "SELECT x.name FROM (SELECT * FROM users WHERE id = 2) x",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("derived bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("execute");
    assert_eq!(result.rows, vec![vec![yyds_gateway::SqlValue::String("bob".into())]]);
    assert_eq!(result.columns, vec!["name".to_string()]);

    // 派生表参与 INNER JOIN，内层过滤仍然生效。
    let joined = run_sql(
        &executor,
        None,
        &catalog,
        "SELECT x.name, o.total FROM (SELECT * FROM users WHERE id = 1) x JOIN orders o ON x.id = o.user_id \
         ORDER BY o.total",
    );
    assert_eq!(
        joined.rows,
        vec![
            vec![yyds_gateway::SqlValue::String("alice".into()), yyds_gateway::SqlValue::Integer(50)],
            vec![yyds_gateway::SqlValue::String("alice".into()), yyds_gateway::SqlValue::Integer(100)],
        ]
    );

    // `*` 展开与逐列投影在派生表上等价。
    let star = run_sql(&executor, None, &catalog, "SELECT * FROM (SELECT id, name FROM users) x ORDER BY x.id");
    assert_eq!(star.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(star.rows.len(), 2);
}

#[test]
fn sql_derived_table_reports_unflattenable_shapes() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");

    // 方言语法本身就要求别名，缺失按 Syntax 拒绝。
    let missing = yyds_gateway::bind_sql_catalog("SELECT * FROM (SELECT * FROM users)", &catalog, 7, SqlDialect::Postgres)
        .expect_err("no alias");
    assert_eq!(missing.kind, yyds_gateway::SqlErrorKind::Syntax, "{}", missing.message);
    assert!(missing.message.contains("requires an alias"), "{}", missing.message);

    for (label, sql, reason) in [
        ("inner LIMIT", "SELECT * FROM (SELECT * FROM users LIMIT 1) x", "LIMIT/OFFSET"),
        ("inner DISTINCT", "SELECT * FROM (SELECT DISTINCT * FROM users) x", "DISTINCT"),
        ("inner GROUP BY", "SELECT * FROM (SELECT id FROM users GROUP BY id) x", "GROUP BY"),
        ("inner aggregate", "SELECT * FROM (SELECT COUNT(*) FROM users) x", "every column"),
        ("narrow projection", "SELECT * FROM (SELECT id FROM users) x", "every column"),
        ("aliased projection", "SELECT * FROM (SELECT id AS ident, name FROM users) x", "every column"),
        ("nested derived", "SELECT * FROM (SELECT * FROM (SELECT * FROM users) y) x", "another derived table"),
        (
            "filtered derived in LEFT JOIN",
            "SELECT * FROM users u LEFT JOIN (SELECT * FROM users WHERE id = 1) x ON u.id = x.id",
            "INNER join",
        ),
    ] {
        let error = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres)
            .expect_err(&format!("{label} must stay outside the gateway subset"));
        assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Unsupported, "{label}: {}", error.message);
        assert!(error.message.contains("derived table"), "{label}: {}", error.message);
        assert!(error.message.contains(reason), "{label}: {}", error.message);
    }
}

#[test]
fn sql_common_table_expression_flattens_onto_its_base_table() {
    let (catalog, executor) = join_fixture();
    let run = |sql: &str| run_sql(&executor, None, &catalog, sql);

    // CTE 是同一张基表的另一个名字，投影与 `WHERE` 都用 CTE 名限定。
    let named = run("WITH u AS (SELECT * FROM users) SELECT u.name FROM u WHERE u.id = 1");
    assert_eq!(named.columns, vec!["name".to_string()]);
    assert_eq!(named.rows, vec![vec![yyds_gateway::SqlValue::String("alice".into())]]);

    // CTE 体的内层过滤并入外层 `WHERE`。
    let filtered = run("WITH u AS (SELECT * FROM users WHERE id = 2) SELECT name FROM u");
    assert_eq!(filtered.rows, vec![vec![yyds_gateway::SqlValue::String("bob".into())]]);

    // 同一条 CTE 被引用两次就像自连接，两次出现各有自己的行槽。
    let twice = run("WITH u AS (SELECT * FROM users) SELECT a.id, b.name FROM u a JOIN u b ON a.id = b.id + 1");
    assert_eq!(twice.rows, vec![vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::String("alice".into())]]);

    // 带过滤的 CTE 与同一张表的另一次出现共存时，过滤落在 CTE 自己的行槽上。
    let shared =
        run("WITH u AS (SELECT * FROM users WHERE id = 2) SELECT a.name, u.name FROM users a JOIN u ON a.id = u.id - 1");
    assert_eq!(
        shared.rows,
        vec![vec![yyds_gateway::SqlValue::String("alice".into()), yyds_gateway::SqlValue::String("bob".into())]]
    );

    // CTE 可以引用更早定义的 CTE。
    let chained = run("WITH a AS (SELECT * FROM users WHERE id = 1), b AS (SELECT * FROM a) SELECT b.name FROM b");
    assert_eq!(chained.rows, vec![vec![yyds_gateway::SqlValue::String("alice".into())]]);

    // CTE 名遮蔽同名 catalog 表。
    let shadows = run("WITH orders AS (SELECT * FROM users) SELECT orders.name FROM orders WHERE orders.id = 2");
    assert_eq!(shadows.rows, vec![vec![yyds_gateway::SqlValue::String("bob".into())]]);
}

#[test]
fn sql_window_ntile_lag_lead_evaluate_over_partitioned_ordering() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64, score: f64, active: bool, tag: utf8? }\n");
    let executor = empty_executor();
    for sql in [
        "INSERT INTO users VALUES (1, 'alice', 30, 1.5, TRUE, 'x')",
        "INSERT INTO users VALUES (2, 'bob', 20, 2.5, FALSE, NULL)",
        "INSERT INTO users VALUES (3, 'carol', 40, 3.25, TRUE, '')",
        "INSERT INTO users VALUES (4, 'dave', 30, 0.5, FALSE, 'y')",
    ] {
        run_sql(&executor, None, &catalog, sql);
    }

    // `NTILE(3)` 按 age 排序分 4 行为 3 桶：余数分给前面的桶（1,1,2,3）。
    let ntile = run_sql(&executor, None, &catalog, "SELECT id, NTILE(3) OVER (ORDER BY age) FROM users ORDER BY id");
    assert_eq!(
        ntile.rows,
        vec![
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::Integer(1)],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::Integer(1)],
            vec![yyds_gateway::SqlValue::Integer(3), yyds_gateway::SqlValue::Integer(3)],
            vec![yyds_gateway::SqlValue::Integer(4), yyds_gateway::SqlValue::Integer(2)],
        ]
    );

    // `LAG` 取排序前一行，越出分区是 NULL；`LEAD` 取排序后一行。
    let values = run_sql(
        &executor,
        None,
        &catalog,
        "SELECT id, LAG(name) OVER (ORDER BY age), LEAD(age) OVER (ORDER BY age) FROM users ORDER BY id",
    );
    assert_eq!(
        values.rows,
        vec![
            vec![
                yyds_gateway::SqlValue::Integer(1),
                yyds_gateway::SqlValue::String("bob".into()),
                yyds_gateway::SqlValue::Integer(30)
            ],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::Null, yyds_gateway::SqlValue::Integer(30)],
            vec![
                yyds_gateway::SqlValue::Integer(3),
                yyds_gateway::SqlValue::String("dave".into()),
                yyds_gateway::SqlValue::Null
            ],
            vec![
                yyds_gateway::SqlValue::Integer(4),
                yyds_gateway::SqlValue::String("alice".into()),
                yyds_gateway::SqlValue::Integer(40)
            ],
        ]
    );

    // 显式偏移与默认值：越出分区时按当前行求值默认表达式。
    let defaulted =
        run_sql(&executor, None, &catalog, "SELECT id, LAG(score, 2, 0.0) OVER (ORDER BY age) FROM users ORDER BY id");
    assert_eq!(
        defaulted.rows,
        vec![
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::Number(0.0)],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::Number(0.0)],
            vec![yyds_gateway::SqlValue::Integer(3), yyds_gateway::SqlValue::Number(1.5)],
            vec![yyds_gateway::SqlValue::Integer(4), yyds_gateway::SqlValue::Number(2.5)],
        ]
    );
}

#[test]
fn sql_window_ntile_lag_lead_reject_invalid_shapes_at_bind_time() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64, score: f64, active: bool, tag: utf8? }\n");

    // 参数个数、常量形状、ORDER BY、DISTINCT、嵌套聚合与显式 frame 全部在绑定期显式拒绝。
    for sql in [
        "SELECT NTILE() OVER (ORDER BY age) FROM users",
        "SELECT NTILE(2, 3) OVER (ORDER BY age) FROM users",
        "SELECT NTILE(age) OVER (ORDER BY age) FROM users",
        "SELECT NTILE(0) OVER (ORDER BY age) FROM users",
        "SELECT NTILE(-1) OVER (ORDER BY age) FROM users",
        "SELECT NTILE(2) OVER (PARTITION BY active) FROM users",
        "SELECT NTILE(2) OVER (ORDER BY age ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM users",
        "SELECT NTILE(2) OVER (ORDER BY age RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM users",
        "SELECT LAG(name) OVER (PARTITION BY active) FROM users",
        "SELECT LAG(name, -1) OVER (ORDER BY age) FROM users",
        "SELECT LAG(name, age) OVER (ORDER BY age) FROM users",
        "SELECT LAG(DISTINCT name) OVER (ORDER BY age) FROM users",
        "SELECT LEAD(name, 1, 2, 3) OVER (ORDER BY age) FROM users",
        "SELECT LEAD(SUM(age)) OVER (ORDER BY age) FROM users",
        "SELECT LAG(name) OVER (ORDER BY age ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM users",
        "SELECT ROW_NUMBER(1) OVER (ORDER BY age) FROM users",
        "SELECT NTILE(2) FROM users",
        "SELECT LAG(name) FROM users",
    ] {
        let error = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres)
            .err()
            .unwrap_or_else(|| panic!("`{sql}` must be rejected at bind time"));
        assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Unsupported, "{sql}: {}", error.message);
    }

    // 未知窗口名保持窗口语境诊断，与不支持参数形状的诊断分离。
    let unknown = yyds_gateway::bind_sql_catalog(
        "SELECT FIRST_VALUE(name) OVER (ORDER BY age) FROM users",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .err()
    .expect("FIRST_VALUE is outside the gateway subset");
    assert_eq!(unknown.kind, yyds_gateway::SqlErrorKind::Unsupported, "{}", unknown.message);
    assert!(unknown.message.contains("window function"), "{}", unknown.message);
}

#[test]
fn sql_window_range_frames_reject_undefined_value_semantics() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64, score: f64, active: bool, tag: utf8? }\n");

    // `RANGE` 没有 `ORDER BY` 就没有可比较的值；数值偏移要求恰好一列排序，
    // 零列与多列都在绑定期显式拒绝，不退化成 `ROWS`。
    for (sql, reason) in [
        ("SELECT id, SUM(age) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM users", "require an ORDER BY"),
        ("SELECT id, SUM(age) OVER (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM users", "exactly one ORDER BY"),
        (
            "SELECT id, SUM(age) OVER (ORDER BY age, score RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) FROM users",
            "exactly one ORDER BY",
        ),
    ] {
        let error = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres)
            .err()
            .unwrap_or_else(|| panic!("`{sql}` must be rejected at bind time"));
        assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Unsupported, "{sql}: {}", error.message);
        assert!(error.message.contains(reason), "{sql}: {}", error.message);
    }

    // 无偏移的 `RANGE` 帧只用到 peer 边界，多列 `ORDER BY` 合法。
    yyds_gateway::bind_sql_catalog(
        "SELECT id, SUM(age) OVER (ORDER BY age, score RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM users",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("peer-bounded RANGE frame over multiple ORDER BY columns binds");
}

#[test]
fn sql_window_range_frame_offsets_reject_ordering_values_without_value_distance() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64, score: f64, active: bool, tag: utf8? }\n");
    let executor = empty_executor();
    for sql in [
        "INSERT INTO users VALUES (1, 'alice', 30, 1.5, TRUE, 'x')",
        "INSERT INTO users VALUES (2, 'bob', 20, 2.5, FALSE, NULL)",
        "INSERT INTO users VALUES (3, 'carol', 40, 3.25, TRUE, '')",
    ] {
        run_sql(&executor, None, &catalog, sql);
    }

    // 非数值排序键没有 SQL 定义的值距离，执行期显式拒绝；`NULL` 排序键不再拒绝。
    for sql in [
        "SELECT id, SUM(age) OVER (ORDER BY name RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM users",
        "SELECT id, SUM(age) OVER (ORDER BY tag RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM users",
    ] {
        let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres).expect("shape binds");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
        let error = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect_err("value axis must fail");
        assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Internal, "{sql}: {}", error.message);
    }
}

#[test]
fn sql_window_range_frame_offsets_keep_null_ordering_keys_off_the_value_axis() {
    use yyds_gateway::SqlValue;

    let catalog = catalog("table users { @@id: i64, age: i64?, amount: i64 }\n");
    let executor = empty_executor();
    for sql in [
        "INSERT INTO users VALUES (1, NULL, 5)",
        "INSERT INTO users VALUES (2, 10, 10)",
        "INSERT INTO users VALUES (3, 11, 20)",
        "INSERT INTO users VALUES (4, 30, 30)",
        "INSERT INTO users VALUES (5, NULL, 40)",
    ] {
        run_sql(&executor, None, &catalog, sql);
    }

    // `NULL` 排序键没有值距离：偏移帧只落在当前行自己的非 `NULL` 连续段上，
    // 前缀或后缀的 `NULL` 段都不进入帧。ASC 下两个 `NULL` 行按 `NULLS FIRST`
    // 排在数值段之前，因此 id=2 的 `1 PRECEDING AND 1 FOLLOWING` 帧只含 10 与 11，
    // 不会把 NULL 行与 age=30 的行卷进来。
    let ascending = run_sql(
        &executor,
        None,
        &catalog,
        "SELECT id, SUM(amount) OVER (ORDER BY age NULLS FIRST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM users ORDER BY id",
    );
    assert_eq!(
        ascending.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(45)],
            vec![SqlValue::Integer(2), SqlValue::Integer(30)],
            vec![SqlValue::Integer(3), SqlValue::Integer(30)],
            vec![SqlValue::Integer(4), SqlValue::Integer(30)],
            vec![SqlValue::Integer(5), SqlValue::Integer(45)],
        ]
    );

    // 当前行为 `NULL` 时偏移 bound 折回当前 peer 组：两个 `NULL` 行只累计彼此的
    // 取值（5 + 40），不读取任何非 `NULL` 行。
    // `DESC` 把值轴取负统一到升序，`NULL` 在 Postgres 默认排在最前，两个方向
    // 得到同一套数字。
    let descending = run_sql(
        &executor,
        None,
        &catalog,
        "SELECT id, SUM(amount) OVER (ORDER BY age DESC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM users ORDER BY id",
    );
    assert_eq!(
        descending.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(45)],
            vec![SqlValue::Integer(2), SqlValue::Integer(30)],
            vec![SqlValue::Integer(3), SqlValue::Integer(30)],
            vec![SqlValue::Integer(4), SqlValue::Integer(30)],
            vec![SqlValue::Integer(5), SqlValue::Integer(45)],
        ]
    );

    // 只写起点偏移：`ASC` 从值轴下方累计到当前 peer 组末尾。
    let ascending_preceding = run_sql(
        &executor,
        None,
        &catalog,
        "SELECT id, SUM(amount) OVER (ORDER BY age NULLS FIRST RANGE BETWEEN 15 PRECEDING AND CURRENT ROW) FROM users ORDER BY id",
    );
    assert_eq!(
        ascending_preceding.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(45)],
            vec![SqlValue::Integer(2), SqlValue::Integer(10)],
            vec![SqlValue::Integer(3), SqlValue::Integer(30)],
            vec![SqlValue::Integer(4), SqlValue::Integer(30)],
            vec![SqlValue::Integer(5), SqlValue::Integer(45)],
        ]
    );

    // `DESC` 下「15 之前」沿排序方向朝更大的值走：30 单独一段，11 与 10 同段。
    let descending_preceding = run_sql(
        &executor,
        None,
        &catalog,
        "SELECT id, SUM(amount) OVER (ORDER BY age DESC RANGE BETWEEN 15 PRECEDING AND CURRENT ROW) FROM users ORDER BY id",
    );
    assert_eq!(
        descending_preceding.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(45)],
            vec![SqlValue::Integer(2), SqlValue::Integer(30)],
            vec![SqlValue::Integer(3), SqlValue::Integer(20)],
            vec![SqlValue::Integer(4), SqlValue::Integer(30)],
            vec![SqlValue::Integer(5), SqlValue::Integer(45)],
        ]
    );
}

#[test]
fn sql_subqueries_inherit_the_enclosing_cte_scope() {
    use yyds_gateway::SqlValue;

    let (catalog, executor) = join_fixture();
    let run = |sql: &str| run_sql(&executor, None, &catalog, sql);

    // `IN (SELECT ...)` 内部可以直接引用外层 `WITH` 定义的 CTE。
    let membership =
        run("WITH u AS (SELECT * FROM users) SELECT id FROM users WHERE id IN (SELECT id FROM u WHERE u.name = 'bob')");
    assert_eq!(membership.rows, vec![vec![SqlValue::Integer(2)]]);

    // CTE 体的内层过滤随扁平化一起并进子查询的 `WHERE`。
    let filtered =
        run("WITH u AS (SELECT * FROM users WHERE name = 'alice') SELECT id FROM users WHERE id IN (SELECT id FROM u)");
    assert_eq!(filtered.rows, vec![vec![SqlValue::Integer(1)]]);

    // `EXISTS (...)` 子查询同样继承外层 CTE。
    let existence =
        run("WITH u AS (SELECT * FROM users) SELECT id FROM users WHERE EXISTS (SELECT 1 FROM u WHERE u.id = 2) ORDER BY id");
    assert_eq!(existence.rows, vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]);

    // 子查询在投影位置引用 CTE 也一样，CTE 名在子查询里被真实展开。
    let projected =
        run("WITH u AS (SELECT * FROM users) SELECT id, EXISTS (SELECT 1 FROM u WHERE u.id = 9) AS any FROM users ORDER BY id");
    assert_eq!(
        projected.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Boolean(false)], vec![SqlValue::Integer(2), SqlValue::Boolean(false)],]
    );

    // 作用域只放宽 CTE 名，未知名字仍然按 `UnknownTable` 拒绝。
    for sql in [
        "WITH u AS (SELECT * FROM users) SELECT id FROM users WHERE id IN (SELECT id FROM missing)",
        "WITH u AS (SELECT * FROM users) SELECT id FROM users WHERE EXISTS (SELECT 1 FROM missing)",
    ] {
        let error = yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres)
            .expect_err("unknown name inside the subquery");
        assert_eq!(error.kind, yyds_gateway::SqlErrorKind::UnknownTable, "{sql}: {}", error.message);
    }
}

#[test]
fn sql_scalar_subquery_yields_one_value_and_pins_its_cardinality() {
    use yyds_gateway::SqlValue;

    let (catalog, executor) = nullable_users();

    // 一行结果 → 取该行唯一列的值。
    let single =
        run_sql(&executor, None, &catalog, "SELECT id, (SELECT age FROM users WHERE id = 3) AS peer FROM users WHERE id = 1");
    assert_eq!(single.columns, vec!["id".to_string(), "peer".to_string()]);
    assert_eq!(single.rows, vec![vec![SqlValue::Integer(1), SqlValue::Integer(10)]]);

    // 0 行 → SQL 基数规则：标量子查询取 `NULL`。
    let empty =
        run_sql(&executor, None, &catalog, "SELECT (SELECT age FROM users WHERE id = 99) AS none FROM users WHERE id = 1");
    assert_eq!(empty.rows, vec![vec![SqlValue::Null]]);

    // 聚合形式 `(SELECT MAX(...) ...)` 同样是一条标量子查询，投影类型取自其唯一列。
    let aggregate = run_sql(&executor, None, &catalog, "SELECT (SELECT COUNT(*) FROM users) AS total FROM users WHERE id = 2");
    assert_eq!(aggregate.columns, vec!["total".to_string()]);
    assert_eq!(aggregate.rows, vec![vec![SqlValue::Integer(3)]]);

    // 比较右值复用同一物化路径。
    let predicate = run_sql(&executor, None, &catalog, "SELECT id FROM users WHERE age = (SELECT age FROM users WHERE id = 3)");
    assert_eq!(predicate.rows, vec![vec![SqlValue::Integer(3)]]);

    // 多行 → 基数违规，按执行错误上报（不是语法错误）。
    let bound = yyds_gateway::bind_sql_catalog(
        "SELECT id FROM users WHERE id = (SELECT id FROM users)",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind scalar subquery");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan scalar subquery");
    let error = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect_err("several rows");
    assert_eq!(error.kind, yyds_gateway::SqlErrorKind::CardinalityViolation, "{}", error.message);
    assert!(error.message.contains("more than one row"), "{}", error.message);

    // 投影多列时无法作为单一值，绑定期就显式拒绝。
    let wide =
        yyds_gateway::bind_sql_catalog("SELECT (SELECT id, age FROM users) FROM users", &catalog, 7, SqlDialect::Postgres)
            .expect_err("multi column scalar subquery");
    assert_eq!(wide.kind, yyds_gateway::SqlErrorKind::Unsupported, "{}", wide.message);
    assert!(wide.message.contains("exactly one column"), "{}", wide.message);
}

/// 执行期语义错误必须带结构化类别，不能退化成通用执行错误码。
#[test]
fn sql_execution_semantic_errors_keep_structured_kinds() {
    let (catalog, executor) = nullable_users();

    let bound = yyds_gateway::bind_sql_catalog("SELECT id, age, COUNT(*) FROM users GROUP BY id", &catalog, 7, SqlDialect::Postgres)
        .expect("bind grouped projection");
    let grouping = yyds_gateway::plan_statement(bound, &catalog).expect_err("grouped projection must fail");
    assert_eq!(grouping.kind, yyds_gateway::SqlErrorKind::InvalidGrouping, "{}", grouping.message);
    assert!(grouping.message.contains("GROUP BY"), "{}", grouping.message);

    let operand_sql = "SELECT id FROM users WHERE id IN (SELECT id, age FROM users)";
    let bound = yyds_gateway::bind_sql_catalog(operand_sql, &catalog, 7, SqlDialect::Postgres).expect("bind IN subquery");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan IN subquery");
    let operand = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect_err("wide IN subquery");
    assert_eq!(operand.kind, yyds_gateway::SqlErrorKind::OperandColumns, "{}", operand.message);
    assert!(operand.message.contains("exactly one column"), "{}", operand.message);
}

#[test]
fn sql_common_table_expression_reports_unflattenable_shapes() {
    let (catalog, _executor) = join_fixture();
    let reject = |sql: &str| {
        yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres)
            .expect_err(&format!("`{sql}` must stay outside the gateway subset"))
    };

    for (label, sql, reason) in [
        ("inner LIMIT", "WITH u AS (SELECT * FROM users LIMIT 1) SELECT * FROM u", "LIMIT/OFFSET"),
        ("narrow projection", "WITH u AS (SELECT id FROM users) SELECT * FROM u", "every column"),
        ("inner aggregate", "WITH u AS (SELECT COUNT(*) FROM users) SELECT * FROM u", "every column"),
        (
            "inner join",
            "WITH u AS (SELECT * FROM users JOIN orders ON users.id = orders.user_id) SELECT * FROM u",
            "joins more than one table",
        ),
        ("nested WITH", "WITH u AS (WITH v AS (SELECT * FROM users) SELECT * FROM v) SELECT * FROM u", "its own WITH clause"),
    ] {
        let error = reject(sql);
        assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Unsupported, "{label}: {}", error.message);
        assert!(error.message.contains("common table expression"), "{label}: {}", error.message);
        assert!(error.message.contains(reason), "{label}: {}", error.message);
    }

    // 未被引用的 CTE 也要可表达，不能因为不在 `FROM` 里就静默吞掉错误。
    let unused = reject("WITH u AS (SELECT * FROM missing) SELECT 1");
    assert_eq!(unused.kind, yyds_gateway::SqlErrorKind::UnknownTable, "{}", unused.message);

    // 非递归 CTE 只能引用更早的定义，前向引用按未知表拒绝。
    let forward = reject("WITH a AS (SELECT * FROM b), b AS (SELECT * FROM users) SELECT * FROM a");
    assert_eq!(forward.kind, yyds_gateway::SqlErrorKind::UnknownTable, "{}", forward.message);

    // 显式列名会重命名 CTE 输出，由重命名测试单独覆盖；这里只保留不可扁平化的体。
    let duplicate = reject("WITH u AS (SELECT * FROM users), u AS (SELECT * FROM users) SELECT * FROM u");
    assert_eq!(duplicate.kind, yyds_gateway::SqlErrorKind::Syntax, "{}", duplicate.message);
    assert!(duplicate.message.contains("more than once"), "{}", duplicate.message);

    // Oak 在解析阶段就拒绝 `WITH RECURSIVE`。
    let recursive = reject("WITH RECURSIVE u AS (SELECT * FROM users) SELECT * FROM u");
    assert_eq!(recursive.kind, yyds_gateway::SqlErrorKind::Syntax, "{}", recursive.message);
    assert!(recursive.message.contains("recursive"), "{}", recursive.message);
}

#[test]
fn sql_common_table_expression_applies_an_explicit_output_column_list() {
    use yyds_gateway::SqlValue;

    let (catalog, executor) = join_fixture();
    let run = |sql: &str| run_sql(&executor, None, &catalog, sql);

    // `*` body：显式列名按 catalog 字段顺序替换输出名，底层行值不动。
    let star = run("WITH u(a, b) AS (SELECT * FROM users) SELECT * FROM u ORDER BY a");
    assert_eq!(star.columns, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(
        star.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::String("alice".into())],
            vec![SqlValue::Integer(2), SqlValue::String("bob".into())],
        ]
    );

    // 裸标识符 body：重命名按书写顺序落到对应字段，而不是 catalog 字段顺序。
    let written = run("WITH u(no, who) AS (SELECT name, id FROM users) SELECT no, who FROM u ORDER BY who");
    assert_eq!(written.columns, vec!["no".to_string(), "who".to_string()]);
    assert_eq!(
        written.rows,
        vec![
            vec![SqlValue::String("alice".into()), SqlValue::Integer(1)],
            vec![SqlValue::String("bob".into()), SqlValue::Integer(2)],
        ]
    );

    // body 的过滤仍按 body 自己的列名绑定，显式列名只改外层看到的名字。
    let filtered = run("WITH u(a, b) AS (SELECT * FROM users WHERE name = 'bob') SELECT * FROM u");
    assert_eq!(filtered.columns, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(filtered.rows, vec![vec![SqlValue::Integer(2), SqlValue::String("bob".into())]]);

    // 旧列名不再属于这个 CTE 的作用域。
    let stale = yyds_gateway::bind_sql_catalog(
        "WITH u(no, who) AS (SELECT name, id FROM users) SELECT id FROM u",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect_err("the explicit column list hides the catalog names");
    assert_eq!(stale.kind, yyds_gateway::SqlErrorKind::UnknownColumn, "{}", stale.message);

    // 同表另一份未重命名的引用仍按原名解析，两个 occurrence 的行槽互不覆盖。
    let joined = run(
        "WITH u(no, who) AS (SELECT name, id FROM users) SELECT x.id, u.no FROM users x JOIN u ON x.id = u.who ORDER BY x.id",
    );
    assert_eq!(joined.columns, vec!["id".to_string(), "no".to_string()]);
    assert_eq!(
        joined.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::String("alice".into())],
            vec![SqlValue::Integer(2), SqlValue::String("bob".into())],
        ]
    );

    // 引用更早的已重命名 CTE 时，递归展开同样带上更早的列名。
    let chained = run("WITH a(x, y) AS (SELECT * FROM users), b(p, q) AS (SELECT x, y FROM a) SELECT * FROM b ORDER BY p");
    assert_eq!(chained.columns, vec!["p".to_string(), "q".to_string()]);
    assert_eq!(
        chained.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::String("alice".into())],
            vec![SqlValue::Integer(2), SqlValue::String("bob".into())],
        ]
    );
}

#[test]
fn sql_common_table_expression_rejects_unmappable_explicit_column_lists() {
    let (catalog, _executor) = join_fixture();
    let reject = |sql: &str| {
        yyds_gateway::bind_sql_catalog(sql, &catalog, 7, SqlDialect::Postgres)
            .expect_err(&format!("`{sql}` must stay outside the gateway subset"))
    };

    // 显式列名按位置替换 body 的输出列，数量必须与投影一一对应。
    for sql in [
        "WITH u(a) AS (SELECT * FROM users) SELECT * FROM u",
        "WITH u(a, b, c) AS (SELECT id, name FROM users) SELECT * FROM u",
    ] {
        let error = reject(sql);
        assert_eq!(error.kind, yyds_gateway::SqlErrorKind::Unsupported, "{sql}: {}", error.message);
        assert!(error.message.contains("explicit column list"), "{sql}: {}", error.message);
        assert!(error.message.contains("body outputs"), "{sql}: {}", error.message);
    }

    // 同一 CTE 的显式列名重复会让输出两列同名，绑定期就拒绝。
    let duplicate = reject("WITH u(a, a) AS (SELECT * FROM users) SELECT * FROM u");
    assert_eq!(duplicate.kind, yyds_gateway::SqlErrorKind::Syntax, "{}", duplicate.message);
    assert!(duplicate.message.contains("more than once"), "{}", duplicate.message);
}

#[test]
fn sql_exists_predicates_evaluate_against_subquery_rows() {
    let (catalog, executor, _directory) = file_join_fixture();
    let ids = |sql: &str| run_sql(&executor, None, &catalog, sql).rows;

    // 子查询有行 → 谓词恒真，保留全部 users。
    assert_eq!(
        ids("SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = 1) ORDER BY id"),
        vec![vec![yyds_gateway::SqlValue::Integer(1)], vec![yyds_gateway::SqlValue::Integer(2)]]
    );

    // 子查询无行 → 谓词恒假。
    assert!(ids("SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = 999)").is_empty());

    // `NOT EXISTS` 与 `EXISTS` 互补。
    assert_eq!(
        ids("SELECT id FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = 999) ORDER BY id"),
        vec![vec![yyds_gateway::SqlValue::Integer(1)], vec![yyds_gateway::SqlValue::Integer(2)]]
    );
    assert!(ids("SELECT id FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = 1)").is_empty());

    // 投影位置与 `HAVING` 位置同样可求值。
    let projected = run_sql(
        &executor,
        None,
        &catalog,
        "SELECT id, EXISTS (SELECT 1 FROM orders WHERE total = 100) AS has FROM users ORDER BY id",
    );
    assert_eq!(projected.columns, vec!["id".to_string(), "has".to_string()]);
    assert_eq!(
        projected.rows,
        vec![
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::Boolean(true)],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::Boolean(true)],
        ]
    );
    let having = run_sql(&executor, None, &catalog, "SELECT COUNT(*) FROM users HAVING EXISTS (SELECT 1 FROM users)");
    assert_eq!(having.rows, vec![vec![yyds_gateway::SqlValue::Integer(2)]]);

    // 写入路径的 `WHERE` 复用同一物化流程。
    let deleted = run_sql(&executor, None, &catalog, "DELETE FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = 9)");
    assert_eq!(deleted.affected_rows, 2);
    assert!(run_sql(&executor, None, &catalog, "SELECT id FROM users").rows.is_empty());
}

#[test]
fn sql_scalar_function_binding_rejects_unknown_name_and_bad_arity() {
    let catalog = catalog("table users { @@id: i64 }\n");
    let unknown =
        yyds_gateway::bind_sql_catalog("SELECT NO_SUCH_FN(1)", &catalog, 7, SqlDialect::Postgres).expect_err("unknown fn");
    assert!(unknown.message.contains("unknown SQL function"), "unexpected message: {}", unknown.message);

    let arity = yyds_gateway::bind_sql_catalog("SELECT LOWER()", &catalog, 7, SqlDialect::Postgres).expect_err("arity");
    assert!(arity.message.contains("LOWER() expects 1 argument"), "unexpected message: {}", arity.message);
}

// --- 事务内读写原子性 ---

/// 在给定执行器 / 事务上下文里跑一条 SQL，返回结果集。
fn run_sql(
    executor: &dyn yyds_execution::KeyValueExecutor,
    transaction: Option<&dyn yyds_execution::TransactionHandle>,
    catalog: &yyds_gateway::SqlCatalog,
    sql: &str,
) -> yyds_gateway::SqlResult {
    let bound = yyds_gateway::bind_sql_catalog(sql, catalog, 7, SqlDialect::Postgres).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, catalog).expect("plan");
    yyds_gateway::execute_plan(executor, transaction, &plan, SqlDialect::Postgres).expect("execute")
}

#[test]
fn sql_transaction_scan_observes_uncommitted_writes() {
    use std::sync::Arc;
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor: Arc<dyn yyds_execution::KeyValueExecutor> = Arc::new(empty_executor());
    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(Arc::clone(&executor), None));

    run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, "INSERT INTO users VALUES (1, 'alice')");
    run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, "INSERT INTO users VALUES (2, 'bob')");
    run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, "UPDATE users SET name = 'carol' WHERE id = 2");

    // Scan 必须合并事务 buffer，否则 SELECT * 看不到本事务尚未提交的写入。
    let scan = run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, "SELECT id, name FROM users ORDER BY id");
    assert_eq!(
        scan.rows,
        vec![
            vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("alice".into())],
            vec![yyds_gateway::SqlValue::Integer(2), yyds_gateway::SqlValue::String("carol".into())],
        ]
    );

    // 事务外仍然看不到未提交数据。
    let outside = run_sql(executor.as_ref(), None, &catalog, "SELECT id FROM users");
    assert!(outside.rows.is_empty(), "uncommitted rows must not leak outside the transaction");

    handle.rollback();
}

#[test]
fn sql_transaction_rollback_discards_insert_update_and_delete() {
    use std::sync::Arc;
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor: Arc<dyn yyds_execution::KeyValueExecutor> =
        Arc::new(make_executor_with_rows("table:1", &[(b"1", &[(1, b"1"), (2, b"original")])]));
    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(Arc::clone(&executor), None));

    run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, "UPDATE users SET name = 'txn' WHERE id = 1");
    run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, "INSERT INTO users VALUES (2, 'pending')");
    run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, "DELETE FROM users WHERE id = 1");

    let inside = run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, "SELECT id FROM users");
    assert_eq!(
        inside.rows,
        vec![vec![yyds_gateway::SqlValue::Integer(2)]],
        "transaction view should show only the pending insert"
    );

    handle.rollback();

    // 回滚后 base 完全恢复：id=1 仍是 original，id=2 不存在。
    let survivors = run_sql(executor.as_ref(), None, &catalog, "SELECT id, name FROM users");
    assert_eq!(
        survivors.rows,
        vec![vec![yyds_gateway::SqlValue::Integer(1), yyds_gateway::SqlValue::String("original".into())]]
    );
    let missing = run_sql(executor.as_ref(), None, &catalog, "SELECT id FROM users WHERE id = 2");
    assert!(missing.rows.is_empty(), "rolled back insert must not persist");
}

#[test]
fn sql_transaction_concurrent_update_conflict_is_rejected_at_commit() {
    use std::sync::Arc;
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor: Arc<dyn yyds_execution::KeyValueExecutor> =
        Arc::new(make_file_executor_with_rows(directory.path(), "table:1", &[(b"1", &[(1, b"1"), (2, b"original")])]));

    // 两个事务都读到同一 pre-image，然后各自做 read-modify-write。
    let t1: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(Arc::clone(&executor), None));
    let t2: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(Arc::clone(&executor), None));
    let first = run_sql(executor.as_ref(), Some(t1.as_ref()), &catalog, "UPDATE users SET name = 't1' WHERE id = 1");
    assert_eq!(first.affected_rows, 1);
    let second = run_sql(executor.as_ref(), Some(t2.as_ref()), &catalog, "UPDATE users SET name = 't2' WHERE id = 1");
    assert_eq!(second.affected_rows, 1);

    t1.commit().expect("first transaction commits");
    let error = t2.commit().expect_err("second transaction must be rejected by OCC");
    assert!(
        matches!(error, yyds_types::Error::CasConflict { .. }),
        "expected CasConflict for the stale pre-image, got: {error:?}"
    );

    // 只保留第一个事务写入，后提交者不能静默覆盖。
    let result = run_sql(executor.as_ref(), None, &catalog, "SELECT name FROM users WHERE id = 1");
    assert_eq!(result.rows, vec![vec![yyds_gateway::SqlValue::String("t1".into())]]);
}

// --- catalog 类型化解码矩阵 ---

/// 一份覆盖 `i32` / `f64` / `bool` / `bytes` / `utf8` / 可空列的 catalog。
fn typed_metrics() -> (yyds_gateway::SqlCatalog, yyds_execution::LocalExecutor<yyds_kv::MemoryShard>) {
    let catalog =
        catalog("table metrics { @@id: i64, small: i32, ratio: f64, active: bool, payload: bytes, code: utf8, note: utf8? }\n");
    assert_eq!(catalog.tables()[0].fields[1].field_type, yyds_gateway::SqlFieldType::Int32);
    assert_eq!(catalog.tables()[0].fields[2].field_type, yyds_gateway::SqlFieldType::Float64);
    assert_eq!(catalog.tables()[0].fields[3].field_type, yyds_gateway::SqlFieldType::Bool);
    assert_eq!(catalog.tables()[0].fields[4].field_type, yyds_gateway::SqlFieldType::Bytes);
    assert_eq!(catalog.tables()[0].fields[5].field_type, yyds_gateway::SqlFieldType::String);
    assert_eq!(
        catalog.tables()[0].fields[6].field_type,
        yyds_gateway::SqlFieldType::Optional(Box::new(yyds_gateway::SqlFieldType::String))
    );

    let executor = make_executor_with_rows(
        "table:1",
        &[
            // 前导零与空串必须保持字符串，bool/bytes/浮点按声明类型解释。
            (b"1", &[(1, b"1"), (2, b"-7"), (3, b"0.5"), (4, b"1"), (5, b"blob"), (6, b"007")]),
            // 缺失字段 = NULL，空串仍是 `''`。
            (b"2", &[(1, b"2"), (2, b"0"), (3, b"-2.25"), (4, b"0"), (5, b""), (6, b""), (7, b"")]),
        ],
    );
    (catalog, executor)
}

#[test]
fn catalog_types_drive_decode_on_read_scan_and_transaction_paths() {
    use std::sync::Arc;
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};
    use yyds_gateway::SqlValue;

    let (catalog, executor) = typed_metrics();
    let columns = "id, small, ratio, active, payload, code, note";

    // 主键等值 → Read 路径。
    let read = run_sql(&executor, None, &catalog, &format!("SELECT {columns} FROM metrics WHERE id = 1"));
    assert_eq!(
        read.rows,
        vec![vec![
            SqlValue::Integer(1),
            SqlValue::Integer(-7),
            SqlValue::Number(0.5),
            SqlValue::Boolean(true),
            SqlValue::Bytes(b"blob".to_vec()),
            SqlValue::String("007".into()),
            SqlValue::Null,
        ]]
    );

    // 无主键条件 → Scan 路径；顺序一致，值必须与 Read 完全相同。
    let scan = run_sql(&executor, None, &catalog, &format!("SELECT {columns} FROM metrics ORDER BY id"));
    assert_eq!(scan.rows.len(), 2);
    assert_eq!(scan.rows[0], read.rows[0]);
    assert_eq!(
        scan.rows[1],
        vec![
            SqlValue::Integer(2),
            SqlValue::Integer(0),
            SqlValue::Number(-2.25),
            SqlValue::Boolean(false),
            // 二进制列的空串是零长 octets，不是空文本。
            SqlValue::Bytes(Vec::new()),
            SqlValue::String(String::new()),
            SqlValue::String(String::new()),
        ]
    );

    // 事务内扫描走同一套 catalog 类型解码，结果必须与事务外一致。
    let executor: Arc<dyn yyds_execution::KeyValueExecutor> = Arc::new(executor);
    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(Arc::clone(&executor), None));
    let txn_scan =
        run_sql(executor.as_ref(), Some(handle.as_ref()), &catalog, &format!("SELECT {columns} FROM metrics ORDER BY id"));
    assert_eq!(txn_scan.rows, scan.rows);
    handle.rollback();
}

#[test]
fn binary_columns_round_trip_raw_octets_and_reject_bad_literals() {
    use yyds_gateway::{SqlErrorKind, SqlValue};

    let catalog = catalog("table payloads { @@id: i64, body: bytes }\n");
    let executor = empty_executor();

    // 写值只能走十六进制 CAST：SQL 文本无法直接承载非 UTF-8 octets。
    run_sql(&executor, None, &catalog, "INSERT INTO payloads VALUES (1, CAST('00FF41' AS BYTEA))");
    run_sql(&executor, None, &catalog, "INSERT INTO payloads VALUES (2, CAST('' AS BLOB))");
    run_sql(&executor, None, &catalog, "INSERT INTO payloads VALUES (3, NULL)");

    // NUL、非 ASCII 字节与零长 octets 都必须原样回读，NULL 保持 NULL。
    assert_eq!(
        run_sql(&executor, None, &catalog, "SELECT body FROM payloads ORDER BY id").rows,
        vec![vec![SqlValue::Bytes(vec![0x00, 0xFF, 0x41])], vec![SqlValue::Bytes(Vec::new())], vec![SqlValue::Null],]
    );
    // `LENGTH` 对二进制按 octets 计数，两种方言一致。
    assert_eq!(
        run_sql(&executor, None, &catalog, "SELECT LENGTH(body) FROM payloads WHERE id = 1").rows,
        vec![vec![SqlValue::Integer(3)]]
    );

    // 非法十六进制拼写必须显式拒绝，不能截断或悄悄写成 NULL。
    let bound = yyds_gateway::bind_sql_catalog(
        "INSERT INTO payloads VALUES (4, CAST('0G' AS BYTEA))",
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let error = yyds_gateway::plan_statement(bound, &catalog).expect_err("bad hex");
    assert!(error.message.contains("non-hexadecimal"), "{}", error.message);

    // `::bytea` 在 Oak 里只有词法没有节点：网关稳定拒绝，避免把转义拼写当载荷存下。
    let error =
        yyds_gateway::bind_sql_catalog("INSERT INTO payloads VALUES (4, '\\x00FF'::bytea)", &catalog, 7, SqlDialect::Postgres)
            .expect_err("`::bytea` must be rejected");
    assert_eq!(error.kind, SqlErrorKind::Unsupported);
    assert!(error.message.contains("::bytea"), "{}", error.message);
    // 字符串字面量里的 `::` 不是类型转换，照常通过。
    assert_eq!(run_sql(&executor, None, &catalog, "SELECT '::bytea'").rows, vec![vec![SqlValue::String("::bytea".into())]]);

    // 非二进制 `::` 同样只有词法没有节点：静默丢掉转换会同时改变列名和声明类型。
    let error = yyds_gateway::bind_sql_catalog("SELECT 1::int AS value", &catalog, 7, SqlDialect::Postgres)
        .expect_err("`::int` must be rejected");
    assert_eq!(error.kind, SqlErrorKind::Unsupported);
    assert!(error.message.contains("CAST(<expression> AS int)"), "{}", error.message);

    // 容量上限是方言 wire 事实：MySQL 裸 BLOB 超过 65 535 octets 必须拒绝并给出诊断，
    // PostgreSQL `BYTEA` 不继承该上限，同一载荷必须原样落盘并回读。
    let too_big = "00".repeat(yyds_gateway::MYSQL_BLOB_CAPACITY + 1);
    let bound = yyds_gateway::bind_sql_catalog(
        &format!("INSERT INTO payloads VALUES (4, CAST('{too_big}' AS BYTEA))"),
        &catalog,
        7,
        SqlDialect::Postgres,
    )
    .expect("bind");
    let error =
        yyds_gateway::plan_statement_for_dialect(bound.clone(), &catalog, SqlDialect::Mysql).expect_err("mysql capacity");
    assert_eq!(error.kind, SqlErrorKind::ValueTooLong);
    assert!(error.message.contains("exceeds the declared capacity"), "{}", error.message);
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("postgres has no dialect-level bytes cap");
    yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect("postgres stores the oversized payload");
    assert_eq!(
        run_sql(&executor, None, &catalog, "SELECT LENGTH(body) FROM payloads WHERE id = 4").rows,
        vec![vec![SqlValue::Integer(65_536)]]
    );

    // 文本函数没有二进制的文本形式，显式拒绝而不是退化成 NULL。
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT LOWER(body) FROM payloads", &catalog, 7, SqlDialect::Postgres).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let error = yyds_gateway::execute_plan(&executor, None, &plan, SqlDialect::Postgres).expect_err("text function on binary");
    assert_eq!(error.kind, SqlErrorKind::TypeMismatch);
}

#[test]
fn catalog_typed_decode_keeps_unknown_fallback_for_unmapped_types() {
    // 未知类型（`decimal`）仍只能宽松推断，并且解码入口必须显式走 Unknown。
    let decoded = yyds_gateway::SqlValue::from_row_bytes_typed(b"007", &yyds_gateway::SqlFieldType::Unknown);
    assert_eq!(decoded, yyds_gateway::SqlValue::Integer(7));
    let typed = yyds_gateway::SqlValue::from_row_bytes_typed(b"007", &yyds_gateway::SqlFieldType::String);
    assert_eq!(typed, yyds_gateway::SqlValue::String("007".into()));
}

// --- 整数精度 ---

#[test]
fn sql_int64_extremes_survive_round_trip_ordering_and_aggregates() {
    use yyds_gateway::SqlValue;

    let catalog = catalog("table users { @@id: i64, age: i64 }\n");
    let values = [i64::MIN, -1, 0, 9_007_199_254_740_993, i64::MAX];
    // 负数行无法经 `INSERT ... VALUES` 表达（写值只接受简单字面量），直接落行等价。
    let rows: Vec<(Vec<u8>, Vec<(u64, Vec<u8>)>)> = values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            (
                (index + 1).to_string().into_bytes(),
                vec![(1, (index + 1).to_string().into_bytes()), (2, value.to_string().into_bytes())],
            )
        })
        .collect();
    let refs: Vec<(&[u8], Vec<(u64, &[u8])>)> = rows
        .iter()
        .map(|(rowid, fields)| (rowid.as_slice(), fields.iter().map(|(id, bytes)| (*id, bytes.as_slice())).collect()))
        .collect();
    let refs: Vec<(&[u8], &[(u64, &[u8])])> = refs.iter().map(|(id, fields)| (*id, fields.as_slice())).collect();
    let executor = make_executor_with_rows("table:1", &refs);

    // 主键与普通列都必须原样回读，`9223372036854775807` 不能被浮点舍入。
    assert_eq!(
        run_sql(&executor, None, &catalog, "SELECT age FROM users WHERE id = 5").rows,
        vec![vec![SqlValue::Integer(i64::MAX)]]
    );
    assert_eq!(
        run_sql(&executor, None, &catalog, "SELECT age FROM users WHERE age = -9223372036854775808").rows,
        vec![vec![SqlValue::Integer(i64::MIN)]]
    );

    // ORDER BY 使用整数比较，超出 2^53 的相邻值不能并列。
    let ordered = run_sql(&executor, None, &catalog, "SELECT age FROM users ORDER BY age");
    assert_eq!(ordered.rows, values.into_iter().map(|value| vec![SqlValue::Integer(value)]).collect::<Vec<_>>());

    // MAX / MIN 保留精确值，SUM 在 i128 内累加后再落回 i64。
    assert_eq!(run_sql(&executor, None, &catalog, "SELECT MAX(age) FROM users").rows, vec![vec![SqlValue::Integer(i64::MAX)]]);
    assert_eq!(run_sql(&executor, None, &catalog, "SELECT MIN(age) FROM users").rows, vec![vec![SqlValue::Integer(i64::MIN)]]);
    let expected_sum: i64 = values.iter().fold(0i128, |sum, value| sum + *value as i128) as i64;
    assert_eq!(expected_sum, 9_007_199_254_740_991);
    assert_eq!(
        run_sql(&executor, None, &catalog, "SELECT SUM(age) FROM users").rows,
        vec![vec![SqlValue::Integer(expected_sum)]]
    );
}

#[test]
fn sql_mixed_integer_float_comparisons_do_not_round_integers() {
    use yyds_gateway::SqlValue;

    let catalog = catalog("table users { @@id: i64, age: i64 }\n");
    let executor = empty_executor();
    run_sql(&executor, None, &catalog, "INSERT INTO users VALUES (1, 5)");
    run_sql(&executor, None, &catalog, "INSERT INTO users VALUES (2, 9007199254740992)");
    run_sql(&executor, None, &catalog, "INSERT INTO users VALUES (3, 9007199254740993)");

    let ids =
        |sql: &str| run_sql(&executor, None, &catalog, sql).rows.into_iter().map(|row| row[0].clone()).collect::<Vec<_>>();

    // 旧实现把 `9007199254740993` 先转成 `f64`（等于 ...992.0），大小比较会判成相等。
    assert_eq!(ids("SELECT id FROM users WHERE age > 9007199254740992.0"), vec![SqlValue::Integer(3)]);
    assert_eq!(ids("SELECT id FROM users WHERE age = 9007199254740992.0"), vec![SqlValue::Integer(2)]);
    // oak 保留字面量文本，但 `SqlValue::Number` 是 `f64`，`...993.0` 已经舍入成
    // `...992.0`，因此只能命中 `age = 9007199254740992` 的行。
    assert_eq!(ids("SELECT id FROM users WHERE age = 9007199254740993.0"), vec![SqlValue::Integer(2)]);
    assert_eq!(ids("SELECT id FROM users WHERE 9007199254740992.0 < age"), vec![SqlValue::Integer(3)]);
    // 小数部分仍参与精确比较：2^53 附近的 `2.5` 无法在 f64 中表示，改用小数值验证。
    assert_eq!(ids("SELECT id FROM users WHERE age < 5.5"), vec![SqlValue::Integer(1)]);
    assert_eq!(ids("SELECT id FROM users WHERE age > 4.5").len(), 3);

    // `i64` 上下界与 `f64` 的 `2^63` 边界比较必须严格。
    assert!(ids("SELECT id FROM users WHERE age >= 9223372036854775808.0").is_empty());
    assert_eq!(ids("SELECT id FROM users WHERE age < 9223372036854775808.0").len(), 3);
    assert_eq!(ids("SELECT id FROM users WHERE -9223372036854775808.0 <= age").len(), 3);
}
