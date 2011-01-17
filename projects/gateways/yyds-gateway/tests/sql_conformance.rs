//! SQL 行为验收集。
//!
//! 每个用例都在独立 fixture 上执行：准备数据 → 执行语句 → 核对返回行、影响行数
//! 与执行后的数据库状态。用例数据由 `yyds-gateway-conformance` 提供，同一份表也
//! 驱动真实 MySQL / PostgreSQL 客户端套件。已知上游 `oaks` 缺口以 `pending` 标注并
//! 跳过，绝不伪造通过。

use std::sync::Arc;

use yyds_execution::{KeyValueExecutor, LocalExecutor, LocalTransactionHandle, TransactionHandle};
use yyds_gateway::{
    SqlCatalog, SqlDialect, SqlErrorKind, SqlParameterStyle, SqlResult, SqlValue, bind_sql_catalog, execute_plan, plan_statement_for_dialect,
    substitute_sql_parameters,
};
use yyds_gateway_conformance::{
    Case, DEFAULT_STATE_COLUMNS, Driver, Lit, REJECTED_SQL, SCHEMA_VERSION, SEED, USERS_SCHEMA, aggregate_empty_cases, crud_cases,
    distinct_cases, group_cases, nested_call_cases, null_order_cases, numeric_cases, order_cases, returning_cases, rows, seed_state,
    set_operation_cases, string_function_cases, type_cases, upsert_cases, window_cases,
};
use yyds_kv::FileShard;

struct Fixture {
    catalog: SqlCatalog,
    executor: Arc<dyn KeyValueExecutor>,
    dialect: SqlDialect,
    /// SQL 事务 commit 要求 durable atomic commit 边界，fixture 挂载 `FileShard`
    /// 并持有其临时目录直到测试结束。
    _directory: boost_fs::temp::TempFolder,
}

impl Fixture {
    fn new() -> Self {
        Self::with_dialect(SqlDialect::Postgres)
    }

    fn with_dialect(dialect: SqlDialect) -> Self {
        let directory = boost_fs::temp::create_folder().expect("temp folder");
        let catalog = catalog(USERS_SCHEMA);
        let shard_id = yyds_types::ShardId("s1".into());
        let routing = yyds_types::ShardMap::new(yyds_types::ShardEpoch(1), vec![shard_id.clone()]).expect("routing");
        let mut shards = std::collections::HashMap::new();
        shards.insert(shard_id, FileShard::open(directory.path().join("s1.yykv")).expect("open file shard"));
        let executor = LocalExecutor::new(routing, shards, Vec::new()).expect("executor");
        Self { catalog, executor: Arc::new(executor), dialect, _directory: directory }
    }

    fn execute(&self, transaction: Option<&dyn TransactionHandle>, sql: &str) -> Result<SqlResult, String> {
        let bound = bind_sql_catalog(sql, &self.catalog, SCHEMA_VERSION, self.dialect).map_err(|error| error.message)?;
        let plan = plan_statement_for_dialect(bound, &self.catalog, self.dialect).map_err(|error| error.message)?;
        execute_plan(self.executor.as_ref(), transaction, &plan, self.dialect).map_err(|error| error.message)
    }

    fn execute_with(&self, sql: &str, parameters: &[Lit], style: SqlParameterStyle) -> Result<SqlResult, String> {
        let values = parameters.iter().copied().map(Lit::value).collect::<Vec<_>>();
        let sql = substitute_sql_parameters(sql, &values, style).map_err(|error| error.message)?;
        self.execute(None, &sql)
    }

    fn snapshot(&self, columns: &str) -> Vec<Vec<SqlValue>> {
        self.execute(None, &format!("SELECT {columns} FROM users ORDER BY id")).expect("state query").rows
    }
}

fn catalog(source: &str) -> SqlCatalog {
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
    SqlCatalog::from_resolved_contract(SCHEMA_VERSION, &contract).expect("gateway catalog")
}

fn run_cases(cases: &[Case]) {
    let mut pending = Vec::new();
    for case in cases {
        if let Some(reason) = case.pending {
            pending.push(format!("{}: {reason}", case.name));
            continue;
        }
        for (dialect, driver) in [(SqlDialect::Postgres, Driver::Postgres), (SqlDialect::Mysql, Driver::Mysql)] {
            if case.only.is_some_and(|only| only != driver) {
                continue;
            }
            let fixture = Fixture::with_dialect(dialect);
            for sql in case.setup {
                fixture.execute(None, sql).unwrap_or_else(|error| panic!("{} setup `{sql}`: {error}", case.name));
            }
            let result = fixture
                .execute_with(case.sql, case.parameters, case.style)
                .unwrap_or_else(|error| panic!("{} `{}` under {driver:?}: {error}", case.name, case.sql));
            assert_eq!(result.rows, rows(case.expected_rows_for(driver)), "{} returned rows for `{}` under {driver:?}", case.name, case.sql);
            assert_eq!(result.affected_rows, case.expected_affected_rows, "{} affected rows for `{}` under {driver:?}", case.name, case.sql);
            assert_eq!(
                fixture.snapshot(case.state_columns),
                rows(case.expected_state),
                "{} database state after `{}` under {driver:?}",
                case.name,
                case.sql
            );
        }
    }
    if pending.is_empty() {
        eprintln!("pending `oaks` / protocol gaps: none");
    }
    else {
        eprintln!("pending `oaks` / protocol gaps:");
        for item in pending {
            eprintln!("  - {item}");
        }
    }
}

#[test]
fn sql_conformance_crud_and_predicates() {
    run_cases(&crud_cases());
}

#[test]
fn sql_conformance_types_aggregates_and_paging() {
    run_cases(&type_cases());
}

#[test]
fn sql_conformance_distinct() {
    run_cases(&distinct_cases());
}

#[test]
fn sql_conformance_group_by_and_having() {
    run_cases(&group_cases());
}

/// 空输入下的聚合：没有 `GROUP BY` 时 SQL 仍返回一行，`COUNT` 为 `0`，
/// `SUM` / `AVG` / `MIN` / `MAX` 为 `NULL`。带 `GROUP BY` 的空输入是零行。
/// 空表与「有行但被 `WHERE` 过滤成空」必须给出同一结果。
#[test]
fn sql_conformance_aggregates_over_empty_input() {
    run_cases(&aggregate_empty_cases());
}

/// 窗口函数：分区 + 排序的排名、默认 frame 的累计/整分区聚合、显式 `ROWS` / `RANGE`
/// frame 与方言 `NULL` 位置。排名函数和聚合窗口都在执行层真实计算，不返回近似值。
#[test]
fn sql_conformance_windows() {
    run_cases(&window_cases());
}

#[test]
fn sql_conformance_order_by() {
    run_cases(&order_cases());
}

#[test]
fn sql_conformance_null_ordering() {
    run_cases(&null_order_cases());
}

#[test]
fn sql_conformance_set_operations() {
    run_cases(&set_operation_cases());
}

#[test]
fn sql_conformance_string_functions() {
    run_cases(&string_function_cases());
}

/// 标量函数实参可以是另一个标量调用：投影、谓词、三层嵌套与窗口聚合实参
/// 都按真实自变量绑定，外层函数拿到内层结果而不是零参数。
#[test]
fn sql_conformance_nested_calls() {
    run_cases(&nested_call_cases());
}

#[test]
fn sql_conformance_numeric_semantics() {
    run_cases(&numeric_cases());
}

/// 精确整数运算越界时两方言都必须报 SQLSTATE `22003`：PostgreSQL 没有厂商错误号，
/// MySQL 用 `1690`。这里断言结构化分类，具体 wire 码由适配器套件覆盖。
#[test]
fn sql_integer_overflow_reports_numeric_out_of_range() {
    let overflow_sql = [
        "SELECT 9223372036854775807 + 1",
        "SELECT -9223372036854775807 - 2",
        "SELECT 9223372036854775807 * 2",
        "SELECT -(-9223372036854775808)",
        "SELECT ABS(-9223372036854775808)",
    ];
    for dialect in [SqlDialect::Postgres, SqlDialect::Mysql] {
        let fixture = Fixture::with_dialect(dialect);
        for sql in overflow_sql {
            let bound = bind_sql_catalog(sql, &fixture.catalog, SCHEMA_VERSION, fixture.dialect)
                .unwrap_or_else(|error| panic!("`{sql}` must bind: {}", error.message));
            let plan = plan_statement_for_dialect(bound, &fixture.catalog, dialect)
                .unwrap_or_else(|error| panic!("`{sql}` must plan: {}", error.message));
            let error = execute_plan(fixture.executor.as_ref(), None, &plan, dialect).expect_err(sql);
            assert_eq!(error.kind, SqlErrorKind::NumericOverflow, "`{sql}` under {dialect:?}: {}", error.message);
            assert!(error.message.contains("overflow"), "`{sql}` under {dialect:?}: {}", error.message);
        }
    }
}

/// `DO UPDATE SET` 的数值表达式继承方言规则：PostgreSQL 的除零报 `DivisionByZero`、
/// 精确整数溢出报 `NumericOverflow`，失败的赋值不落库。MySQL 没有 `ON CONFLICT`，
/// 同一语句按语法错误拒绝，除零与溢出的方言分叉由共享的表达式用例覆盖。
#[test]
fn sql_upsert_arithmetic_keeps_dialect_numeric_rules() {
    // 结构化错误需要未经 `String` 降级的前端结果，因此这里直接走 bind/plan/execute。
    let raw = |fixture: &Fixture, sql: &str, dialect: SqlDialect| {
        let bound = bind_sql_catalog(sql, &fixture.catalog, SCHEMA_VERSION, fixture.dialect)?;
        let plan = plan_statement_for_dialect(bound, &fixture.catalog, dialect)?;
        execute_plan(fixture.executor.as_ref(), None, &plan, dialect)
    };

    let pg = Fixture::with_dialect(SqlDialect::Postgres);
    for sql in SEED {
        pg.execute(None, sql).expect("seed");
    }

    let division = raw(
        &pg,
        "INSERT INTO users VALUES (1, 'zoe', 99, 1.5, TRUE, 'x') ON CONFLICT (id) DO UPDATE SET age = users.age / 0",
        SqlDialect::Postgres,
    )
    .expect_err("PostgreSQL raises on division by zero");
    assert_eq!(division.kind, SqlErrorKind::DivisionByZero, "{}", division.message);

    let overflow = raw(
        &pg,
        "INSERT INTO users VALUES (1, 'zoe', 99, 1.5, TRUE, 'x') \
         ON CONFLICT (id) DO UPDATE SET age = users.age * 9223372036854775807",
        SqlDialect::Postgres,
    )
    .expect_err("exact integer multiplication overflows");
    assert_eq!(overflow.kind, SqlErrorKind::NumericOverflow, "{}", overflow.message);

    // 赋值求值失败时既有行保持原值，不会写回半个结果。
    assert_eq!(pg.snapshot(DEFAULT_STATE_COLUMNS), rows(seed_state()));

    let mysql = Fixture::with_dialect(SqlDialect::Mysql);
    for sql in SEED {
        mysql.execute(None, sql).expect("seed");
    }
    let rejected = raw(
        &mysql,
        "INSERT INTO users VALUES (1, 'zoe', 99, 1.5, TRUE, 'x') ON CONFLICT (id) DO UPDATE SET age = users.age / 0",
        SqlDialect::Mysql,
    )
    .expect_err("MySQL has no ON CONFLICT clause");
    assert_eq!(rejected.kind, SqlErrorKind::Syntax, "{}", rejected.message);
    assert_eq!(mysql.snapshot(DEFAULT_STATE_COLUMNS), rows(seed_state()));
}

/// 字符串函数在两方言上共享的部分：1-based 下标、字符计数与 NULL 传播。
#[test]
fn sql_string_functions_agree_across_dialects() {
    use Lit::{Int, Null, Text};
    for dialect in [SqlDialect::Postgres, SqlDialect::Mysql] {
        let fixture = Fixture::with_dialect(dialect);
        let vals = |sql: &str| fixture.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}` under {dialect:?}: {error}")).rows;
        assert_eq!(vals("SELECT SUBSTRING('abcdef', 2, 3)"), rows(&[&[Text("bcd")]]), "under {dialect:?}");
        assert_eq!(vals("SELECT SUBSTR('abcdef', 4)"), rows(&[&[Text("def")]]), "under {dialect:?}");
        assert_eq!(vals("SELECT SUBSTRING('abcdef', 7)"), rows(&[&[Text("")]]), "under {dialect:?}");
        assert_eq!(vals("SELECT SUBSTRING('abcdef', 3, 0)"), rows(&[&[Text("")]]), "under {dialect:?}");
        assert_eq!(vals("SELECT SUBSTRING(NULL, 2, 3)"), rows(&[&[Null]]), "under {dialect:?}");
        assert_eq!(vals("SELECT SUBSTRING('abcdef', NULL)"), rows(&[&[Null]]), "under {dialect:?}");
        assert_eq!(vals("SELECT CHAR_LENGTH('abc'), CHARACTER_LENGTH('abc'), LEN('abc')"), rows(&[&[Int(3), Int(3), Int(3)]]));
        assert_eq!(vals("SELECT TRIM('  pad  ')"), rows(&[&[Text("pad")]]), "under {dialect:?}");
        assert_eq!(vals("SELECT LTRIM('  pad'), RTRIM('pad  ')"), rows(&[&[Text("pad"), Text("pad")]]));
        assert_eq!(vals("SELECT TRIM(NULL), LTRIM(NULL), RTRIM(NULL)"), rows(&[&[Null, Null, Null]]));
        assert_eq!(vals("SELECT REPLACE('banana', 'na', 'NA')"), rows(&[&[Text("baNANA")]]));
        assert_eq!(vals("SELECT REPLACE('abc', NULL, 'x')"), rows(&[&[Null]]));
        assert_eq!(vals("SELECT CONCAT('a', 'b', 1)"), rows(&[&[Text("ab1")]]), "under {dialect:?}");
    }
}

/// `LENGTH` 的字节/字符、`CONCAT` 的 NULL 传播、`SUBSTRING` 的越界裁剪与两参数
/// `TRIM` 都按方言分叉，两方言各走自己的断言，不能合并成一个期望值。
#[test]
fn sql_string_functions_split_by_dialect() {
    use Lit::{Int, Null, Text};

    let mysql = Fixture::with_dialect(SqlDialect::Mysql);
    let mysql_vals = |sql: &str| mysql.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}` under MySQL: {error}")).rows;
    let pg = Fixture::with_dialect(SqlDialect::Postgres);
    let pg_vals = |sql: &str| pg.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}` under Postgres: {error}")).rows;

    // MySQL 的 `LENGTH` 是 UTF-8 字节数，PostgreSQL 的 `length` 是字符数。
    assert_eq!(mysql_vals("SELECT LENGTH('héllo'), CHAR_LENGTH('héllo')"), rows(&[&[Int(6), Int(5)]]));
    assert_eq!(pg_vals("SELECT LENGTH('héllo'), CHAR_LENGTH('héllo')"), rows(&[&[Int(5), Int(5)]]));

    // MySQL 的 `CONCAT` 传播 NULL，PostgreSQL 的 `concat` 跳过 NULL 参数。
    assert_eq!(mysql_vals("SELECT CONCAT('a', NULL, 'b')"), rows(&[&[Null]]));
    assert_eq!(pg_vals("SELECT CONCAT('a', NULL, 'b')"), rows(&[&[Text("ab")]]));

    // 起点 0：MySQL 返回空串，PostgreSQL 从第 1 个字符开始。
    assert_eq!(mysql_vals("SELECT SUBSTRING('abcdef', 0)"), rows(&[&[Text("")]]));
    assert_eq!(pg_vals("SELECT SUBSTRING('abcdef', 0)"), rows(&[&[Text("abcdef")]]));
    assert_eq!(mysql_vals("SELECT SUBSTRING('abcdef', 0, 3)"), rows(&[&[Text("")]]));
    assert_eq!(pg_vals("SELECT SUBSTRING('abcdef', 0, 3)"), rows(&[&[Text("ab")]]));

    // 负起点：MySQL 从结尾倒数，PostgreSQL 从第 1 个字符开始。
    assert_eq!(mysql_vals("SELECT SUBSTRING('abcdef', -2)"), rows(&[&[Text("ef")]]));
    assert_eq!(pg_vals("SELECT SUBSTRING('abcdef', -2)"), rows(&[&[Text("abcdef")]]));

    // 负长度：MySQL 返回空串，PostgreSQL 报 `22011`。
    assert_eq!(mysql_vals("SELECT SUBSTRING('abcdef', 2, -1)"), rows(&[&[Text("")]]));
    let rejected = pg.execute(None, "SELECT SUBSTRING('abcdef', 2, -1)").expect_err("negative substring length");
    assert!(rejected.contains("negative substring length"), "unexpected reason: {rejected}");

    // 显式 trim 字符集是 PostgreSQL 形式，MySQL 只能写成 `TRIM(chars FROM str)`。
    assert_eq!(pg_vals("SELECT TRIM('xxaxx', 'x'), LTRIM('xxa', 'x'), RTRIM('axx', 'x')"), rows(&[&[Text("a"), Text("a"), Text("a")]]));
    let mysql_trim = mysql.execute(None, "SELECT TRIM('xxaxx', 'x')").expect_err("two-argument trim under MySQL");
    assert!(mysql_trim.contains("PostgreSQL syntax"), "unexpected reason: {mysql_trim}");
}

/// 标量函数的参数个数在绑定期就拒绝，未知名字仍然报 `UnknownFunction`。
#[test]
fn sql_string_function_arity_is_rejected_while_binding() {
    let fixture = Fixture::new();
    for sql in [
        "SELECT SUBSTRING('abc')",
        "SELECT SUBSTR('abc', 1, 2, 3)",
        "SELECT REPLACE('a', 'b')",
        "SELECT CONCAT()",
        "SELECT TRIM('a', 'b', 'c')",
    ] {
        let error = bind_sql_catalog(sql, &fixture.catalog, SCHEMA_VERSION, fixture.dialect).expect_err(sql);
        assert_eq!(error.kind, SqlErrorKind::UnknownFunction, "`{sql}`: {}", error.message);
    }
}

/// 断言值为 `YYYY-MM-DD HH:MM:SS` 形状。
fn assert_timestamp_shape(value: &str) {
    assert_eq!(value.len(), 19, "{value}");
    assert_digits_with_separators(value, &[(4, '-'), (7, '-'), (10, ' '), (13, ':'), (16, ':')]);
}

/// 断言值为 `YYYY-MM-DD` 形状。
fn assert_date_shape(value: &str) {
    assert_eq!(value.len(), 10, "{value}");
    assert_digits_with_separators(value, &[(4, '-'), (7, '-')]);
}

/// 断言值为 `HH:MM:SS` 形状。
fn assert_time_shape(value: &str) {
    assert_eq!(value.len(), 8, "{value}");
    assert_digits_with_separators(value, &[(2, ':'), (5, ':')]);
}

fn assert_digits_with_separators(value: &str, separators: &[(usize, char)]) {
    for (index, character) in value.char_indices() {
        match separators.iter().find(|(separator, _)| *separator == index) {
            Some((_, expected)) => assert_eq!(character, *expected, "{value}"),
            None => assert!(character.is_ascii_digit(), "{value}"),
        }
    }
}

/// 取第一行第 `index` 列的文本值。
fn text_at(result: &SqlResult, index: usize) -> String {
    match &result.rows[0][index] {
        SqlValue::String(text) => text.clone(),
        other => panic!("expected text, got {other:?}"),
    }
}

/// `NOW` / `CURRENT_DATE` / `CURRENT_TIME` 都从主机时钟取 UTC，并统一渲染成 TEXT：
/// 时间戳 19 字符 `YYYY-MM-DD HH:MM:SS`，日期 10 字符，时间 8 字符。YYDS 没有原生
/// 日期类型，网关也不会发出 DATE / TIMESTAMP 的 wire 类型。
#[test]
fn sql_date_functions_render_utc_text_in_both_dialects() {
    for dialect in [SqlDialect::Postgres, SqlDialect::Mysql] {
        let fixture = Fixture::with_dialect(dialect);
        let values = |sql: &str| fixture.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}` under {dialect:?}: {error}"));

        assert_timestamp_shape(&text_at(&values("SELECT NOW()"), 0));
        assert_timestamp_shape(&text_at(&values("SELECT CURRENT_TIMESTAMP()"), 0));
        assert_date_shape(&text_at(&values("SELECT CURRENT_DATE()"), 0));
        assert_time_shape(&text_at(&values("SELECT CURRENT_TIME()"), 0));
        // 同一语句里两个函数各自取值，形状必须一致。
        let pair = values("SELECT CURRENT_DATE(), CURRENT_TIME()");
        assert_date_shape(&text_at(&pair, 0));
        assert_time_shape(&text_at(&pair, 1));
    }
}

/// SQL 标准把 `CURRENT_DATE` / `CURRENT_TIME` / `CURRENT_TIMESTAMP` 写成无括号关键字。
/// Oak 的树里没有对应节点，因此绑定器在列解析失败后回退成无参内建函数，同名列优先。
#[test]
fn sql_bare_date_keywords_bind_as_niladic_functions() {
    use Lit::{Int, Text};
    let fixture = Fixture::new();
    let values = |sql: &str| fixture.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}`: {error}"));

    assert_date_shape(&text_at(&values("SELECT CURRENT_DATE"), 0));
    assert_time_shape(&text_at(&values("SELECT CURRENT_TIME"), 0));
    assert_timestamp_shape(&text_at(&values("SELECT CURRENT_TIMESTAMP"), 0));

    for sql in SEED {
        fixture.execute(None, sql).expect("seed");
    }
    // 带 FROM 时同样回退：`users` 没有同名列也不会失败。
    assert_date_shape(&text_at(&values("SELECT CURRENT_DATE FROM users WHERE id = 1"), 0));
    // 回退后的值仍是普通表达式，可以作为其它函数的参数。
    assert_eq!(values("SELECT CHAR_LENGTH(CURRENT_DATE) FROM users WHERE id = 1").rows, rows(&[&[Int(10)]]));
    assert_eq!(values("SELECT SUBSTRING(CURRENT_TIMESTAMP, 5, 1) FROM users WHERE id = 1").rows, rows(&[&[Text("-")]]));

    // 回退只覆盖标准关键字：`NOW` 在两方言里都必须带括号。
    let error = fixture.execute(None, "SELECT NOW FROM users WHERE id = 1").expect_err("bare NOW");
    assert!(error.contains("unknown SQL column"), "unexpected reason: {error}");
}

/// `CURDATE()` / `CURTIME()` 是 MySQL 拼写。PostgreSQL 没有这两个函数，报 `42883`
/// 而不是把它们当成语法错误或静默接受。
#[test]
fn sql_curdate_and_curtime_are_mysql_spellings() {
    let mysql = Fixture::with_dialect(SqlDialect::Mysql);
    let values = |sql: &str| mysql.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}` under MySQL: {error}"));
    assert_date_shape(&text_at(&values("SELECT CURDATE()"), 0));
    assert_time_shape(&text_at(&values("SELECT CURTIME()"), 0));

    let pg = Fixture::with_dialect(SqlDialect::Postgres);
    for sql in ["SELECT CURDATE()", "SELECT CURTIME()"] {
        let bound = bind_sql_catalog(sql, &pg.catalog, SCHEMA_VERSION, SqlDialect::Postgres).expect("binds under PostgreSQL");
        let plan = plan_statement_for_dialect(bound, &pg.catalog, SqlDialect::Postgres).expect("plans");
        let error = execute_plan(pg.executor.as_ref(), None, &plan, SqlDialect::Postgres).expect_err("PostgreSQL rejects MySQL spelling");
        assert_eq!(error.kind, SqlErrorKind::UnknownFunction, "`{sql}`: {}", error.message);
        assert!(error.message.contains("MySQL spelling"), "`{sql}`: {}", error.message);
    }
}

/// 日期函数的参数个数在绑定期拒绝，`EXTRACT` 仍不在网关子集内。
#[test]
fn sql_date_function_gaps_are_reported_explicitly() {
    let fixture = Fixture::new();
    for sql in ["SELECT CURRENT_DATE(1)", "SELECT CURRENT_TIME('x')", "SELECT NOW(1)", "SELECT CURDATE(1)"] {
        let error = bind_sql_catalog(sql, &fixture.catalog, SCHEMA_VERSION, fixture.dialect).expect_err(sql);
        assert_eq!(error.kind, SqlErrorKind::UnknownFunction, "`{sql}`: {}", error.message);
    }

    // Oak 把 `EXTRACT(field FROM value)` 解析成普通函数调用，网关在函数白名单上拒绝，
    // 因为 YYDS 没有日期类型可供抽取，编造一个语义会比报未支持更糟。
    for sql in ["SELECT EXTRACT(YEAR FROM age) FROM users", "SELECT EXTRACT(age) FROM users"] {
        let error = bind_sql_catalog(sql, &fixture.catalog, SCHEMA_VERSION, fixture.dialect).expect_err("EXTRACT must not bind");
        assert_eq!(error.kind, SqlErrorKind::UnknownFunction, "`{sql}`: {}", error.message);
        assert!(error.message.contains("EXTRACT"), "`{sql}`: {}", error.message);
    }
}

/// `ON CONFLICT DO NOTHING` keeps the stored row and reports zero affected rows,
/// while `DO UPDATE` rewrites it and reports one. `INSERT ... RETURNING` projects
/// the rows the statement actually wrote.
#[test]
fn sql_conformance_upsert_and_returning() {
    run_cases(&upsert_cases());
    run_cases(&returning_cases());
}

#[test]
fn sql_group_by_projection_errors_are_rejected() {
    let fixture = Fixture::new();
    for sql in [
        "SELECT name, COUNT(*) FROM users GROUP BY age",
        "SELECT name FROM users GROUP BY age",
        "SELECT * FROM users GROUP BY age",
        "SELECT age FROM users HAVING COUNT(*) > 1",
        "SELECT name, COUNT(*) FROM users GROUP BY age HAVING name = 'alice'",
        "SELECT name FROM users GROUP BY age ORDER BY name",
    ] {
        let error = fixture.execute(None, sql).expect_err(sql);
        assert!(!error.is_empty(), "`{sql}` must report a reason");
    }
}

#[test]
fn sql_distinct_unsupported_shapes_are_rejected() {
    let fixture = Fixture::new();
    for sql in [
        "SELECT DISTINCT age, COUNT(*) FROM users GROUP BY age",
        "SELECT DISTINCT COUNT(*) FROM users",
        "SELECT DISTINCT age FROM users ORDER BY id",
    ] {
        let error = fixture.execute(None, sql).expect_err(sql);
        assert!(!error.is_empty(), "`{sql}` must report a reason");
    }
}

/// 共享矩阵中列出的上游形式必须在本进程路径上被显式拒绝，而不是被静默改写或
/// 解析成另一种语义。真实客户端套件对同一批语句断言协议错误码。
#[test]
fn sql_rejected_upstream_forms_report_a_reason() {
    let fixture = Fixture::new();
    for (label, sql) in REJECTED_SQL {
        assert!(fixture.execute(None, sql).is_err(), "`{label}` must be rejected explicitly");
    }
}

/// 表别名与自连接在两方言下必须一致：同一份 SQL 都要接受，同一份歧义都要按
/// `AmbiguousColumn` / `AmbiguousTable` 拒绝。wire 层再各自映射成 MySQL `1052` 与
/// PostgreSQL `42702`，网关层不因方言而分叉。
#[test]
fn sql_table_alias_and_self_join_agree_across_dialects() {
    for dialect in [SqlDialect::Postgres, SqlDialect::Mysql] {
        let fixture = Fixture::with_dialect(dialect);
        for sql in SEED {
            fixture.execute(None, sql).expect("seed");
        }

        let aliased = fixture
            .execute(None, "SELECT u.name FROM users u WHERE u.id = 1")
            .unwrap_or_else(|error| panic!("aliased read under {dialect:?}: {error}"));
        assert_eq!(aliased.rows, rows(&[&[Lit::Text("alice")]]), "alias under {dialect:?}");

        // 同一张表通过两个别名自连接，两侧各自保留自己的列值。
        let self_joined = fixture
            .execute(None, "SELECT a.id, b.name FROM users a JOIN users b ON a.id = b.id + 1 ORDER BY a.id")
            .unwrap_or_else(|error| panic!("self join under {dialect:?}: {error}"));
        assert_eq!(
            self_joined.rows,
            rows(&[&[Lit::Int(2), Lit::Text("alice")], &[Lit::Int(3), Lit::Text("bob")]]),
            "self join under {dialect:?}"
        );

        let ambiguous_column =
            bind_sql_catalog("SELECT name FROM users a JOIN users b ON a.id = b.id", &fixture.catalog, SCHEMA_VERSION, fixture.dialect)
                .expect_err("ambiguous shared column");
        assert_eq!(ambiguous_column.kind, SqlErrorKind::AmbiguousColumn, "under {dialect:?}: {}", ambiguous_column.message);

        let ambiguous_table = bind_sql_catalog(
            "SELECT users.name FROM users JOIN users ON users.id = users.id",
            &fixture.catalog,
            SCHEMA_VERSION,
            fixture.dialect,
        )
        .expect_err("duplicate unaliased source");
        assert_eq!(ambiguous_table.kind, SqlErrorKind::AmbiguousTable, "under {dialect:?}: {}", ambiguous_table.message);
    }
}

/// 派生表在两方言下必须一致：网关把 `(SELECT ...) x` 扁平化成同一张基表，内层过滤
/// 并入外层 `WHERE`，所以没有任何方言分叉；无法扁平化的形状两方言都必须显式拒绝。
#[test]
fn sql_derived_table_agrees_across_dialects() {
    for dialect in [SqlDialect::Postgres, SqlDialect::Mysql] {
        let fixture = Fixture::with_dialect(dialect);
        for sql in SEED {
            fixture.execute(None, sql).expect("seed");
        }

        let filtered = fixture
            .execute(None, "SELECT x.name FROM (SELECT * FROM users WHERE id = 2) x")
            .unwrap_or_else(|error| panic!("filtered derived table under {dialect:?}: {error}"));
        assert_eq!(filtered.rows, rows(&[&[Lit::Text("bob")]]), "derived filter under {dialect:?}");

        // 内层逐列投影覆盖整表时与 `*` 等价，列序不影响外层限定名。
        let projected = fixture
            .execute(None, "SELECT x.id, x.age FROM (SELECT name, age, id, score, active, tag FROM users) x ORDER BY x.id")
            .unwrap_or_else(|error| panic!("derived projection under {dialect:?}: {error}"));
        assert_eq!(
            projected.rows,
            rows(&[&[Lit::Int(1), Lit::Int(30)], &[Lit::Int(2), Lit::Int(20)], &[Lit::Int(3), Lit::Int(40)]]),
            "derived projection under {dialect:?}"
        );

        let rejected = fixture.execute(None, "SELECT * FROM (SELECT * FROM users LIMIT 1) x").expect_err("inner LIMIT cannot be flattened");
        assert!(rejected.contains("derived table"), "under {dialect:?}: {rejected}");
    }
}

/// `[NOT] EXISTS` 只关心子查询是否产出行，两方言共用同一物化流程：返回行必须一致，
/// 子查询内部的绑定错误也必须落到同一类别，wire 层再各自映射成对应错误码。
#[test]
fn sql_exists_predicates_agree_across_dialects() {
    for dialect in [SqlDialect::Postgres, SqlDialect::Mysql] {
        let fixture = Fixture::with_dialect(dialect);
        for sql in SEED {
            fixture.execute(None, sql).expect("seed");
        }

        let present = fixture
            .execute(None, "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM users WHERE age = 20) ORDER BY id")
            .unwrap_or_else(|error| panic!("EXISTS with rows under {dialect:?}: {error}"));
        assert_eq!(present.rows, rows(&[&[Lit::Int(1)], &[Lit::Int(2)], &[Lit::Int(3)]]), "EXISTS with rows under {dialect:?}");

        let absent = fixture
            .execute(None, "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM users WHERE age = 99)")
            .unwrap_or_else(|error| panic!("EXISTS without rows under {dialect:?}: {error}"));
        assert!(absent.rows.is_empty(), "EXISTS without rows under {dialect:?}");

        let negated_empty = fixture
            .execute(None, "SELECT id FROM users WHERE NOT EXISTS (SELECT 1 FROM users WHERE age = 99) ORDER BY id")
            .unwrap_or_else(|error| panic!("NOT EXISTS with rows under {dialect:?}: {error}"));
        assert_eq!(negated_empty.rows, rows(&[&[Lit::Int(1)], &[Lit::Int(2)], &[Lit::Int(3)]]), "NOT EXISTS with rows under {dialect:?}");

        let negated_present = fixture
            .execute(None, "SELECT id FROM users WHERE NOT EXISTS (SELECT 1 FROM users WHERE age = 20)")
            .unwrap_or_else(|error| panic!("NOT EXISTS without rows under {dialect:?}: {error}"));
        assert!(negated_present.rows.is_empty(), "NOT EXISTS without rows under {dialect:?}");

        let unknown =
            bind_sql_catalog("SELECT id FROM users WHERE EXISTS (SELECT 1 FROM missing)", &fixture.catalog, SCHEMA_VERSION, fixture.dialect)
                .expect_err("unknown inner table");
        assert_eq!(unknown.kind, SqlErrorKind::UnknownTable, "under {dialect:?}: {}", unknown.message);
    }
}

/// 标量子查询的基数语义两端一致：一行取其值，0 行取 `NULL`，多行按执行错误拒绝，
/// 多列投影在绑定期显式拒绝，两方言都不做近似。
#[test]
fn sql_scalar_subquery_agrees_across_dialects() {
    for dialect in [SqlDialect::Postgres, SqlDialect::Mysql] {
        let fixture = Fixture::with_dialect(dialect);
        for sql in SEED {
            fixture.execute(None, sql).expect("seed");
        }

        let single = fixture
            .execute(None, "SELECT id, (SELECT age FROM users WHERE id = 1) AS peer FROM users WHERE id = 2")
            .unwrap_or_else(|error| panic!("scalar subquery under {dialect:?}: {error}"));
        assert_eq!(single.rows, rows(&[&[Lit::Int(2), Lit::Int(30)]]), "scalar subquery with one row under {dialect:?}");

        let empty = fixture
            .execute(None, "SELECT (SELECT age FROM users WHERE id = 99) AS none FROM users WHERE id = 1")
            .unwrap_or_else(|error| panic!("empty scalar subquery under {dialect:?}: {error}"));
        assert_eq!(empty.rows, rows(&[&[Lit::Null]]), "scalar subquery without rows under {dialect:?}");

        let predicate = fixture
            .execute(None, "SELECT id FROM users WHERE age = (SELECT age FROM users WHERE id = 3)")
            .unwrap_or_else(|error| panic!("scalar predicate under {dialect:?}: {error}"));
        assert_eq!(predicate.rows, rows(&[&[Lit::Int(3)]]), "scalar subquery in predicate under {dialect:?}");

        let error = fixture
            .execute(None, "SELECT id FROM users WHERE id = (SELECT id FROM users)")
            .expect_err("several rows must be a cardinality error");
        assert!(error.contains("more than one row"), "scalar subquery with several rows under {dialect:?}: {error}");

        let wide = bind_sql_catalog("SELECT (SELECT id, age FROM users) FROM users", &fixture.catalog, SCHEMA_VERSION, fixture.dialect)
            .expect_err("scalar subquery must project one column");
        assert_eq!(wide.kind, SqlErrorKind::Unsupported, "under {dialect:?}: {}", wide.message);
    }
}

/// 非递归 CTE 在两方言下必须一致：网关把每条 CTE 展开成它的基表，内层过滤并入
/// 外层 `WHERE`，所以返回值不分叉；无法展开的形状两方言都必须显式拒绝。
#[test]
fn sql_common_table_expressions_agree_across_dialects() {
    for dialect in [SqlDialect::Postgres, SqlDialect::Mysql] {
        let fixture = Fixture::with_dialect(dialect);
        for sql in SEED {
            fixture.execute(None, sql).expect("seed");
        }

        let named = fixture
            .execute(None, "WITH u AS (SELECT * FROM users) SELECT u.id, u.name FROM u WHERE u.id = 2")
            .unwrap_or_else(|error| panic!("named CTE under {dialect:?}: {error}"));
        assert_eq!(named.rows, rows(&[&[Lit::Int(2), Lit::Text("bob")]]), "named CTE under {dialect:?}");

        let filtered = fixture
            .execute(None, "WITH u AS (SELECT * FROM users WHERE age > 20) SELECT u.id FROM u ORDER BY u.id")
            .unwrap_or_else(|error| panic!("filtered CTE under {dialect:?}: {error}"));
        assert_eq!(filtered.rows, rows(&[&[Lit::Int(1)], &[Lit::Int(3)]]), "filtered CTE under {dialect:?}");

        let twice = fixture
            .execute(None, "WITH u AS (SELECT * FROM users) SELECT a.id, b.name FROM u a JOIN u b ON a.id = b.id + 1 ORDER BY a.id")
            .unwrap_or_else(|error| panic!("CTE used twice under {dialect:?}: {error}"));
        assert_eq!(
            twice.rows,
            rows(&[&[Lit::Int(2), Lit::Text("alice")], &[Lit::Int(3), Lit::Text("bob")]]),
            "CTE used twice under {dialect:?}"
        );

        // `WITH` 写在集合运算前面时覆盖左右两侧，右侧引用 CTE 不能被当成未知表。
        let union = fixture
            .execute(
                None,
                "WITH u AS (SELECT * FROM users WHERE age > 20) SELECT id FROM u WHERE id = 1 \
                 UNION SELECT id FROM u WHERE id = 3 ORDER BY id",
            )
            .unwrap_or_else(|error| panic!("CTE over a set operation under {dialect:?}: {error}"));
        assert_eq!(union.rows, rows(&[&[Lit::Int(1)], &[Lit::Int(3)]]), "CTE over a set operation under {dialect:?}");

        let rejected =
            fixture.execute(None, "WITH u AS (SELECT * FROM users LIMIT 1) SELECT * FROM u").expect_err("an inner LIMIT cannot be flattened");
        assert!(rejected.contains("common table expression"), "under {dialect:?}: {rejected}");

        let unknown = bind_sql_catalog(
            "WITH u AS (SELECT * FROM users) SELECT * FROM u JOIN missing ON TRUE",
            &fixture.catalog,
            SCHEMA_VERSION,
            fixture.dialect,
        )
        .expect_err("unknown joined table");
        assert_eq!(unknown.kind, SqlErrorKind::UnknownTable, "under {dialect:?}: {}", unknown.message);
    }
}

/// 外层 `WITH` 的作用域覆盖子查询：`IN (SELECT ...)` 与 `EXISTS (...)` 内部引用
/// 同一个 CTE 时两方言都按同一份扁平化结果执行，未知名字两方言都显式拒绝。
#[test]
fn sql_cte_scope_reaches_into_subqueries_across_dialects() {
    for dialect in [SqlDialect::Postgres, SqlDialect::Mysql] {
        let fixture = Fixture::with_dialect(dialect);
        for sql in SEED {
            fixture.execute(None, sql).expect("seed");
        }

        let membership = fixture
            .execute(None, "WITH u AS (SELECT * FROM users) SELECT id FROM users WHERE id IN (SELECT id FROM u WHERE u.name = 'bob')")
            .unwrap_or_else(|error| panic!("CTE inside IN under {dialect:?}: {error}"));
        assert_eq!(membership.rows, rows(&[&[Lit::Int(2)]]), "CTE inside IN under {dialect:?}");

        let existence = fixture
            .execute(None, "WITH u AS (SELECT * FROM users) SELECT id FROM users WHERE EXISTS (SELECT 1 FROM u WHERE u.id = 3)")
            .unwrap_or_else(|error| panic!("CTE inside EXISTS under {dialect:?}: {error}"));
        assert_eq!(existence.rows, rows(&[&[Lit::Int(1)], &[Lit::Int(2)], &[Lit::Int(3)]]), "CTE inside EXISTS under {dialect:?}");

        // CTE 体的过滤在子查询里同样生效：`age > 25` 只剩 1 与 3，所以 id = 2 查不到。
        let filtered = fixture
            .execute(None, "WITH u AS (SELECT * FROM users WHERE age > 25) SELECT id FROM users WHERE EXISTS (SELECT 1 FROM u WHERE u.id = 2)")
            .unwrap_or_else(|error| panic!("filtered CTE inside EXISTS under {dialect:?}: {error}"));
        assert!(filtered.rows.is_empty(), "filtered CTE inside EXISTS under {dialect:?}");

        for sql in [
            "WITH u AS (SELECT * FROM users) SELECT id FROM users WHERE id IN (SELECT id FROM missing)",
            "WITH u AS (SELECT * FROM users) SELECT id FROM users WHERE EXISTS (SELECT 1 FROM missing)",
        ] {
            let error = bind_sql_catalog(sql, &fixture.catalog, SCHEMA_VERSION, fixture.dialect).expect_err("unknown name inside a subquery");
            assert_eq!(error.kind, SqlErrorKind::UnknownTable, "under {dialect:?} `{sql}`: {}", error.message);
        }
    }
}

/// `IS` 谓词直接由 Oak 的 `IsNull` / `IsTruth` 节点绑定，网关不再从源码 span 文本
/// 回读关键字。这里只通过绑定后的表达式求值，验证 NULL 的三值逻辑。
#[test]
fn sql_is_predicates_follow_three_valued_logic() {
    let fixture = Fixture::new();

    let is_values = |sql: &str| fixture.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}`: {error}")).rows;
    assert_eq!(
        is_values("SELECT NULL IS NULL, NULL IS NOT NULL, NULL IS TRUE, NULL IS NOT TRUE, NULL IS FALSE, NULL IS NOT FALSE"),
        rows(&[&[Lit::Bool(true), Lit::Bool(false), Lit::Bool(false), Lit::Bool(true), Lit::Bool(false), Lit::Bool(true),]])
    );
    assert_eq!(
        is_values("SELECT TRUE IS TRUE, TRUE IS NOT TRUE, FALSE IS FALSE, FALSE IS NOT FALSE"),
        rows(&[&[Lit::Bool(true), Lit::Bool(false), Lit::Bool(true), Lit::Bool(false)]])
    );

    for sql in SEED {
        fixture.execute(None, sql).expect("seed");
    }
    assert_eq!(is_values("SELECT id FROM users WHERE tag IS NULL"), rows(&[&[Lit::Int(2)]]));
    assert_eq!(is_values("SELECT id FROM users WHERE active IS TRUE ORDER BY id"), rows(&[&[Lit::Int(1)], &[Lit::Int(3)]]));
    assert_eq!(is_values("SELECT id FROM users WHERE active IS NOT TRUE"), rows(&[&[Lit::Int(2)]]));
}

/// 纯整数形式的字面量绑定为 `i64`，只在小数点或指数出现时才走浮点。
/// 超出 `i64` 的整数与无法用有限 `f64` 表示的字面量都直接报错。
#[test]
fn sql_numeric_literals_keep_integer_precision() {
    let fixture = Fixture::new();
    let values = |sql: &str| fixture.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}`: {error}")).rows;

    assert_eq!(values("SELECT 9007199254740993"), rows(&[&[Lit::Int(9_007_199_254_740_993)]]));
    assert_eq!(values("SELECT 9223372036854775807"), rows(&[&[Lit::Int(i64::MAX)]]));
    assert_eq!(values("SELECT -9223372036854775808"), rows(&[&[Lit::Int(i64::MIN)]]));
    assert_eq!(values("SELECT 1.0"), rows(&[&[Lit::Float(1.0)]]));
    assert_eq!(
        values("SELECT 9007199254740993 = 9007199254740992, 9007199254740993 - 9007199254740992"),
        rows(&[&[Lit::Bool(false), Lit::Int(1)]])
    );

    assert!(fixture.execute(None, "SELECT 9223372036854775808").is_err());
    assert!(fixture.execute(None, "SELECT 1e309").is_err());
}

#[test]
fn sql_conformance_transaction_commit_and_rollback() {
    use Lit::{Float, Int, Null, Text};

    let fixture = Fixture::new();
    for sql in SEED {
        fixture.execute(None, sql).expect("seed");
    }
    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(Arc::clone(&fixture.executor), None));

    // 事务内写入与扫描：本事务可见，事务外不可见。
    let inserted =
        fixture.execute(Some(handle.as_ref()), "INSERT INTO users VALUES (4, 'dave', 25, 0.5, FALSE, NULL)").expect("transaction insert");
    assert_eq!(inserted.affected_rows, 1);
    let visible = fixture.execute(Some(handle.as_ref()), "SELECT id, name FROM users WHERE id = 4").expect("transaction read");
    assert_eq!(visible.rows, rows(&[&[Int(4), Text("dave")]]));
    assert_eq!(fixture.execute(None, "SELECT id FROM users WHERE id = 4").expect("outside read").rows, Vec::<Vec<SqlValue>>::new());

    // 提交后状态必须持久。`COMMIT` 是 wire 层会话命令，核心层直接调用 handle。
    handle.commit().expect("commit");
    assert_eq!(
        fixture.snapshot(DEFAULT_STATE_COLUMNS),
        rows(&[
            &[Int(1), Text("alice"), Int(30), Float(1.5), Lit::Bool(true), Text("x")],
            &[Int(2), Text("bob"), Int(20), Float(2.5), Lit::Bool(false), Null],
            &[Int(3), Text("carol"), Int(40), Float(3.25), Lit::Bool(true), Text("")],
            &[Int(4), Text("dave"), Int(25), Float(0.5), Lit::Bool(false), Null],
        ])
    );

    // 回滚丢弃全部写入与更新，状态回到提交后的快照。
    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(Arc::clone(&fixture.executor), None));
    let updated = fixture.execute(Some(handle.as_ref()), "UPDATE users SET age = 99 WHERE id = 1").expect("transaction update");
    assert_eq!(updated.affected_rows, 1);
    let deleted = fixture.execute(Some(handle.as_ref()), "DELETE FROM users WHERE id = 2").expect("transaction delete");
    assert_eq!(deleted.affected_rows, 1);
    let rolled_back = fixture.execute(Some(handle.as_ref()), "SELECT COUNT(*) FROM users").expect("transaction state");
    assert_eq!(rolled_back.rows, rows(&[&[Int(3)]]));
    handle.rollback();
    assert_eq!(
        fixture.snapshot(DEFAULT_STATE_COLUMNS),
        rows(&[
            &[Int(1), Text("alice"), Int(30), Float(1.5), Lit::Bool(true), Text("x")],
            &[Int(2), Text("bob"), Int(20), Float(2.5), Lit::Bool(false), Null],
            &[Int(3), Text("carol"), Int(40), Float(3.25), Lit::Bool(true), Text("")],
            &[Int(4), Text("dave"), Int(25), Float(0.5), Lit::Bool(false), Null],
        ])
    );
}

#[test]
fn sql_multi_row_insert_values_are_split_and_executed_together() {
    use Lit::{Int, Text};

    let fixture = Fixture::new();
    let result = fixture
        .execute(
            None,
            "INSERT INTO users VALUES (101, 'ana', 31, 1.0, TRUE, 'x'), (102, 'bob2', 32, 2.0, FALSE, NULL), (103, 'carol2', 33, 3.0, TRUE, '')",
        )
        .expect("multi-row insert");
    assert_eq!(result.affected_rows, 3, "multi-row insert must affect 3 rows");

    let count = fixture.execute(None, "SELECT COUNT(*) FROM users").expect("count after insert");
    assert_eq!(count.rows, rows(&[&[Int(3)]]), "three rows must be visible");

    let all = fixture.execute(None, "SELECT id, name FROM users ORDER BY id").expect("ordered rows");
    assert_eq!(all.rows, rows(&[&[Int(101), Text("ana")], &[Int(102), Text("bob2")], &[Int(103), Text("carol2")],]));
}

/// MySQL `/ 0` 与 `% 0` 返回 NULL 值（wire 层另上报 1365 + 22012）。
/// PostgreSQL `/ 0` 与 `% 0` 抛 DivisionByZero 错误。
#[test]
fn sql_division_by_zero_is_dialect_aware() {
    use Lit::Null;

    let mysql = Fixture::with_dialect(SqlDialect::Mysql);
    let mysql_vals = |sql: &str| mysql.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}` under MySQL: {error}")).rows;
    assert_eq!(mysql_vals("SELECT 1/0"), rows(&[&[Null]]));
    assert_eq!(mysql_vals("SELECT 0/0"), rows(&[&[Null]]));
    assert_eq!(mysql_vals("SELECT 10 % 0"), rows(&[&[Null]]));
    assert_eq!(mysql_vals("SELECT 3.14 / 0"), rows(&[&[Null]]));
    assert_eq!(mysql_vals("SELECT 3.14 % 0"), rows(&[&[Null]]));

    let pg = Fixture::with_dialect(SqlDialect::Postgres);
    for sql in ["SELECT 1/0", "SELECT 0/0", "SELECT 10 % 0", "SELECT 3.14 / 0", "SELECT 3.14 % 0"] {
        let result = pg.execute(None, sql).expect_err(sql);
        assert!(result.contains("division by zero"), "`{sql}` under PostgreSQL must surface the division-by-zero reason got: {result}");
    }
}

/// 普通除法与模运算走方言正确分支：MySQL 整数除法返回浮点，PostgreSQL 整数除法截断。
#[test]
fn sql_division_and_modulo_keep_dialect_defaults() {
    use Lit::{Float, Int};

    let mysql = Fixture::with_dialect(SqlDialect::Mysql);
    let mysql_vals = |sql: &str| mysql.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}` under MySQL: {error}")).rows;
    assert_eq!(mysql_vals("SELECT 7 / 2"), rows(&[&[Float(3.5)]]));
    assert_eq!(mysql_vals("SELECT 7 % 2"), rows(&[&[Int(1)]]));
    assert_eq!(mysql_vals("SELECT -7 % 2"), rows(&[&[Int(-1)]]));
    assert_eq!(mysql_vals("SELECT 7.0 / 2"), rows(&[&[Float(3.5)]]));

    let pg = Fixture::with_dialect(SqlDialect::Postgres);
    let pg_vals = |sql: &str| pg.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}` under Postgres: {error}")).rows;
    assert_eq!(pg_vals("SELECT 7 / 2"), rows(&[&[Int(3)]]));
    assert_eq!(pg_vals("SELECT 7 % 2"), rows(&[&[Int(1)]]));
    assert_eq!(pg_vals("SELECT -7 % 2"), rows(&[&[Int(-1)]]));
    assert_eq!(pg_vals("SELECT 7.0 / 2"), rows(&[&[Float(3.5)]]));
}

#[test]
fn sql_conformance_case_when() {
    use Lit::{Float, Int, Null, Text};
    let fixture = Fixture::new();
    let vals = |sql: &str| fixture.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}`: {error}")).rows;

    // 单 WHEN 命中
    assert_eq!(vals("SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END"), rows(&[&[Text("yes")]]));

    // 单 WHEN 不命中走 ELSE
    assert_eq!(vals("SELECT CASE WHEN 1 = 2 THEN 'yes' ELSE 'no' END"), rows(&[&[Text("no")]]));

    // 多 WHEN 取第一个命中
    assert_eq!(vals("SELECT CASE WHEN 1 = 2 THEN 'a' WHEN 1 = 1 THEN 'b' ELSE 'c' END"), rows(&[&[Text("b")]]));

    // 无 ELSE：全部落空返回 NULL
    assert_eq!(vals("SELECT CASE WHEN 1 = 2 THEN 'hit' END"), rows(&[&[Null]]));

    // 条件为 NULL 继续下一分支
    assert_eq!(vals("SELECT CASE WHEN NULL THEN 'a' WHEN 1 = 1 THEN 'b' END"), rows(&[&[Text("b")]]));

    // THEN 分支求值为 NULL 也是合法返回
    assert_eq!(vals("SELECT CASE WHEN 1 = 1 THEN NULL ELSE 'no' END"), rows(&[&[Null]]));

    // 与数字运算组合
    assert_eq!(vals("SELECT CASE WHEN 1 = 1 THEN 10 ELSE 20 END + 5"), rows(&[&[Int(15)]]));
}

#[test]
fn sql_conformance_cast() {
    use Lit::{Float, Int, Null, Text};
    let fixture = Fixture::new();
    let vals = |sql: &str| fixture.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}`: {error}")).rows;

    // INTEGER → TEXT
    assert_eq!(vals("SELECT CAST(42 AS TEXT)"), rows(&[&[Text("42")]]));

    // TEXT → INTEGER
    assert_eq!(vals("SELECT CAST('99' AS INTEGER)"), rows(&[&[Int(99)]]));

    // REAL → INTEGER (truncation via i64 cast)
    assert_eq!(vals("SELECT CAST(3.7 AS INTEGER)"), rows(&[&[Int(3)]]));

    // INTEGER → REAL
    assert_eq!(vals("SELECT CAST(7 AS REAL)"), rows(&[&[Float(7.0)]]));

    // TEXT → REAL
    assert_eq!(vals("SELECT CAST('3.14' AS FLOAT)"), rows(&[&[Float(3.14)]]));

    // NULL 输入始终返回 NULL
    assert_eq!(vals("SELECT CAST(NULL AS INTEGER)"), rows(&[&[Null]]));
    assert_eq!(vals("SELECT CAST(NULL AS TEXT)"), rows(&[&[Null]]));

    // 无法转换的字符串返回 NULL（与 SQLite 行为一致）
    assert_eq!(vals("SELECT CAST('oops' AS INTEGER)"), rows(&[&[Null]]));

    // 类型别名
    assert_eq!(vals("SELECT CAST(1 AS INT)"), rows(&[&[Int(1)]]));
    assert_eq!(vals("SELECT CAST('42' AS STRING)"), rows(&[&[Text("42")]]));

    // 布尔 → INTEGER
    assert_eq!(vals("SELECT CAST(TRUE AS INTEGER)"), rows(&[&[Int(1)]]));
    assert_eq!(vals("SELECT CAST(FALSE AS INT)"), rows(&[&[Int(0)]]));
}
