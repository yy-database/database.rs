//! 集群 KV 执行器之上的 SQL 网关端到端回归。
//!
//! 钉死两条合同：
//! 1. 整数主键的范围条件（`> >= < <= BETWEEN`）按数值语义过滤，绝不因 rowid 的
//!    十进制 ASCII 字节序而丢行或多行（历史缺陷：`id >= 50` 曾返回 55 行）。
//! 2. `UPDATE ... SET` 右值可以是行表达式（算术、列引用、标量函数），逐行求值。
//!
//! 执行面用 `FileShard`：多行 `INSERT` 与多行 `UPDATE/DELETE` 的原子提交路径要求
//! store 提供 durable 原子提交边界，`MemoryShard` 对该边界返回 `Unsupported`。

use std::{collections::HashMap, fs, path::PathBuf};

use database_cluster::topology::ShardMap;
use database_execution::{cluster::LocalExecutor, sql_exec::execute_plan};
use database_query::{
    SqlCatalogBoundStatement, SqlResult, SqlSchemaRegistry, SqlValue, bind_sql_catalog, parse_ddl, plan_statement_for_dialect,
};
use database_query::sql::{SqlDialect, SqlFrontendError};
use database_storage::cluster_kv::FileShard;
use database_types::cluster::{ShardEpoch, ShardId};

struct Gateway {
    registry: SqlSchemaRegistry,
    executor: LocalExecutor<FileShard>,
    directory: PathBuf,
}

impl Gateway {
    fn new(name: &str) -> Self {
        let directory = std::env::temp_dir().join(format!("database-execution-sql-gateway-{name}-{}", std::process::id()));
        let _ = fs::remove_file(directory.join("local-0.yykv"));
        fs::create_dir_all(&directory).unwrap();
        let shard = ShardId("local-0".into());
        let executor = LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
            HashMap::from([(shard, FileShard::open(directory.join("local-0.yykv")).unwrap())]),
            Vec::new(),
        )
        .unwrap();
        Self { registry: SqlSchemaRegistry::new_in_memory(), executor, directory }
    }

    /// 执行一条 SQL：DDL 走 schema registry，其余走 bind → plan → execute。
    fn sql(&self, text: &str) -> Result<SqlResult, SqlFrontendError> {
        if let Some(statement) = parse_ddl(text, SqlDialect::Mysql)? {
            self.registry.apply_ddl(&statement)?;
            return Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 0 });
        }
        let catalog = self.registry.catalog();
        let bound = bind_sql_catalog(text, &catalog, catalog.schema_version(), SqlDialect::Mysql)?;
        let plan = plan_statement_for_dialect(bound, &catalog, SqlDialect::Mysql)?;
        execute_plan(&self.executor, None, &plan, SqlDialect::Mysql)
    }

    fn expect_sql(&self, text: &str) -> SqlResult {
        self.sql(text).unwrap_or_else(|error| panic!("`{text}` failed: {}", error.message))
    }

    /// 单列单行查询的整数值，用于 `COUNT(*)` 与点查断言。
    fn scalar_integer(&self, text: &str) -> i64 {
        let result = self.expect_sql(text);
        assert_eq!(result.rows.len(), 1, "`{text}` must return exactly one row");
        match &result.rows[0][0] {
            SqlValue::Integer(value) => *value,
            other => panic!("`{text}` expected an integer, got {other:?}"),
        }
    }

    fn scalar_text(&self, text: &str) -> String {
        let result = self.expect_sql(text);
        assert_eq!(result.rows.len(), 1, "`{text}` must return exactly one row");
        match &result.rows[0][0] {
            SqlValue::String(value) => value.clone(),
            other => panic!("`{text}` expected a string, got {other:?}"),
        }
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.directory.join("local-0.yykv"));
        let _ = fs::remove_dir(&self.directory);
    }
}

/// 建一张 id 为 1..=100 的整数主键表。
fn gateway_with_hundred_rows(name: &str) -> Gateway {
    let gateway = Gateway::new(name);
    gateway.expect_sql("CREATE TABLE nums (id INT NOT NULL PRIMARY KEY, v INT)");
    let values: Vec<String> = (1..=100).map(|id| format!("({id}, {id})")).collect();
    gateway.expect_sql(&format!("INSERT INTO nums (id, v) VALUES {}", values.join(", ")));
    gateway
}

/// 整数主键的范围条件必须按数值序过滤：字节序下推曾把 `"55" > "49"` 当成 `55 >= 50`，
/// 把 `"6" > "46"` 当成 `6 > 46`。每个期望值都是真库数值语义的行数。
#[test]
fn integer_primary_key_ranges_filter_by_numeric_order() {
    let gateway = gateway_with_hundred_rows("pk-range");
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM nums WHERE id > 6"), 94);
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM nums WHERE id >= 50"), 51);
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM nums WHERE id BETWEEN 6 AND 46"), 41);
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM nums WHERE id BETWEEN 2 AND 10"), 9);
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM nums WHERE id < 10"), 9);
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM nums WHERE id <= 10"), 10);
}

/// 等值主键仍走单点读；范围条件命中多行时走扫描 + 行级过滤，两者行数一致。
#[test]
fn equality_point_read_and_range_scan_agree() {
    let gateway = gateway_with_hundred_rows("pk-equality");
    assert_eq!(gateway.scalar_integer("SELECT v FROM nums WHERE id = 42"), 42);
    assert_eq!(gateway.scalar_integer("SELECT v FROM nums WHERE id = 100"), 100);
    // 扫描路径逐行返回的行数与数值谓词一致（旧字节边界会漏掉一位数 id）。
    let result = gateway.expect_sql("SELECT id FROM nums WHERE id < 10");
    assert_eq!(result.rows.len(), 9);
    let result = gateway.expect_sql("SELECT id FROM nums WHERE id >= 50");
    assert_eq!(result.rows.len(), 51);
}

/// UPDATE / DELETE 移除字节边界下推后仍按行级谓词命中：范围条件影响真实数值行。
#[test]
fn update_and_delete_range_hits_numeric_rows() {
    let gateway = gateway_with_hundred_rows("pk-dml");
    // `v = -1` 是哨兵值：插入数据 v = 1..100，命中行数只可能来自谓词本身。
    assert_eq!(gateway.expect_sql("UPDATE nums SET v = -1 WHERE id BETWEEN 2 AND 10").affected_rows, 9);
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM nums WHERE v = -1"), 9);
    assert_eq!(gateway.expect_sql("DELETE FROM nums WHERE id >= 50").affected_rows, 51);
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM nums"), 49);
    assert_eq!(gateway.expect_sql("DELETE FROM nums WHERE id < 10").affected_rows, 9);
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM nums"), 40);
}

/// 字符串主键不受本次修复影响：等值点读与前缀 LIKE 扫描都保持原语义。
#[test]
fn string_primary_key_equality_and_like_prefix_still_work() {
    let gateway = Gateway::new("string-pk");
    gateway.expect_sql("CREATE TABLE words (name TEXT NOT NULL PRIMARY KEY, v INT)");
    gateway.expect_sql("INSERT INTO words (name, v) VALUES ('apple', 1), ('apricot', 2), ('banana', 3)");
    assert_eq!(gateway.scalar_integer("SELECT v FROM words WHERE name = 'apple'"), 1);
    let result = gateway.expect_sql("SELECT name FROM words WHERE name LIKE 'ap%'");
    assert_eq!(result.rows.len(), 2);
    let result = gateway.expect_sql("SELECT name FROM words WHERE name LIKE 'a%e'");
    assert_eq!(result.rows.len(), 1);
}

/// `UPDATE ... SET` 右值支持算术表达式：点读路径与扫描路径都按当前行值求值。
#[test]
fn update_set_arithmetic_evaluates_per_row() {
    let gateway = Gateway::new("update-arith");
    gateway.expect_sql("CREATE TABLE inv (id INT NOT NULL PRIMARY KEY, stock INT, price INT, name TEXT)");
    gateway.expect_sql("INSERT INTO inv (id, stock, price, name) VALUES (1, 10, 2, 'a'), (2, 5, 3, 'b')");

    // 点读路径：`SET stock = stock - 1 WHERE id = 1`。
    assert_eq!(gateway.expect_sql("UPDATE inv SET stock = stock - 1 WHERE id = 1").affected_rows, 1);
    assert_eq!(gateway.scalar_integer("SELECT stock FROM inv WHERE id = 1"), 9);

    // 扫描路径：算术含乘法优先级 `SET stock = stock + price * 2`。
    assert_eq!(gateway.expect_sql("UPDATE inv SET stock = stock + price * 2 WHERE id > 0").affected_rows, 2);
    assert_eq!(gateway.scalar_integer("SELECT stock FROM inv WHERE id = 1"), 13);
    assert_eq!(gateway.scalar_integer("SELECT stock FROM inv WHERE id = 2"), 11);

    // 列引用与字符串拼接函数：`SET name = (CONCAT(name, 'x'))`。
    // 括号是必需的：钉住的 oak-sql 解析器把 SET 右值的裸函数调用报成
    // "Missing value in assignment"，括号形式才产出 Expression 节点。
    assert_eq!(gateway.expect_sql("UPDATE inv SET name = (CONCAT(name, 'x')) WHERE id = 1").affected_rows, 1);
    assert_eq!(gateway.scalar_text("SELECT name FROM inv WHERE id = 1"), "ax");

    // 取负一元表达式。
    assert_eq!(gateway.expect_sql("UPDATE inv SET stock = -stock WHERE id = 2").affected_rows, 1);
    assert_eq!(gateway.scalar_integer("SELECT stock FROM inv WHERE id = 2"), -11);
}

/// 多条赋值共享同时赋值语义：右值全部基于改写前的行，`SET a = b, b = a` 完成交换。
#[test]
fn update_set_multiple_assignments_use_pre_image() {
    let gateway = Gateway::new("update-swap");
    gateway.expect_sql("CREATE TABLE pair (id INT NOT NULL PRIMARY KEY, a INT, b INT)");
    gateway.expect_sql("INSERT INTO pair (id, a, b) VALUES (1, 3, 7)");
    assert_eq!(gateway.expect_sql("UPDATE pair SET a = b, b = a WHERE id = 1").affected_rows, 1);
    assert_eq!(gateway.scalar_integer("SELECT a FROM pair WHERE id = 1"), 7);
    assert_eq!(gateway.scalar_integer("SELECT b FROM pair WHERE id = 1"), 3);
}

/// 表达式求值结果仍走同一类型守卫：把文本列写进整数列报 `TypeMismatch`，
/// 纯字面量的计划期校验保持不变。
#[test]
fn update_set_expression_keeps_type_guards() {
    let gateway = Gateway::new("update-guard");
    gateway.expect_sql("CREATE TABLE guard (id INT NOT NULL PRIMARY KEY, n INT, t TEXT)");
    gateway.expect_sql("INSERT INTO guard (id, n, t) VALUES (1, 2, 'x')");
    let error = gateway.sql("UPDATE guard SET n = t WHERE id = 1").expect_err("string must not be written to an INT column");
    assert_eq!(error.kind, database_query::SqlErrorKind::TypeMismatch);
    let error = gateway.sql("UPDATE guard SET n = 'abc' WHERE id = 1").expect_err("non-numeric literal must fail at plan time");
    assert_eq!(error.kind, database_query::SqlErrorKind::TypeMismatch);
    // NOT NULL 列写入 NULL 仍被拒绝：纯字面量在计划期报，表达式在求值后报同一守卫。
    gateway.expect_sql("CREATE TABLE notnull (id INT NOT NULL PRIMARY KEY, n INT NOT NULL)");
    gateway.expect_sql("INSERT INTO notnull (id, n) VALUES (1, 1)");
    let error = gateway.sql("UPDATE notnull SET n = NULL WHERE id = 1").expect_err("NULL into NOT NULL must fail");
    assert_eq!(error.kind, database_query::SqlErrorKind::NotNullViolation);
    let error = gateway.sql("UPDATE notnull SET n = n * 0 / 0 WHERE id = 1").expect_err("MySQL 0/0 is NULL, the NOT NULL guard must fire");
    assert_eq!(error.kind, database_query::SqlErrorKind::NotNullViolation);
    // 谓词不命中任何行：0 影响行，不报错。
    assert_eq!(gateway.expect_sql("UPDATE notnull SET n = n + 1 WHERE id = 999").affected_rows, 0);
}

/// `INSERT` 的 VALUES 仍保持 literal-only 合同（本任务不放开 INSERT）。
#[test]
fn insert_values_still_reject_row_expressions() {
    let gateway = Gateway::new("insert-literal");
    gateway.expect_sql("CREATE TABLE frozen (id INT NOT NULL PRIMARY KEY, n INT)");
    let error = gateway.sql("INSERT INTO frozen (id, n) VALUES (1, 2 + 1)").expect_err("INSERT VALUES must stay literal-only");
    assert!(error.message.contains("literal"), "unexpected error: {}", error.message);
}

/// 聚合遇主键等值必须退回扫描：单点读路径不承载聚合求值。
/// 历史缺陷：`SELECT COUNT(*) ... WHERE id = 等值` 走点读后聚合报 internal error。
#[test]
fn aggregate_over_primary_key_equality_falls_back_to_scan() {
    let gateway = gateway_with_hundred_rows("agg-pk-eq");
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM nums WHERE id = 42"), 1);
    assert_eq!(gateway.scalar_integer("SELECT MAX(v) FROM nums WHERE id = 7"), 7);
    // 等值不命中任何行：聚合仍按扫描语义返回空集口径（COUNT=0）。
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM nums WHERE id = 999"), 0);
}

/// `IN (值列表)` 必须命中列表里的每一个值，且大列表不能因为逐值展开的 `OR`
/// 链过深而耗尽线程栈。历史缺陷：折叠成等值集合时丢掉了第一个字面量。
#[test]
fn in_list_matches_every_listed_value() {
    let gateway = gateway_with_hundred_rows("in-list");
    let ids = gateway.expect_sql("SELECT id FROM nums WHERE id IN (1, 3) ORDER BY id");
    assert_eq!(ids.rows, vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(3)]]);
    // 含 NULL 的列表：命中返回 TRUE，未命中返回 NULL（COUNT 不计），`NOT IN` 同口径。
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM nums WHERE id IN (1, NULL)"), 1);
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM nums WHERE id NOT IN (1, NULL)"), 0);
    // 500 个值：覆盖旧实现在这个规模上栈溢出的形态，结果按表内 100 行计。
    let values: Vec<String> = (1..=500).map(|id| id.to_string()).collect();
    let result = gateway.expect_sql(&format!("SELECT COUNT(*) FROM nums WHERE id IN ({})", values.join(", ")));
    assert_eq!(result.rows[0][0], SqlValue::Integer(100));
}

/// 等值连接只产出匹配组合，基表谓词剪枝不改变结果集。
#[test]
fn equality_join_matches_every_paired_row() {
    let gateway = Gateway::new("join-equality");
    gateway.expect_sql("CREATE TABLE lside (id INT NOT NULL PRIMARY KEY, grp INT)");
    gateway.expect_sql("CREATE TABLE rside (id INT NOT NULL PRIMARY KEY, name TEXT)");
    gateway.expect_sql("INSERT INTO lside (id, grp) VALUES (1, 0), (2, 0), (3, 1)");
    gateway.expect_sql("INSERT INTO rside (id, name) VALUES (1, 'a'), (2, 'b'), (4, 'd')");
    // 基表谓词 `grp = 0` 先剪枝，连接只应产出 2 条匹配。
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM lside l JOIN rside r ON r.id = l.id WHERE l.grp = 0"), 2);
    let names = gateway.expect_sql("SELECT r.name FROM lside l JOIN rside r ON r.id = l.id ORDER BY r.id");
    assert_eq!(names.rows, vec![vec![SqlValue::String("a".into())], vec![SqlValue::String("b".into())]]);
    // LEFT JOIN：未匹配的左侧行保留，共 3 行。
    assert_eq!(gateway.scalar_integer("SELECT COUNT(*) FROM lside l LEFT JOIN rside r ON r.id = l.id"), 3);
}

/// 绑定层形状冒烟：确认 bind 产出的 Update 语句进入计划器而不是被更早的层拒绝。
#[test]
fn update_statement_binds_to_update_variant() {
    let gateway = Gateway::new("bind-shape");
    gateway.expect_sql("CREATE TABLE shape (id INT NOT NULL PRIMARY KEY, n INT)");
    let catalog = gateway.registry.catalog();
    let bound = bind_sql_catalog("UPDATE shape SET n = n + 1 WHERE id = 1", &catalog, catalog.schema_version(), SqlDialect::Mysql).expect("bind");
    assert!(matches!(bound, SqlCatalogBoundStatement::Update(_)));
}
