//! 语句提交边界验收：多行 UPDATE / DELETE 的自动提交必须在一次 durable 批次内发布。
//!
//! SQL 行为本身由 `sql_conformance.rs` 覆盖，这里只观察 gateway 调用了哪条执行器
//! 边界：命中行必须整批进入 `commit_batch`，不能退化成逐行 `execute`。

use std::sync::{Arc, Mutex};

use yyds_execution::{BufferedCommit, CommitCondition, KeyValueExecutor};
use yyds_gateway::{
    RowFieldValues, SqlCatalog, SqlDialect, SqlErrorKind, SqlExecutionPlan, bind_sql_catalog, encode_row, execute_plan,
    plan_statement_for_dialect,
};
use yyds_gateway_conformance::{SCHEMA_VERSION, USERS_SCHEMA};
use yyds_types::{Error, FenceToken, KeyValueAction, KeyValueCommand, KeyValueResult, Namespace};

/// 注入到提交边界的失败类型，让测试走结构化错误而不是字符串匹配。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitFailure {
    CasConflict,
    Storage,
}

/// 可观察的 `KeyValueExecutor` 替身。
///
/// 扫描内容由测试预设，`commit_batch` 收到的批次与逐行 `execute` 命令都记录下来，
/// 因此可以直接断言一条语句是否只经过一次 durable 提交边界。
#[derive(Debug)]
struct RecordingExecutor {
    scanned: Vec<(Vec<u8>, Vec<u8>)>,
    commits: Mutex<Vec<Vec<BufferedCommit>>>,
    writes: Mutex<Vec<KeyValueCommand>>,
    failure: Option<CommitFailure>,
}

impl RecordingExecutor {
    fn new(scanned: Vec<(Vec<u8>, Vec<u8>)>, failure: Option<CommitFailure>) -> Arc<Self> {
        Arc::new(Self { scanned, commits: Mutex::new(Vec::new()), writes: Mutex::new(Vec::new()), failure })
    }

    fn commits(&self) -> Vec<Vec<BufferedCommit>> {
        self.commits.lock().expect("probe commit log").clone()
    }

    fn writes(&self) -> Vec<KeyValueCommand> {
        self.writes.lock().expect("probe write log").clone()
    }
}

impl KeyValueExecutor for RecordingExecutor {
    fn execute(&self, _fence: Option<&FenceToken>, command: KeyValueCommand) -> yyds_types::Result<KeyValueResult> {
        let result = match &command.action {
            KeyValueAction::Get => KeyValueResult::Get(None),
            KeyValueAction::Delete => KeyValueResult::Delete { removed: true },
            _ => KeyValueResult::Put { revision: 1 },
        };
        self.writes.lock().expect("probe write log").push(command);
        Ok(result)
    }

    fn execute_scan(
        &self,
        _fence: Option<&FenceToken>,
        _namespace: &Namespace,
        _prefix: &[u8],
        _after: Option<&[u8]>,
        _before: Option<&[u8]>,
        _limit: usize,
    ) -> yyds_types::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self.scanned.clone())
    }

    fn commit_batch(&self, _fence: Option<&FenceToken>, batch: Vec<BufferedCommit>) -> yyds_types::Result<()> {
        let failure = self.failure;
        self.commits.lock().expect("probe commit log").push(batch);
        match failure {
            None => Ok(()),
            Some(CommitFailure::CasConflict) => Err(Error::CasConflict { key: "probe".into() }),
            Some(CommitFailure::Storage) => Err(Error::Corrupt("injected commit failure")),
        }
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

fn plan(catalog: &SqlCatalog, sql: &str) -> SqlExecutionPlan {
    let bound = bind_sql_catalog(sql, catalog, SCHEMA_VERSION, SqlDialect::Postgres)
        .unwrap_or_else(|error| panic!("`{sql}` must bind: {}", error.message));
    plan_statement_for_dialect(bound, catalog, SqlDialect::Postgres).unwrap_or_else(|error| panic!("`{sql}` must plan: {}", error.message))
}

/// 预设三行扫描结果，每行带上 `age` 字段，供谓词求值使用。
fn scanned_rows(catalog: &SqlCatalog) -> Vec<(Vec<u8>, Vec<u8>)> {
    let age_field = catalog.tables()[0].fields.iter().find(|field| field.name == "age").expect("users.age column").field_id;
    [30i64, 20, 40]
        .iter()
        .enumerate()
        .map(|(index, age)| {
            let mut row = RowFieldValues::new();
            row.insert(age_field, age.to_string().into_bytes());
            (vec![index as u8 + 1], encode_row(&row))
        })
        .collect()
}

#[test]
fn sql_multi_row_update_publishes_one_durable_batch() {
    let catalog = catalog(USERS_SCHEMA);
    let scanned = scanned_rows(&catalog);
    let executor = RecordingExecutor::new(scanned.clone(), None);
    let result =
        execute_plan(executor.as_ref(), None, &plan(&catalog, "UPDATE users SET age = 7"), SqlDialect::Postgres).expect("multi-row update");

    assert_eq!(result.affected_rows, scanned.len() as u64);
    assert!(executor.writes().is_empty(), "多行 UPDATE 不得逐行 execute");
    let commits = executor.commits();
    assert_eq!(commits.len(), 1, "自动提交的多行 UPDATE 只能有一个提交边界");
    let batch = &commits[0];
    assert_eq!(batch.len(), scanned.len(), "所有命中行都必须在同一批次里");
    for (entry, (key, pre_image)) in batch.iter().zip(&scanned) {
        assert_eq!(entry.command.key, *key);
        assert!(matches!(entry.command.action, KeyValueAction::Put(_)));
        assert_eq!(entry.condition, Some(CommitCondition::ValueEquals(Some(pre_image.clone()))));
    }
}

#[test]
fn sql_multi_row_delete_publishes_one_durable_batch() {
    let catalog = catalog(USERS_SCHEMA);
    let scanned = scanned_rows(&catalog);
    let executor = RecordingExecutor::new(scanned.clone(), None);
    let result = execute_plan(executor.as_ref(), None, &plan(&catalog, "DELETE FROM users"), SqlDialect::Postgres).expect("multi-row delete");

    assert_eq!(result.affected_rows, scanned.len() as u64);
    assert!(executor.writes().is_empty(), "多行 DELETE 不得逐行 execute");
    let commits = executor.commits();
    assert_eq!(commits.len(), 1, "自动提交的多行 DELETE 只能有一个提交边界");
    let batch = &commits[0];
    assert_eq!(batch.len(), scanned.len(), "所有命中行都必须在同一批次里");
    for (entry, (key, pre_image)) in batch.iter().zip(&scanned) {
        assert_eq!(entry.command.key, *key);
        assert!(matches!(entry.command.action, KeyValueAction::Delete));
        assert_eq!(entry.condition, Some(CommitCondition::ValueEquals(Some(pre_image.clone()))));
    }
}

#[test]
fn sql_multi_row_update_without_matches_skips_the_commit_boundary() {
    let catalog = catalog(USERS_SCHEMA);
    let executor = RecordingExecutor::new(scanned_rows(&catalog), None);
    let result = execute_plan(executor.as_ref(), None, &plan(&catalog, "UPDATE users SET age = 7 WHERE age > 100"), SqlDialect::Postgres)
        .expect("update without matches");

    assert_eq!(result.affected_rows, 0);
    assert!(executor.commits().is_empty(), "空变更集不得触碰提交边界");
    assert!(executor.writes().is_empty());
}

/// 提交失败时必须整体失败：没有逐行写入留下半提交状态，错误按类别向上传播。
#[test]
fn sql_multi_row_write_propagates_commit_failure_without_row_writes() {
    let statements = ["UPDATE users SET age = 7", "DELETE FROM users"];
    let cases = [(CommitFailure::CasConflict, SqlErrorKind::Conflict), (CommitFailure::Storage, SqlErrorKind::Internal)];
    let catalog = catalog(USERS_SCHEMA);
    for sql in statements {
        for (failure, expected_kind) in cases {
            let executor = RecordingExecutor::new(scanned_rows(&catalog), Some(failure));
            let error = execute_plan(executor.as_ref(), None, &plan(&catalog, sql), SqlDialect::Postgres)
                .expect_err(&format!("`{sql}` must fail when the commit fails ({failure:?})"));
            assert_eq!(error.kind, expected_kind, "`{sql}` under {failure:?}: {}", error.message);
            assert!(executor.writes().is_empty(), "`{sql}` 提交失败后不得逐行写入");
            assert_eq!(executor.commits().len(), 1, "`{sql}` 只尝试一次提交边界，不做静默重试");
        }
    }
}
