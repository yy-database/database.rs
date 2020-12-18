//! Result column metadata derived from an executable SQL plan.
//!
//! Protocol adapters use this to answer statement-level `Describe` without
//! executing the statement and without inferring types from row values.

use std::collections::BTreeMap;

use crate::{
    SqlCatalog, SqlCatalogBoundExpression,
    catalog_bind::{SqlCatalogBoundInsert, SqlFieldType},
    sql::{SqlBoundBinaryOperator, SqlBoundLiteral, SqlBoundUnaryOperator, SqlDialect},
    sql_planner::{JoinPlan, ReadPlan, ReturningProjections, ScanPlan, SqlExecutionPlan, plan_returning},
};

/// One described result column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlColumn {
    /// Column label as reported to the client.
    pub name: String,
    /// Declared gateway field type.
    pub field_type: SqlFieldType,
    /// Field identity when the column projects a stored field directly.
    ///
    /// Protocol adapters use it to look up the declared DDL type, which is more
    /// precise than the projected type: a `DATE` column is `String` in the
    /// gateway but must be reported as a date on the wire.
    pub field_id: Option<u64>,
}

/// Describes the result shape of a planned statement. Writes have no shape.
///
/// Uses PostgreSQL as the result-type dialect; callers that need dialect-exact
/// metadata call [`describe_plan_for_dialect`].
pub fn describe_plan(plan: &SqlExecutionPlan) -> Vec<SqlColumn> {
    describe_plan_for_dialect(plan, SqlDialect::Postgres)
}

/// 描述计划结果形状，按目标方言决定精确结果类型。
///
/// MySQL 对精确操作数的 `SUM`/`AVG` 返回 `DECIMAL`，驱动把它当字符串读回；
/// PostgreSQL 的同类聚合仍是整数或浮点，因此类型推断必须带方言。
pub fn describe_plan_for_dialect(plan: &SqlExecutionPlan, dialect: SqlDialect) -> Vec<SqlColumn> {
    match plan {
        SqlExecutionPlan::Read(plan) => describe_projections(plan, dialect),
        SqlExecutionPlan::Scan(plan) => describe_projections(plan, dialect),
        SqlExecutionPlan::Join(plan) => describe_projections(plan, dialect),
        // 集合运算的结果形状由最左操作数定义：其余操作数已按列数与类型对齐校验过。
        SqlExecutionPlan::SetOperation(plan) => {
            plan.operands.first().map(|operand| describe_plan_for_dialect(operand, dialect)).unwrap_or_default()
        }
        SqlExecutionPlan::Scalar(plan) => plan
            .projections
            .iter()
            .map(|(expression, label)| SqlColumn {
                name: label.clone(),
                field_type: infer_expression_type(expression, &BTreeMap::new(), dialect),
                field_id: projected_field_id(expression),
            })
            .collect(),
        SqlExecutionPlan::Insert(plan) => describe_returning(&plan.returning, &plan.field_types, dialect),
        SqlExecutionPlan::MultiInsert(plans) => {
            plans.first().map(|plan| describe_returning(&plan.returning, &plan.field_types, dialect)).unwrap_or_default()
        }
        SqlExecutionPlan::InsertSelect(plan) => describe_returning(&plan.returning, &plan.field_types, dialect),
        SqlExecutionPlan::Update(_) | SqlExecutionPlan::UpdateScan(_) | SqlExecutionPlan::Delete(_) | SqlExecutionPlan::DeleteScan(_) => {
            Vec::new()
        }
    }
}

/// Describes `INSERT ... RETURNING` from the bound statement alone.
///
/// Write values are only validated at execute time, when the real parameters
/// exist. A metadata describe has no values, so planning it with stand-in
/// literals rejects valid schemas: `0` cannot be written to text columns and
/// `NULL` cannot be a primary key. Column types come from the catalog instead.
pub fn describe_bound_insert(insert: &SqlCatalogBoundInsert, catalog: &SqlCatalog, dialect: SqlDialect) -> Vec<SqlColumn> {
    let Some(table) = catalog.tables().iter().find(|table| table.type_id == insert.table.type_id)
    else {
        return Vec::new();
    };
    let Ok(returning) = plan_returning(insert.returning.clone(), table, dialect)
    else {
        return Vec::new();
    };
    let field_types: BTreeMap<u64, SqlFieldType> = table.fields.iter().map(|field| (field.field_id, field.field_type.clone())).collect();
    describe_returning(&returning, &field_types, dialect)
}

/// `INSERT ... RETURNING` 的结果形状：写语句只有带 `RETURNING` 时才有列定义。
fn describe_returning(
    returning: &Option<ReturningProjections>,
    field_types: &BTreeMap<u64, SqlFieldType>,
    dialect: SqlDialect,
) -> Vec<SqlColumn> {
    let Some(projections) = returning
    else {
        return Vec::new();
    };
    projections
        .iter()
        .map(|(expression, label)| SqlColumn {
            name: label.clone(),
            field_type: infer_expression_type(expression, field_types, dialect),
            field_id: projected_field_id(expression),
        })
        .collect()
}

/// Field identity of a direct column projection, which carries the declared
/// DDL type used for wire metadata.
fn projected_field_id(expression: &SqlCatalogBoundExpression) -> Option<u64> {
    match expression {
        SqlCatalogBoundExpression::Column(column) => Some(column.field_id),
        _ => None,
    }
}

trait ProjectedPlan {
    fn projection_fields(&self) -> &[(u64, String)];
    fn projection_expressions(&self) -> &[(SqlCatalogBoundExpression, String)];
    fn field_types(&self) -> &BTreeMap<u64, SqlFieldType>;
}

impl ProjectedPlan for ReadPlan {
    fn projection_fields(&self) -> &[(u64, String)] {
        &self.projection_fields
    }

    fn projection_expressions(&self) -> &[(SqlCatalogBoundExpression, String)] {
        &self.projection_expressions
    }

    fn field_types(&self) -> &BTreeMap<u64, SqlFieldType> {
        &self.field_types
    }
}

impl ProjectedPlan for ScanPlan {
    fn projection_fields(&self) -> &[(u64, String)] {
        &self.projection_fields
    }

    fn projection_expressions(&self) -> &[(SqlCatalogBoundExpression, String)] {
        &self.projection_expressions
    }

    fn field_types(&self) -> &BTreeMap<u64, SqlFieldType> {
        &self.field_types
    }
}

impl ProjectedPlan for JoinPlan {
    fn projection_fields(&self) -> &[(u64, String)] {
        &self.projection_fields
    }

    fn projection_expressions(&self) -> &[(SqlCatalogBoundExpression, String)] {
        &self.projection_expressions
    }

    fn field_types(&self) -> &BTreeMap<u64, SqlFieldType> {
        &self.field_types
    }
}

/// Execution emits direct field projections before expression projections, so
/// the description follows the same order.
fn describe_projections(plan: &impl ProjectedPlan, dialect: SqlDialect) -> Vec<SqlColumn> {
    let mut columns = Vec::with_capacity(plan.projection_fields().len() + plan.projection_expressions().len());
    for (field_id, label) in plan.projection_fields() {
        let field_type = plan.field_types().get(field_id).cloned().unwrap_or(SqlFieldType::Unknown).inner();
        columns.push(SqlColumn { name: label.clone(), field_type, field_id: Some(*field_id) });
    }
    for (expression, label) in plan.projection_expressions() {
        columns.push(SqlColumn {
            name: label.clone(),
            field_type: infer_expression_type(expression, plan.field_types(), dialect),
            field_id: projected_field_id(expression),
        });
    }
    columns
}

/// Infers the declared type of an expression. The result only needs to be
/// accurate enough to pick a wire type — execution remains the source of truth.
fn infer_expression_type(
    expression: &SqlCatalogBoundExpression,
    field_types: &BTreeMap<u64, SqlFieldType>,
    dialect: SqlDialect,
) -> SqlFieldType {
    use SqlCatalogBoundExpression::*;
    match expression {
        Column(column) => field_types.get(&column.row_id).cloned().unwrap_or(SqlFieldType::Unknown).inner(),
        // 外层列的类型随绑定一起带过来，计划里没有它的行值映射。
        OuterColumn(column) => column.field_type.inner(),
        Literal(literal) => match literal {
            SqlBoundLiteral::Integer(_) => SqlFieldType::Int64,
            SqlBoundLiteral::Number(_) => SqlFieldType::Float64,
            SqlBoundLiteral::String(_) => SqlFieldType::String,
            SqlBoundLiteral::Bytes(_) => SqlFieldType::Bytes,
            SqlBoundLiteral::Boolean(_) => SqlFieldType::Bool,
            SqlBoundLiteral::Null => SqlFieldType::Unknown,
        },
        Wildcard => SqlFieldType::Unknown,
        Function { name, args, .. } => match name.as_str() {
            // `LENGTH`、字符计数与位置函数都返回整数；其余字符串函数退回 `String`。
            "count" | "length" | "char_length" | "character_length" | "len" | "instr" | "strpos" => SqlFieldType::Int64,
            // 排名函数与 PostgreSQL `int8` / MySQL `LONGLONG` 对应。
            "row_number" | "rank" | "dense_rank" => SqlFieldType::Int64,
            "avg" => {
                if matches!(dialect, SqlDialect::Mysql) {
                    SqlFieldType::Decimal
                }
                else {
                    SqlFieldType::Float64
                }
            }
            "sum" => {
                let argument = args.first().map(|arg| infer_expression_type(arg, field_types, dialect)).unwrap_or(SqlFieldType::Unknown);
                // MySQL 的 `SUM` 只在精确操作数上返回 `DECIMAL`，浮点求和仍是 `DOUBLE`。
                if matches!(dialect, SqlDialect::Mysql)
                    && matches!(argument.clone().inner(), SqlFieldType::Int32 | SqlFieldType::Int64 | SqlFieldType::Bool)
                {
                    SqlFieldType::Decimal
                }
                else {
                    argument
                }
            }
            "max" | "min" | "abs" | "round" => args.first().map(|arg| infer_expression_type(arg, field_types, dialect)).unwrap_or(SqlFieldType::Unknown),
            // `COALESCE`/`IFNULL` 的声明类型是所有实际分支的公共类型，NULL 字面量不参与推断。
            "coalesce" | "ifnull" | "nvl" => args
                .iter()
                .map(|arg| infer_expression_type(arg, field_types, dialect))
                .find(|field_type| !matches!(field_type, SqlFieldType::Unknown))
                .unwrap_or(SqlFieldType::Unknown),
            _ => SqlFieldType::String,
        },
        Binary { left, operator, right } => match operator {
            SqlBoundBinaryOperator::And
            | SqlBoundBinaryOperator::Or
            | SqlBoundBinaryOperator::Equal
            | SqlBoundBinaryOperator::NotEqual
            | SqlBoundBinaryOperator::Less
            | SqlBoundBinaryOperator::Greater
            | SqlBoundBinaryOperator::LessEqual
            | SqlBoundBinaryOperator::GreaterEqual
            | SqlBoundBinaryOperator::Like => SqlFieldType::Bool,
            SqlBoundBinaryOperator::Divide | SqlBoundBinaryOperator::Modulo => SqlFieldType::Float64,
            SqlBoundBinaryOperator::Plus | SqlBoundBinaryOperator::Minus | SqlBoundBinaryOperator::Multiply => merge_numeric(
                infer_expression_type(left, field_types, dialect),
                infer_expression_type(right, field_types, dialect),
            ),
        },
        Unary { operator, expression } => match operator {
            SqlBoundUnaryOperator::Not
            | SqlBoundUnaryOperator::IsNull
            | SqlBoundUnaryOperator::IsNotNull
            | SqlBoundUnaryOperator::IsTruth { .. } => SqlFieldType::Bool,
            SqlBoundUnaryOperator::Plus | SqlBoundUnaryOperator::Minus => infer_expression_type(expression, field_types, dialect),
        },
        InSubquery { .. } | InValues { .. } | Exists { .. } => SqlFieldType::Bool,
        // 标量子查询的声明类型就是它唯一输出列的类型，绑定期已保证只有一列。
        ScalarSubquery { plan } => {
            describe_plan_for_dialect(plan, dialect).first().map(|column| column.field_type.clone()).unwrap_or(SqlFieldType::Unknown)
        }
        Case { branches, else_branch } => {
            // 取第一个 THEN 分支的类型作为推断结果；更精确的类型合并留给执行期。
            branches
                .first()
                .map(|(_, then)| infer_expression_type(then, field_types, dialect))
                .or_else(|| else_branch.as_ref().map(|e| infer_expression_type(e, field_types, dialect)))
                .unwrap_or(SqlFieldType::Unknown)
        }
        Cast { target, .. } => cast_target_to_field_type(target),
    }
}

fn merge_numeric(left: SqlFieldType, right: SqlFieldType) -> SqlFieldType {
    use SqlFieldType::*;
    match (left.inner(), right.inner()) {
        (Float64, _) | (_, Float64) => Float64,
        (Decimal, _) | (_, Decimal) => Decimal,
        (Int64, _) | (_, Int64) => Int64,
        (Int32, _) | (_, Int32) => Int32,
        (String, _) | (_, String) => String,
        _ => Unknown,
    }
}

fn cast_target_to_field_type(target: &str) -> SqlFieldType {
    let t = target.trim().to_ascii_uppercase();
    let base_type = t.split('(').next().unwrap_or(&t);
    if crate::sql::is_binary_type_name(base_type) {
        return SqlFieldType::Bytes;
    }
    match base_type {
        "INTEGER" | "INT" | "INT4" | "BIGINT" | "SMALLINT" | "SIGNED" | "UNSIGNED" => SqlFieldType::Int64,
        "REAL" | "DOUBLE" | "FLOAT" | "NUMBER" => SqlFieldType::Float64,
        "TEXT" | "STRING" | "VARCHAR" | "CHAR" | "CHARACTER" => SqlFieldType::String,
        _ => SqlFieldType::Unknown,
    }
}
