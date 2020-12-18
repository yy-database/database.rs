//! 针对已发布 VOS 目录的 SQL 兼容绑定。

use crate::sql::{
    SqlBoundAssignment, SqlBoundBinaryOperator, SqlBoundConflictAction, SqlBoundCte, SqlBoundDelete, SqlBoundExpression, SqlBoundInsert,
    SqlBoundInsertSource, SqlBoundJoinKind, SqlBoundLiteral, SqlBoundOrderByItem, SqlBoundOrderDirection, SqlBoundProjection, SqlBoundSelect,
    SqlBoundSetOperand, SqlBoundSetOperation, SqlBoundSetOperator, SqlBoundSource, SqlBoundStatement, SqlBoundUnaryOperator, SqlBoundUpdate,
    SqlBoundWindowFrame, SqlBoundWindowSpec, SqlDialect, SqlErrorKind, SqlFrontendError, SqlNullsOrder, SqlScalarFunction, bind_sql,
};
use vos::types::{CanonicalScalar, ResolvedCanonicalType};

/// Scalar types the gateway can project from a VOS field contract.
///
/// The gateway does not reproduce the full VOS type algebra — it keeps the
/// information needed to decode row bytes and validate SQL literals. Fields
/// whose type does not map cleanly fall back to [`SqlFieldType::Unknown`] and
/// leave interpretation to the runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlFieldType {
    /// 64-bit signed integer (`i64` / `int64`).
    Int64,
    /// 32-bit signed integer (`i32` / `int32`).
    Int32,
    /// 64-bit floating point (`f64` / `float64`).
    Float64,
    /// UTF-8 string (`utf8`).
    String,
    /// Boolean (`bool`).
    Bool,
    /// Raw opaque bytes (`bytes`).
    ///
    /// This is a precise binary type, not a loose "some opaque value" fallback:
    /// values decode to [`crate::SqlValue::Bytes`] without any UTF-8 conversion.
    /// The declared length limit is a dialect wire fact, not part of this type:
    /// see [`crate::SqlDialect::bytes_capacity`].
    Bytes,
    /// Exact decimal result type. MySQL reports `SUM`/`AVG` over exact operands
    /// as `DECIMAL`, which drivers surface as a string rather than a number.
    /// Result metadata only: stored columns never project to this type.
    Decimal,
    /// Optional wrapper — inner type + NULL. Box breaks the recursion
    Optional(Box<SqlFieldType>),
    /// Anything the gateway does not recognise stays opaque.
    Unknown,
}

impl SqlFieldType {
    /// Extract the innermost non-optional type.
    pub fn inner(&self) -> SqlFieldType {
        match self {
            SqlFieldType::Optional(inner) => inner.inner(),
            other => other.clone(),
        }
    }

    /// Whether NULL is a legal value for this field.
    pub fn is_nullable(&self) -> bool {
        matches!(self, SqlFieldType::Optional(_))
    }
}

/// Map a resolved VOS type to a gateway-facing [`SqlFieldType`].
///
/// Every VOS type is classified on purpose. Types with no SQL equivalent —
/// the `file` user type, the `vector` generic type, `u64`, `decimal`, and the
/// remaining composites — project to [`SqlFieldType::Unknown`] here and are
/// rejected with a field-naming diagnostic by [`reject_unknown_field`] on
/// every bind path; nothing silently degrades into a runtime guess.
/// 将 VOS 解析后的规范类型投影为 SQL 字段类型（无等价时为 Unknown）。
pub fn project_resolved_type(ty: &ResolvedCanonicalType) -> SqlFieldType {
    match ty {
        ResolvedCanonicalType::Scalar(scalar) => match scalar {
            CanonicalScalar::I8 | CanonicalScalar::I16 | CanonicalScalar::I32 => SqlFieldType::Int32,
            CanonicalScalar::I64 | CanonicalScalar::U8 | CanonicalScalar::U16 | CanonicalScalar::U32 => SqlFieldType::Int64,
            CanonicalScalar::F32 | CanonicalScalar::F64 => SqlFieldType::Float64,
            CanonicalScalar::Utf8 | CanonicalScalar::Utf16 => SqlFieldType::String,
            CanonicalScalar::Bool => SqlFieldType::Bool,
            CanonicalScalar::Bytes => SqlFieldType::Bytes,
            CanonicalScalar::Uuid | CanonicalScalar::Date | CanonicalScalar::Time | CanonicalScalar::DateTime => SqlFieldType::String,
            // `u64` cannot narrow into `i64` without lying about the range and
            // `decimal` has no lossless gateway numeric type. Both stay
            // `Unknown` and are rejected at bind time with the VOS type name.
            CanonicalScalar::U64 | CanonicalScalar::Decimal => SqlFieldType::Unknown,
        },
        ResolvedCanonicalType::Optional(inner) => SqlFieldType::Optional(Box::new(project_resolved_type(inner))),
        // VOS `file` is a user-declared identity with canonical path `file`.
        // Explicit branch, no wildcard: it has no SQL equivalent and is
        // rejected at bind time with the original type name.
        ResolvedCanonicalType::User { path, .. } if path.last().map(String::as_str) == Some("file") => SqlFieldType::Unknown,
        // VOS `vector<..>` is a generic type with canonical path `vector`.
        // Same explicit treatment as `file`.
        ResolvedCanonicalType::Generic { path, .. } if path.last().map(String::as_str) == Some("vector") => SqlFieldType::Unknown,
        // Remaining composites (`Reference`, `List`, other `User` / `Generic`)
        // also have no SQL equivalent: `Unknown` plus the bind-time rejection.
        _ => SqlFieldType::Unknown,
    }
}

/// Human-readable VOS type name extracted from a [`ResolvedCanonicalType`].
///
/// Used only for diagnostic messages — the caller maps the resolved type to
/// [`SqlFieldType`] via [`project_resolved_type`] and keeps the display name as
/// an opaque hint when the field falls back to `Unknown`.
fn canonical_type_display_name(ty: &ResolvedCanonicalType) -> String {
    match ty {
        ResolvedCanonicalType::Scalar(scalar) => format!("scalar({scalar:?})"),
        ResolvedCanonicalType::User { path, .. } => path.last().cloned().unwrap_or_else(|| "user".to_string()),
        ResolvedCanonicalType::Generic { path, arguments } => {
            let base = path.last().cloned().unwrap_or_else(|| "generic".to_string());
            if arguments.is_empty() { base } else { format!("{base}<{}>", arguments.len()) }
        }
        ResolvedCanonicalType::List(inner) => format!("list<{}>", canonical_type_display_name(inner)),
        ResolvedCanonicalType::Reference(inner) => canonical_type_display_name(inner),
        ResolvedCanonicalType::Optional(inner) => canonical_type_display_name(inner),
        ResolvedCanonicalType::File => "file".to_string(),
        ResolvedCanonicalType::Vector { dimension, .. } => format!("vector<{dimension}>"),
    }
}

/// A table identity exposed by the cluster catalog to a gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogTable {
    /// Stable 集群-owned type identity.
    pub type_id: u64,
    /// VOS table name used for SQL lookup.
    pub name: String,
    /// Columns in catalog order.
    pub fields: Vec<SqlCatalogField>,
    /// Secondary `UNIQUE` constraints as field identities in declaration order.
    ///
    /// Each inner vector is one constraint. Writes are rejected when they would
    /// produce a duplicate tuple, matching PostgreSQL `23505` and MySQL `1062`.
    pub unique_constraints: Vec<Vec<u64>>,
}

/// A column identity exposed by the cluster catalog to a gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogField {
    /// Stable 集群-owned field identity.
    pub field_id: u64,
    /// VOS field name used for SQL lookup.
    pub name: String,
    /// Declared field type projected from the VOS contract.
    pub field_type: SqlFieldType,
    /// Human-readable VOS type name carried for diagnostics, populated only when
    /// [`Self::field_type`] falls back to [`SqlFieldType::Unknown`].
    pub vos_type_name: Option<String>,
    /// Declared character or octet capacity from local DDL, absent for fields
    /// projected from the published VOS contract.
    pub capacity: Option<u32>,
    /// Raw `DEFAULT` literal text from local DDL, when declared.
    pub default: Option<String>,
}

/// The gateway's immutable view of one published VOS catalog revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalog {
    schema_version: u32,
    tables: Vec<SqlCatalogTable>,
}

impl SqlCatalog {
    /// Builds a gateway view from the newest contract published by 集群.
    pub fn from_catalog(catalog: &database_cluster::catalog::Catalog) -> Result<Self, SqlFrontendError> {
        let schema = catalog.schema().ok_or_else(|| internal_error("集群 catalog has no published schema"))?;
        let contract = catalog.resolved_contract().ok_or_else(|| internal_error("集群 catalog has no resolved contract"))?;
        Self::from_resolved_contract(schema.version, contract)
    }

    /// Projects gateway table identities from a validated resolved contract.
    pub fn from_resolved_contract(schema_version: u32, contract: &vos::types::ResolvedContract) -> Result<Self, SqlFrontendError> {
        if schema_version == 0 {
            return Err(internal_error("catalog schema version must be nonzero"));
        }
        let artifact = contract.to_json().map_err(|_| internal_error("resolved contract encode failed"))?;
        vos::types::ResolvedContract::from_json(&artifact)
            .map_err(|error| internal_error(&format!("invalid resolved VOS contract {}: {}", error.code, error.message)))?;
        let mut type_ids = std::collections::BTreeSet::new();
        let mut field_ids = std::collections::BTreeSet::new();
        let tables = contract
            .types
            .iter()
            .filter(|entry| entry.kind == vos::types::TypeContractKind::Table)
            .map(|entry| {
                if entry.type_id == 0 || !type_ids.insert(entry.type_id) {
                    return Err(internal_error("invalid or duplicate catalog type identity"));
                }
                let name = entry.canonical_path.last().cloned().ok_or_else(|| internal_error("catalog table has no canonical name"))?;
                let fields = entry
                    .fields
                    .iter()
                    .map(|field| {
                        if field.field_id == 0 || !field_ids.insert(field.field_id) {
                            return Err(internal_error("invalid or duplicate catalog field identity"));
                        }
                        let field_type = project_resolved_type(&field.canonical_type);
                        let vos_type_name = match field_type.inner() {
                            SqlFieldType::Unknown => Some(canonical_type_display_name(&field.canonical_type)),
                            _ => None,
                        };
                        Ok(SqlCatalogField {
                            field_id: field.field_id,
                            name: field.canonical_name.clone(),
                            field_type,
                            vos_type_name,
                            capacity: None,
                            default: None,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(SqlCatalogTable { type_id: entry.type_id, name, fields, unique_constraints: Vec::new() })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { schema_version, tables })
    }

    /// Returns the catalog schema revision used by this binding view.
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Builds a gateway view from locally registered DDL tables.
    ///
    /// The registry validates identities while applying DDL, so this constructor
    /// repeats the checks that keep binders from observing ambiguous tables.
    pub fn from_tables(schema_version: u32, tables: Vec<SqlCatalogTable>) -> Result<Self, SqlFrontendError> {
        if schema_version == 0 {
            return Err(internal_error("catalog schema version must be nonzero"));
        }
        let mut type_ids = std::collections::BTreeSet::new();
        let mut field_ids = std::collections::BTreeSet::new();
        for table in &tables {
            if table.name.is_empty() {
                return Err(internal_error("catalog table has no name"));
            }
            if table.type_id == 0 || !type_ids.insert(table.type_id) {
                return Err(internal_error("invalid or duplicate catalog type identity"));
            }
            for field in &table.fields {
                if field.name.is_empty() {
                    return Err(internal_error("catalog field has no name"));
                }
                if field.field_id == 0 || !field_ids.insert(field.field_id) {
                    return Err(internal_error("invalid or duplicate catalog field identity"));
                }
            }
            for constraint in &table.unique_constraints {
                if constraint.is_empty() {
                    return Err(internal_error("catalog unique constraint has no columns"));
                }
                for field_id in constraint {
                    if !table.fields.iter().any(|field| field.field_id == *field_id) {
                        return Err(internal_error("catalog unique constraint references an unknown field"));
                    }
                }
            }
        }
        Ok(Self::assemble(schema_version, tables))
    }

    /// Wraps already validated table identities without repeating the checks.
    pub(crate) fn assemble(schema_version: u32, tables: Vec<SqlCatalogTable>) -> Self {
        Self { schema_version, tables }
    }

    /// Returns the catalog tables in VOS declaration order.
    pub fn tables(&self) -> &[SqlCatalogTable] {
        &self.tables
    }
}

/// Stride between two occurrences of the same catalog table inside one query.
///
/// A self join repeats one catalog table, whose `field_id`s are unique across the
/// catalog. Row values are keyed by a synthesized *row id* so both occurrences can
/// carry their own copy of the same column without overwriting each other. The
/// first occurrence keeps slot `0`, which makes every row id equal to its catalog
/// `field_id` and leaves single-table behavior untouched.
pub const ROW_ID_STRIDE: u64 = 1 << 40;

/// A table reference after SQL names are bound to 集群 catalog identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundTable {
    /// Stable 集群 table identity.
    pub type_id: u64,
    /// Catalog table name.
    pub name: String,
    /// Alias that replaces [`Self::name`] in the column scope, when written.
    pub alias: Option<String>,
    /// Zero-based occurrence of this catalog table inside the query.
    pub row_slot: u64,
    /// Visible column names aligned with the catalog field order when an explicit CTE
    /// column list renamed this occurrence. `None` keeps every catalog field name.
    pub column_names: Option<Vec<String>>,
}

impl SqlCatalogBoundTable {
    /// The name columns are addressed by: the alias when present, the table name otherwise.
    pub fn visible_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }

    /// The row-value key for one catalog field of this occurrence.
    pub fn row_id(&self, field_id: u64) -> u64 {
        field_id + self.row_slot * ROW_ID_STRIDE
    }

    /// The name the column at `field_index` is addressed by in this occurrence.
    ///
    /// A CTE with an explicit column list renames its output positionally, so the
    /// rename lives beside the catalog field order and leaves the catalog name,
    /// `field_id` and row slot untouched.
    pub fn visible_column_name(&self, field_index: usize, field: &SqlCatalogField) -> String {
        self.column_names.as_ref().and_then(|names| names.get(field_index).cloned()).unwrap_or_else(|| field.name.clone())
    }
}

/// A column reference after SQL names are bound to 集群 catalog identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundColumn {
    /// Stable 集群 table identity.
    pub type_id: u64,
    /// Stable 集群 field identity.
    pub field_id: u64,
    /// Row-value key: [`Self::field_id`] shifted by the table occurrence slot.
    pub row_id: u64,
    /// Catalog table name.
    pub table_name: String,
    /// Catalog field name.
    pub field_name: String,
    /// Name the reference was written by in its column scope, which a CTE's explicit
    /// column list can rename away from [`Self::field_name`].
    pub visible_name: String,
    /// Declared character/octet capacity of the bound field, used for write guards.
    pub capacity: Option<u32>,
}

/// A column of the directly enclosing query read by a correlated subquery.
///
/// A correlated subquery is re-executed once per outer row, so the value cannot
/// come from the subquery's own row. The declared type travels with the column
/// because execution decodes the outer row without consulting the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogOuterColumn {
    /// Column identity and the row-value key inside the enclosing query's row.
    pub column: SqlCatalogBoundColumn,
    /// Declared type of the column, taken from the catalog at bind time.
    pub field_type: SqlFieldType,
}

/// SQL expression after catalog binding and before distributed planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlCatalogBoundExpression {
    /// A catalog-resolved column reference.
    Column(SqlCatalogBoundColumn),
    /// A column of the directly enclosing query, supplied per outer row.
    OuterColumn(SqlCatalogOuterColumn),
    /// A SQL literal retaining gateway semantics.
    Literal(SqlBoundLiteral),
    /// A bare `*` argument; only `COUNT(*)` gives it meaning.
    Wildcard,
    /// A SQL function call (aggregate, scalar or window function).
    Function {
        /// Canonical lower-case function name.
        name: String,
        /// Catalog-bound arguments.
        args: Vec<SqlCatalogBoundExpression>,
        /// Whether the aggregate uses `DISTINCT` quantifier.
        distinct: bool,
        /// `OVER (...)` clause when the call is a window function.
        window: Option<SqlCatalogBoundWindowSpec>,
    },
    /// A binary SQL expression.
    Binary {
        /// Left operand.
        left: Box<Self>,
        /// Operator.
        operator: SqlBoundBinaryOperator,
        /// Right operand.
        right: Box<Self>,
    },
    /// A unary SQL expression.
    Unary {
        /// Operator.
        operator: SqlBoundUnaryOperator,
        /// Operand.
        expression: Box<Self>,
    },
    /// `expr IN (<subquery>)` with the subquery already planned.
    InSubquery {
        /// Left-hand side expression.
        expression: Box<Self>,
        /// Executable plan for the subquery.
        plan: Box<crate::sql_planner::SqlExecutionPlan>,
        /// Whether the predicate is spelled `NOT IN`.
        negated: bool,
    },
    /// A scalar subquery `(SELECT ...)` with the subquery already planned.
    ///
    /// The plan is executed once per enclosing row when it reads an enclosing column,
    /// and once per statement otherwise.
    ScalarSubquery {
        /// Executable plan for the subquery.
        plan: Box<crate::sql_planner::SqlExecutionPlan>,
    },
    /// `[NOT] EXISTS (<subquery>)` with the subquery already planned.
    Exists {
        /// Executable plan for the subquery.
        plan: Box<crate::sql_planner::SqlExecutionPlan>,
        /// Whether the predicate is spelled `NOT EXISTS`.
        negated: bool,
    },
    /// `IN` predicate whose subquery rows were materialized before evaluation.
    InValues {
        /// Left-hand side expression.
        expression: Box<Self>,
        /// Literal values produced by the subquery.
        values: Vec<SqlBoundLiteral>,
        /// Whether the predicate is spelled `NOT IN`.
        negated: bool,
    },
    /// `CASE WHEN cond THEN result ... [ELSE fallback] END`.
    Case {
        /// `(when, then)` branches in source order.
        branches: Vec<(Box<Self>, Box<Self>)>,
        /// Optional ELSE fallback.
        else_branch: Option<Box<Self>>,
    },
    /// `CAST(expr AS type_name)`.
    Cast {
        /// The expression being cast.
        expr: Box<Self>,
        /// The target type spelling as written (e.g. `INTEGER`, `TEXT`, `REAL`, `BLOB`).
        target: String,
    },
}

/// SQL projection after catalog binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlCatalogBoundProjection {
    /// All columns of the selected catalog table.
    Star(SqlCatalogBoundTable),
    /// A resolved expression with an optional client alias.
    Expression {
        /// Resolved expression.
        expression: SqlCatalogBoundExpression,
        /// Optional SQL alias.
        alias: Option<String>,
    },
}

/// SQL SELECT request after catalog binding and before 集群 distributed planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundSelect {
    /// Selected table identity, if the query has a source.
    pub source: Option<SqlCatalogBoundTable>,
    /// JOIN clauses in source order, each bound to its own catalog table.
    pub joins: Vec<SqlCatalogBoundJoin>,
    /// Resolved projections.
    pub projections: Vec<SqlCatalogBoundProjection>,
    /// Resolved optional predicate.
    pub predicate: Option<SqlCatalogBoundExpression>,
    /// Resolved optional GROUP BY expressions.
    pub group_by: Option<Vec<SqlCatalogBoundExpression>>,
    /// Resolved optional HAVING expression.
    pub having: Option<SqlCatalogBoundExpression>,
    /// Optional `LIMIT` count expression, propagated from the gateway binder.
    pub limit: Option<SqlCatalogBoundExpression>,
    /// Optional `OFFSET` count expression, propagated from the gateway binder.
    pub offset: Option<SqlCatalogBoundExpression>,
    /// Optional ORDER BY items, propagated from the gateway binder.
    pub order_by: Option<Vec<SqlCatalogBoundOrderByItem>>,
    /// Whether the projection list carries the `DISTINCT` modifier.
    pub distinct: bool,
}

/// One ORDER BY item after SQL names are bound to 集群 catalog identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundOrderByItem {
    /// Ordering expression.
    pub expression: SqlCatalogBoundExpression,
    /// Sort direction.
    pub direction: SqlBoundOrderDirection,
    /// Explicit `NULLS FIRST` / `NULLS LAST`, when written.
    pub nulls: Option<SqlNullsOrder>,
}

/// One `OVER (...)` clause after SQL names are bound to 集群 catalog identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundWindowSpec {
    /// `PARTITION BY` expressions, in source order.
    pub partition_by: Vec<SqlCatalogBoundExpression>,
    /// Window `ORDER BY` items, in source order.
    pub order_by: Vec<SqlCatalogBoundOrderByItem>,
    /// Explicit `ROWS ...` frame, when one was written.
    pub frame: Option<SqlBoundWindowFrame>,
}

/// One JOIN clause after SQL names are bound to 集群 catalog identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundJoin {
    /// Join kind retained from the SQL surface.
    pub kind: SqlBoundJoinKind,
    /// Joined table identity.
    pub table: SqlCatalogBoundTable,
    /// Join condition bound against the tables visible at this point.
    pub on: SqlCatalogBoundExpression,
}

/// SQL INSERT request after catalog binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundInsert {
    /// Target table identity.
    pub table: SqlCatalogBoundTable,
    /// Catalog fields in the order VALUES supplies them.
    pub fields: Vec<SqlCatalogBoundColumn>,
    /// Bound value expressions grouped by row. Every inner vec has length `fields.len()`.
    pub rows: Vec<Vec<SqlCatalogBoundExpression>>,
    /// Catalog-bound `INSERT ... SELECT` source. `None` for a `VALUES` insert.
    pub select: Option<Box<SqlCatalogBoundSelect>>,
    /// `ON CONFLICT` clause, when one was written.
    pub conflict: Option<SqlCatalogBoundConflict>,
    /// `RETURNING` projections, when one was written.
    pub returning: Option<Vec<SqlCatalogBoundProjection>>,
}

/// `ON CONFLICT` clause after catalog binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundConflict {
    /// Conflict target columns as written by the client.
    pub columns: Vec<String>,
    /// Action taken when the target row already exists.
    pub action: SqlCatalogBoundConflictAction,
}

/// Catalog-bound conflict action of an `ON CONFLICT` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlCatalogBoundConflictAction {
    /// `DO NOTHING` — skip the conflicting row.
    DoNothing,
    /// `DO UPDATE SET ... [WHERE ...]` — rewrite the conflicting row in place.
    DoUpdate {
        /// Target column → catalog-bound assignment expression.
        assignments: Vec<(SqlCatalogBoundColumn, SqlCatalogBoundExpression)>,
        /// Optional predicate restricting which conflicting rows are rewritten.
        predicate: Option<SqlCatalogBoundExpression>,
    },
}

/// SQL UPDATE request after catalog binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundUpdate {
    /// Target table identity.
    pub table: SqlCatalogBoundTable,
    /// Catalog field → bound value assignments in source order.
    pub assignments: Vec<(SqlCatalogBoundColumn, SqlCatalogBoundExpression)>,
    /// Resolved optional predicate.
    pub predicate: Option<SqlCatalogBoundExpression>,
}

/// SQL DELETE request after catalog binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundDelete {
    /// Target table identity.
    pub table: SqlCatalogBoundTable,
    /// Resolved optional predicate.
    pub predicate: Option<SqlCatalogBoundExpression>,
}

/// One operand of a catalog-bound SQL set operation.
///
/// The binder folds Oak's flat chain into a precedence tree, so an operand is
/// either a plain `SELECT` binding or an `INTERSECT` group of its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlCatalogBoundSetOperand {
    /// One catalog-bound `SELECT` operand.
    Select(Box<SqlCatalogBoundSelect>),
    /// An `INTERSECT` group produced by the precedence fold.
    SetOperation(Box<SqlCatalogBoundSetOperation>),
}

/// SQL set operation after catalog binding.
///
/// `operands` and `operators` describe one precedence level: `operators` holds
/// `operands.len() - 1` entries, each combining its two neighbours
/// left-associatively. The output column names come from the leftmost operand.
///
/// `ORDER BY` on a set operation references the combined output columns rather
/// than catalog columns, so it stays in surface form until the planner resolves
/// it against the leftmost operand's result shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundSetOperation {
    /// Operands in left-to-right order.
    pub operands: Vec<SqlCatalogBoundSetOperand>,
    /// Operators between consecutive operands.
    pub operators: Vec<SqlBoundSetOperator>,
    /// `ORDER BY` applied to the combined result, resolved during planning.
    pub order_by: Option<Vec<SqlBoundOrderByItem>>,
    /// Optional `LIMIT` count expression applied to the combined result.
    pub limit: Option<SqlCatalogBoundExpression>,
    /// Optional `OFFSET` count expression applied to the combined result.
    pub offset: Option<SqlCatalogBoundExpression>,
}

/// Gateway-owned SQL request after names are bound to a 集群 catalog revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlCatalogBoundStatement {
    /// One catalog-bound SELECT request awaiting distributed planning.
    Select(SqlCatalogBoundSelect),
    /// One catalog-bound INSERT request awaiting planning.
    Insert(SqlCatalogBoundInsert),
    /// One catalog-bound UPDATE request awaiting planning.
    Update(SqlCatalogBoundUpdate),
    /// One catalog-bound DELETE request awaiting planning.
    Delete(SqlCatalogBoundDelete),
    /// One catalog-bound set operation awaiting planning.
    SetOperation(SqlCatalogBoundSetOperation),
}

/// Why a column reference may not reach the directly enclosing query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlCorrelatedBlock {
    /// The subquery sits in a write statement, where no enclosing row is in scope yet.
    WriteStatement,
    /// The subquery is nested inside another subquery; only one correlation level exists.
    Nested,
    /// The reference sits in a `FROM` clause, which is flattened onto the enclosing row.
    FromClause,
}

impl SqlCorrelatedBlock {
    /// Client-facing reason a correlated reference is rejected in this position.
    fn message(self) -> &'static str {
        match self {
            Self::WriteStatement => "SQL correlated subquery is not supported in a write statement",
            Self::Nested => "SQL correlated subquery supports a single nesting level only",
            Self::FromClause => "SQL correlated column cannot appear in a subquery FROM clause",
        }
    }
}

/// Which tables an expression may correlate against, and whether that is allowed.
///
/// A correlated subquery is re-executed once per enclosing row, so the enclosing
/// tables stay visible inside it. `blocked` records positions where correlation is
/// refused instead of silently reading a value from the wrong row.
#[derive(Debug, Clone, Default)]
struct SqlCorrelationScope {
    /// Tables of the directly enclosing query, addressable by a correlated subquery.
    outer: Vec<SqlCatalogBoundTable>,
    /// Reason enclosing columns are rejected here, when one applies.
    blocked: Option<SqlCorrelatedBlock>,
}

impl SqlCorrelationScope {
    /// Scope of a statement-level `SELECT`: no enclosing tables, correlation allowed.
    fn root() -> Self {
        Self::default()
    }

    /// Scope of a write statement: subqueries may not read the written row.
    fn write() -> Self {
        Self { outer: Vec::new(), blocked: Some(SqlCorrelatedBlock::WriteStatement) }
    }

    /// Scope of one subquery whose enclosing query exposes `sources`.
    fn subquery(&self, sources: &[SqlCatalogBoundTable]) -> Self {
        Self { outer: sources.to_vec(), blocked: self.blocked.or_else(|| (!self.outer.is_empty()).then_some(SqlCorrelatedBlock::Nested)) }
    }

    /// Scope of a `FROM` body: enclosing tables stay visible only to be rejected.
    fn from_clause(&self, sources: &[SqlCatalogBoundTable]) -> Self {
        let mut outer = self.outer.clone();
        outer.extend(sources.iter().cloned());
        Self { outer, blocked: Some(SqlCorrelatedBlock::FromClause) }
    }
}

/// Binds a parsed SQL surface to a specific catalog revision.
///
/// `@@`-prefixed references are rejected — use
/// [`bind_sql_catalog_with_variables`] on gateways that emulate a variable table.
pub fn bind_sql_catalog(
    source: &str,
    catalog: &SqlCatalog,
    expected_schema_version: u32,
    dialect: SqlDialect,
) -> Result<SqlCatalogBoundStatement, SqlFrontendError> {
    bind_sql_catalog_with_variables(source, catalog, expected_schema_version, &reject_system_variable, dialect)
}

/// Binds a parsed SQL surface against a catalog revision, resolving system variables.
///
/// `variables` receives the whole `@@[scope.]name` spelling written by the
/// client and returns the literal the variable evaluates to. Dialects without
/// system variables keep using [`bind_sql_catalog`], which always rejects them.
pub fn bind_sql_catalog_with_variables(
    source: &str,
    catalog: &SqlCatalog,
    expected_schema_version: u32,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
    dialect: SqlDialect,
) -> Result<SqlCatalogBoundStatement, SqlFrontendError> {
    if catalog.schema_version != expected_schema_version {
        return Err(internal_error(&format!(
            "SQL catalog schema version mismatch: expected {expected_schema_version}, found {}",
            catalog.schema_version
        )));
    }
    // `::<type>` 在 Oak 里只有词法没有节点，收下就等于静默丢掉转换和列名。
    crate::sql::reject_cast_operator(source)?;
    let statement = bind_sql(source, dialect)?;
    match statement {
        SqlBoundStatement::Select(select) => bind_select(select, catalog, variables).map(SqlCatalogBoundStatement::Select),
        SqlBoundStatement::Insert(insert) => bind_insert(insert, catalog, variables).map(SqlCatalogBoundStatement::Insert),
        SqlBoundStatement::Update(update) => bind_update(update, catalog, variables).map(SqlCatalogBoundStatement::Update),
        SqlBoundStatement::Delete(delete) => bind_delete(delete, catalog, variables).map(SqlCatalogBoundStatement::Delete),
        SqlBoundStatement::SetOperation(set) => bind_set_operation(set, catalog, variables).map(SqlCatalogBoundStatement::SetOperation),
    }
}

/// Default resolver for dialects without `@@` system variables.
fn reject_system_variable(name: &str) -> Result<SqlBoundLiteral, SqlFrontendError> {
    Err(SqlFrontendError::new(SqlErrorKind::UnknownVariable, format!("SQL system variable {name} is not supported by this gateway")))
}

fn bind_insert(
    insert: SqlBoundInsert,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundInsert, SqlFrontendError> {
    let table = find_table(catalog, &insert.table)?;
    let source = [table.clone()];
    // 写语句没有「当前外层行」，子查询引用目标表列会读到错误行，绑定期就拒绝。
    let scope = SqlCorrelationScope::write();
    // `EXCLUDED` 是 `DO UPDATE` 里提议写入的那一行：同一个 catalog 表占用第二个
    // 行槽，它的列因此带上 `ROW_ID_STRIDE` 偏移，与既有行区分开。既有行仍是槽 0。
    let proposed = SqlCatalogBoundTable { alias: Some("EXCLUDED".to_string()), row_slot: 1, ..table.clone() };
    let bound_fields = match insert.columns {
        Some(cols) => cols.iter().map(|name| find_field(name, &source, catalog)).collect::<Result<Vec<_>, _>>()?,
        None => {
            // INSERT INTO t VALUES (...) 无列名 → 按 catalog 字段顺序
            let cat = catalog
                .tables
                .iter()
                .find(|c| c.type_id == table.type_id)
                .ok_or_else(|| internal_error("catalog table identity is not published"))?;
            cat.fields
                .iter()
                .map(|f| {
                    reject_unknown_field(f)?;
                    Ok(SqlCatalogBoundColumn {
                        type_id: table.type_id,
                        field_id: f.field_id,
                        row_id: f.field_id,
                        table_name: table.name.clone(),
                        field_name: f.name.clone(),
                        visible_name: f.name.clone(),
                        capacity: f.capacity,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        }
    };
    let field_count = bound_fields.len();
    if field_count == 0 {
        return Err(SqlFrontendError::syntax("INSERT has no target columns".to_string()));
    }
    // `ON CONFLICT` 目标列保持表层拼写，唯一约束校验由 planner 完成；`DO UPDATE`
    // 赋值与 `WHERE` 在目标表作用域内绑定，因此引用了未知列会显式报错。
    let conflict = match insert.conflict {
        None => None,
        Some(conflict) => Some(SqlCatalogBoundConflict {
            columns: conflict.columns,
            action: match conflict.action {
                SqlBoundConflictAction::DoNothing => SqlCatalogBoundConflictAction::DoNothing,
                SqlBoundConflictAction::DoUpdate { assignments, predicate } => {
                    // 赋值右值可以引用既有行（槽 0，表名限定）与提议行（`EXCLUDED`）。
                    let update_scope = [table.clone(), proposed];
                    let bound_assignments = assignments
                        .into_iter()
                        .map(|assignment| {
                            Ok((
                                find_field(&assignment.column, &source, catalog)?,
                                bind_expression(canonicalize_excluded(assignment.value), &update_scope, &[], &scope, catalog, variables)?,
                            ))
                        })
                        .collect::<Result<Vec<_>, SqlFrontendError>>()?;
                    SqlCatalogBoundConflictAction::DoUpdate {
                        assignments: bound_assignments,
                        // `WHERE` 仍只对既有行求值，因此看不到 `EXCLUDED` 的列。
                        predicate: predicate
                            .map(|predicate| bind_expression(predicate, &source, &[], &scope, catalog, variables))
                            .transpose()?,
                    }
                }
            },
        }),
    };
    let returning = match insert.returning {
        None => None,
        Some(projections) => {
            let mut bound = Vec::new();
            for projection in projections {
                bound.extend(bind_projection(projection, &source, &[], &scope, catalog, variables)?);
            }
            Some(bound)
        }
    };
    // `INSERT ... SELECT` 复用既有 SELECT 绑定；源查询的列数/类型在 planner 层对齐。
    // `DEFAULT VALUES` 的语义是「按列默认值生成一行」，但网关的写入路径从不按
    // 列默认值补值（`SqlSchemaRegistry` 里的 default 只用于元数据展示），
    // 因此这里结构化地拒绝，而不是伪造一行 `NULL`。
    if let SqlBoundInsertSource::DefaultValues = insert.source {
        return Err(SqlFrontendError::unsupported(
            "`INSERT ... DEFAULT VALUES` is not supported: the cluster gateway does not apply column defaults on insert, so there is no default row to write",
        ));
    }
    if let SqlBoundInsertSource::Select(select) = insert.source {
        let select = bind_select(*select, catalog, variables)?;
        return Ok(SqlCatalogBoundInsert {
            table,
            fields: bound_fields,
            rows: Vec::new(),
            select: Some(Box::new(select)),
            conflict,
            returning,
        });
    }
    if insert.rows.is_empty() {
        return Err(SqlFrontendError::syntax("INSERT has no VALUES".to_string()));
    }
    let mut rows = Vec::with_capacity(insert.rows.len());
    for row in insert.rows {
        if row.len() != field_count {
            return Err(SqlFrontendError::syntax(format!("INSERT column/value count mismatch: {} columns, {} values", field_count, row.len())));
        }
        let bound_row =
            row.iter().map(|v| bind_expression(v.clone(), &source, &[], &scope, catalog, variables)).collect::<Result<Vec<_>, _>>()?;
        rows.push(bound_row);
    }
    Ok(SqlCatalogBoundInsert { table, fields: bound_fields, rows, select: None, conflict, returning })
}

/// Normalizes the `EXCLUDED` qualifier of an `ON CONFLICT DO UPDATE` value.
///
/// `EXCLUDED` is a keyword rather than a table name, and PostgreSQL folds
/// unquoted identifiers to lower case, so clients almost always write
/// `excluded.column`. The gateway matches identifiers case-sensitively, so the
/// qualifier is folded onto the sentinel spelling here and left untouched
/// everywhere else.
fn canonicalize_excluded(expression: SqlBoundExpression) -> SqlBoundExpression {
    use SqlBoundExpression::*;
    match expression {
        Identifier(name) => match name.split_once('.') {
            Some((qualifier, column)) if qualifier.trim().eq_ignore_ascii_case("excluded") => Identifier(format!("EXCLUDED.{column}")),
            _ => Identifier(name),
        },
        Binary { left, operator, right } => {
            Binary { left: Box::new(canonicalize_excluded(*left)), operator, right: Box::new(canonicalize_excluded(*right)) }
        }
        Unary { operator, expression } => Unary { operator, expression: Box::new(canonicalize_excluded(*expression)) },
        Function { name, args, distinct, window } => {
            Function { name, args: args.into_iter().map(canonicalize_excluded).collect(), distinct, window }
        }
        InSubquery { expression, select, negated } => InSubquery { expression: Box::new(canonicalize_excluded(*expression)), select, negated },
        Exists { select, negated } => Exists { select, negated },
        // 写语句禁止相关子查询，所以这里的子查询不会把外层列当作 `EXCLUDED` 别名，
        // 折叠无内容可改。
        ScalarSubquery { select } => ScalarSubquery { select },
        Case { branches, else_branch } => Case {
            branches: branches
                .into_iter()
                .map(|(when, then)| (Box::new(canonicalize_excluded(*when)), Box::new(canonicalize_excluded(*then))))
                .collect(),
            else_branch: else_branch.map(|els| Box::new(canonicalize_excluded(*els))),
        },
        Cast { expr, target } => Cast { expr: Box::new(canonicalize_excluded(*expr)), target },
        Literal(literal) => Literal(literal),
        Wildcard => Wildcard,
    }
}

fn bind_update(
    update: SqlBoundUpdate,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundUpdate, SqlFrontendError> {
    let table = find_table(catalog, &update.table)?;
    let source = [table.clone()];
    let scope = SqlCorrelationScope::write();
    let assignments = update
        .assignments
        .into_iter()
        .map(|SqlBoundAssignment { column, value }| {
            let field = find_field(&column, &source, catalog)?;
            let bound_value = bind_expression(value, &source, &[], &scope, catalog, variables)?;
            Ok((field, bound_value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let predicate = update.predicate.map(|p| bind_expression(p, &source, &[], &scope, catalog, variables)).transpose()?;
    Ok(SqlCatalogBoundUpdate { table, assignments, predicate })
}

fn bind_delete(
    delete: SqlBoundDelete,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundDelete, SqlFrontendError> {
    let table = find_table(catalog, &delete.table)?;
    let source = [table.clone()];
    let scope = SqlCorrelationScope::write();
    let predicate = delete.predicate.map(|p| bind_expression(p, &source, &[], &scope, catalog, variables)).transpose()?;
    Ok(SqlCatalogBoundDelete { table, predicate })
}

fn bind_set_operation(
    set: SqlBoundSetOperation,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundSetOperation, SqlFrontendError> {
    bind_set_operation_in(set, &[], &SqlCorrelationScope::root(), catalog, variables)
}

/// Binds one set-operation level whose operands share the inherited CTE scope.
///
/// `WITH` written in front of a set operation covers the whole operation, so
/// its definitions are lifted off the leftmost leaf and every operand inherits
/// them.
fn bind_set_operation_in(
    set: SqlBoundSetOperation,
    inherited: &[SqlCatalogCte],
    scope: &SqlCorrelationScope,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundSetOperation, SqlFrontendError> {
    let SqlBoundSetOperation { mut operands, operators, order_by, limit, offset } = set;
    if operands.len() < 2 || operators.len() + 1 != operands.len() {
        return Err(SqlFrontendError::new(SqlErrorKind::Syntax, "set operation operands and operators do not line up"));
    }
    let own_with = take_leading_with(&mut operands);
    let ctes = resolve_cte_scope(own_with, inherited, catalog, variables, scope)?;
    let limit = bind_row_count(limit, &ctes, catalog, variables)?;
    let offset = bind_row_count(offset, &ctes, catalog, variables)?;
    let operands =
        operands.into_iter().map(|operand| bind_set_operand(operand, &ctes, scope, catalog, variables)).collect::<Result<Vec<_>, _>>()?;
    Ok(SqlCatalogBoundSetOperation { operands, operators, order_by, limit, offset })
}

fn bind_set_operand(
    operand: SqlBoundSetOperand,
    ctes: &[SqlCatalogCte],
    scope: &SqlCorrelationScope,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundSetOperand, SqlFrontendError> {
    match operand {
        SqlBoundSetOperand::Select(select) => {
            bind_select_in(*select, catalog, variables, ctes, scope).map(|select| SqlCatalogBoundSetOperand::Select(Box::new(select)))
        }
        SqlBoundSetOperand::SetOperation(set) => {
            bind_set_operation_in(*set, ctes, scope, catalog, variables).map(|set| SqlCatalogBoundSetOperand::SetOperation(Box::new(set)))
        }
    }
}

/// Removes the leading `WITH` clause from the leftmost operand of a level.
///
/// The parser attaches `WITH` to the first written `SELECT`, so the definition
/// list that scopes the whole set operation is the one on the leftmost leaf.
fn take_leading_with(operands: &mut [SqlBoundSetOperand]) -> Vec<SqlBoundCte> {
    let Some(first) = operands.first_mut()
    else {
        return Vec::new();
    };
    match first {
        SqlBoundSetOperand::Select(select) => std::mem::take(&mut select.with),
        SqlBoundSetOperand::SetOperation(set) => take_leading_with(&mut set.operands),
    }
}

fn bind_select(
    select: SqlBoundSelect,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundSelect, SqlFrontendError> {
    bind_select_in(select, catalog, variables, &[], &SqlCorrelationScope::root())
}

/// Resolves the `WITH` definitions of one select into the scope its `FROM` sees,
/// keeping the definitions inherited from an enclosing scope.
///
/// Definitions apply in source order: each body only sees the definitions written
/// before it, so a body can neither reference itself nor a later CTE, and a name
/// defined here shadows an inherited one. Every definition is flattened as soon as
/// it is resolved, even when nothing references it, so an unexpressible body never
/// hides behind an unused name.
fn resolve_cte_scope(
    own: Vec<SqlBoundCte>,
    inherited: &[SqlCatalogCte],
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
    scope: &SqlCorrelationScope,
) -> Result<Vec<SqlCatalogCte>, SqlFrontendError> {
    let mut ctes: Vec<SqlCatalogCte> = inherited.to_vec();
    for cte in own {
        let definition = SqlCatalogCte { name: cte.name.clone(), columns: cte.columns, query: *cte.query, visible: ctes.clone() };
        let mut probe = std::collections::BTreeMap::new();
        flatten_body(
            &definition.query,
            &definition.name,
            &definition.columns,
            "common table expression",
            &definition.visible,
            &scope.from_clause(&[]),
            catalog,
            variables,
            &mut probe,
        )?;
        ctes.push(definition);
    }
    Ok(ctes)
}

/// Binds one select whose `FROM` may also reference CTEs from an enclosing scope.
fn bind_select_in(
    select: SqlBoundSelect,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
    inherited: &[SqlCatalogCte],
    scope: &SqlCorrelationScope,
) -> Result<SqlCatalogBoundSelect, SqlFrontendError> {
    // CTE 在引用处才被展开成基表，同一条 CTE 被引用两次就得到两个互不覆盖的行槽。
    let ctes = resolve_cte_scope(select.with, inherited, catalog, variables, scope)?;
    let mut occurrences: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
    // 派生表被扁平化成它自己的基表，内层 `WHERE` 改由外层 `WHERE` 表达。
    let mut derived_predicates: Vec<SqlCatalogBoundExpression> = Vec::new();
    let mut source = None;
    // FROM 里的表在解析时还不构成「外层行」，因此这里只把外层表传给 FROM 以显式拒绝。
    if let Some(bound_source) = select.source.as_ref() {
        let (table, predicate) = bind_source(bound_source, catalog, variables, &mut occurrences, &ctes, &scope.from_clause(&[]))?;
        source = Some(table);
        if let Some(predicate) = predicate {
            // 保留侧的过滤移动到外层 `WHERE` 语义不变；可空侧会因 NULL 扩张丢行。
            if select.joins.iter().any(|join| matches!(join.kind, SqlBoundJoinKind::Right | SqlBoundJoinKind::Full)) {
                return Err(unsupported_source("derived table", "a filtered source cannot feed a RIGHT or FULL join"));
            }
            derived_predicates.push(predicate);
        }
    }
    // JOIN 之后列名作用域包含基表与所有已连接表，ON 条件只能引用此前已出现的表。
    let mut sources: Vec<SqlCatalogBoundTable> = source.iter().cloned().collect();
    let mut joins: Vec<SqlCatalogBoundJoin> = Vec::with_capacity(select.joins.len());
    for join in select.joins {
        let (table, derived_predicate) = bind_source(&join.source, catalog, variables, &mut occurrences, &ctes, &scope.from_clause(&sources))?;
        if let Some(predicate) = derived_predicate {
            if !matches!(join.kind, SqlBoundJoinKind::Inner) {
                return Err(unsupported_source("derived table", "a filtered source can only feed an INNER join"));
            }
            derived_predicates.push(predicate);
        }
        if let Some(previous) = sources.iter().find(|visible| visible.visible_name() == table.visible_name()) {
            return Err(SqlFrontendError::new(
                SqlErrorKind::AmbiguousTable,
                format!("SQL table name `{}` appears more than once in FROM, give each occurrence a distinct alias", previous.visible_name()),
            ));
        }
        sources.push(table.clone());
        let on = bind_expression(join.on, &sources, &ctes, scope, catalog, variables)?;
        joins.push(SqlCatalogBoundJoin { kind: join.kind, table, on });
    }
    let mut projections: Vec<SqlCatalogBoundProjection> = Vec::new();
    for projection in select.projections {
        projections.extend(bind_projection(projection, &sources, &ctes, scope, catalog, variables)?);
    }
    let mut predicate =
        select.predicate.map(|expression| bind_expression(expression, &sources, &ctes, scope, catalog, variables)).transpose()?;
    for derived in derived_predicates {
        predicate = merge_predicates(predicate, Some(derived));
    }
    let group_by = select
        .group_by
        .map(|exprs| exprs.into_iter().map(|e| bind_expression(e, &sources, &ctes, scope, catalog, variables)).collect::<Result<Vec<_>, _>>())
        .transpose()?;
    let having = select.having.map(|e| bind_expression(e, &sources, &ctes, scope, catalog, variables)).transpose()?;
    let order_by = match select.order_by {
        Some(items) => {
            let bound = items
                .into_iter()
                .map(|item| {
                    Ok(SqlCatalogBoundOrderByItem {
                        expression: bind_expression(item.expr, &sources, &ctes, scope, catalog, variables)?,
                        direction: item.direction,
                        nulls: item.nulls,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Some(bound)
        }
        None => None,
    };
    Ok(SqlCatalogBoundSelect {
        source,
        joins,
        projections,
        predicate,
        group_by,
        having,
        limit: bind_row_count(select.limit, &ctes, catalog, variables)?,
        offset: bind_row_count(select.offset, &ctes, catalog, variables)?,
        order_by,
        distinct: select.distinct,
    })
}

/// Binds one SQL projection into zero or more catalog projections (`*` expands one
/// entry per visible table so joined queries project both sides in source order).
fn bind_projection(
    projection: SqlBoundProjection,
    sources: &[SqlCatalogBoundTable],
    ctes: &[SqlCatalogCte],
    scope: &SqlCorrelationScope,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<Vec<SqlCatalogBoundProjection>, SqlFrontendError> {
    match projection {
        SqlBoundProjection::Star => {
            if sources.is_empty() {
                return Err(SqlFrontendError::unsupported("SQL star projection requires a catalog table source"));
            }
            Ok(sources.iter().cloned().map(SqlCatalogBoundProjection::Star).collect())
        }
        SqlBoundProjection::Expression { expression, alias } => Ok(vec![SqlCatalogBoundProjection::Expression {
            expression: bind_expression(expression, sources, ctes, scope, catalog, variables)?,
            alias,
        }]),
    }
}

fn bind_expression(
    expression: SqlBoundExpression,
    sources: &[SqlCatalogBoundTable],
    ctes: &[SqlCatalogCte],
    scope: &SqlCorrelationScope,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundExpression, SqlFrontendError> {
    match expression {
        SqlBoundExpression::Identifier(name) => {
            // `@@` 前缀属于会话变量而不是 catalog 列名，必须先交给方言解析器。
            if name.starts_with("@@") {
                return variables(&name).map(SqlCatalogBoundExpression::Literal);
            }
            // 本层作用域解析不到时，再按「直接外层查询的列」找一次：相关子查询按外层行
            // 逐行重算，值不来自子查询自己的行。该位置禁止相关时在这里显式拒绝。
            let outcome = match find_field(&name, sources, catalog) {
                Ok(field) => Ok(SqlCatalogBoundExpression::Column(field)),
                Err(error) => match find_outer_column(&name, scope, catalog) {
                    Ok(Some(column)) => Ok(SqlCatalogBoundExpression::OuterColumn(column)),
                    Ok(None) => Err(error),
                    Err(error) => Err(error),
                },
            };
            match outcome {
                Ok(expression) => Ok(expression),
                // SQL 标准把 `CURRENT_DATE` / `CURRENT_TIME` / `CURRENT_TIMESTAMP` 写成
                // 无括号关键字，Oak 的树里没有对应节点。列解析失败后回退为无参内建函数，
                // 同名列优先，作用域与普通列引用一致。
                Err(error) if error.kind == SqlErrorKind::UnknownColumn => match SqlScalarFunction::from_keyword(&name) {
                    Some(function) => Ok(SqlCatalogBoundExpression::Function {
                        name: function.name().to_ascii_lowercase(),
                        args: Vec::new(),
                        distinct: false,
                        window: None,
                    }),
                    None => Err(error),
                },
                Err(error) => Err(error),
            }
        }
        SqlBoundExpression::Literal(literal) => Ok(SqlCatalogBoundExpression::Literal(literal)),
        SqlBoundExpression::Wildcard => Ok(SqlCatalogBoundExpression::Wildcard),
        SqlBoundExpression::Binary { left, operator, right } => Ok(SqlCatalogBoundExpression::Binary {
            left: Box::new(bind_expression(*left, sources, ctes, scope, catalog, variables)?),
            operator,
            right: Box::new(bind_expression(*right, sources, ctes, scope, catalog, variables)?),
        }),
        SqlBoundExpression::Unary { operator, expression } => Ok(SqlCatalogBoundExpression::Unary {
            operator,
            expression: Box::new(bind_expression(*expression, sources, ctes, scope, catalog, variables)?),
        }),
        SqlBoundExpression::Function { name, args, distinct, window } => {
            let bound_args =
                args.into_iter().map(|arg| bind_expression(arg, sources, ctes, scope, catalog, variables)).collect::<Result<Vec<_>, _>>()?;
            let window = window.map(|spec| bind_window_spec(spec, sources, ctes, scope, catalog, variables)).transpose()?;
            Ok(SqlCatalogBoundExpression::Function { name, args: bound_args, distinct, window })
        }
        SqlBoundExpression::InSubquery { expression, select, negated } => {
            // 子查询自带 FROM 作用域，直接外层查询的列仍可见一层；外层 `WITH` 定义同样
            // 可见。执行期按外层行重算整个子查询，因此只有单层相关可以表达。
            let bound_select = bind_select_in(*select, catalog, variables, ctes, &scope.subquery(sources))?;
            reject_correlated_grouping(&bound_select)?;
            let plan = crate::sql_planner::plan_select(bound_select, catalog)?;
            Ok(SqlCatalogBoundExpression::InSubquery {
                expression: Box::new(bind_expression(*expression, sources, ctes, scope, catalog, variables)?),
                plan: Box::new(plan),
                negated,
            })
        }
        SqlBoundExpression::ScalarSubquery { select } => {
            // 标量子查询与 `EXISTS` 一样按外层行重算，外层 `WITH` 可见，外层列可见一层。
            // 它的值必须是单一列，因此在绑定期就拒绝多列投影，而不是在执行期近似取首列。
            let bound_select = bind_select_in(*select, catalog, variables, ctes, &scope.subquery(sources))?;
            reject_correlated_grouping(&bound_select)?;
            let plan = crate::sql_planner::plan_select(bound_select, catalog)?;
            if crate::sql_describe::describe_plan(&plan).len() != 1 {
                return Err(SqlFrontendError::unsupported("SQL scalar subquery must select exactly one column"));
            }
            Ok(SqlCatalogBoundExpression::ScalarSubquery { plan: Box::new(plan) })
        }
        SqlBoundExpression::Exists { select, negated } => {
            // `EXISTS` 与 `IN` 一样按外层行重算，外层 `WITH` 定义同样可见。
            let bound_select = bind_select_in(*select, catalog, variables, ctes, &scope.subquery(sources))?;
            reject_correlated_grouping(&bound_select)?;
            let plan = crate::sql_planner::plan_select(bound_select, catalog)?;
            Ok(SqlCatalogBoundExpression::Exists { plan: Box::new(plan), negated })
        }
        SqlBoundExpression::Case { branches, else_branch } => {
            let bound_branches = branches
                .into_iter()
                .map(|(when, then)| {
                    let w = bind_expression(*when, sources, ctes, scope, catalog, variables)?;
                    let t = bind_expression(*then, sources, ctes, scope, catalog, variables)?;
                    Ok((Box::new(w), Box::new(t)))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let bound_else = match else_branch {
                Some(els) => Some(Box::new(bind_expression(*els, sources, ctes, scope, catalog, variables)?)),
                None => None,
            };
            Ok(SqlCatalogBoundExpression::Case { branches: bound_branches, else_branch: bound_else })
        }
        SqlBoundExpression::Cast { expr, target } => {
            Ok(SqlCatalogBoundExpression::Cast { expr: Box::new(bind_expression(*expr, sources, ctes, scope, catalog, variables)?), target })
        }
    }
}

/// Binds a `LIMIT` / `OFFSET` expression, which has no row context.
///
/// The gateway binder already rejected column references, `*` and subqueries
/// for these clauses, so binding runs against an empty scope. Keeping the
/// clause in the expression system is what lets the planner fold a parameter or
/// an arithmetic expression instead of a literal-only special case.
fn bind_row_count(
    expression: Option<SqlBoundExpression>,
    ctes: &[SqlCatalogCte],
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<Option<SqlCatalogBoundExpression>, SqlFrontendError> {
    expression.map(|expression| bind_expression(expression, &[], ctes, &SqlCorrelationScope::root(), catalog, variables)).transpose()
}

/// Resolves a column name against the tables of the directly enclosing query.
///
/// `Ok(None)` means the name belongs to neither the current scope nor the enclosing
/// one, so the caller keeps reporting the original unknown-column error. A match in
/// an enclosing scope that the current position forbids reports the reason instead,
/// which keeps `UnknownColumn` from hiding an explicitly unsupported form.
fn find_outer_column(name: &str, scope: &SqlCorrelationScope, catalog: &SqlCatalog) -> Result<Option<SqlCatalogOuterColumn>, SqlFrontendError> {
    if scope.outer.is_empty() {
        return Ok(None);
    }
    let column = match find_field(name, &scope.outer, catalog) {
        Ok(column) => column,
        Err(error) if matches!(error.kind, SqlErrorKind::UnknownColumn | SqlErrorKind::UnknownTableQualifier) => return Ok(None),
        Err(error) => return Err(error),
    };
    if let Some(blocked) = scope.blocked {
        return Err(SqlFrontendError::unsupported(blocked.message()));
    }
    let field_type = field_type_of(catalog, &column)?;
    Ok(Some(SqlCatalogOuterColumn { column, field_type }))
}

/// Looks up the declared type of a bound column through its catalog identity.
fn field_type_of(catalog: &SqlCatalog, column: &SqlCatalogBoundColumn) -> Result<SqlFieldType, SqlFrontendError> {
    let table = catalog
        .tables
        .iter()
        .find(|candidate| candidate.type_id == column.type_id)
        .ok_or_else(|| internal_error("catalog table identity is not published"))?;
    Ok(table
        .fields
        .iter()
        .find(|field| field.field_id == column.field_id)
        .map(|field| field.field_type.clone())
        .unwrap_or(SqlFieldType::Unknown))
}

/// Rejects a correlated subquery whose `GROUP BY` / `HAVING` / `ORDER BY` reads an
/// enclosing column.
///
/// The subquery is re-executed once per outer row, so those terms would be constant
/// for that execution. That is expressible, but keeping it unsupported is what
/// bounds the feature to the placements the planner can validate statically.
fn reject_correlated_grouping(select: &SqlCatalogBoundSelect) -> Result<(), SqlFrontendError> {
    let reads_outer = |clause: &str, hit: bool| {
        if hit { Err(SqlFrontendError::unsupported(format!("SQL correlated column cannot appear in a subquery {clause}"))) } else { Ok(()) }
    };
    reads_outer("GROUP BY", select.group_by.iter().flatten().any(crate::sql_planner::expression_reads_outer))?;
    reads_outer("HAVING", select.having.as_ref().is_some_and(crate::sql_planner::expression_reads_outer))?;
    reads_outer("ORDER BY", select.order_by.iter().flatten().any(|item| crate::sql_planner::expression_reads_outer(&item.expression)))
}

/// Catalog-binds one window spec, keeping partition / ordering terms resolvable
/// against the same table scope as the call itself.
fn bind_window_spec(
    spec: SqlBoundWindowSpec,
    sources: &[SqlCatalogBoundTable],
    ctes: &[SqlCatalogCte],
    scope: &SqlCorrelationScope,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundWindowSpec, SqlFrontendError> {
    let partition_by = spec
        .partition_by
        .into_iter()
        .map(|expr| bind_expression(expr, sources, ctes, scope, catalog, variables))
        .collect::<Result<Vec<_>, _>>()?;
    let mut order_by = Vec::with_capacity(spec.order_by.len());
    for item in spec.order_by {
        order_by.push(SqlCatalogBoundOrderByItem {
            expression: bind_expression(item.expr, sources, ctes, scope, catalog, variables)?,
            direction: item.direction,
            nulls: item.nulls,
        });
    }
    Ok(SqlCatalogBoundWindowSpec { partition_by, order_by, frame: spec.frame })
}

/// One `WITH` definition kept while binding, together with the definitions
/// visible inside its body.
///
/// A CTE is never executed on its own: every reference flattens the body onto the
/// single catalog table it selects from, which is why the same CTE referenced
/// twice behaves like a self join. `visible` holds only the definitions written
/// before this one, so a body can neither reference itself nor a later CTE.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlCatalogCte {
    /// Name the CTE is referenced by.
    name: String,
    /// Explicit output column names, applied positionally to the flattened table.
    columns: Vec<String>,
    /// Bound body of the CTE.
    query: SqlBoundSelect,
    /// Definitions visible inside `query`, in source order.
    visible: Vec<SqlCatalogCte>,
}

/// Binds one `FROM` / `JOIN` source, assigning the occurrence slot used to build row ids.
///
/// Repeated occurrences of the same catalog table get distinct slots so a self join
/// can keep both sides of a column in one merged row. A CTE name shadows a catalog
/// table of the same name. A derived table or a CTE reference also returns the
/// predicate its body carried, for the caller to merge into the outer `WHERE`.
fn bind_source(
    source: &SqlBoundSource,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
    occurrences: &mut std::collections::BTreeMap<u64, u64>,
    ctes: &[SqlCatalogCte],
    scope: &SqlCorrelationScope,
) -> Result<(SqlCatalogBoundTable, Option<SqlCatalogBoundExpression>), SqlFrontendError> {
    match source {
        SqlBoundSource::Table { name, alias } => match ctes.iter().rev().find(|cte| cte.name == *name) {
            Some(cte) => flatten_body(
                &cte.query,
                alias.as_deref().unwrap_or(name),
                &cte.columns,
                "common table expression",
                &cte.visible,
                scope,
                catalog,
                variables,
                occurrences,
            ),
            None => {
                let mut table = find_table(catalog, name)?;
                table.alias = alias.clone();
                Ok((assign_row_slot(table, occurrences), None))
            }
        },
        SqlBoundSource::Derived { query, alias } => {
            // 派生表没有显式列名，只沿用 body 自己的输出名。
            flatten_body(query, alias, &[], "derived table", ctes, scope, catalog, variables, occurrences)
        }
    }
}

/// Assigns the next occurrence slot of one catalog table and returns the table.
fn assign_row_slot(mut table: SqlCatalogBoundTable, occurrences: &mut std::collections::BTreeMap<u64, u64>) -> SqlCatalogBoundTable {
    let slot = occurrences.entry(table.type_id).or_insert(0);
    table.row_slot = *slot;
    *slot += 1;
    table
}

/// Flattens one derived table or CTE body into the single catalog table it
/// selects from.
///
/// The gateway row source is one catalog table, so a body is expressible only
/// when it is that same table seen through a new name: no join, no grouping, no
/// ordering, no paging, no nested `WITH` and a projection that exposes every
/// column. Its source may also be an earlier CTE, which is flattened the same
/// way. The body's filter is returned to the caller, which ANDs it into the
/// outer `WHERE` — sound only while the columns are never NULL-extended by an
/// outer join, which the caller checks against the join kinds.
fn flatten_body(
    query: &SqlBoundSelect,
    alias: &str,
    columns: &[String],
    subject: &str,
    visible: &[SqlCatalogCte],
    scope: &SqlCorrelationScope,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
    occurrences: &mut std::collections::BTreeMap<u64, u64>,
) -> Result<(SqlCatalogBoundTable, Option<SqlCatalogBoundExpression>), SqlFrontendError> {
    if !query.joins.is_empty() {
        return Err(unsupported_source(subject, "its inner query joins more than one table"));
    }
    if !query.with.is_empty() {
        return Err(unsupported_source(subject, "its inner query declares its own WITH clause"));
    }
    for (present, clause) in [
        (query.group_by.is_some(), "GROUP BY"),
        (query.having.is_some(), "HAVING"),
        (query.distinct, "DISTINCT"),
        (query.order_by.is_some(), "ORDER BY"),
        (query.limit.is_some() || query.offset.is_some(), "LIMIT/OFFSET"),
    ] {
        if present {
            return Err(unsupported_source(subject, &format!("its inner query carries {clause}")));
        }
    }
    let Some(source) = query.source.as_ref()
    else {
        return Err(unsupported_source(subject, "its inner query has no table source"));
    };
    let SqlBoundSource::Table { name, alias: inner_alias } = source
    else {
        return Err(unsupported_source(subject, "its inner query selects from another derived table"));
    };
    // 内层来源既可以是 catalog 表，也可以是更早定义的 CTE；非递归保证这条链必然终止。
    // 更早 CTE 的显式列名同样作用于这里的内层展开，否则 `SELECT x FROM renamed_cte`
    // 会在扁平化时按基表字段名解析失败。
    let (table, inner_predicate) = match visible.iter().rev().find(|cte| cte.name == *name) {
        Some(cte) => flatten_body(
            &cte.query,
            inner_alias.as_deref().unwrap_or(name),
            &cte.columns,
            "common table expression",
            &cte.visible,
            scope,
            catalog,
            variables,
            occurrences,
        )?,
        None => {
            let base = find_table(catalog, name)?;
            (assign_row_slot(SqlCatalogBoundTable { alias: inner_alias.clone(), ..base }, occurrences), None)
        }
    };
    // 行槽已在展开时确定，过滤条件必须绑定在这一份行值上：这样一条带过滤的 CTE
    // 与同一张表的另一次出现共存时，过滤不会落到别的行槽上。过滤使用 body 自己的
    // 列名作用域，显式输出列名只改外层看到的列名。
    let Some(field_indices) = projected_field_indices(&query.projections, std::slice::from_ref(&table), catalog)?
    else {
        return Err(unsupported_source(subject, "its inner projection must be `*` or every column of the source table"));
    };
    let predicate = query
        .predicate
        .clone()
        .map(|expression| {
            bind_expression(
                expression,
                std::slice::from_ref(&table),
                visible,
                &scope.from_clause(std::slice::from_ref(&table)),
                catalog,
                variables,
            )
        })
        .transpose()?;
    let mut table = table;
    table.alias = Some(alias.to_string());
    table.column_names = renamed_column_names(&table, &field_indices, columns, alias, subject, catalog)?;
    Ok((table, merge_predicates(inner_predicate, predicate)))
}

/// Joins two row filters with `AND`, keeping whichever side exists.
fn merge_predicates(left: Option<SqlCatalogBoundExpression>, right: Option<SqlCatalogBoundExpression>) -> Option<SqlCatalogBoundExpression> {
    match (left, right) {
        (None, None) => None,
        (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
        (Some(left), Some(right)) => {
            Some(SqlCatalogBoundExpression::Binary { left: Box::new(left), operator: SqlBoundBinaryOperator::And, right: Box::new(right) })
        }
    }
}

/// The catalog field indices a flattened body's projection exposes, in projection
/// order, when the projection names exactly every column of its source table.
///
/// `*` qualifies and yields the catalog field order; so does one unaliased column
/// reference per catalog field, in any order. Any other shape returns `None`, which
/// is what kept the outer query from addressing the source table directly.
fn projected_field_indices(
    projections: &[SqlBoundProjection],
    source: &[SqlCatalogBoundTable],
    catalog: &SqlCatalog,
) -> Result<Option<Vec<usize>>, SqlFrontendError> {
    let table = catalog
        .tables
        .iter()
        .find(|candidate| candidate.type_id == source[0].type_id)
        .ok_or_else(|| internal_error("catalog table identity is not published"))?;
    if matches!(projections, [SqlBoundProjection::Star]) {
        return Ok(Some((0..table.fields.len()).collect()));
    }
    let mut covered = Vec::with_capacity(projections.len());
    for projection in projections {
        let SqlBoundProjection::Expression { expression, alias: None } = projection
        else {
            return Ok(None);
        };
        let SqlBoundExpression::Identifier(name) = expression
        else {
            return Ok(None);
        };
        let Ok(column) = find_field(name, source, catalog)
        else {
            return Ok(None);
        };
        if covered.contains(&column.field_id) {
            return Ok(None);
        }
        covered.push(column.field_id);
    }
    if covered.len() != table.fields.len() || !table.fields.iter().all(|field| covered.contains(&field.field_id)) {
        return Ok(None);
    }
    let indices = table.fields.iter().map(|field| covered.iter().position(|id| *id == field.field_id)).collect::<Option<Vec<_>>>();
    Ok(indices)
}

/// Applies a CTE's explicit output column list to the flattened column scope.
///
/// The body already exposes every column of its single base table, so the written
/// names replace the visible names of exactly those columns, positionally. The
/// result stays aligned with the catalog field order: catalog names, `field_id`s
/// and the row slot are untouched, only the names this occurrence answers to.
fn renamed_column_names(
    table: &SqlCatalogBoundTable,
    field_indices: &[usize],
    columns: &[String],
    alias: &str,
    subject: &str,
    catalog: &SqlCatalog,
) -> Result<Option<Vec<String>>, SqlFrontendError> {
    if columns.is_empty() {
        return Ok(table.column_names.clone());
    }
    let catalog_table = catalog
        .tables
        .iter()
        .find(|candidate| candidate.type_id == table.type_id)
        .ok_or_else(|| internal_error("catalog table identity is not published"))?;
    if columns.len() != field_indices.len() {
        return Err(unsupported_source(
            subject,
            &format!("the explicit column list of `{alias}` names {} columns but its body outputs {}", columns.len(), field_indices.len()),
        ));
    }
    let mut names: Vec<String> =
        catalog_table.fields.iter().enumerate().map(|(index, field)| table.visible_column_name(index, field)).collect();
    for (column, index) in columns.iter().zip(field_indices) {
        names[*index] = column.clone();
    }
    Ok(Some(names))
}

fn unsupported_source(subject: &str, reason: &str) -> SqlFrontendError {
    SqlFrontendError::unsupported(format!("SQL {subject} cannot be flattened: {reason}"))
}

fn find_table(catalog: &SqlCatalog, name: &str) -> Result<SqlCatalogBoundTable, SqlFrontendError> {
    let matches = catalog.tables.iter().filter(|table| table.name == name).collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Err(SqlFrontendError::new(SqlErrorKind::UnknownTable, format!("unknown SQL table: {name}"))),
        [table] => Ok(SqlCatalogBoundTable { type_id: table.type_id, name: table.name.clone(), alias: None, row_slot: 0, column_names: None }),
        _ => Err(SqlFrontendError::new(SqlErrorKind::AmbiguousTable, format!("ambiguous SQL table: {name}"))),
    }
}

/// Resolves a column reference against every table visible at the current scope.
///
/// `table.column` spellings resolve inside the named table. Unqualified names must
/// be unique across the visible tables — an ambiguity is reported instead of
/// guessing.
fn find_field(name: &str, sources: &[SqlCatalogBoundTable], catalog: &SqlCatalog) -> Result<SqlCatalogBoundColumn, SqlFrontendError> {
    if sources.is_empty() {
        return Err(SqlFrontendError::new(SqlErrorKind::UnknownColumn, "SQL column reference requires a table source"));
    }
    if let Some((qualifier, column)) = name.split_once('.') {
        let qualifier = qualifier.trim();
        let column = column.trim();
        let source = sources
            .iter()
            .find(|source| source.visible_name() == qualifier)
            .ok_or_else(|| SqlFrontendError::new(SqlErrorKind::UnknownTableQualifier, format!("unknown SQL table qualifier: {qualifier}")))?;
        return find_field_in_table(column, source, catalog)?
            .ok_or_else(|| SqlFrontendError::new(SqlErrorKind::UnknownColumn, format!("unknown SQL column: {qualifier}.{column}")));
    }
    let mut matches = Vec::new();
    for source in sources {
        if let Some(column) = find_field_in_table(name, source, catalog)? {
            matches.push(column);
        }
    }
    match matches.len() {
        0 => Err(SqlFrontendError::new(SqlErrorKind::UnknownColumn, format!("unknown SQL column: {name}"))),
        1 => Ok(matches.remove(0)),
        _ => Err(SqlFrontendError::new(SqlErrorKind::AmbiguousColumn, format!("ambiguous SQL column: {name} (qualify it with a table name)"))),
    }
}

/// Looks one column up inside a single catalog table.
fn find_field_in_table(
    name: &str,
    source: &SqlCatalogBoundTable,
    catalog: &SqlCatalog,
) -> Result<Option<SqlCatalogBoundColumn>, SqlFrontendError> {
    let table = catalog
        .tables
        .iter()
        .find(|candidate| candidate.type_id == source.type_id)
        .ok_or_else(|| internal_error("catalog table identity is not published"))?;
    // 显式列名把输出列改名后，本 occurrence 的可见作用域整体换名：按基表字段名
    // 解析旧名会读到一个外层看不到的列，因此这里只认这一份可见名。
    let matches =
        table.fields.iter().enumerate().filter(|(index, field)| source.visible_column_name(*index, field) == name).collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(None),
        [(index, field)] => {
            reject_unknown_field(field)?;
            Ok(Some(SqlCatalogBoundColumn {
                type_id: table.type_id,
                field_id: field.field_id,
                row_id: source.row_id(field.field_id),
                table_name: table.name.clone(),
                field_name: field.name.clone(),
                visible_name: source.visible_column_name(*index, field),
                capacity: field.capacity,
            }))
        }
        _ => Err(SqlFrontendError::new(SqlErrorKind::AmbiguousColumn, format!("ambiguous SQL column: {name}"))),
    }
}

/// Returns an `Unsupported` error when the resolved field type has no SQL
/// equivalent (the inner type fell back to [`SqlFieldType::Unknown`]).
///
/// Called from every path that binds a column reference against a catalog —
/// SELECT projections, WHERE clauses, JOIN conditions, UPDATE assignments,
/// INSERT column lists, and `ON CONFLICT DO UPDATE` values. Keeping the check
/// here instead of letting the execution layer crash lets the binder produce
/// one clear diagnostic with the original VOS type name.
pub(crate) fn reject_unknown_field(field: &SqlCatalogField) -> Result<(), SqlFrontendError> {
    if field.field_type.inner() != SqlFieldType::Unknown {
        return Ok(());
    }
    let vos = field.vos_type_name.as_deref().unwrap_or("unknown");
    Err(SqlFrontendError::new(
        SqlErrorKind::Unsupported,
        format!("SQL cannot project field `{}`: VOS composite type `{vos}` has no SQL equivalent — use VOS operations instead", field.name),
    ))
}

/// Recursively collects the row-value key of every column reference in an expression,
/// preserving first-appearance order and skipping duplicates.
pub fn collect_column_ids(expr: &SqlCatalogBoundExpression, out: &mut Vec<u64>) {
    match expr {
        SqlCatalogBoundExpression::Column(col) => {
            if !out.contains(&col.row_id) {
                out.push(col.row_id);
            }
        }
        SqlCatalogBoundExpression::Function { args, window, .. } => {
            for arg in args {
                collect_column_ids(arg, out);
            }
            if let Some(window) = window {
                for expr in &window.partition_by {
                    collect_column_ids(expr, out);
                }
                for item in &window.order_by {
                    collect_column_ids(&item.expression, out);
                }
            }
        }
        SqlCatalogBoundExpression::Binary { left, right, .. } => {
            collect_column_ids(left, out);
            collect_column_ids(right, out);
        }
        SqlCatalogBoundExpression::Unary { expression, .. } => collect_column_ids(expression, out),
        SqlCatalogBoundExpression::InSubquery { expression, plan, .. } => {
            collect_column_ids(expression, out);
            crate::sql_planner::collect_plan_outer_row_ids(plan, out);
        }
        SqlCatalogBoundExpression::InValues { expression, .. } => collect_column_ids(expression, out),
        SqlCatalogBoundExpression::Case { branches, else_branch } => {
            for (w, t) in branches {
                collect_column_ids(w, out);
                collect_column_ids(t, out);
            }
            if let Some(els) = else_branch {
                collect_column_ids(els, out);
            }
        }
        SqlCatalogBoundExpression::Cast { expr, .. } => collect_column_ids(expr, out),
        // 子查询对外层列的引用绑成 `OuterColumn`，值取自外层行而不是本层行值，因此
        // 子查询的 `EXISTS` / 标量子查询本身不为本层贡献行值键；外层行的行值键由
        // 外层计划的表达式收集，这里只把子查询计划里的外层引用透传出去。
        SqlCatalogBoundExpression::Exists { plan, .. } | SqlCatalogBoundExpression::ScalarSubquery { plan } => {
            crate::sql_planner::collect_plan_outer_row_ids(plan, out)
        }
        // 外层列不在本层的行值里，不能要求本层行携带它的键。
        SqlCatalogBoundExpression::OuterColumn(_) => {}
        SqlCatalogBoundExpression::Literal(_) | SqlCatalogBoundExpression::Wildcard => {}
    }
}

fn internal_error(message: &str) -> SqlFrontendError {
    SqlFrontendError::internal(message)
}
