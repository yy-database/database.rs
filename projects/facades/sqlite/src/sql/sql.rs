//! Minimal SQL front end for the format-3 writer.
//!
//! Supported statements, deliberately small and fail-closed:
//!
//! | Statement | Shape |
//! |-----------|-------|
//! | `CREATE TABLE` | column list with `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `DEFAULT` |
//! | `CREATE INDEX` | single or multi-column index, optional `UNIQUE` and `IF NOT EXISTS` |
//! | `CREATE VIEW` | `CREATE VIEW name AS SELECT ...`, re-evaluated on every read |
//! | `CREATE TRIGGER` | `BEFORE` / `AFTER` `INSERT` / `UPDATE [OF cols]` / `DELETE` on one table, one or more body statements |
//! | `INSERT` | optional column list, one or more `VALUES` tuples, optional `RETURNING` |
//! | `UPDATE` | optional `AS` alias, one `SET c = expr` per column (bare left-hand side), optional `WHERE` and `RETURNING` |
//! | `DELETE` | optional `AS` alias, optional `WHERE` and `RETURNING` |
//! | `SELECT` | one or more comma-separated table references, `count(*)`, `*`, or an explicit column list, with optional `WHERE`, `ORDER BY` and `LIMIT` |
//!
//! `WHERE` accepts plain predicates only: `=`, `<>`, `!=`, `<`, `>`, `<=`, `>=`,
//! `AND`, `OR`, `NOT`, `IS NULL`, `IS NOT NULL`, `BETWEEN` / `NOT BETWEEN`,
//! parenthesized groups, column references and literals.
//! `ORDER BY` accepts one or more columns with `ASC` / `DESC`.
//!
//! A `FROM` clause may list several tables separated by commas, each with an
//! optional `AS` alias (`t1 AS a`, `t1 a`). The comma form means a Cartesian
//! product; column references may be qualified by the alias (`a.x`) or left
//! bare when exactly one source defines that column.
//!
//! Scalar subqueries `(SELECT ...)`, `EXISTS (SELECT ...)`,
//! `x [NOT] IN (SELECT ...)` and whole-result aggregates (`count`, `avg`,
//! `sum`, `min`, `max`) are supported inside expressions. A subquery reuses the
//! same `SELECT` evaluator, so correlation and set operators behave identically
//! at every nesting depth.
//!
//! Anything else (functions in `FROM`, row-value `SET (a, b) = (...)`,
//! `UPDATE OR ...` conflict policies, `SET c = DEFAULT`, `RETURNING *, ...`,
//! `ORDER BY` / `LIMIT` on `UPDATE` and `DELETE`, `ALTER`, `WITH RECURSIVE`,
//! the `GROUPS` / `EXCLUDE` frame units) is rejected with a structured error
//! instead of being silently approximated. Every rejection carries the
//! statement kind, the offending clause when one is recognized, and the byte
//! offset inside the input.
//!
//! ## Transitional shapes
//!
//! Four families are parsed and evaluated inside this facade for now:
//!
//! - window specifications: `OVER (...)` with `PARTITION BY` / `ORDER BY` /
//!   `ROWS` / `RANGE` frames, the ranking / offset / value window functions,
//!   and named `WINDOW` clauses;
//! - `LIMIT` / `OFFSET` expressions, including SQLite's integer coercion of
//!   real and text values and the `-1` / negative meaning "no bound";
//! - `CHECK` constraints, both table-level and column-level, captured verbatim
//!   for the violation message and enforced at insert time.
//!
//! `UPDATE` / `DELETE` with their `RETURNING` projection belong to the same
//! family: the engine plans the whole statement against a snapshot, validates
//! uniqueness in ascending rowid order, and only then writes, so the shapes
//! above are the ones a future `oak-sql` node must reproduce.
//!
//! The schema-editing statements (`DROP TABLE`, `DROP INDEX`, `ALTER TABLE`
//! with `RENAME TO` / `RENAME COLUMN` / `ADD COLUMN` / `DROP COLUMN`) also live
//! here, including the token-level rewrite of the stored `sqlite_master.sql`
//! text.
//!
//! These stay here until `oak-sql` grows window / `LIMIT`-expression / `CHECK`
//! / `UPDATE` / `DELETE` / `RETURNING` / schema-editing AST nodes and
//! `yydb-gateway` lowering can execute them; the shapes above are listed so the
//! hand-off is mechanical rather than a re-discovery.

use crate::{ColumnSpec, WriteValue};

use super::error::SqlError;

/// Removes every `schema.` qualifier from `sql` while keeping the rest of the
/// text byte for byte. SQLite stores the objects of an attached database with
/// the schema qualifier removed, so a database attached under another name
/// later still reloads.
pub fn strip_schema_qualifiers(sql: &str, schema: &str) -> String {
    let Ok((tokens, offsets)) = tokenize(sql)
    else {
        return sql.to_string();
    };
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        let matches_schema = match token {
            Token::Word(word) | Token::Quoted(word) => word.eq_ignore_ascii_case(schema),
            _ => false,
        };
        if !matches_schema || !matches!(tokens.get(index + 1), Some(Token::Dot)) {
            continue;
        }
        let start = offsets.get(index).copied().unwrap_or(0);
        let end = offsets.get(index + 1).copied().map(|dot| dot + 1).unwrap_or(start);
        ranges.push((start, end));
    }
    if ranges.is_empty() {
        return sql.to_string();
    }
    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0;
    for (start, end) in ranges {
        out.push_str(sql.get(cursor..start).unwrap_or_default());
        cursor = end;
    }
    out.push_str(sql.get(cursor..).unwrap_or_default());
    out
}

/// One literal value parsed from a `VALUES` tuple or a `DEFAULT` clause.
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    /// SQL `NULL`.
    Null,
    /// Signed 64-bit integer.
    Integer(i64),
    /// IEEE-754 binary64.
    Real(f64),
    /// Text without quotes, with `''` already collapsed to `'`.
    Text(String),
    /// Blob bytes from `X'…'`.
    Blob(Vec<u8>),
}

impl Literal {
    /// Converts the literal into the writer's value type.
    pub fn to_write_value(&self) -> WriteValue {
        match self {
            Self::Null => WriteValue::Null,
            Self::Integer(value) => WriteValue::Integer(*value),
            Self::Real(value) => WriteValue::Real(*value),
            Self::Text(value) => WriteValue::Text(value.clone()),
            Self::Blob(value) => WriteValue::Blob(value.clone()),
        }
    }
}

/// One value inside an `INSERT ... VALUES` tuple.
#[derive(Clone, Debug, PartialEq)]
pub enum InsertValue {
    /// A literal as written.
    Literal(Literal),
    /// A bound parameter placeholder.
    Parameter {
        /// Zero-based binding index.
        index: usize,
        /// Placeholder text as written.
        text: String,
    },
    /// A general expression, used by trigger bodies such as
    /// `INSERT INTO log VALUES (new.a)`.
    Expression(Expr),
}

/// Projection of a `SELECT` statement.
#[derive(Clone, Debug, PartialEq)]
pub enum SelectItems {
    /// `count(*)`.
    CountAll,
    /// `*`.
    Star,
    /// Explicit expression list in source order.
    Named(Vec<Expr>),
}

/// One `SELECT` core: projection, sources and predicate, without the
/// `ORDER BY` / `LIMIT` that may trail a whole compound query.
#[derive(Clone, Debug, PartialEq)]
pub struct SelectCore {
    /// `DISTINCT` was written: duplicate output rows are collapsed before
    /// `ORDER BY` and `LIMIT`.
    pub distinct: bool,
    /// Selected projection.
    pub items: SelectItems,
    /// Source table references in `FROM` order.
    pub from: Vec<TableRef>,
    /// Optional `WHERE` predicate.
    pub filter: Option<Expr>,
    /// `GROUP BY` expressions in source order; empty when the query is not
    /// grouped.
    pub group: Vec<Expr>,
    /// Optional `HAVING` predicate, evaluated per group.
    pub having: Option<Expr>,
    /// `WINDOW name AS (...)` definitions of this core, in source order.
    pub windows: Vec<NamedWindow>,
}

/// One set operator joining two `SELECT` cores.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompoundOp {
    /// `UNION`: distinct rows from both sides.
    Union,
    /// `UNION ALL`: every row from both sides.
    UnionAll,
    /// `INTERSECT`: distinct rows present on both sides.
    Intersect,
    /// `EXCEPT`: distinct left rows missing from the right side.
    Except,
}

impl CompoundOp {
    /// Operator text as written.
    pub fn text(self) -> &'static str {
        match self {
            Self::Union => "UNION",
            Self::UnionAll => "UNION ALL",
            Self::Intersect => "INTERSECT",
            Self::Except => "EXCEPT",
        }
    }

    /// Offending clause reported in diagnostics (`UNION ALL` reports `UNION`).
    pub fn clause(self) -> &'static str {
        match self {
            Self::Union | Self::UnionAll => "UNION",
            Self::Intersect => "INTERSECT",
            Self::Except => "EXCEPT",
        }
    }
}

/// One core joined by a set operator in a compound `SELECT`.
#[derive(Clone, Debug, PartialEq)]
pub struct CompoundTerm {
    /// Operator joining this core to the accumulated result.
    pub op: CompoundOp,
    /// The core on the right-hand side.
    pub core: SelectCore,
    /// Byte offset of the operator keyword.
    pub position: usize,
}

/// How a `FROM` item is joined to the sources already in scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    /// `[INNER] JOIN ... ON ...`: keep only matching pairs.
    Inner,
    /// `LEFT [OUTER] JOIN ... ON ...`: keep unmatched left rows padded with
    /// `NULL` on the right side.
    Left,
    /// `RIGHT [OUTER] JOIN ... ON ...`: keep unmatched right rows padded with
    /// `NULL` on the left side.
    Right,
    /// `FULL [OUTER] JOIN ... ON ...`: keep unmatched rows from both sides.
    Full,
}

/// One table reference in a `FROM` clause.
#[derive(Clone, Debug, PartialEq)]
pub struct TableRef {
    /// Optional `schema.` qualifier of the table name.
    pub schema: Option<String>,
    /// Table name as written; empty for a derived table.
    pub name: String,
    /// `AS` / bare alias as written.
    pub alias: Option<String>,
    /// `FROM ( SELECT ... )` body when this entry is a derived table.
    pub query: Option<Box<Statement>>,
    /// Join that attaches this source to the previous one; `None` for the
    /// first item and for comma / `CROSS JOIN` products.
    pub join: Option<JoinKind>,
    /// `ON` predicate of the join.
    pub on: Option<Expr>,
    /// `USING (col, ...)` column list of the join, as written.
    pub using: Option<Vec<String>>,
    /// `NATURAL` join: the shared columns are computed against the tables.
    pub natural: bool,
}

impl TableRef {
    /// Name column references use for this source: the alias, else the table
    /// name. An unaliased derived table has no name, so the empty label can
    /// never be matched by a qualifier (SQLite accepts it the same way).
    pub fn label(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// One `WITH` entry: `name [(col, ...)] AS (select)`.
#[derive(Clone, Debug, PartialEq)]
pub struct CommonTable {
    /// CTE name as written.
    pub name: String,
    /// Optional declared output column names.
    pub columns: Option<Vec<String>>,
    /// Body of the CTE.
    pub query: Box<Statement>,
}

/// One column reference, optionally qualified by a table alias.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnRef {
    /// Qualifying table name or alias as written.
    pub qualifier: Option<String>,
    /// Column name as written.
    pub name: String,
}

impl ColumnRef {
    /// Reference text for diagnostics (`t1.a` or `a`).
    pub fn display(&self) -> String {
        match &self.qualifier {
            Some(qualifier) => format!("{qualifier}.{}", self.name),
            None => self.name.clone(),
        }
    }
}

/// Prefix operator inside a scalar expression.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    /// `-`.
    Negate,
    /// `+`.
    Plus,
    /// `~`.
    BitNot,
}

impl UnaryOp {
    /// Operator text used in diagnostics and column labels.
    pub fn text(self) -> &'static str {
        match self {
            Self::Negate => "-",
            Self::Plus => "+",
            Self::BitNot => "~",
        }
    }
}

/// Binary arithmetic operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    /// `+`.
    Add,
    /// `-`.
    Subtract,
    /// `*`.
    Multiply,
    /// `/`; integer division truncates toward zero.
    Divide,
    /// `%`; the remainder keeps the dividend's sign.
    Modulo,
    /// `||`: string concatenation, which binds tighter than `*` in SQLite.
    Concat,
    /// `<<`.
    ShiftLeft,
    /// `>>`.
    ShiftRight,
    /// `&`.
    BitAnd,
    /// `|`, which SQLite does support (unlike `^`).
    BitOr,
}

impl BinaryOp {
    /// Operator text used in diagnostics and column labels.
    pub fn text(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
            Self::Concat => "||",
            Self::ShiftLeft => "<<",
            Self::ShiftRight => ">>",
            Self::BitAnd => "&",
            Self::BitOr => "|",
        }
    }
}

/// One of the scalar functions this engine evaluates.
///
/// Anything else is rejected instead of being approximated, so a corpus runner
/// can tell a missing function apart from a wrong result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarFunction {
    /// `abs(x)`.
    Abs,
    /// `coalesce(x, ...)`, two or more arguments.
    Coalesce,
    /// `length(x)`.
    Length,
    /// `typeof(x)`.
    Typeof,
    /// `lower(x)`.
    Lower,
    /// `upper(x)`.
    Upper,
    /// `substr(x, start[, length])`.
    Substr,
    /// `round(x[, digits])`.
    Round,
    /// `hex(x)`, the hexadecimal text of the value's text form.
    Hex,
    /// `quote(x)`, a SQL literal for the value.
    Quote,
    /// `random()`, a random 64-bit integer.
    Random,
    /// `randomblob(n)`, a blob of `n` random bytes (`n < 1` means one byte).
    RandomBlob,
    /// `instr(x, y)`, the one-based character position of `y` inside `x`.
    Instr,
}

impl ScalarFunction {
    /// Maps a written function name to the supported scalar functions.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "abs" => Some(Self::Abs),
            "coalesce" => Some(Self::Coalesce),
            "length" => Some(Self::Length),
            "typeof" => Some(Self::Typeof),
            "lower" => Some(Self::Lower),
            "upper" => Some(Self::Upper),
            "substr" => Some(Self::Substr),
            "round" => Some(Self::Round),
            "hex" => Some(Self::Hex),
            "quote" => Some(Self::Quote),
            "random" => Some(Self::Random),
            "randomblob" => Some(Self::RandomBlob),
            "instr" => Some(Self::Instr),
            _ => None,
        }
    }

    /// Lowercase function name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Abs => "abs",
            Self::Coalesce => "coalesce",
            Self::Length => "length",
            Self::Typeof => "typeof",
            Self::Lower => "lower",
            Self::Upper => "upper",
            Self::Substr => "substr",
            Self::Round => "round",
            Self::Hex => "hex",
            Self::Quote => "quote",
            Self::Random => "random",
            Self::RandomBlob => "randomblob",
            Self::Instr => "instr",
        }
    }

    /// Whether `count` arguments are accepted.
    pub fn accepts(self, count: usize) -> bool {
        match self {
            Self::Abs | Self::Length | Self::Typeof | Self::Lower | Self::Upper | Self::Hex | Self::Quote => count == 1,
            Self::Coalesce => count >= 2,
            Self::Substr => (2..=3).contains(&count),
            Self::Round | Self::RandomBlob => (1..=2).contains(&count),
            Self::Random => count == 0,
            Self::Instr => count == 2,
        }
    }
}

/// One of the whole-result aggregate functions this engine evaluates.
///
/// Aggregation is only defined for a `SELECT` without `GROUP BY`, which is the
/// shape every supported subquery uses. Unknown aggregates still report the
/// documented `unsupported-clause` error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateFunction {
    /// `count(*)`, counting every row.
    CountStar,
    /// `count(expr)`, ignoring `NULL` arguments.
    Count,
    /// `avg(expr)`, returning a real or `NULL` for an empty input.
    Avg,
    /// `sum(expr)`, returning an integer or a real and `NULL` for an empty input.
    Sum,
    /// `total(expr)`, returning a real and `0.0` for an empty input.
    Total,
    /// `min(expr)`, returning `NULL` for an empty input.
    Min,
    /// `max(expr)`, returning `NULL` for an empty input.
    Max,
    /// `group_concat(expr[, sep])`, skipping `NULL` values.
    GroupConcat,
}

impl AggregateFunction {
    /// Maps a written aggregate name to the supported set.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "count" => Some(Self::Count),
            "avg" => Some(Self::Avg),
            "sum" => Some(Self::Sum),
            "total" => Some(Self::Total),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            "group_concat" => Some(Self::GroupConcat),
            _ => None,
        }
    }

    /// Lowercase function name used in diagnostics and column labels.
    pub fn name(self) -> &'static str {
        match self {
            Self::CountStar | Self::Count => "count",
            Self::Avg => "avg",
            Self::Sum => "sum",
            Self::Total => "total",
            Self::Min => "min",
            Self::Max => "max",
            Self::GroupConcat => "group_concat",
        }
    }
}

/// Comparison operator inside a `WHERE` predicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompareOp {
    /// `=`.
    Eq,
    /// `<>` or `!=`.
    NotEq,
    /// `<`.
    Lt,
    /// `<=`.
    LtEq,
    /// `>`.
    Gt,
    /// `>=`.
    GtEq,
}

/// One key column of a `CREATE INDEX` statement.
///
/// Only ascending keys exist: `ASC` is accepted because it is the default,
/// while `DESC` is rejected with `unsupported-clause` instead of silently
/// building an ascending index.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexColumn {
    /// Column name as written; empty for an expression key.
    pub name: String,
    /// Expression key such as `lower(name)`; `None` for a bare column.
    pub expr: Option<Expr>,
    /// `COLLATE <name>` on this key, if written.
    pub collate: Option<String>,
}

/// One `ORDER BY` key: an expression or a constant ordinal.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderKey {
    /// Sort expression as written; an integer literal is an output-column ordinal.
    pub expr: Expr,
    /// `DESC` was written.
    pub descending: bool,
}

/// Window frame unit. `GROUPS` is deliberately absent: it is rejected at
/// parse time with `unsupported-clause` instead of being approximated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameUnits {
    /// `ROWS`: frame edges count physical rows.
    Rows,
    /// `RANGE`: frame edges follow the single numeric `ORDER BY` value.
    Range,
}

/// One edge of an explicit window frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameEdge {
    /// `UNBOUNDED PRECEDING`.
    UnboundedPreceding,
    /// `n PRECEDING`.
    Preceding,
    /// `CURRENT ROW`.
    CurrentRow,
    /// `n FOLLOWING`.
    Following,
    /// `UNBOUNDED FOLLOWING`.
    UnboundedFollowing,
}

/// One resolved frame edge; `offset` is present for `n PRECEDING/FOLLOWING`.
#[derive(Clone, Debug, PartialEq)]
pub struct FrameBound {
    /// Which edge this is.
    pub edge: FrameEdge,
    /// Offset expression, evaluated once as a constant.
    pub offset: Option<Box<Expr>>,
}

/// `ROWS` / `RANGE` frame between two edges.
#[derive(Clone, Debug, PartialEq)]
pub struct Frame {
    /// Unit of the offsets.
    pub units: FrameUnits,
    /// `BETWEEN start AND end` was written rather than the single-edge form.
    pub between: bool,
    /// Starting edge.
    pub start: FrameBound,
    /// Ending edge.
    pub end: FrameBound,
}

/// One window definition written after `OVER`, or in a `WINDOW` clause.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowSpec {
    /// Name of a base window this definition extends.
    pub base: Option<String>,
    /// `PARTITION BY` expressions.
    pub partition: Vec<Expr>,
    /// `ORDER BY` keys.
    pub order: Vec<OrderKey>,
    /// Explicit frame; absent means SQLite's default frame.
    pub frame: Option<Frame>,
}

/// One `WINDOW name AS (...)` entry.
#[derive(Clone, Debug, PartialEq)]
pub struct NamedWindow {
    /// Window name as written.
    pub name: String,
    /// Definition.
    pub spec: WindowSpec,
}

/// One window function call written before `OVER`.
#[derive(Clone, Debug, PartialEq)]
pub enum WindowFunction {
    /// `row_number()`.
    RowNumber,
    /// `rank()`.
    Rank,
    /// `dense_rank()`.
    DenseRank,
    /// `percent_rank()`.
    PercentRank,
    /// `cume_dist()`.
    CumeDist,
    /// `ntile(n)`.
    Ntile(Box<Expr>),
    /// `lag(expr[, offset[, default]])`.
    Lag {
        /// Value read from the target row.
        value: Box<Expr>,
        /// Row offset; `1` when omitted.
        offset: Option<Box<Expr>>,
        /// Value when the target row is outside the partition.
        default: Option<Box<Expr>>,
    },
    /// `lead(expr[, offset[, default]])`.
    Lead {
        /// Value read from the target row.
        value: Box<Expr>,
        /// Row offset; `1` when omitted.
        offset: Option<Box<Expr>>,
        /// Value when the target row is outside the partition.
        default: Option<Box<Expr>>,
    },
    /// `first_value(expr)`.
    FirstValue(Box<Expr>),
    /// `last_value(expr)`.
    LastValue(Box<Expr>),
    /// `nth_value(expr, n)`.
    NthValue {
        /// Value read from the frame.
        value: Box<Expr>,
        /// One-based position inside the frame.
        position: Box<Expr>,
    },
    /// Aggregate function used as a window function.
    Aggregate {
        /// Which supported aggregate.
        function: AggregateFunction,
        /// Argument; `None` only for `count(*)`.
        argument: Option<Box<Expr>>,
        /// `group_concat` separator.
        separator: Option<Box<Expr>>,
    },
}

/// One table-level constraint, in written order.
#[derive(Clone, Debug, PartialEq)]
pub enum TableConstraint {
    /// `[CONSTRAINT name] PRIMARY KEY (a, b)`.
    PrimaryKey {
        /// Key columns in key order.
        columns: Vec<String>,
    },
    /// `[CONSTRAINT name] UNIQUE (a, b)`.
    Unique {
        /// Key columns in key order.
        columns: Vec<String>,
    },
}

impl TableConstraint {
    /// Key columns in key order.
    pub fn columns(&self) -> &[String] {
        match self {
            Self::PrimaryKey { columns, .. } | Self::Unique { columns, .. } => columns,
        }
    }
}

/// One table- or column-level `CHECK` constraint.
#[derive(Clone, Debug, PartialEq)]
pub struct CheckConstraint {
    /// `CONSTRAINT name CHECK (...)` name, when written.
    pub name: Option<String>,
    /// Tested expression.
    pub expr: Expr,
    /// Source text of the expression, echoed in the failure message.
    pub text: String,
}

/// One `REFERENCES` clause, from a column constraint or a table-level
/// `FOREIGN KEY (...) REFERENCES ...`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForeignKey {
    /// Child columns in key order.
    pub columns: Vec<String>,
    /// Parent table name as written.
    pub parent: String,
    /// Parent columns in key order; empty means the parent's primary key.
    pub parent_columns: Vec<String>,
    /// `ON DELETE` action; `NO ACTION` when absent.
    pub on_delete: ForeignKeyAction,
    /// `ON UPDATE` action; `NO ACTION` when absent.
    pub on_update: ForeignKeyAction,
}

/// Referential action written after `ON DELETE` / `ON UPDATE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForeignKeyAction {
    /// `NO ACTION`.
    NoAction,
    /// `RESTRICT`, which SQLite checks immediately like `NO ACTION`.
    Restrict,
    /// `SET NULL`, not implemented by this engine.
    SetNull,
    /// `SET DEFAULT`, not implemented by this engine.
    SetDefault,
    /// `CASCADE`, not implemented by this engine.
    Cascade,
}

/// Trailing `LIMIT` / `OFFSET` of a `SELECT`.
#[derive(Clone, Debug, PartialEq)]
pub struct Limit {
    /// Row count; a negative value means "no upper bound".
    pub count: Expr,
    /// Rows skipped before the count applies.
    pub offset: Option<Expr>,
}

/// One scalar expression.
///
/// A single tree serves projections, `WHERE` and `ORDER BY`, so every value
/// rule (NULL propagation, integer arithmetic, comparison and three-valued
/// logic) lives in exactly one evaluator.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    /// Column reference as written.
    Column(ColumnRef),
    /// Literal value.
    Literal(Literal),
    /// Bound parameter placeholder, addressing the statement's binding order.
    Parameter {
        /// Zero-based index in declaration order.
        index: usize,
        /// Placeholder text as written, used as the derived column label.
        text: String,
    },
    /// Prefix `-` or `+`.
    Unary {
        /// Operator.
        op: UnaryOp,
        /// Operand.
        value: Box<Expr>,
    },
    /// Arithmetic `+`, `-`, `*` or `/`.
    Binary {
        /// Left operand.
        left: Box<Expr>,
        /// Operator.
        op: BinaryOp,
        /// Right operand.
        right: Box<Expr>,
    },
    /// Value comparison; yields `1`, `0` or `NULL`.
    Compare {
        /// Left operand.
        left: Box<Expr>,
        /// Comparison operator.
        op: CompareOp,
        /// Right operand.
        right: Box<Expr>,
    },
    /// `IS NULL` or `IS NOT NULL`.
    IsNull {
        /// Tested value.
        value: Box<Expr>,
        /// `IS NOT NULL` was written.
        negated: bool,
    },
    /// `IS TRUE`, `IS NOT TRUE`, `IS FALSE` or `IS NOT FALSE`.
    IsTruth {
        /// Tested value.
        value: Box<Expr>,
        /// `TRUE` was written; `false` means `FALSE`.
        truth: bool,
        /// `IS NOT ...` was written.
        negated: bool,
    },
    /// `NOT`, with three-valued logic.
    Not(Box<Expr>),
    /// `AND`, with three-valued logic.
    And(Box<Expr>, Box<Expr>),
    /// `OR`, with three-valued logic.
    Or(Box<Expr>, Box<Expr>),
    /// `CASE` in both the searched and the simple form.
    Case {
        /// Simple-form operand (`CASE x WHEN ...`); absent in the searched form.
        operand: Option<Box<Expr>>,
        /// `WHEN condition THEN result` pairs in source order.
        branches: Vec<(Expr, Expr)>,
        /// `ELSE result`; absent means `NULL`.
        otherwise: Option<Box<Expr>>,
    },
    /// `x IN (a, b, c)` or `x NOT IN (a, b, c)`.
    In {
        /// Tested value.
        value: Box<Expr>,
        /// Candidate values in source order.
        list: Vec<Expr>,
        /// `NOT IN` was written.
        negated: bool,
    },
    /// Scalar function call.
    Function {
        /// Which supported function.
        name: ScalarFunction,
        /// Arguments in source order.
        args: Vec<Expr>,
    },
    /// Scalar subquery `(SELECT ...)`.
    ///
    /// Yields the first row's only column, or `NULL` when the subquery returns
    /// no rows; a subquery returning more than one column is rejected at run
    /// time, matching upstream SQLite.
    Subquery(Box<Statement>),
    /// `EXISTS (SELECT ...)`.
    ///
    /// Always yields `1` or `0`: existence is a two-valued test, so a
    /// surrounding `NOT` can never produce `NULL`.
    Exists(Box<Statement>),
    /// `x IN (SELECT ...)` or `x NOT IN (SELECT ...)`.
    InSubquery {
        /// Tested value.
        value: Box<Expr>,
        /// Subquery producing the candidate column.
        query: Box<Statement>,
        /// `NOT IN` was written.
        negated: bool,
    },
    /// Whole-result aggregate call.
    Aggregate {
        /// Which supported aggregate.
        function: AggregateFunction,
        /// Argument; `None` only for `count(*)`.
        argument: Option<Box<Expr>>,
        /// Second argument of `group_concat(x, sep)`, evaluated per row.
        separator: Option<Box<Expr>>,
    },
    /// `expr AS name` in a projection. The alias only changes the output
    /// label; it never changes the value.
    Alias {
        /// Aliased expression.
        value: Box<Expr>,
        /// Output name.
        name: String,
    },
    /// `CAST(expr AS type)`.
    Cast {
        /// Converted expression.
        value: Box<Expr>,
        /// Target type.
        target: CastType,
    },
    /// `x LIKE y`, `x NOT LIKE y`, optionally with `ESCAPE`.
    Like {
        /// Tested value.
        value: Box<Expr>,
        /// Pattern.
        pattern: Box<Expr>,
        /// `NOT LIKE` was written.
        negated: bool,
        /// `ESCAPE c` operand.
        escape: Option<Box<Expr>>,
    },
    /// Window function call with its `OVER` specification.
    Window {
        /// Which window function.
        function: WindowFunction,
        /// Partitioning, ordering and frame.
        spec: WindowSpec,
    },
}

/// Target of a `CAST` expression, collapsed to SQLite's five storage classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CastType {
    /// `INTEGER`
    Integer,
    /// `REAL`
    Real,
    /// `TEXT`
    Text,
    /// `BLOB`
    Blob,
    /// `NUMERIC`
    Numeric,
}

impl CastType {
    /// Maps a declared type name to its storage class, as SQLite does.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "integer" | "int" | "int2" | "int4" | "int8" | "bigint" | "smallint" | "tinyint" => Some(Self::Integer),
            "real" | "double" | "float" => Some(Self::Real),
            "text" | "varchar" | "char" | "character" | "clob" => Some(Self::Text),
            "blob" => Some(Self::Blob),
            "numeric" | "decimal" | "boolean" => Some(Self::Numeric),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Text => "TEXT",
            Self::Blob => "BLOB",
            Self::Numeric => "NUMERIC",
        }
    }
}

impl Expr {
    /// Deterministic column label, mirroring the written expression.
    ///
    /// A bare column reference is labelled with its declared name only: the
    /// table qualifier is used to resolve references in `WHERE` and `ORDER BY`
    /// but never appears in the result header. Derived labels rebuild the
    /// expression text from these labels, so `u.name + 1` is labelled
    /// `name+1`.
    pub fn label(&self) -> String {
        match self {
            Self::Column(reference) => reference.name.clone(),
            Self::Literal(literal) => literal_label(literal),
            Self::Parameter { text, .. } => text.clone(),
            Self::Unary { op, value } => format!("{}{}", op.text(), value.label()),
            Self::Binary { left, op, right } => format!("{}{}{}", left.label(), op.text(), right.label()),
            Self::Compare { left, op, right } => format!("{}{}{}", left.label(), op.text(), right.label()),
            Self::IsNull { value, negated } => {
                format!("{} IS {}NULL", value.label(), if *negated { "NOT " } else { "" })
            }
            Self::IsTruth { value, truth, negated } => {
                format!("{} IS {}{}", value.label(), if *negated { "NOT " } else { "" }, if *truth { "TRUE" } else { "FALSE" })
            }
            Self::Not(inner) => format!("NOT {}", inner.label()),
            Self::And(left, right) => format!("{} AND {}", left.label(), right.label()),
            Self::Or(left, right) => format!("{} OR {}", left.label(), right.label()),
            Self::Case { operand, branches, otherwise } => {
                let mut text = String::from("CASE");
                if let Some(operand) = operand {
                    text.push(' ');
                    text.push_str(&operand.label());
                }
                for (condition, result) in branches {
                    text.push_str(" WHEN ");
                    text.push_str(&condition.label());
                    text.push_str(" THEN ");
                    text.push_str(&result.label());
                }
                if let Some(otherwise) = otherwise {
                    text.push_str(" ELSE ");
                    text.push_str(&otherwise.label());
                }
                text.push_str(" END");
                text
            }
            Self::In { value, list, negated } => {
                let mut text = value.label();
                text.push_str(if *negated { " NOT IN (" } else { " IN (" });
                for (index, item) in list.iter().enumerate() {
                    if index > 0 {
                        text.push(',');
                    }
                    text.push_str(&item.label());
                }
                text.push(')');
                text
            }
            Self::Function { name, args } => {
                let mut text = format!("{}(", name.name());
                for (index, arg) in args.iter().enumerate() {
                    if index > 0 {
                        text.push(',');
                    }
                    text.push_str(&arg.label());
                }
                text.push(')');
                text
            }
            Self::Alias { name, .. } => name.clone(),
            Self::Cast { value, target } => format!("CAST({} AS {})", value.label(), target.name()),
            Self::Like { value, pattern, negated, escape } => {
                let mut text = format!("{} {}LIKE {}", value.label(), if *negated { "NOT " } else { "" }, pattern.label());
                if let Some(escape) = escape {
                    text.push_str(" ESCAPE ");
                    text.push_str(&escape.label());
                }
                text
            }
            Self::Subquery(_) => "subquery".to_string(),
            Self::Exists(_) => "EXISTS subquery".to_string(),
            Self::InSubquery { value, negated, .. } => {
                format!("{}{} IN subquery", value.label(), if *negated { " NOT" } else { "" })
            }
            Self::Aggregate { function, argument, separator } => {
                let mut label = match argument {
                    None => format!("{}(*)", function.name()),
                    Some(argument) => format!("{}({}", function.name(), argument.label()),
                };
                if let Some(separator) = separator {
                    label.push_str(&format!(", {}", separator.label()));
                }
                if argument.is_some() {
                    label.push(')');
                }
                label
            }
            Self::Window { function, spec } => {
                format!("{} OVER {}", window_function_label(function), window_spec_label(spec))
            }
        }
    }
}

/// Rebuilt call text of a window function, as in `sum(v) OVER (...)`.
fn window_function_label(function: &WindowFunction) -> String {
    let mut text = String::new();
    match function {
        WindowFunction::RowNumber => text.push_str("row_number("),
        WindowFunction::Rank => text.push_str("rank("),
        WindowFunction::DenseRank => text.push_str("dense_rank("),
        WindowFunction::PercentRank => text.push_str("percent_rank("),
        WindowFunction::CumeDist => text.push_str("cume_dist("),
        WindowFunction::Ntile(buckets) => {
            text.push_str("ntile(");
            text.push_str(&buckets.label());
        }
        WindowFunction::Lag { value, offset, default } | WindowFunction::Lead { value, offset, default } => {
            let name = if matches!(function, WindowFunction::Lag { .. }) { "lag(" } else { "lead(" };
            text.push_str(name);
            text.push_str(&value.label());
            if let Some(offset) = offset {
                text.push(',');
                text.push_str(&offset.label());
            }
            if let Some(default) = default {
                text.push(',');
                text.push_str(&default.label());
            }
        }
        WindowFunction::FirstValue(value) => {
            text.push_str("first_value(");
            text.push_str(&value.label());
        }
        WindowFunction::LastValue(value) => {
            text.push_str("last_value(");
            text.push_str(&value.label());
        }
        WindowFunction::NthValue { value, position } => {
            text.push_str("nth_value(");
            text.push_str(&value.label());
            text.push(',');
            text.push_str(&position.label());
        }
        WindowFunction::Aggregate { function, argument, separator } => {
            text.push_str(function.name());
            text.push('(');
            match argument {
                None => text.push('*'),
                Some(argument) => text.push_str(&argument.label()),
            }
            if let Some(separator) = separator {
                text.push(',');
                text.push_str(&separator.label());
            }
            text.push(')');
        }
    }
    text
}

/// Rebuilt `OVER` clause text, including the frame when one was written.
fn window_spec_label(spec: &WindowSpec) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(base) = &spec.base {
        parts.push(base.clone());
    }
    if !spec.partition.is_empty() {
        let list: Vec<String> = spec.partition.iter().map(Expr::label).collect();
        parts.push(format!("PARTITION BY {}", list.join(",")));
    }
    if !spec.order.is_empty() {
        let list: Vec<String> =
            spec.order.iter().map(|key| format!("{}{}", key.expr.label(), if key.descending { " DESC" } else { "" })).collect();
        parts.push(format!("ORDER BY {}", list.join(",")));
    }
    if let Some(frame) = &spec.frame {
        parts.push(frame_label(frame));
    }
    if parts.len() == 1 && spec.base.is_some() {
        return spec.base.clone().unwrap_or_default();
    }
    format!("({})", parts.join(" "))
}

fn frame_label(frame: &Frame) -> String {
    let units = match frame.units {
        FrameUnits::Rows => "ROWS",
        FrameUnits::Range => "RANGE",
    };
    if frame.between {
        format!("{} BETWEEN {} AND {}", units, frame_bound_label(&frame.start), frame_bound_label(&frame.end))
    }
    else {
        format!("{} {}", units, frame_bound_label(&frame.start))
    }
}

fn frame_bound_label(bound: &FrameBound) -> String {
    match bound.edge {
        FrameEdge::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
        FrameEdge::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
        FrameEdge::CurrentRow => "CURRENT ROW".to_string(),
        FrameEdge::Preceding => format!("{} PRECEDING", bound.offset.as_ref().map(|expr| expr.label()).unwrap_or_default()),
        FrameEdge::Following => format!("{} FOLLOWING", bound.offset.as_ref().map(|expr| expr.label()).unwrap_or_default()),
    }
}

impl CompareOp {
    /// Operator text used in diagnostics and column labels.
    pub fn text(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "<>",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
        }
    }
}

fn literal_label(literal: &Literal) -> String {
    match literal {
        Literal::Null => "NULL".to_string(),
        Literal::Integer(value) => value.to_string(),
        Literal::Real(value) => value.to_string(),
        Literal::Text(value) => format!("'{}'", value.replace('\'', "''")),
        Literal::Blob(bytes) => {
            let mut text = String::from("X'");
            for byte in bytes {
                text.push_str(&format!("{byte:02x}"));
            }
            text.push('\'');
            text
        }
    }
}

/// One `SET <column> = <expr>` item of an `UPDATE`.
#[derive(Clone, Debug, PartialEq)]
pub struct Assignment {
    /// Target column as written; SQLite accepts a bare name only, so a
    /// table-qualified left-hand side is a syntax error.
    pub column: String,
    /// Right-hand side, evaluated against the row as it was before the
    /// statement started.
    pub value: Expr,
}

/// `ON CONFLICT` clause of an `INSERT`.
#[derive(Clone, Debug, PartialEq)]
pub struct ConflictClause {
    /// Conflict target columns; `None` when no target was written, which
    /// catches every uniqueness conflict.
    pub target: Option<Vec<String>>,
    /// Optional `WHERE` predicate narrowing the target to a partial index.
    pub predicate: Option<Expr>,
    /// `DO NOTHING` or `DO UPDATE SET ...`.
    pub action: ConflictAction,
}

/// What an `ON CONFLICT` clause does with the conflicting row.
#[derive(Clone, Debug, PartialEq)]
pub enum ConflictAction {
    /// Skip the row.
    Nothing,
    /// Rewrite the existing row; `excluded.*` refers to the supplied row.
    Update {
        /// Assignments in source order.
        assignments: Vec<Assignment>,
        /// Optional `WHERE` predicate gating the rewrite.
        predicate: Option<Expr>,
    },
}

/// The single alteration carried by an `ALTER TABLE` statement.
#[derive(Clone, Debug)]
pub enum AlterAction {
    /// `RENAME TO <new name>`.
    RenameTable {
        /// New table name as written.
        new_name: String,
    },
    /// `RENAME [COLUMN] <old> TO <new>`.
    RenameColumn {
        /// Existing column name as written.
        old: String,
        /// New column name as written.
        new: String,
    },
    /// `ADD [COLUMN] <column definition>`.
    AddColumn {
        /// Parsed column definition.
        column: ColumnSpec,
        /// `CHECK` constraints carried by the definition, in source order.
        checks: Vec<CheckConstraint>,
        /// `REFERENCES` clauses carried by the definition, in source order.
        foreign_keys: Vec<ForeignKey>,
        /// Source text of the definition, trimmed exactly as SQLite appends it
        /// to `sqlite_master.sql`.
        sql: String,
    },
    /// `DROP [COLUMN] <name>`; parsed so the engine can reject it by name.
    DropColumn {
        /// Column name as written.
        column: String,
    },
}

/// When a trigger body runs relative to the statement that fired it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TriggerTiming {
    /// Before the event statement.
    Before,
    /// After the event statement.
    After,
    /// In place of the event statement on a view.
    InsteadOf,
}

/// Which statement event fires a trigger.
#[derive(Clone, Debug)]
pub enum TriggerEvent {
    /// `INSERT` on the table.
    Insert,
    /// `UPDATE`, optionally restricted to the listed columns.
    Update(Vec<String>),
    /// `DELETE` on the table.
    Delete,
}

/// One parsed statement.
#[derive(Clone, Debug)]
pub enum Statement {
    /// `CREATE TABLE ...`.
    CreateTable {
        /// Optional `schema.` qualifier of the table name.
        schema: Option<String>,
        /// Table name as written.
        name: String,
        /// `IF NOT EXISTS` was present.
        if_not_exists: bool,
        /// Column definitions in declaration order.
        columns: Vec<ColumnSpec>,
        /// Columns of a table-level `PRIMARY KEY (a, b)` clause, in key order.
        primary_key: Vec<String>,
        /// Table-level constraints in written order, so automatic index
        /// numbers match SQLite (`sqlite_autoindex_<table>_<n>`).
        table_constraints: Vec<TableConstraint>,
        /// Table- and column-level `CHECK` constraints.
        checks: Vec<CheckConstraint>,
        /// Table- and column-level `REFERENCES` clauses, in declaration order.
        foreign_keys: Vec<ForeignKey>,
        /// `STRICT` table option was present.
        strict: bool,
        /// `WITHOUT ROWID` table option was present: the primary key is the
        /// table's b-tree key and there is no implicit `rowid`.
        without_rowid: bool,
        /// `TEMP` / `TEMPORARY`: the table lives in `sqlite_temp_master` and
        /// disappears with the connection.
        temp: bool,
        /// Source text kept verbatim in `sqlite_master.sql`, as SQLite does.
        sql: String,
    },
    /// `CREATE VIEW ... AS SELECT ...`.
    CreateView {
        /// Optional `schema.` qualifier of the view name.
        schema: Option<String>,
        /// View name as written.
        name: String,
        /// `IF NOT EXISTS` was present.
        if_not_exists: bool,
        /// Optional `(a, b)` column list that renames the query output.
        columns: Option<Vec<String>>,
        /// The query the view stores and re-runs on every read.
        query: Box<Statement>,
        /// `TEMP`: the view lives in `sqlite_temp_master` and disappears with
        /// the connection.
        temp: bool,
        /// Source text kept verbatim in `sqlite_master.sql`, as SQLite does.
        sql: String,
    },
    /// `CREATE TRIGGER ... BEGIN ... END`.
    CreateTrigger {
        /// Optional `schema.` qualifier of the trigger name.
        schema: Option<String>,
        /// Trigger name as written.
        name: String,
        /// `IF NOT EXISTS` was present.
        if_not_exists: bool,
        /// `BEFORE`, `AFTER` or `INSTEAD OF`.
        timing: TriggerTiming,
        /// Statement event that fires the trigger.
        event: TriggerEvent,
        /// Optional `schema.` qualifier of the table the trigger is attached to.
        table_schema: Option<String>,
        /// Table the trigger is attached to.
        table: String,
        /// Optional `WHEN` predicate, evaluated before the body.
        when: Option<Expr>,
        /// Body statements in source order.
        body: Vec<Statement>,
        /// Source text kept verbatim in `sqlite_master.sql`.
        sql: String,
    },
    /// `DROP TRIGGER [IF EXISTS] name`.
    DropTrigger {
        /// Optional `schema.` qualifier of the trigger name.
        schema: Option<String>,
        /// Trigger name as written.
        name: String,
        /// `IF EXISTS` was present.
        if_exists: bool,
    },
    /// `DROP VIEW [IF EXISTS] name`.
    DropView {
        /// Optional `schema.` qualifier of the view name.
        schema: Option<String>,
        /// View name as written.
        name: String,
        /// `IF EXISTS` was present.
        if_exists: bool,
    },
    /// `CREATE [UNIQUE] INDEX ... ON ... (...)`.
    CreateIndex {
        /// Optional `schema.` qualifier of the index name.
        schema: Option<String>,
        /// Index name as written.
        name: String,
        /// Indexed table name as written.
        table: String,
        /// `IF NOT EXISTS` was present.
        if_not_exists: bool,
        /// `UNIQUE` was present.
        unique: bool,
        /// Key columns in declaration order.
        columns: Vec<IndexColumn>,
        /// Optional `WHERE` predicate of a partial index.
        predicate: Option<Expr>,
        /// Source text kept verbatim in `sqlite_master.sql`, as SQLite does.
        sql: String,
    },
    /// `INSERT INTO ... VALUES ...` or `INSERT INTO ... DEFAULT VALUES`.
    Insert {
        /// Optional `schema.` qualifier of the target table.
        schema: Option<String>,
        /// Target table name as written.
        table: String,
        /// Optional explicit column list.
        columns: Option<Vec<String>>,
        /// One value tuple per inserted row; empty for `DEFAULT VALUES`.
        rows: Vec<Vec<InsertValue>>,
        /// `DEFAULT VALUES` was written: insert one row built from the column defaults.
        default_values: bool,
        /// Optional `ON CONFLICT` clause.
        conflict: Option<ConflictClause>,
        /// `RETURNING` projection, `*` or an explicit list.
        returning: Option<SelectItems>,
    },
    /// `UPDATE t [AS alias] SET c = expr [, ...] [WHERE expr] [RETURNING ...]`.
    Update {
        /// Optional `schema.` qualifier of the target table.
        schema: Option<String>,
        /// Target table name as written.
        table: String,
        /// `AS alias` of the update target, when one was written.
        alias: Option<String>,
        /// Assignments in source order; a repeated column keeps its last value.
        assignments: Vec<Assignment>,
        /// Optional `WHERE` predicate; every row is visited when absent.
        filter: Option<Expr>,
        /// `RETURNING` projection, `*` or an explicit list.
        returning: Option<SelectItems>,
    },
    /// `DROP TABLE [IF EXISTS] t`.
    DropTable {
        /// Optional `schema.` qualifier of the table name.
        schema: Option<String>,
        /// Table name as written.
        name: String,
        /// `IF EXISTS` was present.
        if_exists: bool,
    },
    /// `DROP INDEX [IF EXISTS] name`.
    DropIndex {
        /// Optional `schema.` qualifier of the index name.
        schema: Option<String>,
        /// Index name as written.
        name: String,
        /// `IF EXISTS` was present.
        if_exists: bool,
    },
    /// `ALTER TABLE t RENAME ...`, `ADD [COLUMN] ...` or `DROP [COLUMN] ...`.
    AlterTable {
        /// Optional `schema.` qualifier of the table name.
        schema: Option<String>,
        /// Table name as written.
        table: String,
        /// The one alteration being applied.
        action: AlterAction,
    },
    /// `DELETE FROM t [AS alias] [WHERE expr] [RETURNING ...]`.
    Delete {
        /// Optional `schema.` qualifier of the target table.
        schema: Option<String>,
        /// Target table name as written.
        table: String,
        /// `AS alias` of the delete target, when one was written.
        alias: Option<String>,
        /// Optional `WHERE` predicate; every row is removed when absent.
        filter: Option<Expr>,
        /// `RETURNING` projection, `*` or an explicit list.
        returning: Option<SelectItems>,
    },
    /// `SELECT ... FROM ... [WHERE ...] [ORDER BY ...] [LIMIT n]`, optionally
    /// joined to further cores by set operators.
    Select {
        /// `WITH` entries visible to every core of this query.
        with: Vec<CommonTable>,
        /// `WITH RECURSIVE` was written. Actual recursion is still rejected
        /// when a CTE references itself.
        recursive: bool,
        /// Leftmost `SELECT` core.
        first: SelectCore,
        /// Set operators and their right-hand cores, in source order.
        compounds: Vec<CompoundTerm>,
        /// `ORDER BY` keys of the whole query in source order.
        order: Vec<OrderKey>,
        /// Optional row limit of the whole query.
        limit: Option<Limit>,
    },
    /// `PRAGMA name [= value]` or `PRAGMA name(value)`.
    Pragma {
        /// Optional `schema.` qualifier of the pragma name.
        schema: Option<String>,
        /// Pragma name as written.
        name: String,
        /// Value as written, when the statement sets the pragma.
        value: Option<String>,
    },
    /// `ATTACH [DATABASE] 'file' AS name`.
    Attach {
        /// Database file as written, already unquoted.
        file: String,
        /// Schema name as written.
        name: String,
    },
    /// `DETACH [DATABASE] name`.
    Detach {
        /// Schema name as written.
        name: String,
    },
    /// `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE` and
    /// `ROLLBACK TO SAVEPOINT`.
    Transaction {
        /// The one transaction action being applied.
        action: TransactionAction,
    },
}

/// One explicit transaction control statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransactionAction {
    /// `BEGIN [DEFERRED | IMMEDIATE | EXCLUSIVE] [TRANSACTION]`.
    Begin,
    /// `COMMIT [TRANSACTION]` or `END [TRANSACTION]`.
    Commit,
    /// `ROLLBACK [TRANSACTION]`; the scope is the whole open transaction.
    Rollback,
    /// `SAVEPOINT name`.
    Savepoint(String),
    /// `RELEASE [SAVEPOINT] name`.
    Release(String),
    /// `ROLLBACK [TRANSACTION] TO [SAVEPOINT] name`.
    RollbackTo(String),
}

/// AST equality, needed because a subquery can appear inside an expression.
///
/// `ColumnSpec` comes from the storage crate and does not implement
/// `PartialEq`, so statements compare through their debug rendering, which is
/// only used by parser assertions.
impl PartialEq for Statement {
    fn eq(&self, other: &Self) -> bool {
        format!("{self:?}") == format!("{other:?}")
    }
}

impl Statement {
    /// Lowercase name reported in `ExecResult::statement`.
    pub fn name(&self) -> &'static str {
        match self {
            Self::CreateTable { .. } => "create-table",
            Self::CreateView { .. } => "create-view",
            Self::CreateTrigger { .. } => "create-trigger",
            Self::DropTrigger { .. } => "drop-trigger",
            Self::DropView { .. } => "drop-view",
            Self::CreateIndex { .. } => "create-index",
            Self::Insert { .. } => "insert",
            Self::Update { .. } => "update",
            Self::Delete { .. } => "delete",
            Self::DropTable { .. } => "drop-table",
            Self::DropIndex { .. } => "drop-index",
            Self::AlterTable { .. } => "alter-table",
            Self::Select { .. } => "select",
            Self::Pragma { .. } => "pragma",
            Self::Attach { .. } => "attach",
            Self::Detach { .. } => "detach",
            Self::Transaction { action } => match action {
                TransactionAction::Begin => "begin",
                TransactionAction::Commit => "commit",
                TransactionAction::Rollback => "rollback",
                TransactionAction::Savepoint(_) => "savepoint",
                TransactionAction::Release(_) => "release",
                TransactionAction::RollbackTo(_) => "rollback-to",
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Word(String),
    Quoted(String),
    Int(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Comma,
    Dot,
    LParen,
    RParen,
    Star,
    Semi,
    Minus,
    Operator(String),
}

impl Token {
    fn describe(&self) -> String {
        match self {
            Self::Word(word) => format!("`{word}`"),
            Self::Quoted(word) => format!("`\"{word}\"`"),
            Self::Int(value) => format!("`{value}`"),
            Self::Real(value) => format!("`{value}`"),
            Self::Text(_) => "a string literal".to_string(),
            Self::Blob(_) => "a blob literal".to_string(),
            Self::Comma => "`,`".to_string(),
            Self::Dot => "`.`".to_string(),
            Self::LParen => "`(`".to_string(),
            Self::RParen => "`)`".to_string(),
            Self::Star => "`*`".to_string(),
            Self::Semi => "`;`".to_string(),
            Self::Minus => "`-`".to_string(),
            Self::Operator(operator) => format!("`{operator}`"),
        }
    }
}

/// Parses one or more semicolon-separated statements.
pub fn parse(source: &str) -> Result<Vec<Statement>, SqlError> {
    let (tokens, offsets) = tokenize(source)?;
    let mut parser = Parser {
        source,
        tokens,
        offsets,
        at: 0,
        end: source.len(),
        kind: None,
        statement_start: 0,
        next_parameter: 0,
        parameter_names: std::collections::BTreeMap::new(),
    };
    let mut statements = Vec::new();
    while !parser.done() {
        if parser.peek() == Some(&Token::Semi) {
            parser.at += 1;
            continue;
        }
        statements.push(parser.statement()?);
        if parser.peek() == Some(&Token::Semi) {
            parser.at += 1;
        }
    }
    Ok(statements)
}

fn tokenize(source: &str) -> Result<(Vec<Token>, Vec<usize>), SqlError> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut offsets = Vec::new();
    let mut at = 0usize;
    while at < bytes.len() {
        let byte = bytes[at];
        match byte {
            byte if byte.is_ascii_whitespace() => at += 1,
            b'-' if bytes.get(at + 1) == Some(&b'-') => {
                while at < bytes.len() && bytes[at] != b'\n' {
                    at += 1;
                }
            }
            b'/' if bytes.get(at + 1) == Some(&b'*') => {
                let start = at;
                at += 2;
                loop {
                    if at + 1 >= bytes.len() {
                        return Err(SqlError::syntax("unterminated `/* ... */` comment", Some(start)));
                    }
                    if bytes[at] == b'*' && bytes[at + 1] == b'/' {
                        at += 2;
                        break;
                    }
                    at += 1;
                }
            }
            b',' => {
                offsets.push(at);
                tokens.push(Token::Comma);
                at += 1;
            }
            b'.' => {
                offsets.push(at);
                tokens.push(Token::Dot);
                at += 1;
            }
            b'(' => {
                offsets.push(at);
                tokens.push(Token::LParen);
                at += 1;
            }
            b')' => {
                offsets.push(at);
                tokens.push(Token::RParen);
                at += 1;
            }
            b'*' => {
                offsets.push(at);
                tokens.push(Token::Star);
                at += 1;
            }
            b';' => {
                offsets.push(at);
                tokens.push(Token::Semi);
                at += 1;
            }
            b'-' => {
                offsets.push(at);
                tokens.push(Token::Minus);
                at += 1;
            }
            // Comparison and arithmetic punctuation is tokenized so a clause
            // rejection (`WHERE x = 1`) reports the clause and offset instead of
            // failing as an unsupported character.
            b'|' => {
                offsets.push(at);
                // `||` is concatenation; a lone `|` has no SQLite meaning and is
                // left to the parser so the error names the operator.
                if bytes.get(at + 1) == Some(&b'|') {
                    tokens.push(Token::Operator("||".to_string()));
                    at += 2;
                }
                else {
                    tokens.push(Token::Operator("|".to_string()));
                    at += 1;
                }
            }
            // `<<` and `>>` are two-character operators; the single-character
            // forms still tokenize separately for the comparisons.
            b'<' if bytes.get(at + 1) == Some(&b'<') => {
                offsets.push(at);
                tokens.push(Token::Operator("<<".to_string()));
                at += 2;
            }
            b'>' if bytes.get(at + 1) == Some(&b'>') => {
                offsets.push(at);
                tokens.push(Token::Operator(">>".to_string()));
                at += 2;
            }
            b'=' | b'<' | b'>' | b'!' | b'+' | b'/' | b'%' | b'&' | b'^' | b'~' | b'?' | b':' => {
                offsets.push(at);
                tokens.push(Token::Operator(char::from(byte).to_string()));
                at += 1;
            }
            b'\'' => {
                let (text, next) = read_quoted(source, at, b'\'', b'\'')?;
                offsets.push(at);
                tokens.push(Token::Text(text));
                at = next;
            }
            b'"' => {
                let (text, next) = read_quoted(source, at, b'"', b'"')?;
                offsets.push(at);
                tokens.push(Token::Quoted(text));
                at = next;
            }
            b'[' => {
                let start = at;
                let mut text = String::new();
                at += 1;
                while at < bytes.len() {
                    if bytes[at] == b']' {
                        break;
                    }
                    text.push(char::from(bytes[at]));
                    at += 1;
                }
                if at >= bytes.len() {
                    return Err(SqlError::syntax("unterminated `[...]` identifier", Some(start)));
                }
                offsets.push(start);
                tokens.push(Token::Quoted(text));
                at += 1;
            }
            b'`' => {
                let (text, next) = read_quoted(source, at, b'`', b'`')?;
                offsets.push(at);
                tokens.push(Token::Quoted(text));
                at = next;
            }
            byte if byte.is_ascii_digit() => {
                let (token, next) = read_number(source, at)?;
                offsets.push(at);
                tokens.push(token);
                at = next;
            }
            byte if is_word_start(byte) => {
                let start = at;
                let mut word = String::new();
                while at < bytes.len() && is_word_part(bytes[at]) {
                    word.push(char::from(bytes[at]));
                    at += 1;
                }
                if word.eq_ignore_ascii_case("x") && bytes.get(at) == Some(&b'\'') {
                    let (text, next) = read_quoted(source, at, b'\'', b'\'')?;
                    offsets.push(start);
                    tokens.push(Token::Blob(decode_hex(&text, start)?));
                    at = next;
                }
                else {
                    offsets.push(start);
                    tokens.push(Token::Word(word));
                }
            }
            other => {
                return Err(SqlError::syntax(format!("unsupported character `{}` in SQL", char::from(other)), Some(at)));
            }
        }
    }
    Ok((tokens, offsets))
}

/// Quotes a name the way SQLite's `%w` formatting does when it appends or
/// substitutes an identifier in stored schema text: bare only when every
/// character is a lowercase letter, digit or underscore and the first
/// character is not a digit.
pub fn quote_identifier(name: &str) -> String {
    let simple = !name.is_empty()
        && !name.as_bytes()[0].is_ascii_digit()
        && name.chars().all(|character| character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_');
    if simple { name.to_string() } else { format!("\"{}\"", name.replace('"', "\"\"")) }
}

/// End offset (exclusive) of one token inside `source`. Only identifiers
/// (`Word` / `Quoted`) are spliced today, but every arm returns a usable end.
fn token_end(source: &str, token: &Token, start: usize) -> usize {
    let bytes = source.as_bytes();
    match token {
        Token::Word(_) => {
            let mut at = start;
            while at < bytes.len() && is_word_part(bytes[at]) {
                at += 1;
            }
            at
        }
        Token::Quoted(_) => match bytes.get(start) {
            Some(b'[') => {
                let mut at = start + 1;
                while at < bytes.len() && bytes[at] != b']' {
                    at += 1;
                }
                (at + 1).min(bytes.len())
            }
            Some(quote) => quoted_end(bytes, start, *quote),
            None => start,
        },
        Token::Text(_) => quoted_end(bytes, start, b'\''),
        Token::Blob(_) => {
            let at = start + 1;
            if bytes.get(at) == Some(&b'\'') { quoted_end(bytes, at, b'\'') } else { at }
        }
        Token::Operator(operator) => start + operator.len(),
        _ => start + 1,
    }
}

/// Offset just past the closing quote, honouring the doubled-quote escape.
fn quoted_end(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut at = start + 1;
    while at < bytes.len() {
        if bytes[at] == quote {
            if bytes.get(at + 1) == Some(&quote) {
                at += 2;
                continue;
            }
            return at + 1;
        }
        at += 1;
    }
    bytes.len()
}

/// Applies `(start, end, replacement)` splices from right to left.
fn splice(source: &str, mut replacements: Vec<(usize, usize, String)>) -> String {
    replacements.sort_by_key(|(start, ..)| *start);
    let mut out = String::with_capacity(source.len() + 16);
    let mut cursor = 0;
    for (start, end, text) in replacements {
        if start > cursor {
            out.push_str(&source[cursor..start]);
        }
        out.push_str(&text);
        cursor = end;
    }
    out.push_str(&source[cursor..]);
    out
}

/// Token index of the object name in a stored `CREATE [UNIQUE] [TEMP] TABLE
/// <name>` or `CREATE ... INDEX <name>` statement.
fn create_object_name(tokens: &[Token], kind: &str) -> Option<usize> {
    let word = |at: usize, expected: &str| matches!(tokens.get(at), Some(Token::Word(text)) if text.eq_ignore_ascii_case(expected));
    if !word(0, "create") {
        return None;
    }
    let mut at = 1;
    if word(at, "temp") || word(at, "temporary") {
        at += 1;
    }
    if word(at, "unique") {
        at += 1;
    }
    if !word(at, kind) {
        return None;
    }
    at += 1;
    if word(at, "if") {
        if !word(at + 1, "not") || !word(at + 2, "exists") {
            return None;
        }
        at += 3;
    }
    match tokens.get(at) {
        Some(Token::Word(_)) | Some(Token::Quoted(_)) => Some(at),
        _ => None,
    }
}

/// Token index of the table name after `ON` in a stored `CREATE INDEX`.
fn index_table_name(tokens: &[Token]) -> Option<usize> {
    let name = create_object_name(tokens, "index")?;
    let mut at = name + 1;
    while at < tokens.len() {
        if matches!(tokens.get(at), Some(Token::Word(word)) if word.eq_ignore_ascii_case("on")) {
            return match tokens.get(at + 1) {
                Some(Token::Word(_)) | Some(Token::Quoted(_)) => Some(at + 1),
                _ => None,
            };
        }
        at += 1;
    }
    None
}

fn identifier_eq(token: &Token, name: &str) -> bool {
    match token {
        Token::Word(word) => word.eq_ignore_ascii_case(name),
        Token::Quoted(word) => word.eq_ignore_ascii_case(name),
        _ => false,
    }
}

/// Collects the splices that rename one identifier token.
fn identifier_splices(source: &str, tokens: &[Token], offsets: &[usize], indexes: &[usize], new: &str) -> Vec<(usize, usize, String)> {
    let text = quote_identifier(new);
    indexes
        .iter()
        .map(|index| {
            let start = offsets[*index];
            (start, token_end(source, &tokens[*index], start), text.clone())
        })
        .collect()
}

/// Rewrites the table name of a stored `CREATE TABLE` statement, quoting the
/// new name the way SQLite does (`CREATE TABLE "u" (...)`, all other bytes
/// untouched).
pub fn rewrite_create_table_name(sql: &str, new_name: &str) -> Option<String> {
    let (tokens, offsets) = tokenize(sql).ok()?;
    let name = create_object_name(&tokens, "table")?;
    let start = offsets[name];
    let end = token_end(sql, &tokens[name], start);
    Some(format!("{}\"{}\"{}", &sql[..start], new_name.replace('"', "\"\""), &sql[end..]))
}

/// Rewrites the `ON <table>` name of a stored `CREATE INDEX` statement.
pub fn rewrite_create_index_table(sql: &str, new_table: &str) -> Option<String> {
    let (tokens, offsets) = tokenize(sql).ok()?;
    let table = index_table_name(&tokens)?;
    let start = offsets[table];
    let end = token_end(sql, &tokens[table], start);
    Some(format!("{}\"{}\"{}", &sql[..start], new_table.replace('"', "\"\""), &sql[end..]))
}

/// Rewrites every reference to `old` in a stored `CREATE TABLE` statement.
/// The table-name token and constraint names are left alone, as SQLite does.
pub fn rewrite_create_table_column(sql: &str, old: &str, new: &str) -> Option<String> {
    let (tokens, offsets) = tokenize(sql).ok()?;
    let table = create_object_name(&tokens, "table")?;
    let mut targets = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if index == table || !identifier_eq(token, old) {
            continue;
        }
        // A name directly followed by `(` is a function call, not a column.
        if matches!(tokens.get(index + 1), Some(Token::LParen)) {
            continue;
        }
        // `CONSTRAINT <name> ...` names a constraint, never a column.
        if index > 0 && matches!(tokens.get(index - 1), Some(Token::Word(word)) if word.eq_ignore_ascii_case("constraint")) {
            continue;
        }
        targets.push(index);
    }
    Some(splice(sql, identifier_splices(sql, &tokens, &offsets, &targets, new)))
}

/// Why a stored `CREATE TABLE` cannot lose one column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropColumnConflict {
    /// A table-level `CHECK` constraint mentions the column.
    TableCheck,
    /// A table-level `FOREIGN KEY (...)` lists the column.
    TableForeignKey,
}

/// Rewrites a stored `CREATE TABLE` by removing the whole definition of
/// `column`, including every constraint written inside it, plus the comma that
/// separated it. Every other byte stays untouched, exactly as SQLite rewrites
/// `sqlite_master.sql` after `ALTER TABLE ... DROP COLUMN`.
///
/// A table-level `CHECK` that mentions the column or a table-level `FOREIGN
/// KEY` whose child list names it cannot survive the drop, so those are
/// reported instead of rewritten. A column-level constraint belongs to the
/// column definition and disappears with it, which is why it is not checked.
pub fn rewrite_create_table_drop_column(sql: &str, column: &str) -> Result<Option<String>, DropColumnConflict> {
    let Ok((tokens, offsets)) = tokenize(sql)
    else {
        return Ok(None);
    };
    let Some(table) = create_object_name(&tokens, "table")
    else {
        return Ok(None);
    };
    let Some(open) = (table + 1..tokens.len()).find(|index| matches!(tokens[*index], Token::LParen))
    else {
        return Ok(None);
    };
    // Split the column list into top-level segments, remembering the commas on
    // both sides so the splice can remove exactly one definition.
    let mut segments: Vec<(usize, usize, Option<usize>, Option<usize>)> = Vec::new();
    let mut start = open + 1;
    let mut before = None;
    let mut depth = 0usize;
    let mut close = None;
    for index in open + 1..tokens.len() {
        match tokens[index] {
            Token::LParen => depth += 1,
            Token::RParen => {
                if depth == 0 {
                    segments.push((start, index, before, None));
                    close = Some(index);
                    break;
                }
                depth -= 1;
            }
            Token::Comma if depth == 0 => {
                segments.push((start, index, before, Some(index)));
                before = Some(index);
                start = index + 1;
            }
            _ => {}
        }
    }
    let Some(_close) = close
    else {
        return Ok(None);
    };
    let mut target = None;
    for (start, end, before, after) in &segments {
        let mut effective = *start;
        if matches!(tokens.get(effective), Some(Token::Word(word)) if word.eq_ignore_ascii_case("constraint")) {
            effective = (effective + 2).min(*end);
        }
        let keyword = match tokens.get(effective) {
            Some(Token::Word(word)) => Some(word.to_ascii_lowercase()),
            _ => None,
        };
        match keyword.as_deref() {
            Some("check") => {
                let rest = tokens.get(effective + 1..*end).unwrap_or(&[]);
                if rest
                    .iter()
                    .enumerate()
                    .any(|(offset, token)| identifier_eq(token, column) && !matches!(tokens.get(effective + 2 + offset), Some(Token::LParen)))
                {
                    return Err(DropColumnConflict::TableCheck);
                }
            }
            Some("foreign") => {
                let mut inside = false;
                for index in effective + 1..*end {
                    match tokens[index] {
                        Token::LParen if !inside => inside = true,
                        Token::RParen if inside => break,
                        ref token if inside && identifier_eq(token, column) => {
                            return Err(DropColumnConflict::TableForeignKey);
                        }
                        _ => {}
                    }
                }
            }
            Some("primary") | Some("unique") => {}
            _ => {
                if effective < *end && identifier_eq(&tokens[effective], column) {
                    target = Some((*start, *end, *before, *after));
                }
            }
        }
    }
    let Some((start, end, before, after)) = target
    else {
        return Ok(None);
    };
    let last = end.saturating_sub(1);
    let end_byte = token_end(sql, &tokens[last], offsets[last]);
    let removal = match (after, before) {
        (Some(comma), _) => (offsets[start], offsets[comma + 1]),
        (None, Some(comma)) => (offsets[comma], end_byte),
        (None, None) => (offsets[start.min(end)], end_byte),
    };
    let _ = close;
    Ok(Some(splice(sql, vec![(removal.0, removal.1, String::new())])))
}

/// Rewrites every reference to `old` in a stored `CREATE INDEX` statement.
pub fn rewrite_create_index_column(sql: &str, old: &str, new: &str) -> Option<String> {
    let (tokens, offsets) = tokenize(sql).ok()?;
    let name = create_object_name(&tokens, "index")?;
    let table = index_table_name(&tokens)?;
    let mut targets = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if index == name || index == table || !identifier_eq(token, old) {
            continue;
        }
        if matches!(tokens.get(index + 1), Some(Token::LParen)) {
            continue;
        }
        targets.push(index);
    }
    Some(splice(sql, identifier_splices(sql, &tokens, &offsets, &targets, new)))
}

/// Token index just after the top-level `AS` of a stored `CREATE VIEW`.
fn view_query_start(tokens: &[Token]) -> Option<usize> {
    let name = create_object_name(tokens, "view")?;
    let mut depth = 0usize;
    for index in name + 1..tokens.len() {
        match tokens[index] {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            Token::Word(ref word) if depth == 0 && word.eq_ignore_ascii_case("as") => return Some(index + 1),
            _ => {}
        }
    }
    None
}

/// The identifier token that names a table at `at`, skipping a `schema.`
/// qualifier.
fn table_name_token(tokens: &[Token], at: usize) -> Option<usize> {
    match tokens.get(at) {
        Some(Token::Word(_)) | Some(Token::Quoted(_)) => {
            if matches!(tokens.get(at + 1), Some(Token::Dot)) && matches!(tokens.get(at + 2), Some(Token::Word(_)) | Some(Token::Quoted(_))) {
                Some(at + 2)
            }
            else {
                Some(at)
            }
        }
        _ => None,
    }
}

/// Token indexes of every table reference inside one view query. `FROM`
/// lists are comma separated and every `JOIN` introduces one more source.
fn view_table_tokens(tokens: &[Token], start: usize) -> Vec<usize> {
    let mut targets = Vec::new();
    let mut depth = 0usize;
    let mut from: Option<usize> = None;
    let mut at = start;
    while at < tokens.len() {
        match &tokens[at] {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth = depth.saturating_sub(1);
                if from.is_some_and(|level| depth < level) {
                    from = None;
                }
            }
            Token::Comma if from == Some(depth) => {
                if let Some(index) = table_name_token(tokens, at + 1) {
                    targets.push(index);
                }
            }
            Token::Word(word) => {
                let lower = word.to_ascii_lowercase();
                match lower.as_str() {
                    "from" => {
                        from = Some(depth);
                        if let Some(index) = table_name_token(tokens, at + 1) {
                            targets.push(index);
                        }
                    }
                    "join" => {
                        if let Some(index) = table_name_token(tokens, at + 1) {
                            targets.push(index);
                        }
                    }
                    "where" | "group" | "order" | "having" | "window" | "limit" | "union" | "intersect" | "except" => {
                        if from == Some(depth) {
                            from = None;
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        at += 1;
    }
    targets
}

/// Rewrites every table reference to `old` in a stored `CREATE VIEW`, quoting
/// the new name the way SQLite does (`from "t2"`). Only the query is touched;
/// the view name and its declared output columns keep their bytes.
pub fn rewrite_view_table(sql: &str, old: &str, new: &str) -> Option<String> {
    let (tokens, offsets) = tokenize(sql).ok()?;
    let start = view_query_start(&tokens)?;
    let mut targets = view_table_tokens(&tokens, start);
    for index in start..tokens.len() {
        if identifier_eq(&tokens[index], old) && matches!(tokens.get(index + 1), Some(Token::Dot)) {
            targets.push(index);
        }
    }
    targets.retain(|index| identifier_eq(&tokens[*index], old));
    targets.sort_unstable();
    targets.dedup();
    if targets.is_empty() {
        return None;
    }
    let quoted = format!("\"{}\"", new.replace('"', "\"\""));
    let replacements = targets
        .iter()
        .map(|index| {
            let start = offsets[*index];
            (start, token_end(sql, &tokens[*index], start), quoted.clone())
        })
        .collect();
    Some(splice(sql, replacements))
}

/// Rewrites every column reference to `old` in a stored `CREATE VIEW`.
/// Aliases (`expr AS old`), function names and table references keep their
/// bytes, exactly like SQLite's column rename.
pub fn rewrite_view_column(sql: &str, old: &str, new: &str) -> Option<String> {
    let (tokens, offsets) = tokenize(sql).ok()?;
    let start = view_query_start(&tokens)?;
    let tables = view_table_tokens(&tokens, start);
    let mut targets = Vec::new();
    for index in start..tokens.len() {
        if !identifier_eq(&tokens[index], old) || tables.contains(&index) {
            continue;
        }
        // A name directly followed by `(` is a function call, not a column.
        if matches!(tokens.get(index + 1), Some(Token::LParen)) {
            continue;
        }
        // `expr AS alias` names the output, never the source column.
        if index > 0 && matches!(tokens.get(index - 1), Some(Token::Word(word)) if word.eq_ignore_ascii_case("as")) {
            continue;
        }
        targets.push(index);
    }
    if targets.is_empty() {
        return None;
    }
    Some(splice(sql, identifier_splices(sql, &tokens, &offsets, &targets, new)))
}

fn is_word_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte == b'$'
}

fn is_word_part(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$'
}

fn read_quoted(source: &str, at: usize, open: u8, close: u8) -> Result<(String, usize), SqlError> {
    let bytes = source.as_bytes();
    debug_assert_eq!(bytes.get(at), Some(&open));
    let mut text = String::new();
    let mut index = at + 1;
    while index < bytes.len() {
        if bytes[index] == close {
            if bytes.get(index + 1) == Some(&close) {
                text.push(char::from(close));
                index += 2;
                continue;
            }
            return Ok((text, index + 1));
        }
        text.push(char::from(bytes[index]));
        index += 1;
    }
    Err(SqlError::syntax("unterminated quoted literal", Some(at)))
}

fn read_number(source: &str, at: usize) -> Result<(Token, usize), SqlError> {
    let bytes = source.as_bytes();
    let start = at;
    let mut index = at;
    if bytes.get(index) == Some(&b'0') && matches!(bytes.get(index + 1), Some(b'x') | Some(b'X')) {
        index += 2;
        let digits_start = index;
        while index < bytes.len() && bytes[index].is_ascii_hexdigit() {
            index += 1;
        }
        let value = i64::from_str_radix(&source[digits_start..index], 16)
            .map_err(|_| SqlError::syntax("hexadecimal integer literal is out of the signed 64-bit range", Some(start)))?;
        return Ok((Token::Int(value), index));
    }
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        index += 1;
    }
    let mut real = false;
    if bytes.get(index) == Some(&b'.') {
        real = true;
        index += 1;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
    }
    if matches!(bytes.get(index), Some(b'e') | Some(b'E')) {
        let next = index + 1;
        let signed = matches!(bytes.get(next), Some(b'+') | Some(b'-'));
        let digits = if signed { next + 1 } else { next };
        if bytes.get(digits).is_some_and(u8::is_ascii_digit) {
            real = true;
            index = digits;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
        }
    }
    let text = &source[start..index];
    if real {
        let value = text.parse::<f64>().map_err(|_| SqlError::syntax(format!("real literal `{text}` is not a finite f64"), Some(start)))?;
        if !value.is_finite() {
            return Err(SqlError::syntax(format!("real literal `{text}` is not a finite f64"), Some(start)));
        }
        Ok((Token::Real(value), index))
    }
    else {
        let value = text
            .parse::<i64>()
            .map_err(|_| SqlError::syntax(format!("integer literal `{text}` is out of the signed 64-bit range"), Some(start)))?;
        Ok((Token::Int(value), index))
    }
}

fn decode_hex(text: &str, at: usize) -> Result<Vec<u8>, SqlError> {
    if text.len() % 2 != 0 {
        return Err(SqlError::syntax("blob literal must have an even number of hex digits", Some(at)));
    }
    let mut bytes = Vec::with_capacity(text.len() / 2);
    let mut index = 0;
    while index < text.len() {
        let byte =
            u8::from_str_radix(&text[index..index + 2], 16).map_err(|_| SqlError::syntax("blob literal contains a non-hex digit", Some(at)))?;
        bytes.push(byte);
        index += 2;
    }
    Ok(bytes)
}

struct Parser<'a> {
    source: &'a str,
    tokens: Vec<Token>,
    offsets: Vec<usize>,
    at: usize,
    end: usize,
    kind: Option<&'static str>,
    /// Token index where the current statement started.
    statement_start: usize,
    /// Next positional index assigned to a `?` or `:name` placeholder.
    next_parameter: usize,
    /// Named placeholders already seen, so a repeated name reuses its index.
    parameter_names: std::collections::BTreeMap<String, usize>,
}

impl<'a> Parser<'a> {
    fn done(&self) -> bool {
        self.at >= self.tokens.len()
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.at)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.at).cloned();
        if token.is_some() {
            self.at += 1;
        }
        token
    }

    fn offset(&self) -> usize {
        self.offsets.get(self.at).copied().unwrap_or(self.end)
    }

    fn error(&self, code: &'static str, message: impl Into<String>) -> SqlError {
        let mut error = SqlError::new(code, message).at(self.offset());
        if let Some(kind) = self.kind {
            error = error.in_statement(kind);
        }
        error
    }

    fn take(&mut self) -> Result<Token, SqlError> {
        self.next().ok_or_else(|| self.error("syntax", "unexpected end of SQL"))
    }

    fn peek_word(&self) -> Option<&str> {
        match self.peek() {
            Some(Token::Word(word)) => Some(word.as_str()),
            _ => None,
        }
    }

    fn peek_word_at(&self, offset: usize) -> Option<&str> {
        match self.tokens.get(self.at + offset) {
            Some(Token::Word(word)) => Some(word.as_str()),
            _ => None,
        }
    }

    fn peek_keyword(&self, keyword: &str) -> bool {
        self.peek_word().is_some_and(|word| word.eq_ignore_ascii_case(keyword))
    }

    fn eat_word(&mut self, keyword: &str) -> bool {
        if self.peek_word().is_some_and(|word| word.eq_ignore_ascii_case(keyword)) {
            self.at += 1;
            true
        }
        else {
            false
        }
    }

    fn expect_word(&mut self, keyword: &str) -> Result<(), SqlError> {
        if self.eat_word(keyword) {
            Ok(())
        }
        else {
            match self.peek() {
                Some(token) => Err(self.error("syntax", format!("expected `{keyword}` but found {}", token.describe()))),
                None => Err(self.error("syntax", format!("expected `{keyword}` but the statement ended"))),
            }
        }
    }

    fn expect_char(&mut self, expected: &Token) -> Result<(), SqlError> {
        match self.peek() {
            Some(token) if token == expected => {
                self.at += 1;
                Ok(())
            }
            Some(token) => Err(self.error("syntax", format!("expected {} but found {}", describe_expected(expected), token.describe()))),
            None => Err(self.error("syntax", format!("expected {} but the statement ended", describe_expected(expected)))),
        }
    }

    fn name(&mut self) -> Result<String, SqlError> {
        match self.next() {
            Some(Token::Word(word)) => Ok(word),
            Some(Token::Quoted(word)) => Ok(word),
            Some(token) => Err(self.error("syntax", format!("expected an identifier but found {}", token.describe()))),
            None => Err(self.error("syntax", "expected an identifier but the statement ended")),
        }
    }

    /// Reads `[schema.]name`, keeping the optional `schema.` qualifier so the
    /// engine can route the statement to an attached database.
    fn qualified_name(&mut self) -> Result<(Option<String>, String), SqlError> {
        let first = self.name()?;
        if self.peek() == Some(&Token::Dot) {
            self.at += 1;
            let second = self.name()?;
            Ok((Some(first), second))
        }
        else {
            Ok((None, first))
        }
    }

    /// Reads the `(col, ...)` list of `JOIN ... USING`. An empty list is a
    /// syntax error in SQLite, so it is rejected here too.
    fn using_columns(&mut self) -> Result<Vec<String>, SqlError> {
        self.expect_char(&Token::LParen)?;
        if self.peek() == Some(&Token::RParen) {
            return Err(self.error("syntax", "near \")\": syntax error"));
        }
        let mut names = Vec::new();
        loop {
            names.push(self.name()?);
            if self.peek() == Some(&Token::Comma) {
                self.at += 1;
                continue;
            }
            break;
        }
        self.expect_char(&Token::RParen)?;
        Ok(names)
    }

    /// Reads a comparison operator, combining the two-token forms.
    ///
    /// The tokenizer emits one character per punctuation token, so `<=`, `>=`,
    /// `<>` and `!=` arrive as two tokens and are joined here.
    fn compare_op(&mut self) -> Option<(CompareOp, usize)> {
        let first = match self.peek() {
            Some(Token::Operator(operator)) => operator.as_str(),
            _ => return None,
        };
        let second = match self.tokens.get(self.at + 1) {
            Some(Token::Operator(operator)) => Some(operator.as_str()),
            _ => None,
        };
        match (first, second) {
            ("=", _) => Some((CompareOp::Eq, 1)),
            ("<", Some("=")) => Some((CompareOp::LtEq, 2)),
            ("<", Some(">")) => Some((CompareOp::NotEq, 2)),
            ("<", _) => Some((CompareOp::Lt, 1)),
            (">", Some("=")) => Some((CompareOp::GtEq, 2)),
            (">", _) => Some((CompareOp::Gt, 1)),
            ("!", Some("=")) => Some((CompareOp::NotEq, 2)),
            _ => None,
        }
    }

    /// `OR` binds loosest, then `AND`, then `NOT`, then comparison, then
    /// `+` / `-`, then `*` / `/`, then prefix `-` / `+`.
    fn or_expr(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.and_expr()?;
        while self.eat_word("or") {
            left = Expr::Or(Box::new(left), Box::new(self.and_expr()?));
        }
        Ok(left)
    }

    fn and_expr(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.not_expr()?;
        while self.eat_word("and") {
            left = Expr::And(Box::new(left), Box::new(self.not_expr()?));
        }
        Ok(left)
    }

    fn not_expr(&mut self) -> Result<Expr, SqlError> {
        if self.eat_word("not") {
            return Ok(Expr::Not(Box::new(self.not_expr()?)));
        }
        self.comparison_expr()
    }

    fn comparison_expr(&mut self) -> Result<Expr, SqlError> {
        let left = self.bitwise_expr()?;
        if let Some((operator, width)) = self.compare_op() {
            self.at += width;
            let right = self.bitwise_expr()?;
            return Ok(Expr::Compare { left: Box::new(left), op: operator, right: Box::new(right) });
        }
        if self.eat_word("is") {
            let negated = self.eat_word("not");
            if self.eat_word("null") {
                return Ok(Expr::IsNull { value: Box::new(left), negated });
            }
            // `IS TRUE` / `IS FALSE` never yield NULL: the truthiness test maps
            // NULL to false, so `NULL IS TRUE` is 0 and `NULL IS NOT TRUE` is 1.
            if self.eat_word("true") {
                return Ok(Expr::IsTruth { value: Box::new(left), truth: true, negated });
            }
            if self.eat_word("false") {
                return Ok(Expr::IsTruth { value: Box::new(left), truth: false, negated });
            }
            let found = self.peek().map(Token::describe).unwrap_or_else(|| "end of input".to_string());
            return Err(self.error("syntax", format!("expected `null`, `true` or `false` after `IS` but found {found}")));
        }
        // `BETWEEN` is lowered to `>= low AND <= high`, so it carries the same
        // three-valued rules as the comparison and conjunction nodes and needs
        // no evaluator of its own. `NOT BETWEEN` is `NOT` around that form.
        if self.peek_keyword("between") || self.not_followed_by("between") {
            let negated = self.eat_word("not");
            self.expect_word("between")?;
            let low = self.bitwise_expr()?;
            // The separator binds tighter than the enclosing `AND` loop, so it
            // is consumed here instead of being mistaken for a conjunction.
            self.expect_word("and")?;
            let high = self.bitwise_expr()?;
            let range = Expr::And(
                Box::new(Expr::Compare { left: Box::new(left.clone()), op: CompareOp::GtEq, right: Box::new(low) }),
                Box::new(Expr::Compare { left: Box::new(left), op: CompareOp::LtEq, right: Box::new(high) }),
            );
            return Ok(if negated { Expr::Not(Box::new(range)) } else { range });
        }
        // `x NOT IN (...)` is one operator; a bare `NOT` is already handled by
        // `not_expr`, so only the two-word form is consumed here.
        let negated = self.not_followed_by("in");
        if negated {
            self.at += 1;
        }
        if self.eat_word("in") {
            return self.in_expr(left, negated);
        }
        // SQLite gives `LIKE` the same precedence as the comparisons above and
        // allows `NOT LIKE`; a trailing `ESCAPE c` applies to the same test.
        let negated_like = self.not_followed_by("like");
        if negated_like {
            self.at += 1;
        }
        if self.eat_word("like") {
            let pattern = self.bitwise_expr()?;
            let escape = if self.eat_word("escape") { Some(Box::new(self.bitwise_expr()?)) } else { None };
            return Ok(Expr::Like { value: Box::new(left), pattern: Box::new(pattern), negated: negated_like, escape });
        }
        Ok(left)
    }

    /// Reads `( expr, ... )` or `( SELECT ... )` after `[NOT] IN`.
    ///
    /// A leading `SELECT` makes the whole parenthesis a subquery; anything else
    /// is a value list that may still contain subquery elements.
    fn in_expr(&mut self, value: Expr, negated: bool) -> Result<Expr, SqlError> {
        if self.query_follows() {
            let query = self.parenthesized_query()?;
            return Ok(Expr::InSubquery { value: Box::new(value), query: Box::new(query), negated });
        }
        self.expect_char(&Token::LParen)?;
        let mut list = Vec::new();
        if self.peek() == Some(&Token::RParen) {
            self.at += 1;
            return Ok(Expr::In { value: Box::new(value), list, negated });
        }
        loop {
            list.push(self.or_expr()?);
            match self.peek() {
                Some(Token::Comma) => self.at += 1,
                Some(Token::RParen) => {
                    self.at += 1;
                    break;
                }
                Some(token) => return Err(self.error("syntax", format!("expected `,` or `)` in the `IN` list but found {}", token.describe()))),
                None => return Err(self.error("syntax", "`IN` value list is not closed")),
            }
        }
        Ok(Expr::In { value: Box::new(value), list, negated })
    }

    /// Reads the bitwise chain, which sits between `+`/`-` and the
    /// comparisons (`1 << 2 + 3` is `1 << 5`). SQLite puts `<<`, `>>`, `&` and
    /// `|` on one left-associative level; `^` does not exist there at all.
    fn bitwise_expr(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.additive_expr()?;
        loop {
            let op = match self.peek() {
                Some(Token::Operator(operator)) if operator == "<<" => BinaryOp::ShiftLeft,
                Some(Token::Operator(operator)) if operator == ">>" => BinaryOp::ShiftRight,
                Some(Token::Operator(operator)) if operator == "&" => BinaryOp::BitAnd,
                Some(Token::Operator(operator)) if operator == "|" => BinaryOp::BitOr,
                _ => break,
            };
            self.at += 1;
            let right = self.additive_expr()?;
            left = Expr::Binary { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn additive_expr(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.multiplicative_expr()?;
        loop {
            let op = match self.peek() {
                Some(Token::Operator(operator)) if operator == "+" => BinaryOp::Add,
                Some(Token::Minus) => BinaryOp::Subtract,
                _ => break,
            };
            self.at += 1;
            let right = self.multiplicative_expr()?;
            left = Expr::Binary { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn multiplicative_expr(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.concat_expr()?;
        loop {
            let op = match self.peek() {
                Some(Token::Star) => BinaryOp::Multiply,
                Some(Token::Operator(operator)) if operator == "/" => BinaryOp::Divide,
                Some(Token::Operator(operator)) if operator == "%" => BinaryOp::Modulo,
                _ => break,
            };
            self.at += 1;
            let right = self.concat_expr()?;
            left = Expr::Binary { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    /// Reads `||` chains. SQLite binds concatenation tighter than `*`, `/` and
    /// `+` (`1 || 2 * 3` is 36) but looser than unary `-` (`-1 || 2` is -12).
    fn concat_expr(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.unary_expr()?;
        while matches!(self.peek(), Some(Token::Operator(operator)) if operator == "||") {
            self.at += 1;
            let right = self.unary_expr()?;
            left = Expr::Binary { left: Box::new(left), op: BinaryOp::Concat, right: Box::new(right) };
        }
        Ok(left)
    }

    fn unary_expr(&mut self) -> Result<Expr, SqlError> {
        if matches!(self.peek(), Some(Token::Minus)) {
            self.at += 1;
            return Ok(Expr::Unary { op: UnaryOp::Negate, value: Box::new(self.unary_expr()?) });
        }
        if matches!(self.peek(), Some(Token::Operator(operator)) if operator == "+") {
            self.at += 1;
            return Ok(Expr::Unary { op: UnaryOp::Plus, value: Box::new(self.unary_expr()?) });
        }
        if matches!(self.peek(), Some(Token::Operator(operator)) if operator == "~") {
            self.at += 1;
            return Ok(Expr::Unary { op: UnaryOp::BitNot, value: Box::new(self.unary_expr()?) });
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Expr, SqlError> {
        if self.peek() == Some(&Token::LParen) {
            if self.query_follows() {
                let query = self.parenthesized_query()?;
                return Ok(Expr::Subquery(Box::new(query)));
            }
            self.at += 1;
            let inner = self.or_expr()?;
            self.expect_char(&Token::RParen)?;
            return Ok(inner);
        }
        if let Some(Token::Word(word)) = self.peek() {
            let lower = word.to_ascii_lowercase();
            match lower.as_str() {
                "null" => {
                    self.at += 1;
                    return Ok(Expr::Literal(Literal::Null));
                }
                "true" => {
                    self.at += 1;
                    return Ok(Expr::Literal(Literal::Integer(1)));
                }
                "false" => {
                    self.at += 1;
                    return Ok(Expr::Literal(Literal::Integer(0)));
                }
                "case" => return self.case_expr(),
                "cast" if matches!(self.tokens.get(self.at + 1), Some(Token::LParen)) => return self.cast_expr(),
                "exists"
                    if matches!(self.tokens.get(self.at + 1), Some(Token::LParen))
                        && matches!(self.tokens.get(self.at + 2), Some(Token::Word(next)) if next.eq_ignore_ascii_case("select") || next.eq_ignore_ascii_case("with")) =>
                {
                    self.at += 1;
                    let query = self.parenthesized_query()?;
                    return Ok(Expr::Exists(Box::new(query)));
                }
                _ => {}
            }
            if matches!(self.tokens.get(self.at + 1), Some(Token::LParen)) {
                return self.function_expr();
            }
        }
        if let Some(Token::Operator(operator)) = self.peek() {
            let operator = operator.clone();
            if let Some((index, text)) = self.parameter(&operator)? {
                return Ok(Expr::Parameter { index, text });
            }
        }
        match self.peek() {
            Some(Token::Int(_)) | Some(Token::Real(_)) | Some(Token::Text(_)) | Some(Token::Blob(_)) => Ok(Expr::Literal(self.literal()?)),
            Some(Token::Word(_)) | Some(Token::Quoted(_)) => Ok(Expr::Column(self.column_ref()?)),
            Some(token) => Err(self.error("syntax", format!("expected an expression but found {}", token.describe()))),
            None => Err(self.error("syntax", "expected an expression but the statement ended")),
        }
    }

    /// Reads `CAST ( expr AS type )`.
    fn cast_expr(&mut self) -> Result<Expr, SqlError> {
        self.expect_word("cast")?;
        self.expect_char(&Token::LParen)?;
        let value = self.or_expr()?;
        self.expect_word("as")?;
        let name = self.name()?;
        let target = CastType::from_name(&name).ok_or_else(|| {
            let mut error = self.error("unsupported-type", format!("`CAST` target type `{name}` is not supported"));
            error.clause = Some("CAST".to_string());
            error
        })?;
        self.expect_char(&Token::RParen)?;
        Ok(Expr::Cast { value: Box::new(value), target })
    }

    /// Reads `CASE [operand] WHEN condition THEN result ... [ELSE result] END`.
    fn case_expr(&mut self) -> Result<Expr, SqlError> {
        self.expect_word("case")?;
        let operand = if self.peek_keyword("when") { None } else { Some(Box::new(self.or_expr()?)) };
        let mut branches = Vec::new();
        while self.eat_word("when") {
            let condition = self.or_expr()?;
            self.expect_word("then")?;
            let result = self.or_expr()?;
            branches.push((condition, result));
        }
        if branches.is_empty() {
            return Err(self.error("syntax", "`CASE` requires at least one `WHEN` branch"));
        }
        let otherwise = if self.eat_word("else") { Some(Box::new(self.or_expr()?)) } else { None };
        self.expect_word("end")?;
        Ok(Expr::Case { operand, branches, otherwise })
    }

    /// Reads `name ( [expr , ...] )` and validates the function name and arity.
    fn function_expr(&mut self) -> Result<Expr, SqlError> {
        let name = self.name()?;
        self.expect_char(&Token::LParen)?;
        // `count(*)` has no expression argument, so it is recognized before the
        // general argument list is read.
        let count_star = AggregateFunction::from_name(&name) == Some(AggregateFunction::Count)
            && matches!(self.peek(), Some(Token::Star))
            && matches!(self.tokens.get(self.at + 1), Some(Token::RParen));
        let mut args = Vec::new();
        if count_star {
            self.at += 2;
        }
        else if self.peek() == Some(&Token::RParen) {
            self.at += 1;
        }
        else {
            loop {
                args.push(self.or_expr()?);
                match self.peek() {
                    Some(Token::Comma) => self.at += 1,
                    Some(Token::RParen) => {
                        self.at += 1;
                        break;
                    }
                    Some(token) => {
                        return Err(self.error("syntax", format!("expected `,` or `)` in the argument list but found {}", token.describe())));
                    }
                    None => return Err(self.error("syntax", "function argument list is not closed")),
                }
            }
        }
        if self.peek_keyword("over") {
            return self.window_expr(&name, args, count_star);
        }
        if count_star {
            return Ok(Expr::Aggregate { function: AggregateFunction::CountStar, argument: None, separator: None });
        }
        if let Some(function) = AggregateFunction::from_name(&name) {
            // Only `group_concat` takes the optional separator argument.
            let wanted = if function == AggregateFunction::GroupConcat { 1..=2 } else { 1..=1 };
            if !wanted.contains(&args.len()) {
                return Err(self.error("wrong-arguments", format!("wrong number of arguments to function {}()", function.name())));
            }
            let mut args = args.into_iter();
            let argument = args.next().expect("argument count checked");
            let separator = args.next().map(Box::new);
            return Ok(Expr::Aggregate { function, argument: Some(Box::new(argument)), separator });
        }
        match ScalarFunction::from_name(&name) {
            Some(function) if function.accepts(args.len()) => Ok(Expr::Function { name: function, args }),
            Some(function) => Err(self.error("wrong-arguments", format!("wrong number of arguments to function {}()", function.name()))),
            None if is_aggregate_name(&name) => {
                let mut error = self.error(
                    "unsupported-clause",
                    format!("aggregate function `{name}(...)` is not supported, except `count(*)` over the whole result"),
                );
                error.clause = Some(name.to_ascii_uppercase());
                Err(error)
            }
            None => Err(self.error("unknown-function", format!("no such function: {name}"))),
        }
    }

    /// Reads `OVER <spec>` after a window function call has been parsed.
    fn window_expr(&mut self, name: &str, args: Vec<Expr>, count_star: bool) -> Result<Expr, SqlError> {
        self.expect_word("over")?;
        let spec = if self.peek() == Some(&Token::LParen) {
            self.at += 1;
            let spec = self.window_spec()?;
            self.expect_char(&Token::RParen)?;
            spec
        }
        else {
            let base = if matches!(self.peek(), Some(Token::Word(_)) | Some(Token::Quoted(_))) { Some(self.name()?) } else { None };
            WindowSpec { base, partition: Vec::new(), order: Vec::new(), frame: None }
        };
        let function = self.window_function(name, args, count_star)?;
        Ok(Expr::Window { function, spec })
    }

    /// Turns the written call into a supported window function, checking arity.
    fn window_function(&mut self, name: &str, args: Vec<Expr>, count_star: bool) -> Result<WindowFunction, SqlError> {
        let lower = name.to_ascii_lowercase();
        let simple = match lower.as_str() {
            "row_number" => Some(WindowFunction::RowNumber),
            "rank" => Some(WindowFunction::Rank),
            "dense_rank" => Some(WindowFunction::DenseRank),
            "percent_rank" => Some(WindowFunction::PercentRank),
            "cume_dist" => Some(WindowFunction::CumeDist),
            _ => None,
        };
        if let Some(function) = simple {
            if !args.is_empty() {
                return Err(self.error("wrong-arguments", format!("wrong number of arguments to function {}()", lower)));
            }
            return Ok(function);
        }
        let arguments = if count_star { 0 } else { args.len() };
        match lower.as_str() {
            "ntile" if arguments == 1 => {
                let mut args = args.into_iter();
                return Ok(WindowFunction::Ntile(Box::new(args.next().expect("arity checked"))));
            }
            "lag" | "lead" if (1..=3).contains(&arguments) => {
                let mut args = args.into_iter();
                let value = Box::new(args.next().expect("arity checked"));
                let offset = args.next().map(Box::new);
                let default = args.next().map(Box::new);
                return Ok(if lower == "lag" {
                    WindowFunction::Lag { value, offset, default }
                }
                else {
                    WindowFunction::Lead { value, offset, default }
                });
            }
            "first_value" | "last_value" if arguments == 1 => {
                let mut args = args.into_iter();
                let value = Box::new(args.next().expect("arity checked"));
                return Ok(if lower == "first_value" { WindowFunction::FirstValue(value) } else { WindowFunction::LastValue(value) });
            }
            "nth_value" if arguments == 2 => {
                let mut args = args.into_iter();
                let value = Box::new(args.next().expect("arity checked"));
                let position = Box::new(args.next().expect("arity checked"));
                return Ok(WindowFunction::NthValue { value, position });
            }
            _ => {}
        }
        if let Some(function) = AggregateFunction::from_name(name) {
            if count_star {
                return Ok(WindowFunction::Aggregate { function: AggregateFunction::CountStar, argument: None, separator: None });
            }
            let wanted = if function == AggregateFunction::GroupConcat { 1..=2 } else { 1..=1 };
            if !wanted.contains(&arguments) {
                return Err(self.error("wrong-arguments", format!("wrong number of arguments to function {}()", function.name())));
            }
            let mut args = args.into_iter();
            let argument = Some(Box::new(args.next().expect("arity checked")));
            let separator = args.next().map(Box::new);
            return Ok(WindowFunction::Aggregate { function, argument, separator });
        }
        if matches!(lower.as_str(), "ntile" | "lag" | "lead" | "first_value" | "last_value" | "nth_value") {
            return Err(self.error("wrong-arguments", format!("wrong number of arguments to function {lower}()")));
        }
        Err(self.error("unknown-function", format!("no such window function: {name}")))
    }

    /// Reads a window definition after its opening parenthesis has been consumed.
    ///
    /// The definition may start with a base window name, then a `PARTITION BY`
    /// list, an `ORDER BY` list and an explicit `ROWS` / `RANGE` frame. `GROUPS`
    /// and `EXCLUDE` are recognized only to be rejected by name.
    fn window_spec(&mut self) -> Result<WindowSpec, SqlError> {
        let mut base = None;
        if matches!(self.peek(), Some(Token::Word(_)) | Some(Token::Quoted(_)))
            && !matches!(
                self.peek_word().map(|word| word.to_ascii_lowercase()).as_deref(),
                Some("partition" | "order" | "rows" | "range" | "groups")
            )
        {
            base = Some(self.name()?);
        }
        let mut partition = Vec::new();
        if self.eat_word("partition") {
            self.expect_word("by")?;
            loop {
                partition.push(self.or_expr()?);
                if self.peek() == Some(&Token::Comma) {
                    self.at += 1;
                    continue;
                }
                break;
            }
        }
        let mut order = Vec::new();
        if self.eat_word("order") {
            self.expect_word("by")?;
            loop {
                order.push(self.order_key()?);
                if self.peek() == Some(&Token::Comma) {
                    self.at += 1;
                    continue;
                }
                break;
            }
        }
        let frame =
            if self.peek_keyword("rows") || self.peek_keyword("range") || self.peek_keyword("groups") { Some(self.frame()?) } else { None };
        if self.peek_keyword("exclude") {
            let mut error = self
                .error("unsupported-clause", "window frame `EXCLUDE` is not supported; frames always include every row between their edges");
            error.clause = Some("EXCLUDE".to_string());
            return Err(error);
        }
        Ok(WindowSpec { base, partition, order, frame })
    }

    /// Reads `ROWS|RANGE|GROUPS <edge> [BETWEEN <edge> AND <edge>]`.
    fn frame(&mut self) -> Result<Frame, SqlError> {
        if self.eat_word("groups") {
            let mut error = self.error("unsupported-clause", "window frame unit `GROUPS` is not supported");
            error.clause = Some("GROUPS".to_string());
            return Err(error);
        }
        let units = if self.eat_word("rows") {
            FrameUnits::Rows
        }
        else {
            self.expect_word("range")?;
            FrameUnits::Range
        };
        if self.eat_word("between") {
            let start = self.frame_bound()?;
            self.expect_word("and")?;
            let end = self.frame_bound()?;
            Ok(Frame { units, between: true, start, end })
        }
        else {
            let start = self.frame_bound()?;
            Ok(Frame { units, between: false, start, end: FrameBound { edge: FrameEdge::CurrentRow, offset: None } })
        }
    }

    /// Reads one frame edge: `UNBOUNDED PRECEDING/FOLLOWING`, `CURRENT ROW`
    /// or `<expr> PRECEDING/FOLLOWING`.
    fn frame_bound(&mut self) -> Result<FrameBound, SqlError> {
        if self.eat_word("unbounded") {
            let edge = if self.eat_word("preceding") {
                FrameEdge::UnboundedPreceding
            }
            else if self.eat_word("following") {
                FrameEdge::UnboundedFollowing
            }
            else {
                return Err(self.error("syntax", "expected `PRECEDING` or `FOLLOWING` after `UNBOUNDED`"));
            };
            return Ok(FrameBound { edge, offset: None });
        }
        if self.eat_word("current") {
            self.expect_word("row")?;
            return Ok(FrameBound { edge: FrameEdge::CurrentRow, offset: None });
        }
        let offset = self.or_expr()?;
        let edge = if self.eat_word("preceding") {
            FrameEdge::Preceding
        }
        else if self.eat_word("following") {
            FrameEdge::Following
        }
        else {
            return Err(self.error("syntax", "expected `PRECEDING` or `FOLLOWING` after a frame offset"));
        };
        Ok(FrameBound { edge, offset: Some(Box::new(offset)) })
    }

    /// Reads one `LIMIT` / `OFFSET` operand. A placeholder is accepted like any
    /// other expression and takes its bound integer value at evaluation time.
    fn limit_term(&mut self, _clause: &str) -> Result<Expr, SqlError> {
        self.or_expr()
    }

    /// True when the current token is `NOT` and the next one is `keyword`.
    fn not_followed_by(&self, keyword: &str) -> bool {
        let is_not = self.peek_keyword("not");
        let next = match self.tokens.get(self.at + 1) {
            Some(Token::Word(word)) => word.eq_ignore_ascii_case(keyword),
            _ => false,
        };
        is_not && next
    }

    fn order_key(&mut self) -> Result<OrderKey, SqlError> {
        let expr = self.or_expr()?;
        let descending = if self.eat_word("desc") {
            true
        }
        else {
            self.eat_word("asc");
            false
        };
        Ok(OrderKey { expr, descending })
    }

    /// Reads a column reference, optionally qualified as `table.column`.
    fn column_ref(&mut self) -> Result<ColumnRef, SqlError> {
        let first = self.name()?;
        if self.peek() == Some(&Token::Dot) {
            self.at += 1;
            let name = self.name()?;
            Ok(ColumnRef { qualifier: Some(first), name })
        }
        else {
            Ok(ColumnRef { qualifier: None, name: first })
        }
    }

    /// Reads one `FROM` entry: a table name or `( select )`, then an optional
    /// `AS` / bare alias.
    fn table_ref(&mut self) -> Result<TableRef, SqlError> {
        let query = if self.peek() == Some(&Token::LParen) { Some(Box::new(self.parenthesized_query()?)) } else { None };
        let (schema, name) = if query.is_some() { (None, String::new()) } else { self.qualified_name()? };
        let alias = if self.eat_word("as") {
            Some(self.name()?)
        }
        else if matches!(self.peek(), Some(Token::Quoted(_))) {
            Some(self.name()?)
        }
        else {
            let bare = matches!(self.peek(), Some(Token::Word(word)) if !is_clause_keyword(word));
            if bare { Some(self.name()?) } else { None }
        };
        Ok(TableRef { schema, name, alias, query, join: None, on: None, using: None, natural: false })
    }

    /// Reads `WITH [RECURSIVE] name [(col, ...)] AS (select) [, ...] select`.
    fn with_select(&mut self) -> Result<Statement, SqlError> {
        self.expect_word("with")?;
        let recursive = self.eat_word("recursive");
        let mut entries: Vec<CommonTable> = Vec::new();
        loop {
            let position = self.offset();
            let name = self.name()?;
            if entries.iter().any(|entry| entry.name.eq_ignore_ascii_case(&name)) {
                let mut error = self.error("duplicate-table", format!("duplicate `WITH` table name: {name}"));
                error.clause = Some("WITH".to_string());
                error.position = Some(position);
                return Err(error);
            }
            let columns = if self.peek() == Some(&Token::LParen) {
                self.at += 1;
                let mut names = Vec::new();
                loop {
                    names.push(self.name()?);
                    match self.peek() {
                        Some(Token::Comma) => self.at += 1,
                        Some(Token::RParen) => {
                            self.at += 1;
                            break;
                        }
                        Some(token) => {
                            return Err(
                                self.error("syntax", format!("expected `,` or `)` in the `WITH` column list but found {}", token.describe()))
                            );
                        }
                        None => return Err(self.error("syntax", "`WITH` column list is not closed")),
                    }
                }
                Some(names)
            }
            else {
                None
            };
            self.expect_word("as")?;
            self.expect_char(&Token::LParen)?;
            let query = if self.peek_keyword("with") { self.with_select()? } else { self.select_body()? };
            self.expect_char(&Token::RParen)?;
            entries.push(CommonTable { name, columns, query: Box::new(query) });
            if self.peek() == Some(&Token::Comma) {
                self.at += 1;
                continue;
            }
            break;
        }
        let mut statement = self.select_body()?;
        if let Statement::Select { with, recursive: flag, .. } = &mut statement {
            *with = entries;
            *flag = recursive;
        }
        Ok(statement)
    }

    /// Reads a `SELECT` body after the `select` keyword has been consumed.
    fn select_body(&mut self) -> Result<Statement, SqlError> {
        if self.peek_keyword("with") {
            return Err(self.error("syntax", "`WITH` must precede the whole `SELECT`, not follow `AS (...)`"));
        }
        self.expect_word("select")?;
        self.select()
    }

    /// Reads `( select )`, accepting an optional `WITH` prefix inside.
    fn parenthesized_query(&mut self) -> Result<Statement, SqlError> {
        self.expect_char(&Token::LParen)?;
        let statement = if self.peek_keyword("with") { self.with_select()? } else { self.select_body()? };
        self.expect_char(&Token::RParen)?;
        Ok(statement)
    }

    /// Whether the current token is `(` and the next one starts a query.
    fn query_follows(&self) -> bool {
        matches!(self.peek(), Some(Token::LParen))
            && matches!(
                self.tokens.get(self.at + 1),
                Some(Token::Word(word)) if word.eq_ignore_ascii_case("select") || word.eq_ignore_ascii_case("with")
            )
    }

    fn statement(&mut self) -> Result<Statement, SqlError> {
        self.statement_start = self.at;
        if self.eat_word("create") {
            if self.eat_word("temp") || self.eat_word("temporary") {
                if self.eat_word("table") {
                    self.kind = Some("create-table");
                    return self.create_table(true);
                }
                if self.eat_word("view") {
                    self.kind = Some("create-view");
                    return self.create_view(true);
                }
                return Err(self.error("unsupported-clause", "`TEMP` is only accepted on `CREATE TABLE` and `CREATE VIEW`"));
            }
            if self.eat_word("unique") {
                self.kind = Some("create-index");
                return self.create_index(true);
            }
            if matches!(self.peek_word(), Some(word) if word.eq_ignore_ascii_case("index")) {
                self.kind = Some("create-index");
                return self.create_index(false);
            }
            if self.eat_word("view") {
                self.kind = Some("create-view");
                return self.create_view(false);
            }
            if self.eat_word("trigger") {
                self.kind = Some("create-trigger");
                return self.create_trigger();
            }
            self.kind = Some("create-table");
            return self.create_table(false);
        }
        if self.eat_word("insert") {
            self.kind = Some("insert");
            return self.insert();
        }
        if self.eat_word("update") {
            self.kind = Some("update");
            return self.update();
        }
        if self.eat_word("delete") {
            self.kind = Some("delete");
            return self.delete();
        }
        if self.eat_word("drop") {
            return self.drop();
        }
        if self.eat_word("alter") {
            return self.alter_table();
        }
        if self.eat_word("pragma") {
            self.kind = Some("pragma");
            return self.pragma();
        }
        if self.eat_word("attach") {
            self.kind = Some("attach");
            return self.attach();
        }
        if self.eat_word("detach") {
            self.kind = Some("detach");
            return self.detach_statement();
        }
        if self.peek_keyword("select") {
            self.kind = Some("select");
            self.at += 1;
            return self.select();
        }
        if self.peek_keyword("with") {
            self.kind = Some("select");
            return self.with_select();
        }
        if self.eat_word("begin") {
            self.kind = Some("begin");
            return self.begin_transaction();
        }
        if self.eat_word("commit") || self.eat_word("end") {
            self.kind = Some("commit");
            return self.commit_transaction();
        }
        if self.eat_word("rollback") {
            self.kind = Some("rollback");
            return self.rollback_transaction();
        }
        if self.eat_word("savepoint") {
            self.kind = Some("savepoint");
            let name = self.savepoint_name()?;
            self.expect_end("SAVEPOINT")?;
            return Ok(Statement::Transaction { action: TransactionAction::Savepoint(name) });
        }
        if self.eat_word("release") {
            self.kind = Some("release");
            self.eat_word("savepoint");
            let name = self.savepoint_name()?;
            self.expect_end("RELEASE")?;
            return Ok(Statement::Transaction { action: TransactionAction::Release(name) });
        }
        let found = self.peek().map(Token::describe).unwrap_or_else(|| "end of SQL".to_string());
        Err(self.error(
            "unsupported-statement",
            format!(
                "unsupported statement starting at {found}; this engine accepts `CREATE TABLE`, `CREATE INDEX`, `INSERT`, `UPDATE`, `DELETE`, `DROP TABLE`, `DROP INDEX`, `ALTER TABLE`, `PRAGMA foreign_keys`, transaction control, and `SELECT` only"
            ),
        ))
    }

    /// Reads `BEGIN [DEFERRED | IMMEDIATE | EXCLUSIVE] [TRANSACTION]`. The
    /// locking mode is accepted and ignored because this engine serializes
    /// every writer itself.
    fn begin_transaction(&mut self) -> Result<Statement, SqlError> {
        if self.eat_word("deferred") || self.eat_word("immediate") || self.eat_word("exclusive") {
            // Lock mode has no observable effect here.
        }
        self.eat_word("transaction");
        self.expect_end("BEGIN")?;
        Ok(Statement::Transaction { action: TransactionAction::Begin })
    }

    /// Reads `COMMIT [TRANSACTION]` or `END [TRANSACTION]`.
    fn commit_transaction(&mut self) -> Result<Statement, SqlError> {
        self.eat_word("transaction");
        self.expect_end("COMMIT")?;
        Ok(Statement::Transaction { action: TransactionAction::Commit })
    }

    /// Reads `ROLLBACK [TRANSACTION] [TO [SAVEPOINT] name]`.
    fn rollback_transaction(&mut self) -> Result<Statement, SqlError> {
        self.eat_word("transaction");
        let action = if self.eat_word("to") {
            self.eat_word("savepoint");
            TransactionAction::RollbackTo(self.savepoint_name()?)
        }
        else {
            TransactionAction::Rollback
        };
        self.expect_end("ROLLBACK")?;
        Ok(Statement::Transaction { action })
    }

    /// Reads a savepoint name: a word or a quoted identifier.
    fn savepoint_name(&mut self) -> Result<String, SqlError> {
        let name = self.name()?;
        self.reject_qualified(&name)?;
        Ok(name)
    }

    /// Reads `PRAGMA <name> [= value]` or `PRAGMA <name>(value)`.
    fn pragma(&mut self) -> Result<Statement, SqlError> {
        let (schema, name) = self.qualified_name()?;
        let value = if matches!(self.peek(), Some(Token::Operator(operator)) if operator == "=") {
            self.at += 1;
            Some(self.pragma_value()?)
        }
        else if self.peek() == Some(&Token::LParen) {
            self.at += 1;
            let value = self.pragma_value()?;
            self.expect_char(&Token::RParen)?;
            Some(value)
        }
        else {
            None
        };
        self.expect_end("PRAGMA")?;
        Ok(Statement::Pragma { schema, name, value })
    }

    /// Reads one pragma value as written: a word, a quoted name, or an integer.
    fn pragma_value(&mut self) -> Result<String, SqlError> {
        match self.peek() {
            Some(Token::Word(_)) | Some(Token::Quoted(_)) => self.name(),
            Some(Token::Int(value)) => {
                let value = *value;
                self.at += 1;
                Ok(value.to_string())
            }
            Some(token) => Err(self.error("syntax", format!("expected a pragma value but found {}", token.describe()))),
            None => Err(self.error("syntax", "expected a pragma value but the statement ended")),
        }
    }

    /// Reads `ATTACH [DATABASE] 'file' AS name`.
    fn attach(&mut self) -> Result<Statement, SqlError> {
        self.eat_word("database");
        let file = match self.next() {
            Some(Token::Text(text)) => text,
            Some(token) => return Err(self.error("syntax", format!("expected a string database file but found {}", token.describe()))),
            None => return Err(self.error("syntax", "expected a string database file but the statement ended")),
        };
        self.expect_word("as")?;
        let name = self.name()?;
        self.expect_end("ATTACH")?;
        Ok(Statement::Attach { file, name })
    }

    /// Reads `DETACH [DATABASE] name`.
    fn detach_statement(&mut self) -> Result<Statement, SqlError> {
        self.eat_word("database");
        let name = self.name()?;
        self.reject_qualified(&name)?;
        self.expect_end("DETACH")?;
        Ok(Statement::Detach { name })
    }

    /// Reads `VIEW [IF NOT EXISTS] <name> [(a, b)] AS <select>`.
    ///
    /// The query is parsed once here and kept as an ordinary `SELECT`, so a
    /// later read re-evaluates it through the same evaluator as any other
    /// query. As in SQLite, the base tables are not resolved at DDL time.
    fn create_view(&mut self, temp: bool) -> Result<Statement, SqlError> {
        let if_not_exists = if self.eat_word("if") {
            self.expect_word("not")?;
            self.expect_word("exists")?;
            true
        }
        else {
            false
        };
        let name_token = self.at;
        let (schema, name) = self.qualified_name()?;
        // The token gap after the name preserves its verbatim spelling before
        // the optional column list or `AS`.
        let name_gap_end = self.offsets.get(self.at).copied().unwrap_or(self.source.len());
        let columns = if self.peek() == Some(&Token::LParen) {
            self.at += 1;
            let mut columns = Vec::new();
            loop {
                columns.push(self.name()?);
                match self.peek() {
                    Some(Token::Comma) => {
                        self.at += 1;
                    }
                    Some(Token::RParen) => {
                        self.at += 1;
                        break;
                    }
                    Some(token) => {
                        return Err(self.error("syntax", format!("expected `,` or `)` in the view column list but found {}", token.describe())));
                    }
                    None => return Err(self.error("syntax", "view column list is not closed")),
                }
            }
            Some(columns)
        }
        else {
            None
        };
        self.expect_word("as")?;
        let query = if self.peek_keyword("with") {
            self.with_select()?
        }
        else {
            self.expect_word("select")?;
            self.select()?
        };
        if !matches!(query, Statement::Select { .. }) {
            return Err(self.error("syntax", "a view definition must be a `SELECT` query"));
        }
        self.expect_end("CREATE VIEW")?;
        let sql = self.canonical_create_sql("CREATE VIEW", name_token, name_gap_end);
        Ok(Statement::CreateView { schema, name, if_not_exists, columns, query: Box::new(query), temp, sql })
    }

    /// Reads `TRIGGER [IF NOT EXISTS] <name> <timing> <event> ON <table>
    /// [FOR EACH ROW] [WHEN <expr>] BEGIN <statement> ; ... END`.
    fn create_trigger(&mut self) -> Result<Statement, SqlError> {
        let if_not_exists = if self.eat_word("if") {
            self.expect_word("not")?;
            self.expect_word("exists")?;
            true
        }
        else {
            false
        };
        let name_token = self.at;
        let (schema, name) = self.qualified_name()?;
        let name_gap_end = self.offsets.get(self.at).copied().unwrap_or(self.source.len());
        let timing = if self.eat_word("before") {
            TriggerTiming::Before
        }
        else if self.eat_word("after") {
            TriggerTiming::After
        }
        else if self.eat_word("instead") {
            self.expect_word("of")?;
            TriggerTiming::InsteadOf
        }
        else {
            return Err(self.near_syntax_error());
        };
        let event = if self.eat_word("insert") {
            TriggerEvent::Insert
        }
        else if self.eat_word("update") {
            let mut columns = Vec::new();
            if self.eat_word("of") {
                loop {
                    columns.push(self.name()?);
                    if self.peek() == Some(&Token::Comma) {
                        self.at += 1;
                    }
                    else {
                        break;
                    }
                }
            }
            TriggerEvent::Update(columns)
        }
        else if self.eat_word("delete") {
            TriggerEvent::Delete
        }
        else {
            return Err(self.near_syntax_error());
        };
        self.expect_word("on")?;
        let (table_schema, table) = self.qualified_name()?;
        if self.eat_word("for") {
            self.expect_word("each")?;
            self.expect_word("row")?;
        }
        let when = if self.eat_word("when") { Some(self.or_expr()?) } else { None };
        self.expect_word("begin")?;
        let mut body = Vec::new();
        loop {
            while self.peek() == Some(&Token::Semi) {
                self.at += 1;
            }
            if self.eat_word("end") {
                break;
            }
            if self.peek().is_none() {
                return Err(self.error("syntax", "the trigger body is not closed by `END`"));
            }
            body.push(self.statement()?);
        }
        let gap_end = name_gap_end.min(self.source.len());
        let end = self.offsets.get(self.at).copied().unwrap_or(self.source.len());
        // SQLite stores a temp trigger without its `temp.` qualifier while the
        // body keeps the text it was written with.
        let name_start = self.offsets.get(name_token).copied().unwrap_or(0);
        let raw_name = self.source[name_start..gap_end].trim_end();
        let name_end = name_start + raw_name.len();
        let stored_name = if schema.is_some() { name.clone() } else { raw_name.to_string() };
        let sql = format!("CREATE TRIGGER {stored_name}{}", self.source[name_end..end].trim_end());
        Ok(Statement::CreateTrigger { schema, name, if_not_exists, timing, event, table_schema, table, when, body, sql })
    }

    /// Reads `DROP TABLE [IF EXISTS] name` and `DROP INDEX [IF EXISTS] name`.
    fn drop(&mut self) -> Result<Statement, SqlError> {
        enum Object {
            Table,
            Index,
            View,
            Trigger,
        }
        let object = if self.eat_word("table") {
            Object::Table
        }
        else if self.eat_word("index") {
            Object::Index
        }
        else if self.eat_word("view") {
            Object::View
        }
        else if self.eat_word("trigger") {
            Object::Trigger
        }
        else {
            let found = self.peek().map(Token::describe).unwrap_or_else(|| "end of SQL".to_string());
            let mut error = self.error(
                "unsupported-clause",
                format!(
                    "unsupported `DROP` object {found}; this engine accepts `DROP TABLE`, `DROP INDEX`, `DROP VIEW` and `DROP TRIGGER` only"
                ),
            );
            error.clause = Some("DROP".to_string());
            return Err(error);
        };
        let label = match object {
            Object::Table => "DROP TABLE",
            Object::Index => "DROP INDEX",
            Object::View => "DROP VIEW",
            Object::Trigger => "DROP TRIGGER",
        };
        self.kind = Some(match object {
            Object::Table => "drop-table",
            Object::Index => "drop-index",
            Object::View => "drop-view",
            Object::Trigger => "drop-trigger",
        });
        let if_exists = if self.eat_word("if") {
            self.expect_word("exists")?;
            true
        }
        else {
            false
        };
        let (schema, name) = self.qualified_name()?;
        self.expect_end(label)?;
        Ok(match object {
            Object::Table => Statement::DropTable { schema, name, if_exists },
            Object::Index => Statement::DropIndex { schema, name, if_exists },
            Object::View => Statement::DropView { schema, name, if_exists },
            Object::Trigger => Statement::DropTrigger { schema, name, if_exists },
        })
    }

    /// Reads the three `ALTER TABLE` forms this engine understands. `DROP
    /// COLUMN` parses but is rejected by the engine by name, so the caller gets
    /// a structured rejection instead of a generic syntax error.
    fn alter_table(&mut self) -> Result<Statement, SqlError> {
        self.kind = Some("alter-table");
        self.expect_word("table")?;
        let (schema, table) = self.qualified_name()?;
        let action = if self.eat_word("rename") {
            if self.eat_word("column") {
                let old = self.name()?;
                self.expect_word("to")?;
                let new = self.name()?;
                AlterAction::RenameColumn { old, new }
            }
            else if self.peek_keyword("to") {
                self.at += 1;
                AlterAction::RenameTable { new_name: self.name()? }
            }
            else {
                // `RENAME <old> TO <new>` is the SQLite spelling that omits the
                // optional `COLUMN` keyword.
                let old = self.name()?;
                self.expect_word("to")?;
                let new = self.name()?;
                AlterAction::RenameColumn { old, new }
            }
        }
        else if self.eat_word("add") {
            self.eat_word("column");
            let start = self.offset();
            let mut checks = Vec::new();
            let mut foreign_keys = Vec::new();
            let column = self.column_spec(&mut checks, &mut foreign_keys)?;
            let end = self.offset();
            let sql = self.source.get(start..end).unwrap_or_default().trim_end().to_string();
            AlterAction::AddColumn { column, checks, foreign_keys, sql }
        }
        else if self.eat_word("drop") {
            self.eat_word("column");
            AlterAction::DropColumn { column: self.name()? }
        }
        else {
            let found = self.peek().map(Token::describe).unwrap_or_else(|| "end of SQL".to_string());
            let mut error = self.error(
                "unsupported-clause",
                format!("unsupported `ALTER TABLE` action {found}; this engine accepts `RENAME TO`, `RENAME COLUMN`, and `ADD COLUMN` only"),
            );
            error.clause = Some("ALTER".to_string());
            return Err(error);
        };
        self.expect_end("ALTER TABLE")?;
        Ok(Statement::AlterTable { schema, table, action })
    }

    /// Rejects a `schema.object` qualifier. SQLite accepts it on `DROP TABLE`
    /// and silently ignores the schema; this engine keeps one schema, so the
    /// qualifier is named instead of being half-understood.
    fn reject_qualified(&mut self, name: &str) -> Result<(), SqlError> {
        if self.peek() == Some(&Token::Dot) {
            let mut error =
                self.error("unsupported-clause", format!("schema-qualified object names are not supported (`{name}.` was written)"));
            error.clause = Some("schema".to_string());
            return Err(error);
        }
        Ok(())
    }

    /// Requires the statement to stop after `kind`.
    fn expect_end(&self, kind: &str) -> Result<(), SqlError> {
        match self.peek() {
            None | Some(Token::Semi) => Ok(()),
            Some(token) => Err(self.error("syntax", format!("unexpected {} after `{kind}`", token.describe()))),
        }
    }

    fn create_table(&mut self, temp: bool) -> Result<Statement, SqlError> {
        if !temp {
            self.expect_word("table")?;
        }
        let if_not_exists = if self.eat_word("if") {
            self.expect_word("not")?;
            self.expect_word("exists")?;
            true
        }
        else {
            false
        };
        let name_token = self.at;
        let (schema, name) = self.qualified_name()?;
        if temp && schema.is_some() {
            return Err(self.error("syntax", "temporary table name must be unqualified"));
        }
        let name_gap_end = self.offsets.get(self.at).copied().unwrap_or(self.source.len());
        self.expect_char(&Token::LParen)?;
        let mut columns = Vec::new();
        let mut primary_key: Vec<String> = Vec::new();
        let mut table_constraints: Vec<TableConstraint> = Vec::new();
        let mut checks: Vec<CheckConstraint> = Vec::new();
        let mut foreign_keys: Vec<ForeignKey> = Vec::new();
        loop {
            let constraint = if self.eat_word("constraint") { Some(self.name()?) } else { None };
            if self.peek_keyword("primary") {
                // Table-level `PRIMARY KEY (a, b)`: SQLite keeps the table as a
                // rowid table and materializes one automatic unique index.
                self.at += 1;
                self.expect_word("key")?;
                self.expect_char(&Token::LParen)?;
                let mut key_columns = Vec::new();
                loop {
                    key_columns.push(self.name()?);
                    match self.peek() {
                        Some(Token::Comma) => {
                            self.at += 1;
                        }
                        Some(Token::RParen) => {
                            self.at += 1;
                            break;
                        }
                        Some(token) => {
                            return Err(
                                self.error("syntax", format!("expected `,` or `)` in the primary key list but found {}", token.describe()))
                            );
                        }
                        None => return Err(self.error("syntax", "primary key column list is not closed")),
                    }
                }
                primary_key.clone_from(&key_columns);
                table_constraints.push(TableConstraint::PrimaryKey { columns: key_columns });
            }
            else if self.peek_keyword("check") {
                checks.push(self.check_constraint(constraint)?);
            }
            else if self.eat_word("foreign") {
                // Table-level `[CONSTRAINT name] FOREIGN KEY (a, b)
                // REFERENCES parent [(x, y)]`.
                self.expect_word("key")?;
                let columns = self.column_list()?;
                self.expect_word("references")?;
                foreign_keys.push(self.references_clause(columns)?);
            }
            else if self.peek_keyword("unique") {
                // Table-level `UNIQUE (a, b)`: one `sqlite_autoindex_<table>_<n>`
                // unique index, numbered in written constraint order.
                self.at += 1;
                let unique_columns = self.column_list()?;
                table_constraints.push(TableConstraint::Unique { columns: unique_columns });
            }
            else if constraint.is_some() {
                let clause = self.peek_word().unwrap_or_default().to_string();
                let mut error = self.error(
                    "unsupported-column-constraint",
                    format!("unsupported named table constraint `{clause}`; only `CONSTRAINT name CHECK (...)` is supported"),
                );
                error.clause = Some(clause.to_ascii_uppercase());
                return Err(error);
            }
            else {
                columns.push(self.column_spec(&mut checks, &mut foreign_keys)?);
            }
            match self.peek() {
                Some(Token::Comma) => {
                    self.at += 1;
                }
                Some(Token::RParen) => {
                    self.at += 1;
                    break;
                }
                Some(token) => {
                    return Err(self.error("syntax", format!("expected `,` or `)` after a column definition but found {}", token.describe())));
                }
                None => return Err(self.error("syntax", "create table column list is not closed")),
            }
        }
        let mut strict = false;
        let mut without_rowid = false;
        loop {
            if self.eat_word("strict") {
                strict = true;
            }
            else if self.peek_keyword("without") {
                self.at += 1;
                self.expect_word("rowid")?;
                without_rowid = true;
            }
            else {
                break;
            }
            if self.peek() == Some(&Token::Comma) {
                self.at += 1;
            }
            else {
                break;
            }
        }
        let sql = self.canonical_create_sql("CREATE TABLE", name_token, name_gap_end);
        Ok(Statement::CreateTable {
            schema,
            name,
            if_not_exists,
            columns,
            primary_key,
            table_constraints,
            checks,
            foreign_keys,
            strict,
            without_rowid,
            temp,
            sql,
        })
    }

    /// Reads `CHECK ( expr )` after an optional `CONSTRAINT name`, and keeps the
    /// expression text exactly as written for the failure message.
    fn check_constraint(&mut self, name: Option<String>) -> Result<CheckConstraint, SqlError> {
        self.expect_word("check")?;
        self.expect_char(&Token::LParen)?;
        let start = self.offset();
        let expr = self.or_expr()?;
        let end = self.offset();
        let text = self.source.get(start..end).unwrap_or_default().trim().to_string();
        self.expect_char(&Token::RParen)?;
        Ok(CheckConstraint { name, expr, text })
    }

    /// Rebuilds `<prefix> <name>` with canonical keyword case and keeps the
    /// rest of the statement exactly as written. SQLite stores `CREATE TABLE`,
    /// `CREATE INDEX` and `CREATE VIEW` rows in `sqlite_master.sql` this way:
    /// the leading keywords are normalized, the name and tail are verbatim,
    /// and `IF NOT EXISTS` does not appear.
    fn canonical_create_sql(&self, prefix: &str, name_token: usize, name_gap_end: usize) -> String {
        let name_start = self.offsets.get(name_token).copied().unwrap_or(0);
        let gap_end = name_gap_end.min(self.source.len());
        let raw_name = self.source[name_start..gap_end].trim_end();
        let name_end = name_start + raw_name.len();
        let end = self.offsets.get(self.at).copied().unwrap_or(self.source.len());
        format!("{prefix} {}{}", raw_name, self.source[name_end..end].trim_end())
    }

    /// Reports the token the parser stopped on with SQLite's own wording, used
    /// where the grammar has a closed keyword set and anything else is a plain
    /// syntax error (`near "on": syntax error`).
    fn near_syntax_error(&self) -> SqlError {
        let message = match self.peek() {
            Some(Token::Word(word)) => format!("near \"{word}\": syntax error"),
            Some(token) => format!("near {}: syntax error", token.describe()),
            None => "incomplete input".to_string(),
        };
        self.error("syntax", message)
    }

    /// Reads `INDEX <name> ON <table> ( <column> [ASC] , ... )`.
    ///
    /// `ASC` is accepted because it names the default key order. `DESC` is
    /// rejected with `unsupported-clause`: the writer builds ascending b-trees
    /// only, and silently accepting the keyword would claim an index the image
    /// cannot honor.
    fn create_index(&mut self, unique: bool) -> Result<Statement, SqlError> {
        self.expect_word("index")?;
        let if_not_exists = if self.eat_word("if") {
            self.expect_word("not")?;
            self.expect_word("exists")?;
            true
        }
        else {
            false
        };
        let name_token = self.at;
        let (schema, name) = self.qualified_name()?;
        let name_gap_end = self.offsets.get(self.at).copied().unwrap_or(self.source.len());
        self.expect_word("on")?;
        let table = self.name()?;
        self.expect_char(&Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            // A bare column name is followed by `,`, `)`, `ASC`, `DESC` or
            // `COLLATE`; anything else starts an expression key.
            let next = self.tokens.get(self.at + 1);
            let plain_column = matches!(self.peek(), Some(Token::Word(_)) | Some(Token::Quoted(_)))
                && (matches!(next, Some(Token::Comma) | Some(Token::RParen))
                    || matches!(next, Some(Token::Word(word)) if word.eq_ignore_ascii_case("asc")
                        || word.eq_ignore_ascii_case("desc")
                        || word.eq_ignore_ascii_case("collate")));
            let (name, expr) = if plain_column { (self.name()?, None) } else { (String::new(), Some(self.or_expr()?)) };
            if matches!(self.peek_word(), Some(word) if word.eq_ignore_ascii_case("desc")) {
                let key = if name.is_empty() { "expression".to_string() } else { format!("`{name}`") };
                let mut error = self.error(
                    "unsupported-clause",
                    format!("`DESC` index key {key} is not supported; this engine builds ascending key order only"),
                );
                error.clause = Some("DESC".to_string());
                return Err(error);
            }
            self.eat_word("asc");
            let collate = if self.eat_word("collate") { Some(self.name()?) } else { None };
            columns.push(IndexColumn { name, expr, collate });
            match self.peek() {
                Some(Token::Comma) => {
                    self.at += 1;
                }
                Some(Token::RParen) => {
                    self.at += 1;
                    break;
                }
                Some(token) => {
                    return Err(self.error("syntax", format!("expected `,` or `)` in the index column list but found {}", token.describe())));
                }
                None => return Err(self.error("syntax", "create index column list is not closed")),
            }
        }
        let predicate = if self.eat_word("where") { Some(self.or_expr()?) } else { None };
        match self.peek() {
            None | Some(Token::Semi) => {
                let prefix = if unique { "CREATE UNIQUE INDEX" } else { "CREATE INDEX" };
                let sql = self.canonical_create_sql(prefix, name_token, name_gap_end);
                Ok(Statement::CreateIndex { schema, name, table, if_not_exists, unique, columns, predicate, sql })
            }
            Some(token) => {
                let mut error = self.error(
                    "unsupported-clause",
                    format!(
                        "unsupported `CREATE INDEX` clause starting at {}; this engine builds plain ascending indexes only",
                        token.describe()
                    ),
                );
                error.clause = Some(match token {
                    Token::Word(word) => word.to_ascii_uppercase(),
                    token => token.describe(),
                });
                Err(error)
            }
        }
    }

    fn column_spec(&mut self, checks: &mut Vec<CheckConstraint>, foreign_keys: &mut Vec<ForeignKey>) -> Result<ColumnSpec, SqlError> {
        let name = self.name()?;
        let declared_type = self.declared_type()?;
        let mut spec = ColumnSpec {
            name,
            declared_type,
            not_null: false,
            default_value: None,
            default_text: None,
            primary_key: false,
            unique: false,
            autoincrement: false,
        };
        loop {
            if self.eat_word("primary") {
                self.expect_word("key")?;
                spec.primary_key = true;
            }
            else if self.eat_word("autoincrement") {
                // SQLite only accepts AUTOINCREMENT directly after
                // `PRIMARY KEY`; anything else is reported as a syntax error at
                // the keyword itself.
                if !spec.primary_key {
                    return Err(self.error("syntax", "near \"AUTOINCREMENT\": syntax error"));
                }
                spec.autoincrement = true;
            }
            else if self.eat_word("not") {
                self.expect_word("null")?;
                spec.not_null = true;
            }
            else if self.eat_word("unique") {
                spec.unique = true;
            }
            else if self.eat_word("null") {
                // Explicit `NULL` is the SQLite default; nothing to record.
            }
            else if self.eat_word("default") {
                let start = self.offsets.get(self.at).copied().unwrap_or(self.source.len());
                let value = self.literal()?;
                let end = self.offsets.get(self.at).copied().unwrap_or(self.source.len());
                // SQLite echoes the written `DEFAULT` text, so keep the span
                // instead of rendering the evaluated literal.
                spec.default_text = self.source.get(start..end).map(|text| text.trim().to_string());
                spec.default_value = Some(value.to_write_value());
            }
            else if self.peek_keyword("constraint") {
                // Column-level `CONSTRAINT name CHECK (...)` or
                // `CONSTRAINT name REFERENCES ...`; other named constraints
                // stay rejected.
                self.at += 1;
                let constraint = self.name()?;
                if self.peek_keyword("check") {
                    checks.push(self.check_constraint(Some(constraint))?);
                }
                else if self.peek_keyword("references") {
                    self.at += 1;
                    foreign_keys.push(self.references_clause(vec![spec.name.clone()])?);
                }
                else {
                    let mut error = self.error("unsupported-column-constraint", format!("unsupported column constraint in `{}`", spec.name));
                    error.clause = Some(self.constraint_name());
                    return Err(error);
                }
            }
            else if self.eat_word("check") {
                self.at -= 1;
                checks.push(self.check_constraint(None)?);
            }
            else if self.eat_word("references") {
                foreign_keys.push(self.references_clause(vec![spec.name.clone()])?);
            }
            else if self.eat_word("collate") {
                let mut error = self.error("unsupported-column-constraint", format!("unsupported column constraint in `{}`", spec.name));
                error.clause = Some("COLLATE".to_string());
                return Err(error);
            }
            else if self.eat_word("generated") {
                let mut error = self.error("unsupported-generated-column", format!("unsupported generated column `{}`", spec.name));
                error.clause = Some("GENERATED".to_string());
                return Err(error);
            }
            else {
                break;
            }
        }
        if spec.autoincrement && !spec.declared_type.eq_ignore_ascii_case("INTEGER") {
            return Err(self.error("autoincrement-integer-primary-key", "AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY"));
        }
        Ok(spec)
    }

    /// Reads `REFERENCES parent [(col, ...)]` plus the optional `ON DELETE` /
    /// `ON UPDATE` actions and deferral clauses.
    ///
    /// `CASCADE`, `SET NULL` and `SET DEFAULT` are rejected here: this engine
    /// checks foreign keys immediately and never rewrites the child row, so
    /// accepting the action would silently drop its effect. `DEFERRABLE
    /// INITIALLY DEFERRED` is rejected for the same reason.
    fn references_clause(&mut self, columns: Vec<String>) -> Result<ForeignKey, SqlError> {
        let parent = self.name()?;
        self.reject_qualified(&parent)?;
        let parent_columns = if self.peek() == Some(&Token::LParen) { self.column_list()? } else { Vec::new() };
        let mut key =
            ForeignKey { columns, parent, parent_columns, on_delete: ForeignKeyAction::NoAction, on_update: ForeignKeyAction::NoAction };
        loop {
            if self.eat_word("on") {
                if self.eat_word("delete") {
                    key.on_delete = self.foreign_key_action("DELETE")?;
                }
                else if self.eat_word("update") {
                    key.on_update = self.foreign_key_action("UPDATE")?;
                }
                else {
                    return Err(self.error("syntax", "expected `DELETE` or `UPDATE` after `ON` in a foreign key"));
                }
            }
            else if self.peek_keyword("not")
                && matches!(self.tokens.get(self.at + 1), Some(Token::Word(word)) if word.eq_ignore_ascii_case("deferrable"))
            {
                // `NOT DEFERRABLE` is the default and has no effect here.
                self.at += 2;
            }
            else if self.eat_word("deferrable") {
                if self.eat_word("initially") {
                    if self.eat_word("deferred") {
                        let mut error = self.error(
                            "unsupported-deferred-foreign-key",
                            "`DEFERRABLE INITIALLY DEFERRED` is not supported: foreign keys are checked immediately",
                        );
                        error.clause = Some("DEFERRABLE".to_string());
                        return Err(error);
                    }
                    self.expect_word("immediate")?;
                }
            }
            else {
                break;
            }
        }
        Ok(key)
    }

    /// Reads one referential action after `ON DELETE` / `ON UPDATE`.
    fn foreign_key_action(&mut self, clause: &str) -> Result<ForeignKeyAction, SqlError> {
        if self.eat_word("no") {
            self.expect_word("action")?;
            Ok(ForeignKeyAction::NoAction)
        }
        else if self.eat_word("restrict") {
            Ok(ForeignKeyAction::Restrict)
        }
        else if self.eat_word("cascade") {
            Err(self.unsupported_foreign_key_action(clause, "CASCADE"))
        }
        else if self.eat_word("set") {
            if self.eat_word("null") {
                Err(self.unsupported_foreign_key_action(clause, "SET NULL"))
            }
            else if self.eat_word("default") {
                Err(self.unsupported_foreign_key_action(clause, "SET DEFAULT"))
            }
            else {
                Err(self.error("syntax", format!("expected `NULL` or `DEFAULT` after `SET` in `ON {clause}`")))
            }
        }
        else {
            Err(self.error("syntax", format!("expected a referential action after `ON {clause}`")))
        }
    }

    fn unsupported_foreign_key_action(&self, clause: &str, action: &str) -> SqlError {
        let mut error = self.error(
            "unsupported-foreign-key-action",
            format!("`ON {clause} {action}` is not supported: cascading referential actions are not implemented"),
        );
        error.clause = Some(action.to_string());
        error
    }

    /// Reads a parenthesized column-name list, as in `(a, b)`.
    fn column_list(&mut self) -> Result<Vec<String>, SqlError> {
        self.expect_char(&Token::LParen)?;
        if self.peek() == Some(&Token::RParen) {
            return Err(self.error("syntax", "near \")\": syntax error"));
        }
        let mut names = Vec::new();
        loop {
            names.push(self.name()?);
            if self.peek() == Some(&Token::Comma) {
                self.at += 1;
                continue;
            }
            break;
        }
        self.expect_char(&Token::RParen)?;
        Ok(names)
    }

    /// Reads a column's declared type: type-name words plus an optional
    /// `( … )` parameter list, as in `VARCHAR(30)` or `DECIMAL(10,2)`.
    ///
    /// SQLite keeps the declared text verbatim in `sqlite_master` and derives
    /// the column affinity from it (fileformat2 §3.1): a name containing `INT`
    /// is INTEGER, `CHAR`/`CLOB`/`TEXT` is TEXT, `BLOB` or an omitted type is
    /// BLOB, `REAL`/`FLOA`/`DOUB` is REAL, and everything else (`DECIMAL`,
    /// `NUMERIC`, ...) is NUMERIC. The storage layer already owns that
    /// derivation, so the parser accepts the parameters and records the type
    /// exactly as written.
    fn declared_type(&mut self) -> Result<String, SqlError> {
        let mut words = Vec::new();
        while let Some(word) = self.peek_word() {
            let lower = word.to_ascii_lowercase();
            if matches!(
                lower.as_str(),
                "primary"
                    | "not"
                    | "null"
                    | "unique"
                    | "default"
                    | "constraint"
                    | "check"
                    | "references"
                    | "collate"
                    | "generated"
                    | "autoincrement"
            ) {
                break;
            }
            words.push(word.to_string());
            self.at += 1;
        }
        let mut declared = if words.is_empty() { "BLOB".to_string() } else { words.join(" ") };
        if self.peek() == Some(&Token::LParen) {
            self.at += 1;
            let mut parameters = Vec::new();
            loop {
                let negative = matches!(self.peek(), Some(Token::Minus));
                if negative {
                    self.at += 1;
                }
                let parameter = match self.take()? {
                    Token::Int(value) if negative => format!("-{value}"),
                    Token::Real(value) if negative => format!("-{value}"),
                    Token::Int(value) => value.to_string(),
                    Token::Real(value) => format!("{value}"),
                    other => return Err(self.error("syntax", format!("expected a numeric type parameter but found {}", other.describe()))),
                };
                parameters.push(parameter);
                match self.peek() {
                    Some(Token::Comma) => {
                        self.at += 1;
                    }
                    Some(Token::RParen) => {
                        self.at += 1;
                        break;
                    }
                    Some(token) => {
                        return Err(
                            self.error("syntax", format!("expected `,` or `)` in the type parameter list but found {}", token.describe()))
                        );
                    }
                    None => return Err(self.error("syntax", "column type parameter list is not closed")),
                }
            }
            declared.push('(');
            declared.push_str(&parameters.join(","));
            declared.push(')');
        }
        Ok(declared)
    }

    fn constraint_name(&self) -> String {
        let at = self.at.saturating_sub(1);
        match self.tokens.get(at) {
            Some(Token::Word(word)) => word.to_ascii_uppercase(),
            Some(token) => token.describe(),
            None => "constraint".to_string(),
        }
    }

    /// Parses `?`, `?NNN` or `:name` at the current position, returning the
    /// zero-based binding index and the text as written. Other operators are
    /// left for the expression parser.
    fn parameter(&mut self, operator: &str) -> Result<Option<(usize, String)>, SqlError> {
        match operator {
            "?" => {
                let marker = self.offset();
                self.at += 1;
                let (index, text) = match self.peek().cloned() {
                    Some(Token::Int(value)) if self.offset() == marker + 1 => {
                        if value <= 0 {
                            return Err(self.error("syntax", "parameter indexes are one-based"));
                        }
                        self.at += 1;
                        (value as usize - 1, format!("?{value}"))
                    }
                    _ => {
                        let index = self.next_parameter;
                        self.next_parameter += 1;
                        (index, "?".to_string())
                    }
                };
                // `?NNN` fixes that slot, so later bare `?` placeholders
                // continue after the highest index seen so far.
                self.next_parameter = self.next_parameter.max(index + 1);
                Ok(Some((index, text)))
            }
            ":" => {
                self.at += 1;
                let name = match self.next() {
                    Some(Token::Word(word)) => word,
                    _ => return Err(self.error("syntax", "expected a parameter name after `:`")),
                };
                let text = format!(":{name}");
                let index = match self.parameter_names.get(&name) {
                    Some(index) => *index,
                    None => {
                        let index = self.next_parameter;
                        self.next_parameter += 1;
                        self.parameter_names.insert(name, index);
                        index
                    }
                };
                Ok(Some((index, text)))
            }
            _ => Ok(None),
        }
    }

    /// Parses one `VALUES` tuple element: a bound placeholder, a literal, or a
    /// general expression when the token cannot start a literal.
    fn insert_value(&mut self) -> Result<InsertValue, SqlError> {
        if let Some(Token::Operator(operator)) = self.peek() {
            let operator = operator.clone();
            if let Some((index, text)) = self.parameter(&operator)? {
                return Ok(InsertValue::Parameter { index, text });
            }
        }
        let literal_head = match self.peek() {
            Some(Token::Int(_)) | Some(Token::Real(_)) | Some(Token::Text(_)) | Some(Token::Blob(_)) => true,
            Some(Token::Word(word)) => {
                word.eq_ignore_ascii_case("null") || word.eq_ignore_ascii_case("true") || word.eq_ignore_ascii_case("false")
            }
            Some(Token::Minus) => matches!(self.tokens.get(self.at + 1), Some(Token::Int(_)) | Some(Token::Real(_))),
            _ => false,
        };
        if literal_head {
            return Ok(InsertValue::Literal(self.literal()?));
        }
        Ok(InsertValue::Expression(self.or_expr()?))
    }

    fn literal(&mut self) -> Result<Literal, SqlError> {
        let negative = matches!(self.peek(), Some(Token::Minus));
        if negative {
            self.at += 1;
        }
        let token = self.take()?;
        let literal = match token {
            Token::Int(value) if negative => {
                Literal::Integer(value.checked_neg().ok_or_else(|| self.error("syntax", "integer literal underflow"))?)
            }
            Token::Real(value) if negative => Literal::Real(-value),
            Token::Int(value) => Literal::Integer(value),
            Token::Real(value) => Literal::Real(value),
            Token::Text(value) => Literal::Text(value),
            Token::Blob(value) => Literal::Blob(value),
            Token::Word(word) if word.eq_ignore_ascii_case("null") => Literal::Null,
            Token::Word(word) if word.eq_ignore_ascii_case("true") => Literal::Integer(1),
            Token::Word(word) if word.eq_ignore_ascii_case("false") => Literal::Integer(0),
            other => return Err(self.error("syntax", format!("expected a literal but found {}", other.describe()))),
        };
        if negative && matches!(literal, Literal::Integer(_) | Literal::Real(_)) {
            return Ok(literal);
        }
        if negative {
            return Err(self.error("syntax", "`-` must prefix a numeric literal"));
        }
        Ok(literal)
    }

    fn insert(&mut self) -> Result<Statement, SqlError> {
        self.expect_word("into")?;
        let (schema, table) = self.qualified_name()?;
        let columns = if self.peek() == Some(&Token::LParen) {
            self.at += 1;
            let mut names = Vec::new();
            loop {
                names.push(self.name()?);
                match self.peek() {
                    Some(Token::Comma) => {
                        self.at += 1;
                    }
                    Some(Token::RParen) => {
                        self.at += 1;
                        break;
                    }
                    Some(token) => {
                        return Err(self.error("syntax", format!("expected `,` or `)` in the column list but found {}", token.describe())));
                    }
                    None => return Err(self.error("syntax", "insert column list is not closed")),
                }
            }
            Some(names)
        }
        else {
            None
        };
        if columns.is_none() && self.peek_keyword("default") {
            // `INSERT INTO t DEFAULT VALUES`: one row whose every slot falls back to
            // the declared default. An explicit column list is not allowed here.
            self.expect_word("default")?;
            self.expect_word("values")?;
            let conflict = self.conflict_clause()?;
            let returning = if self.eat_word("returning") { Some(self.returning()?) } else { None };
            return Ok(Statement::Insert { schema, table, columns, rows: Vec::new(), default_values: true, conflict, returning });
        }
        self.expect_word("values")?;
        let mut rows = Vec::new();
        loop {
            self.expect_char(&Token::LParen)?;
            let mut values = Vec::new();
            loop {
                values.push(self.insert_value()?);
                match self.peek() {
                    Some(Token::Comma) => {
                        self.at += 1;
                    }
                    Some(Token::RParen) => {
                        self.at += 1;
                        break;
                    }
                    Some(token) => {
                        return Err(self.error("syntax", format!("expected `,` or `)` in a VALUES tuple but found {}", token.describe())));
                    }
                    None => return Err(self.error("syntax", "insert VALUES tuple is not closed")),
                }
            }
            rows.push(values);
            if self.peek() == Some(&Token::Comma) {
                self.at += 1;
                continue;
            }
            break;
        }
        let conflict = self.conflict_clause()?;
        let returning = if self.eat_word("returning") { Some(self.returning()?) } else { None };
        Ok(Statement::Insert { schema, table, columns, rows, default_values: false, conflict, returning })
    }

    /// Parses `ON CONFLICT [(target)] [WHERE ...] DO NOTHING | DO UPDATE SET ...`.
    fn conflict_clause(&mut self) -> Result<Option<ConflictClause>, SqlError> {
        let matches = self.peek_word().is_some_and(|word| word.eq_ignore_ascii_case("on"))
            && self.peek_word_at(1).is_some_and(|word| word.eq_ignore_ascii_case("conflict"));
        if !matches {
            return Ok(None);
        }
        self.at += 2;
        let target = if self.peek() == Some(&Token::LParen) { Some(self.column_list()?) } else { None };
        let predicate = if self.eat_word("where") { Some(self.or_expr()?) } else { None };
        self.expect_word("do")?;
        if self.eat_word("nothing") {
            return Ok(Some(ConflictClause { target, predicate, action: ConflictAction::Nothing }));
        }
        self.expect_word("update")?;
        self.expect_word("set")?;
        let mut assignments = Vec::new();
        loop {
            if self.peek() == Some(&Token::LParen) {
                let mut error = self
                    .error("unsupported-clause", "row-value `SET (a, b) = (...)` assignments are not supported; assign one column per item");
                error.clause = Some("SET".to_string());
                return Err(error);
            }
            let column = self.name()?;
            match self.peek() {
                Some(Token::Operator(operator)) if operator == "=" => self.at += 1,
                Some(token) => return Err(self.error("syntax", format!("expected `=` in a `SET` assignment but found {}", token.describe()))),
                None => return Err(self.error("syntax", "`SET` assignment is incomplete")),
            }
            let value = self.or_expr()?;
            assignments.push(Assignment { column, value });
            if self.peek() == Some(&Token::Comma) {
                self.at += 1;
                continue;
            }
            break;
        }
        let update_predicate = if self.eat_word("where") { Some(self.or_expr()?) } else { None };
        Ok(Some(ConflictClause { target, predicate, action: ConflictAction::Update { assignments, predicate: update_predicate } }))
    }

    /// Rejects a clause that SQLite does not accept on `UPDATE` / `DELETE`.
    fn statement_ends(&self, kind: &str) -> Result<(), SqlError> {
        match self.peek() {
            None | Some(Token::Semi) => Ok(()),
            Some(token) => {
                let mut error = self.error(
                    "unsupported-clause",
                    format!(
                        "unsupported clause after `{kind}` starting at {}; `ORDER BY`, `LIMIT` and `OFFSET` are not accepted here",
                        token.describe()
                    ),
                );
                error.clause = Some(match token {
                    Token::Word(word) => word.to_ascii_uppercase(),
                    other => other.describe(),
                });
                Err(error)
            }
        }
    }

    /// Reads the `*` or expression list of a `RETURNING` clause.
    fn returning(&mut self) -> Result<SelectItems, SqlError> {
        if self.peek() == Some(&Token::Star) {
            self.at += 1;
            if self.peek() == Some(&Token::Comma) {
                let mut error =
                    self.error("unsupported-clause", "`RETURNING *, ...` mixes `*` with further items; list the columns explicitly instead");
                error.clause = Some("RETURNING".to_string());
                return Err(error);
            }
            return Ok(SelectItems::Star);
        }
        let mut items = Vec::new();
        loop {
            let mut item = self.or_expr()?;
            if self.eat_word("as") {
                let name = self.name()?;
                item = Expr::Alias { value: Box::new(item), name };
            }
            items.push(item);
            if self.peek() == Some(&Token::Comma) {
                self.at += 1;
                continue;
            }
            break;
        }
        Ok(SelectItems::Named(items))
    }

    fn update(&mut self) -> Result<Statement, SqlError> {
        if self.eat_word("or") {
            let found = self.peek().map(Token::describe).unwrap_or_else(|| "end of SQL".to_string());
            let mut error = self.error(
                "unsupported-clause",
                format!("`UPDATE OR ...` conflict resolution is not supported; only the default ABORT policy is implemented (found {found})"),
            );
            error.clause = Some("OR".to_string());
            return Err(error);
        }
        let (schema, table) = self.qualified_name()?;
        let alias = if self.eat_word("as") { Some(self.name()?) } else { None };
        self.expect_word("set")?;
        let mut assignments = Vec::new();
        loop {
            if self.peek() == Some(&Token::LParen) {
                let mut error = self
                    .error("unsupported-clause", "row-value `SET (a, b) = (...)` assignments are not supported; assign one column per item");
                error.clause = Some("SET".to_string());
                return Err(error);
            }
            let column = self.name()?;
            if self.peek() == Some(&Token::Dot) {
                return Err(self.error("syntax", "`UPDATE ... SET` takes a bare column name; a table qualifier is not accepted before `.`"));
            }
            match self.peek() {
                Some(Token::Operator(operator)) if operator == "=" => self.at += 1,
                Some(token) => return Err(self.error("syntax", format!("expected `=` in a `SET` assignment but found {}", token.describe()))),
                None => return Err(self.error("syntax", "`SET` assignment is incomplete")),
            }
            let found_default = self.peek_word().is_some_and(|word| word.eq_ignore_ascii_case("default"));
            if found_default {
                let mut error = self.error(
                    "unsupported-clause",
                    "`SET <column> = DEFAULT` is not supported; SQLite has no column default in an `UPDATE`, so state the value explicitly",
                );
                error.clause = Some("DEFAULT".to_string());
                return Err(error);
            }
            let value = self.or_expr()?;
            assignments.push(Assignment { column, value });
            if self.peek() == Some(&Token::Comma) {
                self.at += 1;
                continue;
            }
            break;
        }
        let filter = if self.eat_word("where") { Some(self.or_expr()?) } else { None };
        let returning = if self.eat_word("returning") { Some(self.returning()?) } else { None };
        self.statement_ends("UPDATE")?;
        Ok(Statement::Update { schema, table, alias, assignments, filter, returning })
    }

    fn delete(&mut self) -> Result<Statement, SqlError> {
        self.expect_word("from")?;
        let (schema, table) = self.qualified_name()?;
        let alias = if self.eat_word("as") { Some(self.name()?) } else { None };
        let filter = if self.eat_word("where") { Some(self.or_expr()?) } else { None };
        let returning = if self.eat_word("returning") { Some(self.returning()?) } else { None };
        self.statement_ends("DELETE")?;
        Ok(Statement::Delete { schema, table, alias, filter, returning })
    }

    fn select(&mut self) -> Result<Statement, SqlError> {
        let first = self.select_core()?;
        let mut compounds = Vec::new();
        loop {
            let position = self.offset();
            let Some(op) = self.compound_operator()
            else {
                break;
            };
            self.expect_word("select")?;
            compounds.push(CompoundTerm { op, core: self.select_core()?, position });
        }
        let order = if self.eat_word("order") {
            self.expect_word("by")?;
            let mut keys = Vec::new();
            loop {
                keys.push(self.order_key()?);
                if self.peek() == Some(&Token::Comma) {
                    self.at += 1;
                    continue;
                }
                break;
            }
            keys
        }
        else {
            Vec::new()
        };
        let limit = if self.eat_word("limit") {
            let first = self.limit_term("LIMIT")?;
            if self.peek() == Some(&Token::Comma) {
                // SQLite's `LIMIT <offset>, <count>` form.
                self.at += 1;
                Some(Limit { count: self.limit_term("LIMIT")?, offset: Some(first) })
            }
            else {
                let offset = if self.eat_word("offset") { Some(self.limit_term("OFFSET")?) } else { None };
                Some(Limit { count: first, offset })
            }
        }
        else {
            None
        };
        match self.peek() {
            None | Some(Token::Semi) | Some(Token::RParen) => {
                Ok(Statement::Select { with: Vec::new(), recursive: false, first, compounds, order, limit })
            }
            Some(Token::Word(word)) if is_compound_keyword(word) => {
                let mut error = self.error("syntax", format!("`ORDER BY` clause should come after {} not before", word.to_ascii_uppercase()));
                error.clause = Some(word.to_ascii_uppercase());
                Err(error)
            }
            Some(token) => {
                let mut error = self.error(
                    "unsupported-clause",
                    format!(
                        "unsupported `SELECT` clause starting at {}; this engine supports `FROM`, `WHERE`, `ORDER BY`, `LIMIT` and `UNION` / `INTERSECT` / `EXCEPT` only",
                        token.describe()
                    ),
                );
                error.clause = Some(match token {
                    Token::Word(word) => word.to_ascii_uppercase(),
                    token => token.describe(),
                });
                Err(error)
            }
        }
    }

    /// Parses one `SELECT` core, stopping before any set operator or trailing
    /// `ORDER BY` / `LIMIT`.
    fn select_core(&mut self) -> Result<SelectCore, SqlError> {
        let distinct = self.eat_word("distinct");
        // `SELECT ALL` is the default and changes nothing.
        self.eat_word("all");
        let items = if self.peek() == Some(&Token::Star) {
            self.at += 1;
            SelectItems::Star
        }
        else if self.peek_word().is_some_and(|word| word.eq_ignore_ascii_case("count"))
            && matches!(self.tokens.get(self.at + 1), Some(Token::LParen))
            && matches!(self.tokens.get(self.at + 2), Some(Token::Star))
            && matches!(self.tokens.get(self.at + 3), Some(Token::RParen))
            && matches!(self.tokens.get(self.at + 4), Some(Token::Word(word)) if word.eq_ignore_ascii_case("from"))
        {
            self.at += 4;
            SelectItems::CountAll
        }
        else {
            let mut names = Vec::new();
            loop {
                // SQLite names an unaliased projection after the exact input
                // span (`a + b`, `'x'`, `1.50`), so the span is captured before
                // the expression consumes its tokens.
                let start = self.offsets.get(self.at).copied().unwrap_or(self.source.len());
                let mut item = self.or_expr()?;
                let end = self.offsets.get(self.at).copied().unwrap_or(self.source.len());
                if self.eat_word("as") {
                    let name = self.name()?;
                    item = Expr::Alias { value: Box::new(item), name };
                }
                else if !matches!(&item, Expr::Column(_)) {
                    // A bare column keeps the schema's own spelling; every
                    // other expression keeps the bytes written in the query.
                    let text = self.source.get(start..end).unwrap_or_default().trim();
                    if !text.is_empty() {
                        item = Expr::Alias { value: Box::new(item), name: text.to_string() };
                    }
                }
                names.push(item);
                // A projection with no `FROM` ends at a clause keyword, a set
                // operator or the end of the enclosing statement, exactly like
                // `SELECT 1` in SQLite.
                let ends = match self.peek() {
                    Some(Token::Comma) => false,
                    Some(Token::Word(word)) => match word.to_ascii_lowercase().as_str() {
                        "from" | "union" | "intersect" | "except" | "where" | "having" | "window" => true,
                        "limit" => true,
                        "order" | "group" => matches!(self.tokens.get(self.at + 1), Some(Token::Word(next)) if next.eq_ignore_ascii_case("by")),
                        _ => false,
                    },
                    Some(Token::RParen) | Some(Token::Semi) | None => true,
                    _ => false,
                };
                if ends {
                    break;
                }
                match self.peek() {
                    Some(Token::Comma) => {
                        self.at += 1;
                    }
                    Some(token) => {
                        return Err(self.error("syntax", format!("expected `,` or `FROM` in the projection but found {}", token.describe())));
                    }
                    None => return Err(self.error("syntax", "select projection is incomplete")),
                }
            }
            SelectItems::Named(names)
        };
        let has_from = self.eat_word("from");
        if matches!(items, SelectItems::Star) && !has_from {
            let mut error = self.error("no-tables", "no tables specified");
            error.clause = Some("FROM".to_string());
            return Err(error);
        }
        let mut from: Vec<TableRef> = Vec::new();
        let mut starts = Vec::new();
        if has_from {
            loop {
                starts.push(self.offset());
                from.push(self.table_ref()?);
                // A chain of joins attaches to the entry just read. `CROSS JOIN`
                // is the same row set as a comma, so it is represented as a plain
                // product.
                loop {
                    let word = match self.peek_word() {
                        Some(word) => word.to_ascii_lowercase(),
                        None => break,
                    };
                    let (kind, natural) = match word.as_str() {
                        "join" => {
                            self.at += 1;
                            (JoinKind::Inner, false)
                        }
                        "inner" => {
                            self.at += 1;
                            self.expect_word("join")?;
                            (JoinKind::Inner, false)
                        }
                        "left" => {
                            self.at += 1;
                            if matches!(self.peek_word(), Some(word) if word.eq_ignore_ascii_case("outer")) {
                                self.at += 1;
                            }
                            self.expect_word("join")?;
                            (JoinKind::Left, false)
                        }
                        "right" => {
                            self.at += 1;
                            if matches!(self.peek_word(), Some(word) if word.eq_ignore_ascii_case("outer")) {
                                self.at += 1;
                            }
                            self.expect_word("join")?;
                            (JoinKind::Right, false)
                        }
                        "full" => {
                            self.at += 1;
                            if matches!(self.peek_word(), Some(word) if word.eq_ignore_ascii_case("outer")) {
                                self.at += 1;
                            }
                            self.expect_word("join")?;
                            (JoinKind::Full, false)
                        }
                        "cross" => {
                            self.at += 1;
                            self.expect_word("join")?;
                            starts.push(self.offset());
                            from.push(self.table_ref()?);
                            continue;
                        }
                        "natural" => {
                            self.at += 1;
                            let kind = match self.peek_word().map(|word| word.to_ascii_lowercase()).as_deref() {
                                Some("left") => {
                                    self.at += 1;
                                    if matches!(self.peek_word(), Some(word) if word.eq_ignore_ascii_case("outer")) {
                                        self.at += 1;
                                    }
                                    JoinKind::Left
                                }
                                Some("right") => {
                                    self.at += 1;
                                    if matches!(self.peek_word(), Some(word) if word.eq_ignore_ascii_case("outer")) {
                                        self.at += 1;
                                    }
                                    JoinKind::Right
                                }
                                Some("full") => {
                                    self.at += 1;
                                    if matches!(self.peek_word(), Some(word) if word.eq_ignore_ascii_case("outer")) {
                                        self.at += 1;
                                    }
                                    JoinKind::Full
                                }
                                Some("inner") => {
                                    self.at += 1;
                                    JoinKind::Inner
                                }
                                _ => JoinKind::Inner,
                            };
                            self.expect_word("join")?;
                            (kind, true)
                        }
                        _ => break,
                    };
                    starts.push(self.offset());
                    let mut table = self.table_ref()?;
                    table.join = Some(kind);
                    table.natural = natural;
                    if natural {
                        if self.peek_keyword("on") || self.peek_keyword("using") {
                            let mut error = self.error("syntax", "a NATURAL join may not have an ON or USING clause");
                            error.clause = Some("JOIN".to_string());
                            return Err(error);
                        }
                    }
                    else if self.eat_word("using") {
                        table.using = Some(self.using_columns()?);
                        if self.peek_keyword("on") {
                            return Err(self.error("syntax", "near \"ON\": syntax error"));
                        }
                    }
                    else if self.eat_word("on") {
                        table.on = Some(self.or_expr()?);
                    }
                    // A join without `ON` or `USING` is legal; SQLite treats the
                    // whole pair set as matching, the same as a product.
                    from.push(table);
                }
                if self.peek() == Some(&Token::Comma) {
                    self.at += 1;
                    continue;
                }
                break;
            }
        }
        // Each source needs a distinct label; a repeat would make every
        // qualified reference ambiguous, so it is rejected at parse time.
        let mut labels: Vec<String> = Vec::with_capacity(from.len());
        for (index, table) in from.iter().enumerate() {
            // An unaliased derived table exposes no qualifier at all, so its
            // empty label may repeat without making references ambiguous.
            if table.label().is_empty() {
                continue;
            }
            let label = table.label().to_ascii_lowercase();
            if labels.contains(&label) {
                let mut error = self.error("duplicate-alias", format!("table alias `{}` is used more than once in `FROM`", table.label()));
                error.position = Some(starts[index]);
                return Err(error);
            }
            labels.push(label);
        }
        let filter = if self.eat_word("where") { Some(self.or_expr()?) } else { None };
        let group = if self.eat_word("group") {
            self.expect_word("by")?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.or_expr()?);
                if self.peek() == Some(&Token::Comma) {
                    self.at += 1;
                    continue;
                }
                break;
            }
            exprs
        }
        else {
            Vec::new()
        };
        let having = if self.eat_word("having") { Some(self.or_expr()?) } else { None };
        let mut windows = Vec::new();
        if self.eat_word("window") {
            loop {
                let name = self.name()?;
                self.expect_word("as")?;
                self.expect_char(&Token::LParen)?;
                let spec = self.window_spec()?;
                self.expect_char(&Token::RParen)?;
                windows.push(NamedWindow { name, spec });
                if self.peek() == Some(&Token::Comma) {
                    self.at += 1;
                    continue;
                }
                break;
            }
        }
        Ok(SelectCore { distinct, items, from, filter, group, having, windows })
    }

    /// Consumes a set operator keyword when one follows a core.
    fn compound_operator(&mut self) -> Option<CompoundOp> {
        let word = self.peek_word()?.to_ascii_lowercase();
        let base = match word.as_str() {
            "union" => CompoundOp::Union,
            "intersect" => CompoundOp::Intersect,
            "except" => CompoundOp::Except,
            _ => return None,
        };
        self.at += 1;
        // SQLite only allows `ALL` after `UNION`; `INTERSECT ALL` and
        // `EXCEPT ALL` are syntax errors. Leaving the keyword in place lets
        // the caller's statement-end check report it.
        if base == CompoundOp::Union && self.eat_word("all") {
            return Some(CompoundOp::UnionAll);
        }
        Some(base)
    }
}

/// Whether a CTE body reads the CTE's own name from a `FROM` position.
///
/// A `WITH` nested inside the body may shadow the name, so its entries are
/// skipped; derived tables and expression subqueries are followed because
/// they still run in the same name scope.
pub(crate) fn statement_references(statement: &Statement, name: &str) -> bool {
    let Statement::Select { first, compounds, order, .. } = statement
    else {
        return false;
    };
    core_references(first, name)
        || compounds.iter().any(|term| core_references(&term.core, name))
        || order.iter().any(|key| expr_references(&key.expr, name))
}

/// Whether one `SELECT` core reads `name` in its `FROM`, projection,
/// predicate, grouping or window clauses.
pub(crate) fn core_references(core: &SelectCore, name: &str) -> bool {
    let source = core.from.iter().any(|table| match &table.query {
        Some(query) => statement_references(query, name),
        None => table.name.eq_ignore_ascii_case(name),
    });
    if source {
        return true;
    }
    let items = match &core.items {
        SelectItems::Named(items) => items.iter().any(|item| expr_references(item, name)),
        SelectItems::Star | SelectItems::CountAll => false,
    };
    items
        || core.filter.as_ref().is_some_and(|expr| expr_references(expr, name))
        || core.group.iter().any(|expr| expr_references(expr, name))
        || core.having.as_ref().is_some_and(|expr| expr_references(expr, name))
}

fn expr_references(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Column(_) | Expr::Literal(_) | Expr::Parameter { .. } => false,
        Expr::Unary { value, .. } => expr_references(value, name),
        Expr::Binary { left, right, .. } => expr_references(left, name) || expr_references(right, name),
        Expr::Compare { left, right, .. } => expr_references(left, name) || expr_references(right, name),
        Expr::IsNull { value, .. } | Expr::IsTruth { value, .. } => expr_references(value, name),
        Expr::Not(inner) => expr_references(inner, name),
        Expr::And(left, right) | Expr::Or(left, right) => expr_references(left, name) || expr_references(right, name),
        Expr::Case { operand, branches, otherwise } => {
            operand.as_ref().is_some_and(|expr| expr_references(expr, name))
                || branches.iter().any(|(condition, result)| expr_references(condition, name) || expr_references(result, name))
                || otherwise.as_ref().is_some_and(|expr| expr_references(expr, name))
        }
        Expr::In { value, list, .. } => expr_references(value, name) || list.iter().any(|item| expr_references(item, name)),
        Expr::Function { args, .. } => args.iter().any(|arg| expr_references(arg, name)),
        Expr::Subquery(statement) | Expr::Exists(statement) => statement_references(statement, name),
        Expr::InSubquery { value, query, .. } => expr_references(value, name) || statement_references(query, name),
        Expr::Aggregate { argument, .. } => argument.as_ref().is_some_and(|expr| expr_references(expr, name)),
        Expr::Alias { value, .. } => expr_references(value, name),
        Expr::Cast { value, .. } => expr_references(value, name),
        Expr::Like { value, pattern, escape, .. } => {
            expr_references(value, name)
                || expr_references(pattern, name)
                || escape.as_ref().is_some_and(|escape| expr_references(escape, name))
        }
        Expr::Window { function, spec } => window_references(function, spec, name),
    }
}

/// True when any expression of the window call or its `OVER` clause names `name`.
fn window_references(function: &WindowFunction, spec: &WindowSpec, name: &str) -> bool {
    let function_arguments = match function {
        WindowFunction::Ntile(value) | WindowFunction::FirstValue(value) | WindowFunction::LastValue(value) => {
            vec![value.as_ref()]
        }
        WindowFunction::Lag { value, offset, default } | WindowFunction::Lead { value, offset, default } => {
            let mut arguments = vec![value.as_ref()];
            if let Some(offset) = offset {
                arguments.push(offset.as_ref());
            }
            if let Some(default) = default {
                arguments.push(default.as_ref());
            }
            arguments
        }
        WindowFunction::NthValue { value, position } => vec![value.as_ref(), position.as_ref()],
        WindowFunction::Aggregate { argument, separator, .. } => {
            let mut arguments = Vec::new();
            if let Some(argument) = argument {
                arguments.push(argument.as_ref());
            }
            if let Some(separator) = separator {
                arguments.push(separator.as_ref());
            }
            arguments
        }
        _ => Vec::new(),
    };
    if function_arguments.iter().any(|argument| expr_references(argument, name)) {
        return true;
    }
    spec.partition.iter().any(|expr| expr_references(expr, name))
        || spec.order.iter().any(|key| expr_references(&key.expr, name))
        || spec.frame.as_ref().is_some_and(|frame| {
            let bound = |bound: &FrameBound| bound.offset.as_ref().is_some_and(|expr| expr_references(expr, name));
            bound(&frame.start) || bound(&frame.end)
        })
}

/// Set operator keywords that may only appear before the trailing clause.
fn is_compound_keyword(word: &str) -> bool {
    matches!(word.to_ascii_lowercase().as_str(), "union" | "intersect" | "except")
}

fn describe_expected(token: &Token) -> String {
    token.describe()
}

/// Keywords that terminate a `FROM` entry, so a bare alias may not take them.
fn is_clause_keyword(word: &str) -> bool {
    matches!(
        word.to_ascii_lowercase().as_str(),
        "as" | "from"
            | "where"
            | "order"
            | "limit"
            | "offset"
            | "group"
            | "having"
            | "join"
            | "inner"
            | "outer"
            | "left"
            | "right"
            | "full"
            | "cross"
            | "natural"
            | "on"
            | "using"
            | "union"
            | "except"
            | "intersect"
            | "window"
            | "index"
            | "case"
            | "when"
            | "then"
            | "else"
            | "end"
            | "in"
    )
}

/// Aggregate names that are reported as unsupported rather than "no such
/// function", because SQLite does know them but this engine does not aggregate.
fn is_aggregate_name(name: &str) -> bool {
    matches!(name.to_ascii_lowercase().as_str(), "count" | "avg" | "sum" | "total" | "min" | "max")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(sql: &str) -> Expr {
        let statements = parse(sql).expect("statement parses");
        match statements.into_iter().next().expect("one statement") {
            Statement::Select { first: SelectCore { filter: Some(filter), .. }, .. } => filter,
            other => panic!("unexpected statement: {other:?}"),
        }
    }

    fn column(name: &str) -> Expr {
        Expr::Column(ColumnRef { qualifier: None, name: name.to_string() })
    }

    fn integer(value: i64) -> Expr {
        Expr::Literal(Literal::Integer(value))
    }

    /// `left >= low AND left <= high`, the lowering `BETWEEN` must produce.
    fn lowered_range(left: Expr, low: Expr, high: Expr) -> Expr {
        Expr::And(
            Box::new(Expr::Compare { left: Box::new(left.clone()), op: CompareOp::GtEq, right: Box::new(low) }),
            Box::new(Expr::Compare { left: Box::new(left), op: CompareOp::LtEq, right: Box::new(high) }),
        )
    }

    #[test]
    fn between_lowers_to_two_comparisons() {
        assert_eq!(filter("SELECT * FROM t WHERE a between 1 and 2"), lowered_range(column("a"), integer(1), integer(2)));
    }

    #[test]
    fn between_accepts_arithmetic_bounds() {
        // `c BETWEEN b-2 AND d+2` keeps both bounds as full expressions.
        let low = Expr::Binary { left: Box::new(column("b")), op: BinaryOp::Subtract, right: Box::new(integer(2)) };
        let high = Expr::Binary { left: Box::new(column("d")), op: BinaryOp::Add, right: Box::new(integer(2)) };
        assert_eq!(filter("SELECT * FROM t WHERE c BETWEEN b-2 AND d+2"), lowered_range(column("c"), low, high));
    }

    #[test]
    fn not_between_is_not_around_the_lowered_range() {
        assert_eq!(
            filter("SELECT * FROM t WHERE a NOT BETWEEN 110 AND 150"),
            Expr::Not(Box::new(lowered_range(column("a"), integer(110), integer(150))))
        );
    }

    #[test]
    fn between_keeps_the_enclosing_conjunction() {
        let expr = filter("SELECT * FROM t WHERE a BETWEEN 1 AND 2 AND b = 3");
        match expr {
            Expr::And(left, right) => {
                assert_eq!(*left, lowered_range(column("a"), integer(1), integer(2)));
                assert!(matches!(*right, Expr::Compare { op: CompareOp::Eq, .. }));
            }
            other => panic!("expected AND, found {other:?}"),
        }
    }

    #[test]
    fn between_without_separator_is_a_syntax_error() {
        let error = parse("SELECT * FROM t WHERE a BETWEEN 1 2").expect_err("missing AND");
        assert_eq!(error.code, "syntax");
        assert_eq!(error.statement, Some("select"));
    }

    #[test]
    fn parses_parenthesized_or_of_comparisons() {
        let expr = filter("SELECT * FROM t WHERE (a = b) OR (c > d)");
        match expr {
            Expr::Or(left, right) => {
                assert!(matches!(*left, Expr::Compare { op: CompareOp::Eq, .. }));
                assert!(matches!(*right, Expr::Compare { op: CompareOp::Gt, .. }));
            }
            other => panic!("expected OR of comparisons, found {other:?}"),
        }
    }

    #[test]
    fn parses_nested_grouping_under_unary_not() {
        let expr = filter("SELECT * FROM t WHERE NOT ((a = 1) OR (b < 2))");
        match expr {
            Expr::Not(inner) => assert!(matches!(*inner, Expr::Or(..))),
            other => panic!("expected NOT around a group, found {other:?}"),
        }
    }
}
