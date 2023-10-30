//! SQL values, row TLV encoding and expression evaluation shared by planning and execution.

use std::collections::BTreeMap;

use crate::{
    catalog_bind::{SqlCatalogBoundExpression, collect_column_ids},
    sql::{
        SqlBoundBinaryOperator, SqlBoundLiteral, SqlBoundOrderDirection, SqlBoundUnaryOperator, SqlDialect, SqlErrorKind, SqlFrontendError,
        SqlNullsOrder, SqlScalarFunction,
    },
    sql_planner::ScanPlan,
};

mod expression;
mod scalar;
mod values;

pub(crate) use self::expression::eval_constant;
pub use self::{
    expression::{
        compare_values, compare_values_with_nulls, eval_binary, eval_cast, eval_expression, eval_in_values, eval_unary, normalize_integer_values,
        order_nulls_first, projected_rows_equal, retain_distinct_projected_rows, values_equal,
    },
    scalar::{eval_scalar_function, format_number},
    values::{
        RowFieldValues, SqlResult, SqlValue, cmp_string, decode_row, decode_row_fields, encode_row, eq_string, expression_needed_field_ids,
        scan_needed_field_ids,
    },
};

/// Payload byte cursor used by the row TLV decoder.
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn read_u16(&mut self) -> Result<u16, SqlFrontendError> {
        if self.remaining() < 2 {
            return Err(internal_error("row TLV truncated reading u16"));
        }
        let value = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, SqlFrontendError> {
        if self.remaining() < 4 {
            return Err(internal_error("row TLV truncated reading u32"));
        }
        let bytes: [u8; 4] = self.data[self.pos..self.pos + 4].try_into().unwrap();
        let value = u32::from_be_bytes(bytes);
        self.pos += 4;
        Ok(value)
    }

    fn read_u64(&mut self) -> Result<u64, SqlFrontendError> {
        if self.remaining() < 8 {
            return Err(internal_error("row TLV truncated reading u64"));
        }
        let bytes: [u8; 8] = self.data[self.pos..self.pos + 8].try_into().unwrap();
        let value = u64::from_be_bytes(bytes);
        self.pos += 8;
        Ok(value)
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>, SqlFrontendError> {
        if self.remaining() < len {
            return Err(internal_error("row TLV truncated reading bytes"));
        }
        let out = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(out)
    }
}

pub(crate) fn internal_error(message: impl Into<String>) -> SqlFrontendError {
    SqlFrontendError::internal(message)
}

pub(crate) fn division_by_zero_error() -> SqlFrontendError {
    SqlFrontendError::new(SqlErrorKind::DivisionByZero, "division by zero")
}

/// 精确整数运算越界。两方言都要报 SQLSTATE `22003`，因此不降级成通用执行错误。
pub(crate) fn numeric_overflow_error(stage: &str) -> SqlFrontendError {
    SqlFrontendError::new(SqlErrorKind::NumericOverflow, format!("{stage} overflowed the integer range"))
}

/// SQL 三值逻辑里的真值：非 bool 值没有真值。
pub(crate) fn sql_truth(value: &SqlValue) -> Option<bool> {
    match value {
        SqlValue::Boolean(value) => Some(*value),
        SqlValue::Null => None,
        SqlValue::Integer(value) => Some(*value != 0),
        SqlValue::Number(value) => Some(*value != 0.0),
        SqlValue::String(value) => Some(!value.is_empty()),
        SqlValue::Bytes(value) => Some(!value.is_empty()),
    }
}

pub(crate) fn sql_and(left: &SqlValue, right: &SqlValue) -> SqlValue {
    match (sql_truth(left), sql_truth(right)) {
        (Some(false), _) | (_, Some(false)) => SqlValue::Boolean(false),
        (Some(true), Some(true)) => SqlValue::Boolean(true),
        _ => SqlValue::Null,
    }
}

pub(crate) fn sql_or(left: &SqlValue, right: &SqlValue) -> SqlValue {
    match (sql_truth(left), sql_truth(right)) {
        (Some(true), _) | (_, Some(true)) => SqlValue::Boolean(true),
        (Some(false), Some(false)) => SqlValue::Boolean(false),
        _ => SqlValue::Null,
    }
}
