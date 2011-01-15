//! UDF 边界值与执行运行时值之间的转换。

use crate::program::{Type, Value};
use database_udf::{Result, UdfError, UdfType, UdfValue};

/// 将运行时值转换为 UDF 边界子集。
pub fn from_execution(value: &Value) -> Result<UdfValue> {
    match value {
        Value::Null => Ok(UdfValue::Null),
        Value::Bool(value) => Ok(UdfValue::Bool(*value)),
        Value::I64(value) => Ok(UdfValue::I64(*value)),
        Value::Text(value) => Ok(UdfValue::Text(value.clone())),
        _ => Err(UdfError::UnsupportedType),
    }
}

/// 将边界值转换为执行运行时模型。
pub fn into_execution(value: UdfValue) -> Result<Value> {
    match value {
        UdfValue::Null => Ok(Value::Null),
        UdfValue::Bool(value) => Ok(Value::Bool(value)),
        UdfValue::I64(value) => Ok(Value::I64(value)),
        UdfValue::Text(value) => Ok(Value::Text(value)),
    }
}

/// 将执行运行时类型映射到 Phase-1 UDF 子集。
pub fn type_from_execution(ty: Type) -> Result<UdfType> {
    match ty {
        Type::Null => Ok(UdfType::Null),
        Type::Bool => Ok(UdfType::Bool),
        Type::I64 => Ok(UdfType::I64),
        Type::Text => Ok(UdfType::Text),
        _ => Err(UdfError::UnsupportedType),
    }
}

/// 将 UDF 类型映射到执行运行时类型。
pub fn type_to_execution(ty: UdfType) -> Type {
    match ty {
        UdfType::Null => Type::Null,
        UdfType::Bool => Type::Bool,
        UdfType::I64 => Type::I64,
        UdfType::Text => Type::Text,
        UdfType::Any => {
            unreachable!("UdfType::Any 是合同通配符，不是具体执行类型")
        }
    }
}
