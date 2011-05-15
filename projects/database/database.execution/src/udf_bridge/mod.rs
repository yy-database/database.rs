//! 执行程序与 `database.udf` 之间的桥接。
//!
//! 将已校验的执行体挂到 UDF 注册表，并在边界值与执行值之间转换。
//! 合同、注册表与宿主/原生适配归 `database.udf` 所有。

mod execution;
mod value;

pub use execution::{ExecutionProgramImplementation, ExecutionUdfBinding};
pub use value::{from_execution, into_execution, type_from_execution, type_to_execution};
