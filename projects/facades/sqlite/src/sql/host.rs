//! 宿主自定义函数(UDF)的注册面与会话函数表。
//!
//! 本模块不另造注册表语义:注册项就是 `database-udf` 合同的形态——
//! [`HostMicroDefinition`] 元数据(身份、签名、策略、不透明句柄、指纹)
//! 加上 [`HostRuntimeAdapter`](宿主运行时中真实函数注册表的适配器),
//! 经合同的 [`HostMicroImplementation`] 组合成可调用实现。引擎按折叠
//! 小写名派发,与内建函数同一口径(提交 281ef1e 确立的大小写折叠规则)。
//!
//! 同名替换:折叠后同名再次注册即替换旧实现;折叠名与内建函数同名且
//! 元数一致时宿主实现优先——与 SQLite `sqlite3_create_function` 替换
//! 同名同元数内建函数的真实行为一致。元数不一致时各走各的派发,
//! 也与 SQLite 相同。
//!
//! 值边界是合同的 Phase-1 子集(`Null` / `I64` / `Text`):`Cell::Real`
//! 与 `Cell::Blob` 落在子集之外,求值期如实拒绝,不做有损编码;宿主
//! 返回的 `Bool` 按 SQLite 的存储类别折叠为整数 `0` / `1`。

use std::{collections::BTreeMap, sync::Arc};

use database_udf::{HostMicroDefinition, HostMicroImplementation, HostRuntimeAdapter, UdfError, UdfImplementation, UdfValue};

use super::{Cell, SqlError};

/// 宿主函数名的折叠:ASCII 小写,与内建函数派发同一口径。
pub fn fold_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// 会话级宿主函数表,按折叠小写名索引。
///
/// 克隆只复制 `Arc` 句柄,已附加库的引擎借此与主引擎共享同一注册表,
/// 使宿主函数在跨库语句与附加库视图展开里同样可解析。
#[derive(Default, Clone)]
pub struct HostFunctionTable {
    entries: BTreeMap<String, Arc<HostMicroImplementation>>,
}

impl HostFunctionTable {
    /// 校验元数据并构造可安装实现;不合合同的定义在注册前即被拒绝。
    pub fn build(definition: HostMicroDefinition, adapter: Arc<dyn HostRuntimeAdapter>) -> Result<Arc<HostMicroImplementation>, SqlError> {
        definition.validate().map_err(registration_error)?;
        Ok(Arc::new(HostMicroImplementation::new(definition, adapter)))
    }

    /// 安装实现;折叠同名时替换旧实现,即 JS API 的注册语义。
    pub fn install(&mut self, implementation: Arc<HostMicroImplementation>) {
        let name = fold_name(implementation.identity().name());
        self.entries.insert(name, implementation);
    }

    /// 按书写名查找实现(内部折叠)。
    pub fn get(&self, name: &str) -> Option<&Arc<HostMicroImplementation>> {
        self.entries.get(&fold_name(name))
    }

    /// 是否注册了该折叠名。
    pub fn contains(&self, name: &str) -> bool {
        self.entries.contains_key(&fold_name(name))
    }

    /// 注册名的固定元数;外层 `None` 表示未注册,内层 `None` 表示变元。
    pub fn expected_arity(&self, name: &str) -> Option<Option<usize>> {
        self.get(name).map(|entry| entry.signature().expected_arity())
    }

    /// 当前注册的宿主函数名(折叠小写,字典序)。
    pub fn names(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }
}

/// 把注册期的合同错误映射为结构化拒绝。
fn registration_error(error: UdfError) -> SqlError {
    SqlError::new("udf-invalid-definition", format!("host function definition is not valid: {error}"))
}

/// 把调用期的合同错误映射为结构化 `SqlError`,错误码沿用引擎既有口径。
pub(super) fn udf_error_to_sql_error(name: &str, error: UdfError) -> SqlError {
    match error {
        UdfError::ArityMismatch => SqlError::new("wrong-arguments", format!("wrong number of arguments to function {name}()")),
        UdfError::SignatureMismatch | UdfError::UnsupportedType => SqlError::new(
            "unsupported-type",
            format!("argument types do not match the signature of host function `{name}`"),
        ),
        UdfError::ExecutionFailed { message } => {
            SqlError::new("udf-error", format!("user-defined function `{name}` failed: {message}"))
        }
        other => SqlError::new("udf-error", format!("host function `{name}` failed: {other}")),
    }
}

/// 把引擎 `Cell` 转换为合同的边界值。
///
/// Phase-1 UDF 边界只承载 `Null` / `I64` / `Text`;`Real` 与 `Blob`
/// 在子集之外,如实以 `unsupported-type` 拒绝,不做有损编码。
pub(super) fn cell_to_udf_value(name: &str, cell: &Cell) -> Result<UdfValue, SqlError> {
    match cell {
        Cell::Null => Ok(UdfValue::Null),
        Cell::Integer(value) => Ok(UdfValue::I64(*value)),
        Cell::Text(text) => Ok(UdfValue::Text(text.clone())),
        Cell::Real(_) => Err(SqlError::new("unsupported-type", format!("host function `{name}` does not accept `real` arguments in the Phase-1 UDF subset"))),
        Cell::Blob(_) => Err(SqlError::new("unsupported-type", format!("host function `{name}` does not accept `blob` arguments in the Phase-1 UDF subset"))),
    }
}

/// 把合同的边界值折回引擎 `Cell`;SQLite 无布尔存储类别,`Bool` 落为整数。
pub(super) fn udf_value_to_cell(value: UdfValue) -> Cell {
    match value {
        UdfValue::Null => Cell::Null,
        UdfValue::Bool(value) => Cell::Integer(i64::from(value)),
        UdfValue::I64(value) => Cell::Integer(value),
        UdfValue::Text(text) => Cell::Text(text),
    }
}
