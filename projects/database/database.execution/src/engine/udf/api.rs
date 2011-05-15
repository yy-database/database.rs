//! 嵌入式宿主上的 UDF 面。
//!
//! - **简单路径：** [`ScalarUdf`] + [`crate::engine::Connection::create_scalar`]。
//! - **进阶路径：** 下方从 `database-udf` 再导出的 [`UdfRegistry`] 等
//!   （`use database_execution::udf::UdfRegistry`）。
//!
//! 其他语言应通过 `database-client` 走服务端协议，而不是嵌入本 crate。

use crate::program::{UdfEffect, UdfPlacement, ValidatedUdf};
use database_types::{Result, Value};

/// Validates a local read-only execution body before UDF registration.
pub(crate) fn validate_local_execution_body(body: &ValidatedUdf) -> Result<()> {
    if body.effect() != UdfEffect::Read || body.placement() != UdfPlacement::Local {
        return Err(database_types::Error::Unsupported("local scalar execution requires a local read-only UDF"));
    }
    if !body.parameter_types().is_empty() {
        return Err(database_types::Error::Unsupported("local scalar execution does not bind captured parameters"));
    }
    Ok(())
}

/// Object-safe scalar UDF invoked with [`Value`] arguments.
pub trait ScalarUdf: Send + Sync {
    /// Fixed arity, or `None` for variadic.
    fn arity(&self) -> Option<usize>;

    /// Execute the function.
    fn call(&self, args: &[Value]) -> Result<Value>;
}

/// Type alias for closure-style scalar UDFs.
pub type ScalarFn = dyn Fn(&[Value]) -> Result<Value> + Send + Sync;

/// Registry, host adapters, and contract types from `database-udf`.
pub use crate::udf::{
    CatalogUdfEntry, CatalogUdfRegistry, HostFunctionHandle, HostMicroDefinition, HostMicroImplementation, HostRuntimeAdapter,
    ImplementationEntry, ImplementationRegistry, RegisterOptions, SessionUdfEntry, SessionUdfRegistry, Signature, UdfDefinition, UdfError,
    UdfIdentity, UdfRegistry, UdfType, UdfValue,
};
