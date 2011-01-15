//! `database-udf` 是数据库家族的 UDF 能力包。
//!
//! 职责：
//! - 纯合同：身份、签名、策略、边界值、宿主元数据
//! - 注册表与调用运行时
//! - 宿主 / 原生实现适配
//!
//! 与具体执行程序（`ValidatedUdf`）的桥接留在 `database.execution`，
//! 避免本包反向依赖执行器。

#![deny(missing_docs)]

/// 宿主与原生实现适配。
pub mod adapters;
/// 可跨协议与运行时共享的 UDF 合同。
pub mod contract;
/// 分层注册表（catalog / session / host / builtin）。
pub mod registry;
/// 调用上下文与实现策略。
pub mod runtime;

pub use adapters::{HostMicroImplementation, HostRuntimeAdapter, NativeHandler, NativeImplementation, NativeUdfDefinition};
pub use contract::{
    CapabilitySet, Determinism, Effect, HostFunctionHandle, HostMicroDefinition, ImplementationKind, Placement, Result, Signature,
    UdfDefinition, UdfError, UdfFingerprint, UdfIdentity, UdfPolicy, UdfType, UdfValue,
};
pub use registry::{
    CatalogUdfEntry, CatalogUdfRegistry, ImplementationEntry, ImplementationRegistry, RegisterOptions, SessionUdfEntry, SessionUdfRegistry,
    UdfRegistry,
};
pub use runtime::{Budget, InvocationMode, UdfContext, UdfImplementation, UdfInvocation};
