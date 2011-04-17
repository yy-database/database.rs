//! 宿主运行时适配与原生处理器。

mod host;
mod native;

pub use host::{HostMicroImplementation, HostRuntimeAdapter};
pub use native::{NativeHandler, NativeImplementation, NativeUdfDefinition};
