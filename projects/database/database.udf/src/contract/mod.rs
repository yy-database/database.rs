//! 纯 UDF 合同：不依赖执行器或线编解码。

mod error;
mod host;
mod identity;
mod policy;
mod signature;
mod value;

pub use error::{Result, UdfError};
pub use host::{HostFunctionHandle, HostMicroDefinition};
pub use identity::{UdfFingerprint, UdfIdentity};
pub use policy::{CapabilitySet, Determinism, Effect, Placement, UdfPolicy};
pub use signature::{ImplementationKind, Signature, UdfDefinition, UdfType};
pub use value::UdfValue;
