//! 调用上下文与实现策略。

mod implementation;
mod invocation;

pub use implementation::UdfImplementation;
pub use invocation::{Budget, InvocationMode, UdfContext, UdfInvocation};
