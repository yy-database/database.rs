//! 注册表派发器调用的实现策略。

use super::invocation::{InvocationMode, UdfContext};
use crate::contract::{Result, Signature, UdfError, UdfIdentity, UdfValue};

/// 已注册的 UDF 实现策略。
pub trait UdfImplementation: Send + Sync {
    /// 本实现服务的逻辑身份。
    fn identity(&self) -> &UdfIdentity;

    /// 调用签名。
    fn signature(&self) -> &Signature;

    /// 以标量参数调用 UDF。
    fn invoke(&self, context: &mut UdfContext, args: &[UdfValue]) -> Result<UdfValue>;

    /// 在支持时以有界批量调用 UDF。
    fn invoke_batch(&self, context: &mut UdfContext, batches: &[Vec<UdfValue>]) -> Result<Vec<UdfValue>> {
        let _ = context;
        let _ = batches;
        Err(UdfError::UnsupportedEffect)
    }

    /// 规划器偏好的调用模式。
    fn preferred_mode(&self) -> InvocationMode {
        InvocationMode::Scalar
    }
}
