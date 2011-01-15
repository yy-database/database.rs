//! 宿主运行时适配与嵌入式宿主实现策略。

use crate::{
    contract::{HostFunctionHandle, HostMicroDefinition, Result, Signature, UdfIdentity, UdfValue},
    runtime::{InvocationMode, UdfContext, UdfImplementation},
};

/// 外部宿主运行时中真实函数注册表的适配器。
pub trait HostRuntimeAdapter: Send + Sync {
    /// 通过宿主运行时发起一次标量调用。
    fn invoke_scalar(&self, handle: &HostFunctionHandle, args: &[UdfValue]) -> Result<UdfValue>;

    /// 通过宿主运行时发起一次有界批量调用。
    fn invoke_batch(&self, handle: &HostFunctionHandle, batches: &[Vec<UdfValue>]) -> Result<Vec<UdfValue>>;
}

/// 由运行时适配器支撑的宿主侧实现策略。
pub struct HostMicroImplementation {
    definition: HostMicroDefinition,
    adapter: std::sync::Arc<dyn HostRuntimeAdapter>,
}

impl HostMicroImplementation {
    /// 由元数据与适配器构造宿主实现。
    pub fn new(definition: HostMicroDefinition, adapter: std::sync::Arc<dyn HostRuntimeAdapter>) -> Self {
        Self { definition, adapter }
    }
}

impl UdfImplementation for HostMicroImplementation {
    fn identity(&self) -> &UdfIdentity {
        &self.definition.identity
    }

    fn signature(&self) -> &Signature {
        &self.definition.signature
    }

    fn invoke(&self, context: &mut UdfContext, args: &[UdfValue]) -> Result<UdfValue> {
        self.definition.signature.ensure_args(&UdfValue::argument_types(args))?;
        context.check_cancelled()?;
        context.charge(1)?;
        self.adapter.invoke_scalar(&self.definition.handle, args)
    }

    fn invoke_batch(&self, context: &mut UdfContext, batches: &[Vec<UdfValue>]) -> Result<Vec<UdfValue>> {
        for batch in batches {
            self.definition.signature.ensure_args(&UdfValue::argument_types(batch))?;
        }
        context.check_cancelled()?;
        context.charge(batches.len() as u64)?;
        self.adapter.invoke_batch(&self.definition.handle, batches)
    }

    fn preferred_mode(&self) -> InvocationMode {
        InvocationMode::Batch
    }
}
