//! Rust 原生 UDF 定义与嵌入式处理器。

use std::sync::Arc;

use crate::{
    contract::{Effect, ImplementationKind, Placement, Result, Signature, UdfDefinition, UdfError, UdfIdentity, UdfPolicy, UdfValue},
    runtime::{UdfContext, UdfImplementation},
};

/// 经 UDF 边界调用的原生处理器。
pub type NativeHandler = Arc<dyn Fn(&mut UdfContext, &[UdfValue]) -> Result<UdfValue> + Send + Sync>;

/// Rust 原生 UDF 的注册合同。
#[derive(Clone)]
pub struct NativeUdfDefinition {
    /// 逻辑身份。
    pub identity: UdfIdentity,
    /// 类型化签名。
    pub signature: Signature,
    /// 执行策略。
    pub policy: UdfPolicy,
    /// 进程内处理器。
    pub handler: NativeHandler,
    /// 用于重开校验的实现指纹。
    pub fingerprint: [u8; 32],
}

impl NativeUdfDefinition {
    /// 构造本嵌入式原生 micro 的会话元数据。
    pub fn session_definition(&self) -> UdfDefinition {
        UdfDefinition {
            identity: self.identity.clone(),
            signature: self.signature.clone(),
            policy: self.policy,
            placement: Placement::Embedded,
            implementation_kind: ImplementationKind::Native,
            fingerprint: self.fingerprint,
        }
    }

    /// 注册前校验定义。
    pub fn validate(&self) -> Result<()> {
        self.session_definition().validate()?;
        if self.policy.effect != Effect::Pure {
            return Err(UdfError::UnsupportedEffect);
        }
        Ok(())
    }

    /// 将处理器包装为 [`UdfImplementation`]。
    pub fn into_implementation(self) -> NativeImplementation {
        NativeImplementation { definition: self }
    }
}

/// 嵌入式 Rust 实现策略。
#[derive(Clone)]
pub struct NativeImplementation {
    definition: NativeUdfDefinition,
}

impl UdfImplementation for NativeImplementation {
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
        (self.definition.handler)(context, args)
    }
}
