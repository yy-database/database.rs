//! 将已校验执行程序挂到 UDF 注册表的产品桥。

use crate::program::ValidatedUdf;
use database_udf::{
    ImplementationKind, Placement, Result, Signature, UdfContext, UdfDefinition, UdfError, UdfIdentity, UdfImplementation, UdfPolicy, UdfValue,
};

use super::value::{from_execution, into_execution};

/// 包装已校验执行程序，供注册表安装。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionUdfBinding {
    /// 注册时分配的逻辑身份。
    pub identity: UdfIdentity,
    /// 由程序推导的边界签名。
    pub signature: Signature,
    /// 注册策略。
    pub policy: UdfPolicy,
    /// 放置要求。
    pub placement: Placement,
    /// 已校验的本地执行体。
    pub program: ValidatedUdf,
    /// 原始源指纹（若已知）。
    pub source_fingerprint: [u8; 32],
}

impl ExecutionUdfBinding {
    /// 执行子系统内保存的身份字符串。
    pub fn execution_id(&self) -> &str {
        self.program.id()
    }

    /// 构造会话元数据。
    pub fn session_definition(&self) -> UdfDefinition {
        UdfDefinition {
            identity: self.identity.clone(),
            signature: self.signature.clone(),
            policy: self.policy,
            placement: self.placement,
            implementation_kind: ImplementationKind::ExecutionProgram,
            fingerprint: self.source_fingerprint,
        }
    }
}

/// 嵌入式执行程序实现策略。
pub struct ExecutionProgramImplementation {
    binding: ExecutionUdfBinding,
}

impl ExecutionProgramImplementation {
    /// 由已校验绑定构造实现。
    pub fn new(binding: ExecutionUdfBinding) -> Self {
        Self { binding }
    }
}

impl UdfImplementation for ExecutionProgramImplementation {
    fn identity(&self) -> &UdfIdentity {
        &self.binding.identity
    }

    fn signature(&self) -> &Signature {
        &self.binding.signature
    }

    fn invoke(&self, context: &mut UdfContext, args: &[UdfValue]) -> Result<UdfValue> {
        self.binding.signature.ensure_args(&UdfValue::argument_types(args))?;
        context.check_cancelled()?;
        context.charge(1)?;
        let execution_args = args.iter().cloned().map(into_execution).collect::<Result<Vec<_>>>()?;
        let result =
            self.binding.program.evaluate(&[], &execution_args).map_err(|error| UdfError::ExecutionFailed { message: format!("{error:?}") })?;
        from_execution(&result)
    }
}
