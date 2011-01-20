//! YYDS 自有的分布式执行合同。
//!
//! 与 `yydb-execution` 刻意解耦：这里描述分片 / 协调器上的类型化值、
//! 部分结果与分布式 UDF 放置。VOS 与方言前端各自投影进本模型，
//! 不得把 YYDB 单机执行计划直接塞进来。

/// 分布式标量程序中的节点编号。
pub type NodeId = u32;

/// 分布式执行片段的标识。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FragmentId(pub u32);

/// 分布式片段的放置角色。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentRole {
    /// 在数据分片上本地求值。
    Shard,
    /// 在协调器上汇聚或部分聚合。
    Coordinator,
    /// 在靠近客户端的边缘节点执行。
    Edge,
}

/// 片段之间的数据交换方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exchange {
    /// 把全部部分行送到单一协调器。
    Gather,
    /// 向每个目标片段复制一份。
    Broadcast,
    /// 按稳定目录字段身份重新分区。
    Repartition {
        /// 用于重分区的目录字段身份。
        field_id: u64,
    },
    /// 合并兼容的部分聚合状态。
    Merge,
}

/// Retry contract for one fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Maximum number of attempts including the first attempt.
    pub max_attempts: u16,
    /// Whether replay is safe for the fragment effect.
    pub idempotent: bool,
}

/// 分布式计划请求的读一致性。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consistency {
    /// 在同一快照世代上读。
    Snapshot,
    /// 允许最终一致的滞后读。
    Eventual,
    /// 允许有界陈旧，超过阈值必须刷新。
    BoundedStaleness {
        /// 可接受的最大陈旧毫秒数。
        max_millis: u64,
    },
}

/// Approximation contract attached to a distributed result.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Approximation {
    /// Whether an approximate result is accepted.
    pub allowed: bool,
    /// Optional absolute or relative error bound defined by the operator.
    pub error_bound: Option<f64>,
    /// Optional confidence level in the interval `(0, 1]`.
    pub confidence: Option<f64>,
}

/// One independently scheduled shard, coordinator, or edge fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fragment {
    /// Stable fragment identity within the plan.
    pub id: FragmentId,
    /// Physical placement role.
    pub role: FragmentRole,
    /// Upstream fragment identities.
    pub inputs: Vec<FragmentId>,
    /// Exchange used to consume upstream partials.
    pub exchange: Option<Exchange>,
    /// Typed scalar body or fragment-local expression.
    pub program: Program,
    /// Replay policy after transport or lease failure.
    pub retry: RetryPolicy,
}

/// A validated distributed execution plan.
#[derive(Debug, Clone, PartialEq)]
pub struct DistributedPlan {
    /// Fragments in deterministic plan order.
    pub fragments: Vec<Fragment>,
    /// Coordinator or edge root that publishes the result.
    pub root: FragmentId,
    /// Read consistency contract.
    pub consistency: Consistency,
    /// Approximation contract.
    pub approximation: Approximation,
}

impl DistributedPlan {
    /// Validate fragment references, root placement, retries, and approximation metadata.
    pub fn validate(self) -> Result<ValidatedDistributedPlan, PlanValidationError> {
        if self.fragments.is_empty() {
            return Err(PlanValidationError::Empty);
        }
        let mut published = std::collections::BTreeSet::new();
        for fragment in &self.fragments {
            if !published.insert(fragment.id) {
                return Err(PlanValidationError::DuplicateFragment);
            }
        }
        if self.fragments.iter().filter(|fragment| fragment.id == self.root).count() != 1 {
            return Err(PlanValidationError::InvalidRoot);
        }
        if !self
            .fragments
            .iter()
            .any(|fragment| fragment.id == self.root && matches!(fragment.role, FragmentRole::Coordinator | FragmentRole::Edge))
        {
            return Err(PlanValidationError::RootMustPublish);
        }
        let mut preceding = std::collections::BTreeSet::new();
        for fragment in &self.fragments {
            if fragment.retry.max_attempts == 0 || (!fragment.retry.idempotent && fragment.retry.max_attempts > 1) {
                return Err(PlanValidationError::RetryPolicy);
            }
            if fragment.inputs.iter().any(|input| !self.fragments.iter().any(|candidate| candidate.id == *input)) {
                return Err(PlanValidationError::MissingInput);
            }
            if fragment.inputs.iter().any(|input| !preceding.contains(input)) {
                return Err(PlanValidationError::InvalidDependencyOrder);
            }
            if fragment.inputs.is_empty() != fragment.exchange.is_none() {
                return Err(PlanValidationError::InvalidExchange);
            }
            ValidatedProgram::validate(fragment.program.clone()).map_err(|_| PlanValidationError::InvalidProgram)?;
            preceding.insert(fragment.id);
        }
        let by_id = self.fragments.iter().map(|fragment| (fragment.id, fragment)).collect::<std::collections::BTreeMap<_, _>>();
        let mut reachable = std::collections::BTreeSet::new();
        let mut pending = vec![self.root];
        while let Some(id) = pending.pop() {
            if !reachable.insert(id) {
                continue;
            }
            if let Some(fragment) = by_id.get(&id) {
                pending.extend(fragment.inputs.iter().copied());
            }
        }
        if reachable.len() != self.fragments.len() {
            return Err(PlanValidationError::UnreachableFragment);
        }
        if self.approximation.confidence.is_some_and(|value| !(0.0 < value && value <= 1.0)) {
            return Err(PlanValidationError::ApproximationMetadata);
        }
        if self.approximation.error_bound.is_some_and(|value| !value.is_finite() || value < 0.0) {
            return Err(PlanValidationError::ApproximationMetadata);
        }
        if !self.approximation.allowed && (self.approximation.error_bound.is_some() || self.approximation.confidence.is_some()) {
            return Err(PlanValidationError::ApproximationMetadata);
        }
        Ok(ValidatedDistributedPlan { plan: self })
    }
}

/// A distributed plan accepted by the scheduler boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedDistributedPlan {
    plan: DistributedPlan,
}

impl ValidatedDistributedPlan {
    /// Return the validated plan.
    pub fn plan(&self) -> &DistributedPlan {
        &self.plan
    }
}

/// Validation failure for a distributed plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanValidationError {
    /// More than one fragment uses the same identity.
    DuplicateFragment,
    /// Dependencies must refer to earlier fragments, excluding cycles.
    InvalidDependencyOrder,
    /// An exchange must be present exactly when upstream fragments exist.
    InvalidExchange,
    /// A fragment contains an invalid typed scalar body.
    InvalidProgram,
    /// No fragments were supplied.
    Empty,
    /// Root does not identify exactly one fragment.
    InvalidRoot,
    /// Root is not a result-publishing role.
    RootMustPublish,
    /// A fragment references an unknown input.
    MissingInput,
    /// A fragment is not connected to the published result root.
    UnreachableFragment,
    /// Retry count conflicts with idempotency.
    RetryPolicy,
    /// Error or confidence metadata is invalid.
    ApproximationMetadata,
}

/// 分布式向量值附带的度量。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMetric {
    /// 余弦相似度。
    Cosine,
    /// 欧氏距离。
    Euclidean,
    /// 点积。
    Dot,
}

/// YYDS 数据面与控制面共用的运行时类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    /// 显式空值。
    Null,
    /// 布尔。
    Bool,
    /// 有符号 64 位整数。
    I64,
    /// UTF-8 文本。
    Text,
    /// 不透明字节。
    Bytes,
    /// 不可变对象清单引用。
    File,
    /// 固定维度向量。
    Vector {
        /// 分量个数；构造时必须非零。
        dimension: u32,
        /// 该向量声明的度量。
        metric: VectorMetric,
    },
    /// 带 schema 身份的分布式记录。
    Record {
        /// 非零 schema 身份。
        schema_id: u64,
        /// 字段个数（与布局发布一致）。
        field_count: u32,
    },
}

/// 指向不可变对象清单的引用。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRef {
    /// 对象内容身份。
    pub object_id: String,
    /// 对象世代；世代前进表示内容替换。
    pub generation: u64,
    /// 对象字节长度。
    pub byte_len: u64,
}

/// A finite vector value carried by a shard or coordinator.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorValue {
    values: Vec<f32>,
    metric: VectorMetric,
}

impl Eq for VectorValue {}

impl VectorValue {
    /// Construct a vector and reject empty or non-finite values.
    pub fn new(values: Vec<f32>, metric: VectorMetric) -> Result<Self, VectorValueError> {
        if values.is_empty() {
            return Err(VectorValueError::InvalidDimension);
        }
        if let Some(index) = values.iter().position(|value| !value.is_finite()) {
            return Err(VectorValueError::NonFiniteComponent { index });
        }
        Ok(Self { values, metric })
    }

    /// Return the number of components.
    pub fn dimension(&self) -> u32 {
        self.values.len() as u32
    }

    /// Return the components.
    pub fn values(&self) -> &[f32] {
        &self.values
    }

    /// Return the metric.
    pub fn metric(&self) -> VectorMetric {
        self.metric
    }
}

/// 构造分布式向量时的错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorValueError {
    /// 维度为空或与声明不符。
    InvalidDimension,
    /// 某个分量不是有限浮点。
    NonFiniteComponent {
        /// 非法分量的下标。
        index: usize,
    },
}

/// A schema-bound record value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordValue {
    schema_id: u64,
    fields: Vec<Value>,
}

impl RecordValue {
    /// Construct a record with a non-zero schema identity.
    pub fn new(schema_id: u64, fields: Vec<Value>) -> Result<Self, RecordValueError> {
        if schema_id == 0 {
            return Err(RecordValueError::InvalidSchemaId);
        }
        Ok(Self { schema_id, fields })
    }

    /// Return the schema identity.
    pub fn schema_id(&self) -> u64 {
        self.schema_id
    }

    /// Return logical fields in catalog order.
    pub fn fields(&self) -> &[Value] {
        &self.fields
    }
}

/// 构造记录值时的错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordValueError {
    /// schema 身份为零或非法。
    InvalidSchemaId,
}

/// 已发布的分布式记录布局中的一个字段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayoutField {
    /// 目录级字段身份。
    pub field_id: u64,
    /// 布局内的稳定下标。
    pub index: u32,
    /// 字段运行时类型。
    pub ty: Type,
}

/// A layout published by the distributed catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordLayout {
    schema_id: u64,
    fields: Vec<LayoutField>,
}

impl RecordLayout {
    /// Create a layout and reject zero schema identities.
    pub fn new(schema_id: u64, fields: Vec<LayoutField>) -> Result<Self, RecordLayoutError> {
        if schema_id == 0 {
            return Err(RecordLayoutError::InvalidSchemaId);
        }
        Ok(Self { schema_id, fields })
    }

    /// Return the schema identity.
    pub fn schema_id(&self) -> u64 {
        self.schema_id
    }

    /// Return published fields.
    pub fn fields(&self) -> &[LayoutField] {
        &self.fields
    }
}

/// 发布分布式布局时的错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordLayoutError {
    /// schema 身份为零或非法。
    InvalidSchemaId,
}

/// A runtime value in shard or coordinator execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// Explicit null.
    Null,
    /// Boolean value.
    Bool(bool),
    /// Signed integer.
    I64(i64),
    /// UTF-8 text.
    Text(String),
    /// Opaque bytes.
    Bytes(Vec<u8>),
    /// Object reference.
    File(FileRef),
    /// Vector value.
    Vector(VectorValue),
    /// Schema-bound record.
    Record(RecordValue),
}

impl Value {
    /// Return the runtime type of this value.
    pub fn ty(&self) -> Type {
        match self {
            Self::Null => Type::Null,
            Self::Bool(_) => Type::Bool,
            Self::I64(_) => Type::I64,
            Self::Text(_) => Type::Text,
            Self::Bytes(_) => Type::Bytes,
            Self::File(_) => Type::File,
            Self::Vector(value) => Type::Vector { dimension: value.dimension(), metric: value.metric() },
            Self::Record(value) => Type::Record { schema_id: value.schema_id(), field_count: value.fields().len() as u32 },
        }
    }
}

/// 单个分布式执行片段求值的标量节点。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    /// 字面量。
    Literal(Value),
    /// 按位参数。
    Parameter {
        /// 参数下标。
        index: u32,
        /// 期望类型。
        ty: Type,
    },
    /// 按位输入。
    Input {
        /// 输入下标。
        index: u32,
        /// 期望类型。
        ty: Type,
    },
    /// 检查溢出的 i64 加法。
    AddI64 {
        /// 左操作数节点。
        left: NodeId,
        /// 右操作数节点。
        right: NodeId,
    },
}

/// A typed scalar fragment before shard or coordinator planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Program {
    /// Parameter types.
    pub parameters: Vec<Type>,
    /// Fragment input types.
    pub inputs: Vec<Type>,
    /// Nodes in topological order.
    pub nodes: Vec<Node>,
    /// Returned node.
    pub output: NodeId,
    /// Declared output type.
    pub output_type: Type,
}

/// A program accepted by the YYDS fragment validator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedProgram {
    program: Program,
}

impl ValidatedProgram {
    /// Validate node references and the declared output type.
    pub fn validate(program: Program) -> Result<Self, ValidationError> {
        if program.nodes.is_empty() || program.output as usize >= program.nodes.len() {
            return Err(ValidationError::InvalidProgram);
        }
        let mut types = Vec::with_capacity(program.nodes.len());
        for node in &program.nodes {
            let ty = match node {
                Node::Literal(value) => value.ty(),
                Node::Parameter { index, ty } => {
                    if program.parameters.get(*index as usize) != Some(ty) {
                        return Err(ValidationError::TypeMismatch);
                    }
                    *ty
                }
                Node::Input { index, ty } => {
                    if program.inputs.get(*index as usize) != Some(ty) {
                        return Err(ValidationError::TypeMismatch);
                    }
                    *ty
                }
                Node::AddI64 { left, right } => {
                    if *left as usize >= types.len() || *right as usize >= types.len() {
                        return Err(ValidationError::InvalidReference);
                    }
                    if types[*left as usize] != Type::I64 || types[*right as usize] != Type::I64 {
                        return Err(ValidationError::TypeMismatch);
                    }
                    Type::I64
                }
            };
            types.push(ty);
        }
        if types[program.output as usize] != program.output_type {
            return Err(ValidationError::TypeMismatch);
        }
        Ok(Self { program })
    }

    /// Evaluate a validated fragment against parameters and shard inputs.
    pub fn evaluate(&self, parameters: &[Value], inputs: &[Value]) -> Result<Value, EvalError> {
        let udf = Udf {
            id: "validated-program".into(),
            version: 1,
            deterministic: true,
            effect: UdfEffect::Read,
            placement: UdfPlacement::Shard,
            program: self.program.clone(),
        };
        udf.validate().map_err(|_| EvalError::InvalidReference)?.evaluate(parameters, inputs)
    }
}

/// 分布式执行片段的校验错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationError {
    /// 程序形状非法（空节点、越界输出等）。
    InvalidProgram,
    /// 引用了不存在的节点 / 参数 / 输入。
    InvalidReference,
    /// 运算数类型与算子声明不符。
    TypeMismatch,
}

/// 分布式 UDF 的放置策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdfPlacement {
    /// 优先在分片本地执行。
    Shard,
    /// 必须在协调器执行。
    Coordinator,
    /// 可在边缘节点执行。
    Edge,
}

/// 分布式 UDF 的副作用合同。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdfEffect {
    /// 只读；可安全重放。
    Read,
    /// 可能改写存储状态。
    Write,
    /// 依赖外部系统；重放语义由放置方声明。
    External,
}

/// A VOS-authored UDF after YYDS binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Udf {
    /// Stable function identity.
    pub id: String,
    /// Implementation version.
    pub version: u32,
    /// Whether replay is allowed.
    pub deterministic: bool,
    /// Side effect class.
    pub effect: UdfEffect,
    /// Requested distributed placement.
    pub placement: UdfPlacement,
    /// Typed body.
    pub program: Program,
}

impl Udf {
    /// Validate the distributed UDF metadata and body shape.
    pub fn validate(self) -> Result<ValidatedUdf, UdfValidationError> {
        if self.id.is_empty() {
            return Err(UdfValidationError::EmptyId);
        }
        if self.version == 0 {
            return Err(UdfValidationError::InvalidVersion);
        }
        if self.program.nodes.is_empty() || self.program.output as usize >= self.program.nodes.len() {
            return Err(UdfValidationError::InvalidProgram);
        }
        ValidatedProgram::validate(self.program.clone()).map_err(|_| UdfValidationError::InvalidProgram)?;
        Ok(ValidatedUdf { metadata: self })
    }
}

/// A validated distributed UDF contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedUdf {
    metadata: Udf,
}

impl ValidatedUdf {
    /// Return the function identity.
    pub fn id(&self) -> &str {
        &self.metadata.id
    }

    /// Return the implementation version.
    pub fn version(&self) -> u32 {
        self.metadata.version
    }

    /// Evaluate the supported scalar fragment.
    pub fn evaluate(&self, parameters: &[Value], inputs: &[Value]) -> Result<Value, EvalError> {
        let mut values = Vec::with_capacity(self.metadata.program.nodes.len());
        for node in &self.metadata.program.nodes {
            let value = match node {
                Node::Literal(value) => value.clone(),
                Node::Parameter { index, ty } => fetch_value(parameters, *index, *ty)?,
                Node::Input { index, ty } => fetch_value(inputs, *index, *ty)?,
                Node::AddI64 { left, right } => {
                    let left = values.get(*left as usize).ok_or(EvalError::InvalidReference)?;
                    let right = values.get(*right as usize).ok_or(EvalError::InvalidReference)?;
                    match (left, right) {
                        (Value::I64(left), Value::I64(right)) => left.checked_add(*right).map(Value::I64).ok_or(EvalError::Overflow)?,
                        _ => return Err(EvalError::TypeMismatch),
                    }
                }
            };
            values.push(value);
        }
        let result = values.get(self.metadata.program.output as usize).ok_or(EvalError::InvalidReference)?.clone();
        if result.ty() != self.metadata.program.output_type {
            return Err(EvalError::TypeMismatch);
        }
        Ok(result)
    }
}

fn fetch_value(values: &[Value], index: u32, ty: Type) -> Result<Value, EvalError> {
    let value = values.get(index as usize).ok_or(EvalError::InvalidReference)?.clone();
    if value.ty() != ty {
        return Err(EvalError::TypeMismatch);
    }
    Ok(value)
}

/// 分布式 UDF 合同的校验错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdfValidationError {
    /// 函数身份为空。
    EmptyId,
    /// 实现版本为零或非法。
    InvalidVersion,
    /// 函数体程序无法通过片段校验。
    InvalidProgram,
}

/// 分布式标量片段的求值错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvalError {
    /// 引用了不存在的节点 / 参数 / 输入。
    InvalidReference,
    /// 运算数类型与算子声明不符。
    TypeMismatch,
    /// 检查算术溢出。
    Overflow,
}
