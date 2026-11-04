//! YYDS-owned distributed execution contracts.
//!
//! This model is intentionally independent from `yydb-execution`. It describes
//! typed shard and coordinator values, partial results, and distributed UDF
//! placement. VOS and SQL bind into this model through their respective YYDS
//! frontends.

/// A node identifier in a distributed scalar program.
pub type NodeId = u32;

/// Identifier of a distributed execution fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FragmentId(pub u32);

/// Placement role of a distributed fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentRole {
    Shard,
    Coordinator,
    Edge,
}

/// Exchange performed between distributed fragments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exchange {
    /// Send all partial rows to one coordinator.
    Gather,
    /// Send a copy to every target fragment.
    Broadcast,
    /// Repartition rows by a stable catalog field identity.
    Repartition { field_id: u64 },
    /// Merge compatible partial aggregate states.
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

/// Read consistency requested by a distributed plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consistency {
    Snapshot,
    Eventual,
    BoundedStaleness { max_millis: u64 },
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
        if !self.approximation.allowed && (self.approximation.error_bound.is_some() || self.approximation.confidence.is_some())
        {
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

/// Metric attached to a distributed vector value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMetric {
    Cosine,
    Euclidean,
    Dot,
}

/// Runtime type understood by the YYDS data and control planes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    /// Explicit null value.
    Null,
    /// Boolean value.
    Bool,
    /// Signed 64-bit integer.
    I64,
    /// UTF-8 text.
    Text,
    /// Opaque bytes.
    Bytes,
    /// Immutable object manifest reference.
    File,
    /// Fixed-dimension vector.
    Vector { dimension: u32, metric: VectorMetric },
    /// Schema-identified distributed record.
    Record { schema_id: u64, field_count: u32 },
}

/// A reference to an immutable object manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRef {
    pub object_id: String,
    pub generation: u64,
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

/// Error raised while constructing a distributed vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorValueError {
    InvalidDimension,
    NonFiniteComponent { index: usize },
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

/// Error raised while constructing a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordValueError {
    InvalidSchemaId,
}

/// One published distributed record layout field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayoutField {
    pub field_id: u64,
    pub index: u32,
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

/// Error raised while publishing a distributed layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordLayoutError {
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

/// A scalar node evaluated by one distributed execution fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    /// Literal value.
    Literal(Value),
    /// Positional parameter.
    Parameter { index: u32, ty: Type },
    /// Positional input.
    Input { index: u32, ty: Type },
    /// Checked integer addition.
    AddI64 { left: NodeId, right: NodeId },
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

/// Validation errors for a distributed execution fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationError {
    InvalidProgram,
    InvalidReference,
    TypeMismatch,
}

/// A distributed UDF placement policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdfPlacement {
    Shard,
    Coordinator,
    Edge,
}

/// A distributed UDF effect contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdfEffect {
    Read,
    Write,
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
                        (Value::I64(left), Value::I64(right)) => {
                            left.checked_add(*right).map(Value::I64).ok_or(EvalError::Overflow)?
                        }
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

/// Validation errors for distributed UDF contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdfValidationError {
    EmptyId,
    InvalidVersion,
    InvalidProgram,
}

/// Evaluation errors for a distributed scalar fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvalError {
    InvalidReference,
    TypeMismatch,
    Overflow,
}
