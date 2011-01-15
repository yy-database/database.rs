use yyds_types::{
    Approximation, Consistency, DistributedPlan, FileRef, Fragment, FragmentId, FragmentRole, LayoutField, Node, Program,
    RecordLayout, RecordValue, RetryPolicy, Type, ValidatedProgram, Value, VectorMetric, VectorValue,
};

#[test]
fn yyds_validates_a_shard_to_coordinator_plan() {
    let program = Program {
        parameters: vec![],
        inputs: vec![Type::I64],
        nodes: vec![Node::Input { index: 0, ty: Type::I64 }],
        output: 0,
        output_type: Type::I64,
    };
    let plan = DistributedPlan {
        fragments: vec![
            Fragment {
                id: FragmentId(1),
                role: FragmentRole::Shard,
                inputs: vec![],
                exchange: None,
                program: program.clone(),
                retry: RetryPolicy { max_attempts: 3, idempotent: true },
            },
            Fragment {
                id: FragmentId(2),
                role: FragmentRole::Coordinator,
                inputs: vec![FragmentId(1)],
                exchange: Some(yyds_types::Exchange::Gather),
                program,
                retry: RetryPolicy { max_attempts: 1, idempotent: false },
            },
        ],
        root: FragmentId(2),
        consistency: Consistency::Snapshot,
        approximation: Approximation { allowed: false, error_bound: None, confidence: None },
    };
    assert!(plan.validate().is_ok());
}

#[test]
fn yyds_rejects_an_unreachable_fragment() {
    let program = Program {
        parameters: vec![],
        inputs: vec![Type::I64],
        nodes: vec![Node::Input { index: 0, ty: Type::I64 }],
        output: 0,
        output_type: Type::I64,
    };
    let plan = DistributedPlan {
        fragments: vec![
            Fragment {
                id: FragmentId(1),
                role: FragmentRole::Shard,
                inputs: vec![],
                exchange: None,
                program: program.clone(),
                retry: RetryPolicy { max_attempts: 1, idempotent: false },
            },
            Fragment {
                id: FragmentId(2),
                role: FragmentRole::Coordinator,
                inputs: vec![],
                exchange: None,
                program: program.clone(),
                retry: RetryPolicy { max_attempts: 1, idempotent: false },
            },
            Fragment {
                id: FragmentId(3),
                role: FragmentRole::Shard,
                inputs: vec![],
                exchange: None,
                program,
                retry: RetryPolicy { max_attempts: 1, idempotent: false },
            },
        ],
        root: FragmentId(2),
        consistency: Consistency::Snapshot,
        approximation: Approximation { allowed: false, error_bound: None, confidence: None },
    };
    assert_eq!(plan.validate(), Err(yyds_types::PlanValidationError::UnreachableFragment));
}

#[test]
fn yyds_evaluates_its_distributed_execution_model() {
    let program = ValidatedProgram::validate(Program {
        parameters: vec![Type::I64],
        inputs: vec![Type::I64],
        nodes: vec![
            Node::Parameter { index: 0, ty: Type::I64 },
            Node::Input { index: 0, ty: Type::I64 },
            Node::AddI64 { left: 0, right: 1 },
        ],
        output: 2,
        output_type: Type::I64,
    })
    .expect("program is valid");

    assert_eq!(program.evaluate(&[Value::I64(7)], &[Value::I64(5)]), Ok(Value::I64(12)));
}

#[test]
fn yyds_preserves_file_and_vector_execution_values() {
    let file = Value::File(FileRef { object_id: "asset-1".into(), generation: 3, byte_len: 4096 });
    assert_eq!(file.ty(), Type::File);

    let vector = VectorValue::new(vec![0.25, 0.5, 0.75], VectorMetric::Cosine).expect("finite vector is valid");
    assert_eq!(Value::Vector(vector).ty(), Type::Vector { dimension: 3, metric: VectorMetric::Cosine });
}

#[test]
fn yyds_preserves_schema_identified_records() {
    let record =
        RecordValue::new(11, vec![Value::I64(9), Value::Text("ready".into())]).expect("record schema identity is valid");
    assert_eq!(record.schema_id(), 11);
    assert_eq!(record.fields().len(), 2);
    assert_eq!(Value::Record(record).ty(), Type::Record { schema_id: 11, field_count: 2 });
}

#[test]
fn yyds_preserves_published_record_layouts() {
    let layout =
        RecordLayout::new(11, vec![LayoutField { field_id: 101, index: 0, ty: Type::I64 }]).expect("record layout is valid");
    assert_eq!(layout.schema_id(), 11);
    assert_eq!(layout.fields()[0].field_id, 101);
}
