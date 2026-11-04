use yyds_types::{lower_vos_udf, Node, Program, Type, Udf, UdfEffect, UdfPlacement, Value};

#[test]
fn yyds_evaluates_a_manually_constructed_distributed_udf() {
    let udf = Udf {
        id: "score.add".into(),
        version: 1,
        deterministic: true,
        effect: UdfEffect::Read,
        placement: UdfPlacement::Shard,
        program: Program {
            parameters: vec![],
            inputs: vec![Type::I64, Type::I64],
            nodes: vec![
                Node::Input { index: 0, ty: Type::I64 },
                Node::Input { index: 1, ty: Type::I64 },
                Node::AddI64 { left: 0, right: 1 },
            ],
            output: 2,
            output_type: Type::I64,
        },
    }
    .validate()
    .expect("distributed UDF program is valid");

    assert_eq!(udf.id(), "score.add");
    assert_eq!(udf.evaluate(&[], &[Value::I64(4), Value::I64(6)]), Ok(Value::I64(10)));
}

#[test]
fn yyds_lowers_an_oak_validated_vos_shard_udf() {
    let udf = lower_vos_udf("micro sum(left: i64, right: i64) -> i64 { left + right }", 3)
        .expect("VOS UDF lowers into YYDS shard execution");
    assert_eq!(udf.id(), "sum");
    assert_eq!(udf.version(), 3);
    assert_eq!(udf.evaluate(&[], &[Value::I64(4), Value::I64(6)]), Ok(Value::I64(10)));
}

#[test]
fn yyds_rejects_oak_vos_udf_syntax_and_non_i64_lowering() {
    assert!(lower_vos_udf("micro broken(left: i64) -> i64 { left + }", 1).is_err());
    assert!(lower_vos_udf("micro broken(left: bool) -> bool { left }", 1).is_err());
}
