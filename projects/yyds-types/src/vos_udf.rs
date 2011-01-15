use std::collections::HashMap;

use vos::syntax::{VosBinaryOp, VosExpr, VosFnDecl, VosFnKind, VosLiteral, VosTypeSyntax};

use crate::{Error, Node, Program, Type, Udf, UdfEffect, UdfPlacement, Value};

/// Lowers the Oak-validated VOS scalar subset into YYDS's shard execution model.
pub fn lower_vos_udf(source: &str, version: u32) -> Result<crate::ValidatedUdf, Error> {
    let parsed = vos::parse_program(source).map_err(|diagnostics| Error::Udf { name: "<vos>".into(), message: diagnostics })?;
    if !parsed.statements.is_empty() || parsed.result.is_some() || parsed.micros.len() != 1 {
        return udf_error("<vos>", "VOS UDF source must contain exactly one micro declaration");
    }
    let declaration = &parsed.micros[0];
    if declaration.kind != VosFnKind::Micro {
        return udf_error(&declaration.name, "only VOS micro declarations can bind as distributed UDFs");
    }
    lower_declaration(declaration, version)
}

fn lower_declaration(declaration: &VosFnDecl, version: u32) -> Result<crate::ValidatedUdf, Error> {
    let mut inputs = Vec::with_capacity(declaration.parameters.len());
    let mut names = HashMap::with_capacity(declaration.parameters.len());
    for (index, parameter) in declaration.parameters.iter().enumerate() {
        let ty = lower_type(&parameter.type_expr, &declaration.name)?;
        if names.insert(parameter.name.clone(), (index as u32, ty)).is_some() {
            return udf_error(&declaration.name, "duplicate VOS UDF parameter");
        }
        inputs.push(ty);
    }
    if !declaration.body.statements.is_empty() || declaration.body.micros.len() != 0 {
        return udf_error(&declaration.name, "distributed scalar VOS UDF bodies currently require one expression");
    }
    let expression = declaration.body.result.as_ref().ok_or_else(|| Error::Udf {
        name: declaration.name.clone(),
        message: "VOS UDF body must return an expression".into(),
    })?;
    let mut nodes = Vec::new();
    let output_type = lower_expr(expression, &names, &mut nodes, &declaration.name)?;
    if let Some(return_ty) = &declaration.return_type {
        if lower_type(return_ty, &declaration.name)? != output_type {
            return udf_error(&declaration.name, "VOS UDF return type does not match its body");
        }
    }
    let output = u32::try_from(nodes.len() - 1).expect("lowered VOS UDF has at least one node");
    Udf {
        id: declaration.name.clone(),
        version,
        deterministic: true,
        effect: UdfEffect::Read,
        placement: UdfPlacement::Shard,
        program: Program { parameters: Vec::new(), inputs, nodes, output, output_type },
    }
    .validate()
    .map_err(|error| Error::Udf {
        name: declaration.name.clone(),
        message: format!("lowered VOS UDF rejected by distributed execution model: {error:?}"),
    })
}

fn lower_expr(
    expression: &VosExpr,
    inputs: &HashMap<String, (u32, Type)>,
    nodes: &mut Vec<Node>,
    name: &str,
) -> Result<Type, Error> {
    match expression {
        VosExpr::Name { name: input, .. } => {
            let (index, ty) = inputs
                .get(input)
                .ok_or_else(|| Error::Udf { name: name.into(), message: format!("unknown VOS UDF input `{input}`") })?;
            nodes.push(Node::Input { index: *index, ty: *ty });
            Ok(*ty)
        }
        VosExpr::Literal(VosLiteral::Int(value)) => {
            let value = value
                .parse::<i64>()
                .map_err(|_| Error::Udf { name: name.into(), message: "VOS UDF integer literal does not fit i64".into() })?;
            nodes.push(Node::Literal(Value::I64(value)));
            Ok(Type::I64)
        }
        VosExpr::Binary { op: VosBinaryOp::Add, left, right, .. } => {
            let left_ty = lower_expr(left, inputs, nodes, name)?;
            let left_node = u32::try_from(nodes.len() - 1).expect("left expression has a node");
            let right_ty = lower_expr(right, inputs, nodes, name)?;
            let right_node = u32::try_from(nodes.len() - 1).expect("right expression has a node");
            if left_ty != Type::I64 || right_ty != Type::I64 {
                return udf_error(name, "YYDS shard UDF addition requires i64 operands");
            }
            nodes.push(Node::AddI64 { left: left_node, right: right_node });
            Ok(Type::I64)
        }
        _ => udf_error(name, "VOS UDF expression is outside the YYDS shard scalar subset"),
    }
}

fn lower_type(ty: &VosTypeSyntax, name: &str) -> Result<Type, Error> {
    match ty {
        VosTypeSyntax::Named { path, .. } if path.len() == 1 && path[0] == "i64" => Ok(Type::I64),
        _ => udf_error(name, "YYDS shard UDF currently accepts only i64 types"),
    }
}

fn udf_error<T>(name: &str, message: &str) -> Result<T, Error> {
    Err(Error::Udf { name: name.into(), message: message.into() })
}
