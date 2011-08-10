//! Contract projection and the canonical schema fingerprint.

use super::{render::operation_kind_label, *};

pub(crate) struct MappedFieldType {
    pub(crate) rust_ty: String,
    pub(crate) vos_type: String,
    pub(crate) optional: bool,
    pub(crate) is_uuid: bool,
    pub(crate) reference_target: Option<String>,
}

pub(crate) fn type_label(ty: &CanonicalType) -> String {
    match ty {
        CanonicalType::Named(path) => path.join("::"),
        CanonicalType::Optional(inner) => format!("{}?", type_label(inner)),
        CanonicalType::List(inner) => format!("[{}]", type_label(inner)),
        CanonicalType::Reference(inner) => format!("&{}", type_label(inner)),
        CanonicalType::Generic { path, arguments } => {
            let arguments = arguments
                .iter()
                .map(|argument| match argument {
                    CanonicalTypeArgument::Type(value) => type_label(value),
                    CanonicalTypeArgument::Literal(value) => value.clone(),
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}<{arguments}>", path.join("::"))
        }
    }
}

pub(crate) fn map_resolved_field_type(ty: &vos::ResolvedCanonicalType) -> Result<MappedFieldType> {
    use vos::ResolvedCanonicalType;
    match ty {
        ResolvedCanonicalType::Optional(inner) => {
            let mut mapped = map_resolved_field_type(inner)?;
            mapped.optional = true;
            mapped.vos_type.push('?');
            mapped.rust_ty = format!("Option<{}>", mapped.rust_ty);
            Ok(mapped)
        }
        ResolvedCanonicalType::List(inner) => {
            let mapped = map_resolved_field_type(inner)?;
            Ok(MappedFieldType {
                rust_ty: format!("Vec<{}>", mapped.rust_ty),
                vos_type: format!("[{}]", mapped.vos_type),
                optional: mapped.optional,
                is_uuid: false,
                reference_target: mapped.reference_target,
            })
        }
        ResolvedCanonicalType::Reference(inner) => {
            let target = reference_target_from_resolved(inner)?;
            Ok(MappedFieldType {
                rust_ty: format!("{target}Ref"),
                vos_type: format!("&{target}"),
                optional: false,
                is_uuid: false,
                reference_target: Some(target),
            })
        }
        ResolvedCanonicalType::Scalar(scalar) => {
            let name = scalar.as_str();
            let label = carrier::faithful_label(name).ok_or_else(|| Error::UnsupportedType(name.to_owned()))?;
            let rust_ty = carrier::rust_carrier_for_label(label).ok_or_else(|| Error::UnsupportedType(name.to_owned()))?.to_owned();
            Ok(MappedFieldType { rust_ty, vos_type: label.to_owned(), optional: false, is_uuid: label == "uuid", reference_target: None })
        }
        ResolvedCanonicalType::User { path, .. } => {
            let name = path.join("::");
            Ok(MappedFieldType { rust_ty: name.clone(), vos_type: name, optional: false, is_uuid: false, reference_target: None })
        }
        ResolvedCanonicalType::Vector { dimension, .. } => Ok(MappedFieldType {
            rust_ty: "Vec<f32>".into(),
            vos_type: format!("vector<{dimension}>"),
            optional: false,
            is_uuid: false,
            reference_target: None,
        }),
        ResolvedCanonicalType::File => Ok(MappedFieldType {
            rust_ty: "iris::FileDescriptor".into(),
            vos_type: "file".into(),
            optional: false,
            is_uuid: false,
            reference_target: None,
        }),
        other => Err(Error::UnsupportedType(format!("resolved type {other:?}"))),
    }
}

pub(crate) fn reference_target_from_resolved(ty: &vos::ResolvedCanonicalType) -> Result<String> {
    use vos::ResolvedCanonicalType;
    match ty {
        ResolvedCanonicalType::User { path, .. } => Ok(path.join("::")),
        ResolvedCanonicalType::Scalar(scalar) => Ok(scalar.as_str().to_owned()),
        other => Err(Error::UnsupportedType(format!("resolved reference target `{other:?}`"))),
    }
}

pub(crate) fn map_field_type(ty: &CanonicalType) -> Result<MappedFieldType> {
    match ty {
        CanonicalType::Optional(inner) => {
            let mut mapped = map_field_type(inner)?;
            mapped.optional = true;
            mapped.vos_type = format!("{}?", mapped.vos_type);
            mapped.rust_ty = format!("Option<{}>", mapped.rust_ty);
            Ok(mapped)
        }
        CanonicalType::List(inner) => {
            let mapped = map_field_type(inner)?;
            Ok(MappedFieldType {
                rust_ty: format!("Vec<{}>", mapped.rust_ty),
                vos_type: format!("[{}]", mapped.vos_type),
                optional: mapped.optional,
                is_uuid: false,
                reference_target: mapped.reference_target,
            })
        }
        CanonicalType::Reference(inner) => {
            let target = reference_target_name(inner)?;
            Ok(MappedFieldType {
                rust_ty: format!("{target}Ref"),
                vos_type: format!("&{target}"),
                optional: false,
                is_uuid: false,
                reference_target: Some(target),
            })
        }
        CanonicalType::Named(path) if path.len() == 1 => {
            let name = path[0].as_str();
            match carrier::builtin_rust_and_label(name) {
                Some((rust, vos)) => {
                    Ok(MappedFieldType { rust_ty: rust.into(), is_uuid: vos == "uuid", vos_type: vos, optional: false, reference_target: None })
                }
                None => Ok(MappedFieldType {
                    rust_ty: name.to_owned(),
                    vos_type: name.to_owned(),
                    optional: false,
                    is_uuid: false,
                    reference_target: None,
                }),
            }
        }
        CanonicalType::Named(path) => {
            Ok(MappedFieldType { rust_ty: path.join("::"), vos_type: path.join("::"), optional: false, is_uuid: false, reference_target: None })
        }
        CanonicalType::Generic { path, arguments } if path.len() == 1 && path[0] == "vector" => {
            let [CanonicalTypeArgument::Literal(text)] = arguments.as_slice()
            else {
                return Err(Error::UnsupportedType(format!("{ty:?}")));
            };
            let dimension: u32 =
                text.parse().map_err(|_| Error::UnsupportedType(format!("vector dimension `{text}` is not a positive integer")))?;
            if dimension == 0 {
                return Err(Error::UnsupportedType("vector dimension must be greater than zero".into()));
            }
            Ok(MappedFieldType {
                rust_ty: "Vec<f32>".into(),
                vos_type: format!("vector<{dimension}>"),
                optional: false,
                is_uuid: false,
                reference_target: None,
            })
        }
        other => Err(Error::UnsupportedType(format!("{other:?}"))),
    }
}

pub(crate) fn reference_target_name(ty: &CanonicalType) -> Result<String> {
    match ty {
        CanonicalType::Named(path) => Ok(path.join("::")),
        other => Err(Error::UnsupportedType(format!("reference target `{other:?}`"))),
    }
}

pub(crate) fn ensure_single_primary(table: &TypeContract) -> Result<()> {
    let primary_count = table.fields.iter().flat_map(|field| field.attributes.iter()).filter(|attribute| attribute.name == "primary").count();
    if primary_count != 1 {
        return Err(Error::Vos(format!("table `{}` requires exactly one primary field", table.canonical_path.join("::"))));
    }
    Ok(())
}

/// Project only schema declarations through the VOS schema contract.
///
/// Operation declarations (for example durable macros) stay in the Oak input
/// and are projected separately, so they never block table/class projection.
pub(crate) fn project_schema(input: &vos::ContractInput) -> Result<SchemaProjection> {
    use vos::syntax::VosDeclarationKind;
    let root = input.clone().into_oak();
    let declarations = root
        .declarations
        .iter()
        .filter(|declaration| matches!(declaration.kind, VosDeclarationKind::Namespace | VosDeclarationKind::Table | VosDeclarationKind::Class))
        .cloned()
        .collect();
    vos::ContractInput::from_oak(vos::syntax::VosRoot { source: root.source, syntax: root.syntax, declarations })
        .project_schema()
        .map_err(|diagnostics| Error::Vos(format_diagnostics(&diagnostics)))
}

pub(crate) fn format_diagnostics(diagnostics: &[ContractDiagnostic]) -> String {
    diagnostics.iter().map(|diagnostic| format!("{}: {}", diagnostic.code, diagnostic.message)).collect::<Vec<_>>().join("\n")
}

/// Version of the canonical schema fingerprint model.
///
/// Bump this whenever the canonical structure below changes, so a model change
/// is an explicit decision instead of an unexplained fingerprint drift.
pub(crate) const SCHEMA_FINGERPRINT_VERSION: &str = "iris/schema-fingerprint/v0";

/// Canonical schema fingerprint model hashed by [`projection_fingerprint`].
///
/// Only semantics that change generation participate: declaration kind and
/// canonical path, field name/type/generation attributes, and operation
/// signatures. Spans, internal indices and source order stay out.
#[derive(Serialize)]
pub(crate) struct CanonicalSchemaFingerprint {
    fingerprint_version: &'static str,
    types: Vec<CanonicalFingerprintType>,
    operations: Vec<CanonicalFingerprintOperation>,
}

#[derive(Serialize)]
pub(crate) struct CanonicalFingerprintType {
    path: Vec<String>,
    kind: String,
    fields: Vec<CanonicalFingerprintField>,
}

#[derive(Serialize)]
pub(crate) struct CanonicalFingerprintField {
    name: String,
    ty: CanonicalType,
    attributes: Vec<String>,
}

#[derive(Serialize)]
pub(crate) struct CanonicalFingerprintOperation {
    kind: String,
    name: String,
    params: Vec<CanonicalFingerprintParam>,
    return_type: Option<CanonicalType>,
}

#[derive(Serialize)]
pub(crate) struct CanonicalFingerprintParam {
    name: String,
    ty: CanonicalType,
}

pub(crate) fn type_kind_label(kind: TypeContractKind) -> String {
    match kind {
        TypeContractKind::Table => "table",
        TypeContractKind::Class => "class",
    }
    .to_owned()
}

/// Content-addressed schema fingerprint (SHA-256, lowercase hex).
///
/// The canonical model above is serialized as JSON and hashed, so unrelated
/// code, internal layout, `Debug` derives and dependency versions cannot move
/// the value. Types are hashed structurally rather than as display labels, so
/// formatting changes cannot move it either. Declaration and field order are
/// normalized because order is not identity.
pub(crate) fn projection_fingerprint(projection: &SchemaProjection, operations: &[vos::OperationProjection]) -> String {
    let mut types = projection
        .types
        .iter()
        .map(|item| {
            let mut fields = item
                .fields
                .iter()
                .map(|field| {
                    let mut attributes = field.attributes.iter().map(|attribute| attribute.name.clone()).collect::<Vec<_>>();
                    attributes.sort();
                    attributes.dedup();
                    CanonicalFingerprintField { name: field.canonical_name.clone(), ty: field.canonical_type.clone(), attributes }
                })
                .collect::<Vec<_>>();
            fields.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| canonical_json(a).cmp(&canonical_json(b))));
            CanonicalFingerprintType { path: item.canonical_path.clone(), kind: type_kind_label(item.kind), fields }
        })
        .collect::<Vec<_>>();
    types.sort_by(|a, b| a.path.cmp(&b.path).then_with(|| a.kind.cmp(&b.kind)).then_with(|| canonical_json(a).cmp(&canonical_json(b))));

    let mut operations = operations
        .iter()
        .map(|operation| CanonicalFingerprintOperation {
            kind: operation_kind_label(operation.kind),
            name: operation.name.clone(),
            params: operation
                .parameters
                .iter()
                .map(|param| CanonicalFingerprintParam { name: param.name.clone(), ty: param.type_expr.clone() })
                .collect(),
            return_type: operation.return_type.clone(),
        })
        .collect::<Vec<_>>();
    operations.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.kind.cmp(&b.kind)).then_with(|| canonical_json(a).cmp(&canonical_json(b))));

    let wire = serde_json::to_vec(&CanonicalSchemaFingerprint { fingerprint_version: SCHEMA_FINGERPRINT_VERSION, types, operations })
        .expect("canonical schema fingerprint model serializes");
    Sha256::digest(wire).iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn canonical_json<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("canonical fingerprint component serializes")
}
