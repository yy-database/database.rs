//! Iris 代码生成（Dejavu 后端）· **Rust 宿主**。
//!
//! 共享 `.dejavu` 模板与 GenerationModel；各宿主用各自的 Dejavu 门面渲染
//!（本 crate 用 Rust `dejavu`）。生成调用 `iris::*` 的 Rust 绑定，不为其它
//! 语言做 FFI 包装。其它宿主（如 TS）按同一模板合同自备生成器。

#![deny(missing_docs)]

use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use dejavu::{Dejavu, IrDocument};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use vos::{CanonicalType, CanonicalTypeArgument, ContractDiagnostic, SchemaProjection, TypeContract, TypeContractKind};

/// Generator errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Unknown template stem.
    #[error("unknown dejavu template `{0}`")]
    UnknownTemplate(String),
    /// Dejavu render/parse failure.
    #[error("{0}")]
    Dejavu(String),
    /// Embedded AOT IR JSON could not be deserialized.
    #[error("invalid AOT IR for `{0}`: {1}")]
    InvalidIr(String, String),
    /// VOS schema parse failure.
    #[error("vos: {0}")]
    Vos(String),
    /// Unsupported VOS type for the Rust emitter.
    #[error("unsupported VOS type for Rust emit: {0}")]
    UnsupportedType(String),
    /// VOS type label not in the faithful carrier table.
    #[error("unknown VOS label `{0}` is not in the carrier table")]
    UnknownVosLabel(String),
    /// Operation declaration kind outside the projected VOS operation set.
    #[error("operation `{operation}` uses unsupported kind `{kind}` (expected query, udf, micro or macro)")]
    UnsupportedOperationKind {
        /// Operation author name.
        operation: String,
        /// Declared kind label that is not a VOS operation kind.
        kind: String,
    },
    /// Operation signature references a model that the schema does not declare.
    #[error("operation `{operation}` references undeclared model `{ty}`")]
    UndeclaredOperationModel {
        /// Operation author name.
        operation: String,
        /// Named type no declared table provides.
        ty: String,
    },
    /// Operation signature type the declared-vos execution path cannot carry.
    #[error("operation `{operation}` uses unsupported type `{ty}`: {reason}")]
    UnsupportedOperationType {
        /// Operation author name.
        operation: String,
        /// Offending VOS type label.
        ty: String,
        /// Why the type has no execution carrier.
        reason: &'static str,
    },
    /// Operation without a durable VOS operation identity.
    #[error("operation `{operation}` has no durable VOS operation identity")]
    MissingOperationIdentity {
        /// Operation author name.
        operation: String,
    },
    /// Two operations claim the same durable identity.
    #[error("duplicate operation identity `{operation_id}`")]
    DuplicateOperationId {
        /// Durable operation identity claimed more than once.
        operation_id: String,
    },
    /// Field label with no typed filter surface on the generated client.
    #[error("unsupported filter type `{vos_type}` on `{entity}.{field}`")]
    UnsupportedFilterType {
        /// Entity that declares the field.
        entity: String,
        /// VOS wire field name.
        field: String,
        /// VOS type label that has no generated filter.
        vos_type: String,
    },
    /// Unsupported generate target.
    #[error("unsupported generate target `{0}` (use rust or typescript)")]
    UnsupportedTarget(String),
    /// Rust client target requested without a datasource kind to bind against.
    #[error("rust client generation requires a datasource kind (the adapter binding is chosen per datasource)")]
    MissingDatasource,
    /// Datasource kind whose adapter surface has not converged for a Rust client binding.
    #[error(
        "datasource `{0}` has no converged Rust adapter binding (only `mysql` and `yydb` are bound; sqlite/postgres/redis/yyds/reference adapter surfaces have not converged)"
    )]
    UnsupportedDatasource(String),
    /// Two VOS fields map to the same TypeScript author name.
    #[error("TypeScript field naming collision on `{entity}`: `{wire_a}` and `{wire_b}` map to `{ts_name}`")]
    NamingCollision {
        /// Entity name where the collision was detected.
        entity: String,
        /// First colliding VOS wire field name.
        wire_a: String,
        /// Second colliding VOS wire field name.
        wire_b: String,
        /// Target TypeScript name both fields try to occupy.
        ts_name: String,
    },
    /// Operation projection artifact decode / validation failure.
    #[error("operation artifact: {0}")]
    OperationArtifact(#[from] operations_artifact::OperationArtifactError),
    /// I/O while writing generated files.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Result alias.
pub type Result<T> = std::result::Result<T, Error>;

mod carrier;
mod operations_artifact;
pub use operations_artifact::IrisOperationArtifact;
mod python;
mod python_view;
mod rust_view;
mod ts_view;
mod typescript;

include!(concat!(env!("OUT_DIR"), "/aot_registry.rs"));

pub use python::{emit_python_client, write_python_client};

/// Fixed output directory of the Python target under a project root.
pub const PYTHON_GENERATE_OUT: &str = "src/generated/iris";
pub use typescript::{emit_typescript_client, emit_typescript_runtime, write_typescript_client, write_typescript_runtime};

/// Output directory for the Python target (`generate_root`, same as TypeScript).
pub fn python_target_dir(generate_root: &std::path::Path) -> std::path::PathBuf {
    generate_root.to_path_buf()
}

/// One VOS field in the generation model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldModel {
    /// VOS-assigned durable field identity, absent only in legacy source models.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_id: Option<u64>,
    /// VOS-assigned virtual slot, absent only in legacy source models.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub virtual_field_index: Option<u32>,
    /// Field name.
    pub name: String,
    /// Rust type text for the emitter.
    pub rust_ty: String,
    /// VOS type label.
    pub vos_type: String,
    /// Primary key marker.
    pub primary: bool,
    /// Optional / nullable.
    pub optional: bool,
    /// True when the VOS type is `uuid` (including `uuid?`).
    #[serde(default)]
    pub is_uuid: bool,
    /// Target entity for `&T` reference fields (`None` for scalars).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference_target: Option<String>,
}

/// One VOS macro parameter in the generation model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MacroParamModel {
    /// Parameter name.
    pub name: String,
    /// VOS type label.
    pub vos_type: String,
    /// TypeScript parameter type.
    pub ts_type: String,
}

/// One VOS macro in the generation model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MacroModel {
    /// Macro name in schema.
    pub name: String,
    /// Return type label (VOS surface).
    pub return_type: String,
    /// Positional parameters from the macro signature.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<MacroParamModel>,
}

/// One operation parameter (shared across Query / Udf / Micro / Macro).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationParamModel {
    /// Parameter author name.
    pub name: String,
    /// VOS type label (surface syntax).
    pub vos_type: String,
    /// TypeScript parameter carrier.
    pub ts_type: String,
}

/// One projected operation consumed from VOS OperationProjectionArtifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationModel {
    /// Opaque stable operation identity (`op_<hex>`).
    pub operation_id: String,
    /// Operation declaration kind (`query` | `udf` | `micro` | `macro`).
    pub kind: String,
    /// Operation author name.
    pub name: String,
    /// Canonical signature key (human-readable stable key).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_key: Option<String>,
    /// Operation parameters.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<OperationParamModel>,
    /// Return type label (VOS surface), absent when the operation returns unit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub return_type: Option<String>,
}

/// One VOS table in the generation model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableModel {
    /// VOS-assigned durable type identity, absent only in legacy source models.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_id: Option<u64>,
    /// VOS table name.
    pub name: String,
    /// Rust struct name (same as table for Phase 8).
    pub rust_type: String,
    /// Fields.
    pub fields: Vec<FieldModel>,
}

/// Versioned, deterministic generation input (templates must not see raw AST).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationModel {
    /// Generator crate version.
    pub generator_version: String,
    /// Schema fingerprint.
    pub schema_fingerprint: String,
    /// Tables.
    pub tables: Vec<TableModel>,
    /// Schema macros (always emitted in generated TS regardless of backend capability).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub macros: Vec<MacroModel>,
    /// Projected operations consumed from VOS OperationProjectionArtifact.
    /// Always sorted by `operation_id` for determinism; order does not matter
    /// to identity resolution.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operations: Vec<OperationModel>,
    /// Original VOS schema text when the model came from source.
    ///
    /// Generated clients embed it so a managed adapter can apply the schema it
    /// was generated from; artifact-only models leave it empty.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub schema_source: String,
}

impl GenerationModel {
    /// Builds from a strict VOS resolved artifact without parsing source or allocating identities.
    pub fn from_resolved_artifact(artifact: &str) -> Result<Self> {
        let contract = vos::ResolvedContract::from_json(artifact).map_err(|error| Error::Vos(format!("{}: {}", error.code, error.message)))?;
        let mut tables = Vec::new();
        for item in &contract.types {
            if item.kind != TypeContractKind::Table || item.canonical_path.len() != 1 {
                return Err(Error::UnsupportedType(format!("resolved entity {}", item.canonical_path.join("::"))));
            }
            let mut fields = Vec::new();
            for field in &item.fields {
                let mapped = map_resolved_field_type(&field.canonical_type)?;
                fields.push(FieldModel {
                    field_id: Some(field.field_id),
                    virtual_field_index: field.virtual_field_index,
                    name: field.canonical_name.clone(),
                    rust_ty: mapped.rust_ty,
                    vos_type: mapped.vos_type,
                    primary: field.attributes.iter().any(|attribute| attribute.name == "primary"),
                    optional: mapped.optional,
                    is_uuid: mapped.is_uuid,
                    reference_target: mapped.reference_target,
                });
            }
            fields.sort_by_key(|field| field.virtual_field_index);
            tables.push(TableModel {
                type_id: Some(item.type_id),
                name: item.canonical_path[0].clone(),
                rust_type: item.canonical_path[0].clone(),
                fields,
            });
        }
        tables.sort_by_key(|table| table.type_id);
        Ok(Self {
            generator_version: env!("CARGO_PKG_VERSION").into(),
            schema_fingerprint: contract.schema_fingerprint,
            tables,
            macros: Vec::new(),
            operations: Vec::new(),
            schema_source: String::new(),
        })
    }

    /// Builds from a resolved contract artifact **and** a separate VOS
    /// operation projection artifact.
    ///
    /// Operations are sorted by `operation_id` for deterministic output. Macro
    /// operations are also duplicated into `macros` so that existing TS macro
    /// templates keep working unchanged.
    pub fn from_artifacts(contract_json: &str, operations_json: &str) -> Result<Self> {
        let mut model = Self::from_resolved_artifact(contract_json)?;
        let artifact = operations_artifact::IrisOperationArtifact::from_json(operations_json)?;
        let declared: HashSet<String> = model.tables.iter().map(|table| table.name.clone()).collect();
        let mut operations = Vec::with_capacity(artifact.len());
        let mut macros: Vec<MacroModel> = Vec::new();
        let mut seen_macro = std::collections::HashSet::new();
        for op in &artifact.operations {
            let kind_str = operation_kind_label(op.kind);
            let params: Vec<OperationParamModel> = op
                .parameters
                .iter()
                .map(|p| {
                    let vos_ty = type_label(&p.type_expr);
                    let carrier = resolve_operation_carrier(&op.name, &vos_ty, &declared)?;
                    Ok(OperationParamModel { name: p.name.clone(), ts_type: carrier.ts_type, vos_type: vos_ty })
                })
                .collect::<Result<Vec<_>>>()?;
            let ret = op.return_type.as_ref().map(type_label);
            operations.push(OperationModel {
                operation_id: op.operation_id.clone(),
                kind: kind_str,
                name: op.name.clone(),
                identity_key: Some(op.identity_key.clone()),
                params: params.clone(),
                return_type: ret.clone(),
            });
            if op.kind == vos::types::OperationKind::Macro && seen_macro.insert(op.name.clone()) {
                macros.push(MacroModel {
                    name: op.name.clone(),
                    return_type: ret.unwrap_or_else(|| "unit".into()),
                    params: params
                        .iter()
                        .map(|p| MacroParamModel { name: p.name.clone(), vos_type: p.vos_type.clone(), ts_type: p.ts_type.clone() })
                        .collect(),
                });
            }
        }
        operations.sort_by(|a, b| a.operation_id.cmp(&b.operation_id));
        macros.sort_by(|a, b| a.name.cmp(&b.name));
        model.operations = operations;
        if !macros.is_empty() {
            model.macros = macros;
        }
        // Prefer envelope fingerprint when present; keep contract fingerprint
        // as fallback so callers that only ever pass one artifact still get a
        // deterministic value.
        if let Some(fp) = artifact.schema_fingerprint() {
            model.schema_fingerprint = fp.to_owned();
        }
        model.validate_operation_surface()?;
        Ok(model)
    }

    /// Build from a VOS schema document string.
    pub fn from_vos_schema(source: &str) -> Result<Self> {
        let input = vos::parse_oak(source).map_err(Error::Vos)?;
        if !source.trim().is_empty() && input.declarations().is_empty() {
            return Err(Error::Vos("schema document contains no VOS declarations".into()));
        }
        let mut model = Self::from_document(&input)?;
        model.schema_source = source.to_owned();
        Ok(model)
    }

    /// Build from a parsed VOS contract input.
    pub fn from_document(input: &vos::ContractInput) -> Result<Self> {
        let projection = project_schema(input)?;
        let operations = input.project_operations().map_err(|diagnostics| Error::Vos(format_diagnostics(&diagnostics)))?;
        let mut tables = Vec::new();
        for table in projection.types.iter().filter(|item| item.kind == TypeContractKind::Table) {
            ensure_single_primary(table)?;
            let mut fields = Vec::new();
            for field in &table.fields {
                let mapped = map_field_type(&field.canonical_type)?;
                fields.push(FieldModel {
                    field_id: None,
                    virtual_field_index: None,
                    name: field.canonical_name.clone(),
                    rust_ty: mapped.rust_ty,
                    vos_type: mapped.vos_type,
                    primary: field.attributes.iter().any(|attribute| attribute.name == "primary"),
                    optional: mapped.optional,
                    is_uuid: mapped.is_uuid,
                    reference_target: mapped.reference_target,
                });
            }
            let Some(name) = table.canonical_path.last()
            else {
                return Err(Error::UnsupportedType("table declaration without a name".into()));
            };
            tables.push(TableModel { type_id: None, name: name.clone(), rust_type: name.clone(), fields });
        }
        let declared: HashSet<String> = tables.iter().map(|table| table.name.clone()).collect();
        let mut macros = Vec::new();
        let mut operation_models = Vec::new();
        for operation in &operations {
            let params: Vec<OperationParamModel> = operation
                .parameters
                .iter()
                .map(|param| {
                    let vos_ty = type_label(&param.type_expr);
                    let carrier = resolve_operation_carrier(&operation.name, &vos_ty, &declared)?;
                    Ok(OperationParamModel { name: param.name.clone(), ts_type: carrier.ts_type, vos_type: vos_ty })
                })
                .collect::<Result<Vec<_>>>()?;
            let ret = operation.return_type.as_ref().map(type_label);
            let kind_str = operation_kind_label(operation.kind);
            operation_models.push(OperationModel {
                operation_id: operation.operation_id.to_string(),
                kind: kind_str,
                name: operation.name.clone(),
                identity_key: None,
                params: params.clone(),
                return_type: ret.clone(),
            });
            if operation.kind == vos::OperationKind::Macro {
                macros.push(MacroModel {
                    name: operation.name.clone(),
                    return_type: ret.unwrap_or_else(|| "unit".into()),
                    params: params
                        .iter()
                        .map(|p| MacroParamModel { name: p.name.clone(), vos_type: p.vos_type.clone(), ts_type: p.ts_type.clone() })
                        .collect(),
                });
            }
        }
        let mut model = Self {
            generator_version: env!("CARGO_PKG_VERSION").into(),
            schema_fingerprint: projection_fingerprint(&projection, &operations),
            tables,
            macros,
            operations: operation_models,
            schema_source: String::new(),
        };
        model.operations.sort_by(|a, b| a.operation_id.cmp(&b.operation_id));
        model.validate_operation_surface()?;
        Ok(model)
    }

    /// Hard-fails when a projected operation leaves the declared VOS surface.
    ///
    /// A generated delegate is a projection of declared operations: it may not
    /// invent an entry point, a type carrier, or an identity the schema does not
    /// declare. Unknown operation kinds, signature types without a declared-vos
    /// carrier, models no table declares, and operations without a durable
    /// identity fail here instead of silently degrading to a guessed type.
    fn validate_operation_surface(&self) -> Result<()> {
        let declared: HashSet<String> = self.tables.iter().map(|table| table.name.clone()).collect();
        let mut seen: HashSet<&str> = HashSet::with_capacity(self.operations.len());
        for operation in &self.operations {
            if operation_kind_rust_doc(operation.kind.as_str()).is_none() {
                return Err(Error::UnsupportedOperationKind { operation: operation.name.clone(), kind: operation.kind.clone() });
            }
            if operation.operation_id.is_empty() {
                return Err(Error::MissingOperationIdentity { operation: operation.name.clone() });
            }
            if !seen.insert(operation.operation_id.as_str()) {
                return Err(Error::DuplicateOperationId { operation_id: operation.operation_id.clone() });
            }
            for param in &operation.params {
                resolve_operation_carrier(&operation.name, &param.vos_type, &declared)?;
            }
            if let Some(ret) = operation.return_type.as_ref() {
                resolve_operation_carrier(&operation.name, ret, &declared)?;
            }
        }
        Ok(())
    }

    /// JSON value for Dejavu contexts.
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).expect("GenerationModel serializes")
    }
}

/// Host carriers of one declared operation parameter or return type.
struct OperationCarrier {
    rust_ty: String,
    ts_type: String,
}

/// Resolves one operation signature type to declared-vos host carriers.
///
/// Only two shapes may cross a generated delegate: a VOS builtin label the
/// faithful carrier table maps, or a type the schema itself declares.
/// `bytes`, `vector` and `file` cross as their typed VOS carriers
/// (`Vec<u8>` / `Uint8Array`, `Vec<f32>` / `Float32Array`,
/// `iris::FileDescriptor` / the inline descriptor shape) because the backend
/// channel carries payloads, never text. Collection/generic shapes,
/// undeclared models and unknown labels all hard-fail here so generation
/// never guesses a carrier.
fn resolve_operation_carrier(operation: &str, vos_type: &str, declared: &HashSet<String>) -> Result<OperationCarrier> {
    let trimmed = vos_type.trim();
    let optional = trimmed.ends_with('?');
    let is_reference = trimmed.starts_with('&');
    let unsupported =
        |reason: &'static str| Error::UnsupportedOperationType { operation: operation.to_owned(), ty: vos_type.to_owned(), reason };

    // Collection and generic shapes keep their raw spelling here: an
    // operation signature carries `vector` only in its dimension-free form,
    // so `vector<N>` hard-fails rather than silently dropping the declared
    // dimension the field surface would inline.
    if trimmed.contains('[') || trimmed.contains('<') || trimmed.contains(',') {
        return Err(unsupported("collection and generic shapes have no declared-vos operation carrier"));
    }
    let base = carrier::base_vos_type(trimmed);
    if let Some(label) = carrier::faithful_label(base) {
        if label == "unit" {
            return Ok(OperationCarrier { rust_ty: "()".into(), ts_type: "void".into() });
        }
        let (Some(rust_ty), Some(ts_type)) = (carrier::rust_carrier_for_label(label), carrier::ts_carrier_for_label(label))
        else {
            return Err(unsupported("no host carrier exists for this VOS label"));
        };
        let rust_ty = if optional { format!("Option<{rust_ty}>") } else { rust_ty.to_owned() };
        return Ok(OperationCarrier { rust_ty, ts_type: ts_type.to_owned() });
    }
    if base.contains("::") || base.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
        if !declared.contains(base) {
            return Err(Error::UndeclaredOperationModel { operation: operation.to_owned(), ty: base.to_owned() });
        }
        let rust_ty = if is_reference {
            format!("{base}Ref")
        }
        else if optional {
            format!("Option<{base}>")
        }
        else {
            base.to_owned()
        };
        let ts_type = if is_reference { format!("{base}Reference") } else { base.to_owned() };
        return Ok(OperationCarrier { rust_ty, ts_type });
    }
    Err(Error::UnknownVosLabel(base.to_owned()))
}

/// Render a registered template by stem name.
pub fn render(name: &str, ctx: &Value) -> Result<String> {
    render_registered(name, ctx)
}

/// Whether this build prefers AOT.
pub const fn prefers_aot() -> bool {
    cfg!(feature = "aot")
}

/// Default TypeScript generate root when `iris.config.ts` `generate.out` is `.` (legacy).
pub const DEFAULT_TYPESCRIPT_GENERATE_OUT: &str = "src/generated/iris";

/// Resolve `generate.out` (project-relative) to the TypeScript client root.
pub fn resolve_typescript_generate_root(project_root: &std::path::Path, generate_out: &str) -> std::path::PathBuf {
    let relative = if generate_out == "." { DEFAULT_TYPESCRIPT_GENERATE_OUT } else { generate_out };
    project_root.join(relative)
}

/// Output directory for the Rust target (`{generate_root}/rust/`).
pub fn rust_target_dir(generate_root: &std::path::Path) -> std::path::PathBuf {
    generate_root.join("rust")
}

/// Output directory for the TypeScript target (`generate.out` from `iris.config.ts`).
pub fn typescript_target_dir(generate_root: &std::path::Path) -> std::path::PathBuf {
    generate_root.to_path_buf()
}

fn rust_file_header(model: &GenerationModel) -> Result<String> {
    render(
        "rust/file_header",
        &json!({
            "generator_version": model.generator_version,
            "schema_fingerprint": model.schema_fingerprint,
        }),
    )
}

/// Emit Rust domain + client as a single string (tests / inspection).
pub fn emit_rust_domain(model: &GenerationModel, datasource: &str) -> Result<String> {
    let files = emit_rust_files(model, datasource)?;
    let mut combined = String::new();
    for (name, content) in files {
        combined.push_str(&format!("\n// ===== {name} =====\n"));
        combined.push_str(&content);
    }
    Ok(combined)
}

/// Emit Rust target files: `mod` / `models` / `operations` / `metadata` / `errors`.
pub fn emit_rust_files(model: &GenerationModel, datasource: &str) -> Result<Vec<(String, String)>> {
    model.validate_operation_surface()?;
    let ctx = model.rust_template_context(datasource)?;
    let header = rust_file_header(model)?;
    let structs = unescape_rust_template(&render("rust/domain_mod", &model.to_json())?);
    let helpers = render("rust/models_helpers", &ctx)?;
    let impls = unescape_rust_template(&render("rust/models_impl", &ctx)?);
    let models = format!("{header}\n{structs}\n{helpers}\n{impls}");
    let metadata = format!("{header}\n{}\n", render("rust/metadata", &ctx)?);
    let errors = format!("{header}\n{}\n", render("rust/errors", &ctx)?);
    let base_ops = format!("{header}\n{}\n", unescape_rust_template(&render("rust/operations", &ctx)?),);
    let operations = if model.operations.is_empty() { base_ops } else { format!("{base_ops}\n{}", emit_rust_operation_stubs(model)?) };
    let index = format!("{header}\n{}\n", render("rust/rust_index", &ctx)?);

    Ok(vec![
        ("mod.rs".into(), index),
        ("models.rs".into(), models),
        ("operations.rs".into(), operations),
        ("metadata.rs".into(), metadata),
        ("errors.rs".into(), errors),
    ])
}

/// Dejavu template mode HTML-escapes output; generated source must be unescaped.
pub(crate) fn unescape_dejavu_template(text: &str) -> String {
    text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&#39;", "'")
}

/// Rust-only alias (angle brackets in generics and lifetimes).
fn unescape_rust_template(text: &str) -> String {
    unescape_dejavu_template(text)
}

fn write_files_atomic(root: &std::path::Path, files: Vec<(String, String)>) -> Result<Vec<std::path::PathBuf>> {
    std::fs::create_dir_all(root)?;
    let mut written = Vec::new();
    for (name, content) in files {
        let target = root.join(&name);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = root.join(format!("{name}.tmp"));
        std::fs::write(&tmp, content)?;
        std::fs::rename(&tmp, &target)?;
        written.push(target);
    }
    Ok(written)
}

/// Write Rust bindings into `{generate_root}/rust/`.
pub fn write_rust_domain(model: &GenerationModel, datasource: &str, generate_root: &std::path::Path) -> Result<Vec<std::path::PathBuf>> {
    let root = rust_target_dir(generate_root);
    write_files_atomic(&root, emit_rust_files(model, datasource)?)
}

/// Generate bindings for `target` (`rust` | `typescript`) under `generate_root`.
///
/// The `rust` target binds the generated client to one datasource kind; the
/// binding table only covers adapter surfaces that have converged.
pub fn generate(
    model: &GenerationModel,
    target: &str,
    generate_root: &std::path::Path,
    datasource: Option<&str>,
) -> Result<Vec<std::path::PathBuf>> {
    match target {
        "rust" => write_rust_domain(model, datasource.ok_or(Error::MissingDatasource)?, generate_root),
        "typescript" | "ts" => write_typescript_client(model, generate_root),
        "python" | "py" => write_python_client(model, generate_root),
        other => Err(typescript::unsupported_target(other)),
    }
}

/// Emit target file contents from a `GenerationModel` without writing to the
/// filesystem (used by WASM / in-memory hosts).
pub fn emit_target_files(model: &GenerationModel, target: &str, datasource: Option<&str>) -> Result<Vec<(String, String)>> {
    match target {
        "rust" => emit_rust_files(model, datasource.ok_or(Error::MissingDatasource)?),
        "typescript" | "ts" => emit_typescript_client(model),
        "python" | "py" => emit_python_client(model),
        other => Err(typescript::unsupported_target(other)),
    }
}

/// Parse schema source and generate bindings for `target`.
pub fn generate_from_source(
    source: &str,
    target: &str,
    generate_root: &std::path::Path,
    datasource: Option<&str>,
) -> Result<(GenerationModel, Vec<std::path::PathBuf>)> {
    let model = GenerationModel::from_vos_schema(source)?;
    let paths = generate(&model, target, generate_root, datasource)?;
    Ok((model, paths))
}

/// Build a `GenerationModel` from pre-resolved VOS artifacts and generate
/// bindings for `target` under `generate_root`.
///
/// Route B counterpart of [`generate_from_source`]: skips schema parsing and
/// identity allocation when the caller already holds a resolved contract JSON
/// and a VOS operation projection artifact.
pub fn generate_from_artifacts(
    contract_json: &str,
    operations_json: &str,
    target: &str,
    generate_root: &std::path::Path,
    datasource: Option<&str>,
) -> Result<(GenerationModel, Vec<std::path::PathBuf>)> {
    let model = GenerationModel::from_artifacts(contract_json, operations_json)?;
    let paths = generate(&model, target, generate_root, datasource)?;
    Ok((model, paths))
}

/// Emit target file contents from pre-resolved VOS artifacts without writing
/// to the filesystem (used by WASM hosts).
///
/// Returns `(model, files)` where each `(filename, content)` pair is one
/// generated artifact.
pub fn emit_from_artifacts(
    contract_json: &str,
    operations_json: &str,
    target: &str,
    datasource: Option<&str>,
) -> Result<(GenerationModel, Vec<(String, String)>)> {
    let model = GenerationModel::from_artifacts(contract_json, operations_json)?;
    let files = emit_target_files(&model, target, datasource)?;
    Ok((model, files))
}

mod projection;
mod render;

use projection::*;
pub(crate) use render::{emit_rust_operation_stubs, emit_ts_operation_stubs};
use render::{operation_kind_label, operation_kind_rust_doc, render_aot_ir};

pub use render::is_stub;
