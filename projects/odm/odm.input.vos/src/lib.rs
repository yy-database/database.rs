//! VOS DDL 输入面：降到统一的 [`odm_types::MappingModel`]。
//!
//! 本包只负责作者面解析与字段投影，不打开数据库、不执行迁移。

#![deny(missing_docs)]

use odm_types::{EntityDecl, EntityKindHint, FieldDecl, MappingModel, RelationDecl};
use vos::types::{CanonicalType, CanonicalTypeArgument, SchemaProjection, TypeContractKind};

/// 把一段 VOS schema 源文本降到统一映射模型。
pub fn lower_source(source: &str) -> Result<MappingModel, String> {
    let projection = odm_types::parse_schema(source)?;
    Ok(from_schema_projection("vos", &projection))
}

/// 从工程配置的 schema 模式加载并降级。
pub fn lower_project(project_dir: &std::path::Path, schema_pattern: &str) -> Result<MappingModel, String> {
    let projection = odm_types::load_schema_document(project_dir, schema_pattern)?;
    Ok(from_schema_projection("vos", &projection))
}

/// 将已投影的 VOS schema 合同转为统一模型。
pub fn from_schema_projection(name: &str, projection: &SchemaProjection) -> MappingModel {
    let mut entities = Vec::new();
    for ty in &projection.types {
        let Some(entity_name) = ty.canonical_path.last()
        else {
            continue;
        };
        let namespace = if ty.canonical_path.len() > 1 { Some(ty.canonical_path[..ty.canonical_path.len() - 1].join(".")) } else { None };
        let kind_hint = match ty.kind {
            TypeContractKind::Table => EntityKindHint::Unspecified,
            TypeContractKind::Class => EntityKindHint::Document,
        };
        let fields = ty
            .fields
            .iter()
            .map(|field| {
                let type_name = render_canonical_type(&field.canonical_type);
                let attrs: Vec<&str> = field.attributes.iter().map(|a| a.name.as_str()).collect();
                FieldDecl {
                    name: field.canonical_name.clone(),
                    type_name,
                    nullable: matches!(field.canonical_type, CanonicalType::Optional(_))
                        || attrs.iter().any(|a| *a == "optional" || *a == "nullable"),
                    primary_key: attrs.iter().any(|a| *a == "primary" || *a == "pk" || *a == "id"),
                    unique: attrs.iter().any(|a| *a == "unique"),
                }
            })
            .collect();
        entities.push(EntityDecl { name: entity_name.clone(), namespace, fields, kind_hint });
    }
    MappingModel { name: name.to_string(), entities, relations: Vec::<RelationDecl>::new() }
}

fn render_canonical_type(ty: &CanonicalType) -> String {
    match ty {
        CanonicalType::Named(path) => path.join("."),
        CanonicalType::Reference(inner) => format!("ref<{}>", render_canonical_type(inner)),
        CanonicalType::Optional(inner) => format!("optional<{}>", render_canonical_type(inner)),
        CanonicalType::List(inner) => format!("list<{}>", render_canonical_type(inner)),
        CanonicalType::Generic { path, arguments } => {
            let args = arguments
                .iter()
                .map(|arg| match arg {
                    CanonicalTypeArgument::Type(t) => render_canonical_type(t),
                    CanonicalTypeArgument::Literal(text) => text.clone(),
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("{}<{args}>", path.join("."))
        }
    }
}
