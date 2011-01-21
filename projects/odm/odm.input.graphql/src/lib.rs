//! GraphQL SDL 输入面：降到统一的 [`odm_types::MappingModel`]。
//!
//! 第一阶段只识别 `type` / `input` 对象类型与字段声明骨架；完整 GraphQL
//! 规范解析后续接入。本包不执行查询，只做 DDL → 映射模型。

#![deny(missing_docs)]

use odm_types::{EntityDecl, EntityKindHint, FieldDecl, MappingModel};

/// 把 GraphQL SDL 源文本降到统一映射模型。
pub fn lower_source(source: &str) -> Result<MappingModel, String> {
    let mut entities = Vec::new();
    let mut cursor = 0usize;
    let bytes = source.as_bytes();
    while cursor < source.len() {
        let rest = &source[cursor..];
        let trimmed = rest.trim_start();
        let skipped = rest.len() - trimmed.len();
        cursor += skipped;
        if trimmed.is_empty() {
            break;
        }
        let kind = if trimmed.starts_with("type ") {
            "type"
        }
        else if trimmed.starts_with("input ") {
            "input"
        }
        else {
            cursor = advance_line(bytes, cursor);
            continue;
        };
        let after_kw = &trimmed[kind.len()..];
        let after_kw = after_kw.trim_start();
        let (name, after_name) = take_ident(after_kw).ok_or_else(|| format!("{kind} 缺少类型名"))?;
        if name == "Query" || name == "Mutation" || name == "Subscription" {
            cursor += kind.len() + (trimmed.len() - after_kw.len()) + name.len();
            continue;
        }
        let after_name = after_name.trim_start();
        let open = after_name.find('{').ok_or_else(|| format!("类型 `{name}` 缺少字段块"))?;
        let body_start = open + 1;
        let close = find_matching_brace(&after_name[open..]).ok_or_else(|| format!("类型 `{name}` 字段块未闭合"))?;
        let body = &after_name[body_start..open + close];
        let fields = parse_fields(body)?;
        entities.push(EntityDecl { name: name.to_string(), namespace: None, fields, kind_hint: EntityKindHint::Document });
        let consumed = (rest.len() - after_name.len()) + open + close + 1;
        cursor += consumed;
    }
    if entities.is_empty() {
        return Err("未找到任何 GraphQL object/input 类型".into());
    }
    Ok(MappingModel { name: "graphql".into(), entities, relations: Vec::new() })
}

fn take_ident(input: &str) -> Option<(&str, &str)> {
    let end = input.char_indices().find(|(_, c)| !(c.is_ascii_alphanumeric() || *c == '_')).map(|(i, _)| i).unwrap_or(input.len());
    if end == 0 {
        return None;
    }
    Some((&input[..end], &input[end..]))
}

fn find_matching_brace(input: &str) -> Option<usize> {
    let mut depth = 0i32;
    for (i, ch) in input.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

fn advance_line(bytes: &[u8], from: usize) -> usize {
    match bytes[from..].iter().position(|&b| b == b'\n') {
        Some(rel) => from + rel + 1,
        None => bytes.len(),
    }
}

fn parse_fields(body: &str) -> Result<Vec<FieldDecl>, String> {
    let mut fields = Vec::new();
    for raw in body.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, rest) = take_ident(line).ok_or_else(|| format!("无法解析字段名：{line}"))?;
        let rest = rest.trim_start();
        if !rest.starts_with(':') {
            continue;
        }
        let type_part = rest[1..].trim_start();
        let (type_name, nullable) = parse_gql_type(type_part);
        fields.push(FieldDecl { name: name.to_string(), type_name, nullable, primary_key: name == "id", unique: name == "id" });
    }
    if fields.is_empty() {
        return Err("GraphQL 类型未解析到任何字段".into());
    }
    Ok(fields)
}

fn parse_gql_type(input: &str) -> (String, bool) {
    let token = input.split_whitespace().next().unwrap_or("Unknown");
    let nullable = !token.ends_with('!');
    let type_name = token.trim_end_matches('!').trim_matches(|c: char| c == '[' || c == ']');
    (type_name.to_string(), nullable)
}
