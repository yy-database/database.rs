//! SQL DDL 输入面：降到统一的 [`odm_types::MappingModel`]。
//!
//! 第一阶段只接受极简 `CREATE TABLE` 文本形态的骨架降级；完整 Oak SQL AST
//! 路径在后续补齐。本包不执行 DDL，也不绑定具体引擎方言产品。

#![deny(missing_docs)]

use odm_types::{EntityDecl, EntityKindHint, FieldDecl, MappingModel};

/// 把 SQL DDL 源文本降到统一映射模型。
///
/// 当前实现按行扫描 `CREATE TABLE <name> (...)` 骨架，抽取列名与类型名；
/// 解析失败时返回可读错误，不吞掉输入。
pub fn lower_source(source: &str) -> Result<MappingModel, String> {
    let mut entities = Vec::new();
    let upper = source.to_ascii_uppercase();
    let mut search_from = 0usize;
    while let Some(rel) = upper[search_from..].find("CREATE TABLE") {
        let start = search_from + rel;
        let after_kw = start + "CREATE TABLE".len();
        let rest = source.get(after_kw..).ok_or_else(|| "CREATE TABLE 后源文本截断".to_string())?;
        let (name, after_name) = take_ident(rest.trim_start()).ok_or_else(|| "CREATE TABLE 缺少表名".to_string())?;
        let after_name = after_name.trim_start();
        let open = after_name.find('(').ok_or_else(|| format!("表 `{name}` 缺少列定义括号"))?;
        let body_start = open + 1;
        let close = find_matching_paren(&after_name[open..]).ok_or_else(|| format!("表 `{name}` 列定义括号未闭合"))?;
        let body = &after_name[body_start..open + close];
        let fields = parse_columns(body)?;
        entities.push(EntityDecl { name: name.to_string(), namespace: None, fields, kind_hint: EntityKindHint::Relational });
        search_from = after_kw + after_name[..open + close + 1].len();
    }
    if entities.is_empty() {
        return Err("未找到任何 CREATE TABLE".into());
    }
    Ok(MappingModel { name: "sql".into(), entities, relations: Vec::new() })
}

fn take_ident(input: &str) -> Option<(&str, &str)> {
    let input = input.trim_start_matches(|c: char| c == '`' || c == '"' || c == '[');
    let end = input.char_indices().find(|(_, c)| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '.')).map(|(i, _)| i).unwrap_or(input.len());
    if end == 0 {
        return None;
    }
    let name = input[..end].trim_end_matches(|c: char| c == '`' || c == '"' || c == ']');
    Some((name, &input[end..]))
}

fn find_matching_paren(input: &str) -> Option<usize> {
    let mut depth = 0i32;
    for (i, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
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

fn parse_columns(body: &str) -> Result<Vec<FieldDecl>, String> {
    let mut fields = Vec::new();
    for raw in body.split(',') {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let upper = line.to_ascii_uppercase();
        if upper.starts_with("PRIMARY KEY")
            || upper.starts_with("UNIQUE")
            || upper.starts_with("CONSTRAINT")
            || upper.starts_with("FOREIGN KEY")
            || upper.starts_with("CHECK")
            || upper.starts_with("INDEX")
        {
            continue;
        }
        let (name, rest) = take_ident(line).ok_or_else(|| format!("无法解析列名：{line}"))?;
        let type_token = rest.trim_start().split_whitespace().next().unwrap_or("unknown").trim_matches(|c: char| c == ',' || c == ')');
        let upper_line = line.to_ascii_uppercase();
        fields.push(FieldDecl {
            name: name.to_string(),
            type_name: type_token.to_string(),
            nullable: !upper_line.contains("NOT NULL"),
            primary_key: upper_line.contains("PRIMARY KEY"),
            unique: upper_line.contains("UNIQUE"),
        });
    }
    if fields.is_empty() {
        return Err("CREATE TABLE 未解析到任何列".into());
    }
    Ok(fields)
}
