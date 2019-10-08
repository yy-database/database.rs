//! `mongodump` 形态 `.bson` 流导入。
//!
//! 流由首尾相接的 BSON 文档组成，没有外层容器。缺少 `_id` 的文档会补一个生成的
//! ObjectId，写入走后端的覆盖路径，不做重复检查。

use std::path::Path;

use database_protocol_mongo::bson::{BsonValue, encode_bson, parse_bson, value_to_key};

use crate::{id::generate_object_id, storage::MongoStorage};

/// 读出 `.bson` 流里的每个文档并写入 `namespace`。
///
/// `namespace` 必须是 `<database>.<collection>`；返回值是写入条数。
pub fn import_bson_documents(storage: &dyn MongoStorage, namespace: &str, bytes: &[u8]) -> Result<usize, String> {
    if namespace.is_empty() || !namespace.contains('.') {
        return Err("namespace must be `<database>.<collection>`".to_string());
    }
    let mut offset = 0_usize;
    let mut entries = Vec::new();
    while offset < bytes.len() {
        let length = bytes
            .get(offset..offset + 4)
            .and_then(|head| <[u8; 4]>::try_from(head).ok())
            .map(i32::from_le_bytes)
            .filter(|length| *length >= 5)
            .ok_or_else(|| format!("invalid BSON length at offset {offset}"))? as usize;
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| format!("BSON document truncated at offset {offset}"))?;
        let mut document = parse_bson(&bytes[offset..end]).map_err(|error| format!("invalid BSON at offset {offset}: {error:?}"))?;
        if !document.fields.iter().any(|(field, _)| field == "_id") {
            document.push("_id".to_string(), BsonValue::ObjectId(generate_object_id()));
        }
        let identifier = document.fields.iter().find(|(field, _)| field == "_id").map(|(_, value)| value.clone()).unwrap();
        entries.push((value_to_key(&identifier), encode_bson(&document)));
        offset = end;
    }
    crate::commands::maintain_imported_indexes(storage, namespace, &entries)?;
    storage.import(namespace, &entries).map_err(|error| error.to_string())
}

/// 把一个 `mongodump` `.bson` 文件读入单个 namespace。
pub fn import_bson_file(storage: &dyn MongoStorage, namespace: &str, path: impl AsRef<Path>) -> Result<usize, String> {
    let bytes = std::fs::read(path).map_err(|error| error.to_string())?;
    import_bson_documents(storage, namespace, &bytes)
}
