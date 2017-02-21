//! `local.oplog.rs` 条目形态。
//!
//! 这里的每种形态都在 `mongod 8.0.9`（单成员副本集、无鉴权）上实测过。本模块渲染的
//! 条目由 `MongoStorage::commit_oplog` 写入存储，本模块只决定一次写操作在 oplog 里
//! 长什么样。条目不再携带历史上的 `h` 哈希字段，因为 `mongod 8.0.9` 已经不写它。

use database_protocol_mongo::bson::{BsonDocument, BsonValue};

/// One oplog entry before `ts`, `t`, `v` and `wall` are attached.
#[derive(Debug, Clone, PartialEq)]
pub struct OplogEntry {
    /// Collection or command namespace (`<db>.<collection>` or `<db>.$cmd`).
    namespace: String,
    /// Collection UUID (`ui`) when the operation names a collection.
    uuid: Option<BsonValue>,
    /// `i`, `u`, `d` or `c`.
    operation: char,
    /// The operation description.
    object: BsonDocument,
    /// The old object: `{_id}` for updates and inserts, index spec for
    /// `dropIndexes`, record count for `drop`.
    old_object: Option<BsonDocument>,
    /// `dropDatabase` entries carry this before `ts`, as `mongod` writes it.
    from_migrate: Option<bool>,
}

impl OplogEntry {
    /// `op: "i"` for one inserted document.
    pub fn insert(namespace: &str, uuid: BsonValue, document: &BsonDocument) -> Self {
        Self {
            namespace: namespace.to_string(),
            uuid: Some(uuid),
            operation: 'i',
            object: document.clone(),
            old_object: Some(identifier_document(document)),
            from_migrate: None,
        }
    }

    /// `op: "u"` for one updated document, with the computed `$v: 2` diff.
    pub fn update(namespace: &str, uuid: BsonValue, old: &BsonDocument, new: &BsonDocument) -> Self {
        let mut object = BsonDocument::new();
        object.push("$v".to_string(), BsonValue::Int32(2));
        object.push("diff".to_string(), BsonValue::Document(document_diff(old, new)));
        Self {
            namespace: namespace.to_string(),
            uuid: Some(uuid),
            operation: 'u',
            object,
            old_object: Some(identifier_document(old)),
            from_migrate: None,
        }
    }

    /// `op: "d"` for one deleted document: `o` holds only its `_id`.
    pub fn delete(namespace: &str, uuid: BsonValue, identifier: &BsonValue) -> Self {
        let mut object = BsonDocument::new();
        object.push("_id".to_string(), identifier.clone());
        Self { namespace: namespace.to_string(), uuid: Some(uuid), operation: 'd', object, old_object: None, from_migrate: None }
    }

    /// `op: "c"` for a collection or database command.
    pub fn command(namespace: &str, uuid: Option<BsonValue>, object: BsonDocument, old_object: Option<BsonDocument>) -> Self {
        Self { namespace: namespace.to_string(), uuid, operation: 'c', object, old_object, from_migrate: None }
    }

    /// `dropDatabase` carries `fromMigrate: false`, so it is rendered here.
    pub fn database_command(namespace: &str, object: BsonDocument) -> Self {
        Self { namespace: namespace.to_string(), uuid: None, operation: 'c', object, old_object: None, from_migrate: Some(false) }
    }

    /// One `applyOps` entry holding several operations (a multi-document insert).
    ///
    /// `mongod` puts the batch under `admin.$cmd`, not under the target database.
    pub fn apply_ops(operations: Vec<OplogEntry>) -> Self {
        let mut array = Vec::with_capacity(operations.len());
        for operation in operations {
            let mut inner = BsonDocument::new();
            inner.push("op".to_string(), BsonValue::String(operation.operation.to_string()));
            inner.push("ns".to_string(), BsonValue::String(operation.namespace.clone()));
            if let Some(uuid) = operation.uuid.clone() {
                inner.push("ui".to_string(), uuid);
            }
            inner.push("o".to_string(), BsonValue::Document(operation.object));
            if let Some(old) = operation.old_object {
                inner.push("o2".to_string(), BsonValue::Document(old));
            }
            array.push(BsonValue::Document(inner));
        }
        let mut object = BsonDocument::new();
        object.push("applyOps".to_string(), BsonValue::Array(array));
        Self { namespace: "admin.$cmd".to_string(), uuid: None, operation: 'c', object, old_object: None, from_migrate: None }
    }

    /// Renders the full entry in `mongod`'s field order:
    /// `op, ns, ui, o, o2, [fromMigrate], ts, t, v, wall`.
    pub fn render(&self, seconds: u32, increment: u32, term: u32, wall_millis: i64) -> BsonDocument {
        let mut entry = BsonDocument::new();
        entry.push("op".to_string(), BsonValue::String(self.operation.to_string()));
        entry.push("ns".to_string(), BsonValue::String(self.namespace.clone()));
        if let Some(uuid) = self.uuid.clone() {
            entry.push("ui".to_string(), uuid);
        }
        entry.push("o".to_string(), BsonValue::Document(self.object.clone()));
        if let Some(old) = self.old_object.clone() {
            entry.push("o2".to_string(), BsonValue::Document(old));
        }
        if let Some(from_migrate) = self.from_migrate {
            entry.push("fromMigrate".to_string(), BsonValue::Bool(from_migrate));
        }
        entry.push("ts".to_string(), BsonValue::Timestamp { t: seconds, i: increment });
        entry.push("t".to_string(), BsonValue::Int64(i64::from(term)));
        entry.push("v".to_string(), BsonValue::Int32(2));
        entry.push("wall".to_string(), BsonValue::DateTime(wall_millis));
        entry
    }

    /// Namespace this entry targets.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

/// `{_id: <value>}` for a document that always has an `_id` by the time a
/// write is logged.
fn identifier_document(document: &BsonDocument) -> BsonDocument {
    let mut identifier = BsonDocument::new();
    if let Some(value) = document.get("_id") {
        identifier.push("_id".to_string(), value.clone());
    }
    identifier
}

/// The `$v: 2` diff of an update.
///
/// `mongod` records the update its executor planned. This server records the
/// change that actually happened: `u` maps changed paths to their new value,
/// `d` maps removed paths to `false`. Nested documents are diffed at leaf paths,
/// arrays are replaced as a whole (which is what an operator update does to a
/// field).
fn document_diff(old: &BsonDocument, new: &BsonDocument) -> BsonDocument {
    let mut updated = BsonDocument::new();
    let mut deleted = BsonDocument::new();
    diff_fields("", old, new, &mut updated, &mut deleted);
    let mut diff = BsonDocument::new();
    if !updated.fields.is_empty() {
        diff.push("u".to_string(), BsonValue::Document(updated));
    }
    if !deleted.fields.is_empty() {
        diff.push("d".to_string(), BsonValue::Document(deleted));
    }
    diff
}

fn diff_fields(prefix: &str, old: &BsonDocument, new: &BsonDocument, updated: &mut BsonDocument, deleted: &mut BsonDocument) {
    for (field, value) in &new.fields {
        let path = if prefix.is_empty() { field.clone() } else { format!("{prefix}.{field}") };
        match old.get(field) {
            None => updated.push(path, value.clone()),
            Some(BsonValue::Document(old_document)) => match value {
                BsonValue::Document(new_document) => {
                    diff_fields(&path, old_document, new_document, updated, deleted);
                }
                _ => {
                    if old.get(field) != Some(value) {
                        updated.push(path, value.clone());
                    }
                }
            },
            Some(old_value) => {
                if old_value != value {
                    updated.push(path, value.clone());
                }
            }
        }
    }
    for (field, _) in &old.fields {
        if new.get(field).is_none() {
            let path = if prefix.is_empty() { field.clone() } else { format!("{prefix}.{field}") };
            deleted.push(path, BsonValue::Bool(false));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document(fields: Vec<(&str, BsonValue)>) -> BsonDocument {
        let mut document = BsonDocument::new();
        for (field, value) in fields {
            document.push(field.to_string(), value);
        }
        document
    }

    fn keys(document: &BsonDocument) -> Vec<&str> {
        document.fields.iter().map(|(field, _)| field.as_str()).collect()
    }

    #[test]
    fn insert_entry_keeps_mongod_field_order() {
        let entry = OplogEntry::insert(
            "db.c",
            BsonValue::String("uuid".to_string()),
            &document(vec![("_id", BsonValue::Int32(1)), ("a", BsonValue::Int32(1))]),
        );
        let rendered = entry.render(100, 1, 1, 1000);
        assert_eq!(keys(&rendered), ["op", "ns", "ui", "o", "o2", "ts", "t", "v", "wall"]);
        assert_eq!(rendered.get("op"), Some(&BsonValue::String("i".to_string())));
        assert_eq!(rendered.get("o2"), Some(&BsonValue::Document(document(vec![("_id", BsonValue::Int32(1))]))));
        assert_eq!(rendered.get("ts"), Some(&BsonValue::Timestamp { t: 100, i: 1 }));
        assert_eq!(rendered.get("t"), Some(&BsonValue::Int64(1)));
        assert_eq!(rendered.get("v"), Some(&BsonValue::Int32(2)));
        assert_eq!(rendered.get("wall"), Some(&BsonValue::DateTime(1000)));
        assert_eq!(rendered.get("h"), None);
    }

    #[test]
    fn delete_entry_has_no_old_object() {
        let entry = OplogEntry::delete("db.c", BsonValue::String("uuid".to_string()), &BsonValue::Int32(2));
        let rendered = entry.render(100, 2, 1, 1000);
        assert_eq!(keys(&rendered), ["op", "ns", "ui", "o", "ts", "t", "v", "wall"]);
        assert_eq!(rendered.get("o"), Some(&BsonValue::Document(document(vec![("_id", BsonValue::Int32(2))]))));
    }

    #[test]
    fn update_entry_carries_a_version_two_diff() {
        let old = document(vec![
            ("_id", BsonValue::Int32(1)),
            ("a", BsonValue::Document(document(vec![("b", BsonValue::Int32(1)), ("c", BsonValue::Int32(2))]))),
        ]);
        let new = document(vec![("_id", BsonValue::Int32(1)), ("a", BsonValue::Document(document(vec![("b", BsonValue::Int32(3))])))]);
        let entry = OplogEntry::update("db.c", BsonValue::String("uuid".to_string()), &old, &new);
        let rendered = entry.render(100, 1, 1, 1000);
        let expected = document(vec![
            ("$v", BsonValue::Int32(2)),
            (
                "diff",
                BsonValue::Document(document(vec![
                    ("u", BsonValue::Document(document(vec![("a.b", BsonValue::Int32(3))]))),
                    ("d", BsonValue::Document(document(vec![("a.c", BsonValue::Bool(false))]))),
                ])),
            ),
        ]);
        assert_eq!(rendered.get("o"), Some(&BsonValue::Document(expected)));
    }

    #[test]
    fn apply_ops_entry_lands_on_admin_cmd() {
        let first = OplogEntry::insert("db.c", BsonValue::String("uuid".to_string()), &document(vec![("_id", BsonValue::Int32(1))]));
        let second = OplogEntry::insert("db.c", BsonValue::String("uuid".to_string()), &document(vec![("_id", BsonValue::Int32(2))]));
        let entry = OplogEntry::apply_ops(vec![first, second]);
        assert_eq!(entry.namespace(), "admin.$cmd");
        let rendered = entry.render(100, 1, 1, 1000);
        let Some(BsonValue::Array(operations)) = rendered.get("o").and_then(|value| match value {
            BsonValue::Document(object) => object.get("applyOps"),
            _ => None,
        })
        else {
            panic!("applyOps must be an array");
        };
        assert_eq!(operations.len(), 2);
        let Some(BsonValue::Document(inner)) = operations.first()
        else {
            panic!("an applyOps entry must be a document");
        };
        assert_eq!(keys(inner), ["op", "ns", "ui", "o", "o2"]);
    }
}
