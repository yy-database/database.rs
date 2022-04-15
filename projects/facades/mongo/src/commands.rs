//! 逐条命令原语。
//!
//! 每个函数都能被库使用者单独调用：没有隐藏的统一入口，也没有全局状态。宿主把
//! 这些原语组合成自己的 server 循环与 CLI。

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};

use database_protocol_mongo::bson::{BinaryData, BsonDocument, BsonValue, encode_bson, integral_value, parse_bson, value_to_key};

use database_protocol_mongo::{decimal128::Decimal128, json};

use crate::{
    cursor::{CursorStore, GetMoreError},
    id::generate_object_id,
    oplog::OplogEntry,
    storage::{
        CollectionOptions, IndexSpec, MongoStorage, SessionWriteState, StoreError, TransactionAbort, TransactionBegin, TransactionCommit,
        TransactionWrite,
    },
};

/// Filter operators this gateway implements. Anything outside this list is a
/// hard error: an unrecognised operator must never silently match nothing.
pub const FILTER_OPERATORS: [&str; 21] = [
    "$eq",
    "$ne",
    "$gt",
    "$gte",
    "$lt",
    "$lte",
    "$in",
    "$nin",
    "$exists",
    "$regex",
    "$options",
    "$size",
    "$type",
    "$elemMatch",
    "$all",
    "$not",
    "$mod",
    "$bitsAllSet",
    "$bitsAnySet",
    "$bitsAllClear",
    "$bitsAnyClear",
];

/// Logical operators accepted at the top of a filter.
pub const LOGICAL_OPERATORS: [&str; 3] = ["$and", "$or", "$nor"];

/// Update operators this gateway implements.
pub const UPDATE_OPERATORS: [&str; 12] =
    ["$set", "$unset", "$inc", "$mul", "$min", "$max", "$rename", "$push", "$pop", "$addToSet", "$pull", "$setOnInsert"];

/// Command failure carrying the MongoDB error code.
#[derive(Debug, Clone, PartialEq)]
pub struct CommandError {
    /// MongoDB error code (`59` unsupported, `73` invalid namespace, `43` no such cursor).
    pub code: i32,
    /// Message drivers surface as `errmsg`.
    pub message: String,
    /// Extra top-level fields the real reply carries after `codeName`, such as
    /// `keyPattern` / `keyValue` on an index-build duplicate key failure.
    pub details: Vec<(String, BsonValue)>,
}

impl CommandError {
    /// Builds one command failure.
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self { code, message: message.into(), details: Vec::new() }
    }

    /// Appends one top-level field to the error reply.
    pub fn with_detail(mut self, key: &str, value: BsonValue) -> Self {
        self.details.push((key.to_string(), value));
        self
    }

    /// `CommandNotFound`: the command or stage is outside the documented surface.
    pub fn unsupported(stage: &str) -> Self {
        Self::new(59, stage)
    }
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for CommandError {}

/// A storage failure is a server-side failure, never a client mistake and never
/// "not found". It is surfaced as `InternalError` (`1`) with the backend reason
/// kept verbatim so a broken store cannot look like an empty collection.
impl From<StoreError> for CommandError {
    fn from(error: StoreError) -> Self {
        Self::new(1, format!("storage failure: {error}"))
    }
}

/// Rejects every filter that uses an operator outside [`FILTER_OPERATORS`].
///
/// The message always names the operator, the operation that received it, and
/// where it appeared (`fieldIndex` within the filter, `operatorIndex` within
/// the operator document), so a wrong query is reported instead of silently
/// returning an empty cursor.
pub fn validate_filter(filter: &BsonDocument, operation: &str) -> Result<(), CommandError> {
    for (field_index, (field, expected)) in filter.fields.iter().enumerate() {
        if field.starts_with('$') {
            if LOGICAL_OPERATORS.contains(&field.as_str()) {
                let BsonValue::Array(clauses) = expected
                else {
                    return Err(CommandError::new(
                        2,
                        format!("{field} requires an array of filter documents (operation: {operation}, fieldIndex: {field_index})"),
                    ));
                };
                if clauses.is_empty() {
                    return Err(CommandError::new(
                        2,
                        format!(
                            "{field} with an empty array is not defined by the MongoDB manual; refusing to guess (operation: {operation}, fieldIndex: {field_index})"
                        ),
                    ));
                }
                for (clause_index, clause) in clauses.iter().enumerate() {
                    let BsonValue::Document(clause) = clause
                    else {
                        return Err(CommandError::new(
                            2,
                            format!("{field} element {clause_index} is not a filter document (operation: {operation})"),
                        ));
                    };
                    validate_filter(clause, operation)?;
                }
                continue;
            }
            if field == "$expr" {
                // `$expr` carries an aggregation expression, validated when it is
                // evaluated against each candidate document.
                continue;
            }
            return Err(CommandError::new(
                2,
                format!("unknown top level operator: {field} (operation: {operation}, fieldIndex: {field_index})"),
            ));
        }
        let BsonValue::Document(operators) = expected
        else {
            continue;
        };
        if !has_operator(operators) {
            continue;
        }
        validate_operator_document(operators, operation, field, field_index)?;
    }
    Ok(())
}

fn has_operator(document: &BsonDocument) -> bool {
    document.fields.iter().any(|(field, _)| field.starts_with('$'))
}

fn validate_operator_document(operators: &BsonDocument, operation: &str, field: &str, field_index: usize) -> Result<(), CommandError> {
    for (operator_index, (operator, operand)) in operators.fields.iter().enumerate() {
        if !FILTER_OPERATORS.contains(&operator.as_str()) {
            return Err(unknown_operator(operator, operation, field, field_index, operator_index));
        }
        let bad = |detail: &str| {
            CommandError::new(
                2,
                format!(
                    "{operator} {detail} (operation: {operation}, field: \"{field}\", fieldIndex: {field_index}, operatorIndex: {operator_index})"
                ),
            )
        };
        match operator.as_str() {
            "$in" | "$nin" => {
                let BsonValue::Array(values) = operand
                else {
                    return Err(bad("requires an array"));
                };
                if values.is_empty() {
                    return Err(bad("with an empty array is not defined by the MongoDB manual; refusing to guess"));
                }
            }
            "$all" => {
                if !matches!(operand, BsonValue::Array(_)) {
                    return Err(bad("requires an array"));
                }
            }
            "$exists" => {
                if !matches!(operand, BsonValue::Bool(_)) {
                    return Err(bad("requires a boolean"));
                }
            }
            "$size" => {
                if integer_of(operand).filter(|size| *size >= 0).is_none() {
                    return Err(bad("requires a non-negative integer"));
                }
            }
            "$mod" => {
                parse_mod_operand(operand).map_err(bad)?;
            }
            "$bitsAllSet" | "$bitsAnySet" | "$bitsAllClear" | "$bitsAnyClear" => {
                bit_mask(operand).map_err(bad)?;
            }
            "$regex" => {
                let BsonValue::String(pattern) = operand
                else {
                    return Err(bad("requires a string pattern"));
                };
                let options = match operators.get("$options") {
                    Some(BsonValue::String(options)) => options.as_str(),
                    _ => "",
                };
                // Compile during validation: a bad pattern or option must fail even
                // when the collection has no documents to scan.
                compile_regex(pattern, options).map_err(|error| bad(&error.message))?;
            }
            "$options" => {
                if !matches!(operand, BsonValue::String(_)) {
                    return Err(bad("requires a string"));
                }
                if !operators.fields.iter().any(|(name, _)| name == "$regex") {
                    return Err(bad("requires a $regex in the same operator document"));
                }
            }
            "$type" => {
                parse_type_operand(operand).map_err(|error| bad(&error.message))?;
            }
            "$elemMatch" => {
                let BsonValue::Document(specification) = operand
                else {
                    return Err(bad("requires a document"));
                };
                if has_operator(specification) {
                    validate_operator_document(specification, operation, field, field_index)?;
                }
                else {
                    validate_filter(specification, operation)?;
                }
            }
            "$not" => {
                let BsonValue::Document(specification) = operand
                else {
                    return Err(bad("requires an operator document"));
                };
                if !has_operator(specification) || specification.fields.iter().any(|(name, _)| !name.starts_with('$')) {
                    return Err(bad("requires an operator document"));
                }
                validate_operator_document(specification, operation, field, field_index)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn unknown_operator(operator: &str, operation: &str, field: &str, field_index: usize, operator_index: usize) -> CommandError {
    CommandError::new(
        2,
        format!(
            "unknown operator: {operator} (operation: {operation}, field: \"{field}\", fieldIndex: {field_index}, operatorIndex: {operator_index})"
        ),
    )
}

/// Result document for a failed command: `{ ok: 0, errmsg, code, codeName, ... }`.
///
/// `codeName` strings below were read from `mongod 8.0.9`, covering every code
/// the index commands, the index-build duplicate key path and the aggregation
/// stages can emit. Codes outside that set keep the three-field shape rather
/// than guessing a name.
pub fn error_document(error: &CommandError) -> BsonDocument {
    let mut document = BsonDocument::new();
    document.push("ok".to_string(), BsonValue::Double(0.0));
    document.push("errmsg".to_string(), BsonValue::String(error.message.clone()));
    document.push("code".to_string(), BsonValue::Int32(error.code));
    if let Some(name) = code_name(error.code) {
        document.push("codeName".to_string(), BsonValue::String(name.to_string()));
    }
    for (key, value) in &error.details {
        document.push(key.clone(), value.clone());
    }
    document
}

/// Symbolic name `mongod` reports next to a numeric error code.
pub fn code_name(code: i32) -> Option<&'static str> {
    Some(match code {
        2 => "BadValue",
        9 => "FailedToParse",
        14 => "TypeMismatch",
        16 => "InvalidLength",
        13 => "Unauthorized",
        26 => "NamespaceNotFound",
        27 => "IndexNotFound",
        48 => "NamespaceExists",
        43 => "CursorNotFound",
        67 => "CannotCreateIndex",
        72 => "InvalidOptions",
        73 => "InvalidNamespace",
        85 => "IndexOptionsConflict",
        86 => "IndexKeySpecsConflict",
        11000 => "DuplicateKey",
        112 => "WriteConflict",
        117 => "ConflictingOperationInProgress",
        20 => "IllegalOperation",
        121 => "DocumentValidationFailure",
        175 => "QueryPlanKilled",
        251 => "NoSuchTransaction",
        225 => "TransactionTooOld",
        256 => "TransactionCommitted",
        66 => "ImmutableField",
        28 => "PathNotViable",
        16990 => "Location16990",
        17124 => "Location17124",
        40101 => "Location40101",
        40102 => "Location40102",
        40105 => "Location40105",
        40228 => "Location40228",
        40414 => "IDLFailedToParse",
        40415 => "IDLUnknownField",
        51132 => "Location51132",
        51178 => "Location51178",
        51181 => "Location51181",
        51183 => "Location51183",
        51186 => "Location51186",
        51187 => "Location51187",
        40148 => "Location40148",
        40149 => "Location40149",
        5255100 => "Location5255100",
        5255000 => "Location5255000",
        5371602 => "Location5371602",
        7158303 => "Location7158303",
        13113 => "MergeStageNoMatchingDocument",
        16764 => "Location16764",
        31303 => "Location31303",
        28745 => "Location28745",
        28746 => "Location28746",
        28747 => "Location28747",
        28748 => "Location28748",
        28749 => "Location28749",
        40600 => "Location40600",
        40601 => "Location40601",
        51047 => "Location51047",
        16410 => "Location16410",
        17053 => "Location17053",
        17072 => "Location17072",
        17276 => "Location17276",
        5733201 => "Location5733201",
        5733401 => "Location5733401",
        5733402 => "Location5733402",
        5733408 => "Location5733408",
        5733409 => "Location5733409",
        5946802 => "Location5946802",
        605001 => "Location605001",
        6050200 => "Location6050200",
        6050201 => "Location6050201",
        6050202 => "Location6050202",
        6050203 => "Location6050203",
        6050204 => "Location6050204",
        6053600 => "Location6053600",
        6586400 => "Location6586400",
        _ => return None,
    })
}

/// `hello` / `isMaster` handshake answer.
///
/// The static limits are the ones `mongod 8.0.9` reports for wire version 25.
/// Topology fields (`setName`, `hosts`, `topologyVersion`, `connectionId`) belong
/// to the listener, which owns the replica set state and connection counter.
pub fn hello() -> BsonDocument {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|elapsed| elapsed.as_millis() as i64).unwrap_or(0);
    let mut document = BsonDocument::new();
    document.push("isWritablePrimary".to_string(), BsonValue::Bool(true));
    document.push("maxBsonObjectSize".to_string(), BsonValue::Int32(16_777_216));
    document.push("maxMessageSizeBytes".to_string(), BsonValue::Int32(48_000_000));
    document.push("maxWriteBatchSize".to_string(), BsonValue::Int32(100_000));
    document.push("localTime".to_string(), BsonValue::DateTime(now));
    document.push("logicalSessionTimeoutMinutes".to_string(), BsonValue::Int32(30));
    document.push("minWireVersion".to_string(), BsonValue::Int32(0));
    document.push("maxWireVersion".to_string(), BsonValue::Int32(25));
    document.push("readOnly".to_string(), BsonValue::Bool(false));
    document.push("ok".to_string(), BsonValue::Double(1.0));
    document
}

/// `ping` answer.
pub fn ping() -> BsonDocument {
    let mut document = BsonDocument::new();
    document.push("ok".to_string(), BsonValue::Double(1.0));
    document
}

/// Renders an `_id` the way MongoDB's duplicate-key message does.
pub fn describe_id(value: &BsonValue) -> String {
    match value {
        BsonValue::String(text) => format!("\"{text}\""),
        BsonValue::Int32(number) => number.to_string(),
        BsonValue::Int64(number) => number.to_string(),
        BsonValue::Double(number) => {
            if number.is_finite() && number.fract() == 0.0 {
                format!("{number:.1}")
            }
            else {
                number.to_string()
            }
        }
        BsonValue::ObjectId(bytes) => {
            let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            format!("ObjectId('{hex}')")
        }
        other => format!("{other:?}"),
    }
}

/// One `writeErrors` entry. Field order follows `mongod 8.0.9`: `index`, `code`,
/// `errmsg`, then `keyPattern` / `keyValue` for duplicate-key failures.
struct WriteError {
    index: usize,
    code: i32,
    message: String,
    key_pattern: Option<BsonDocument>,
    key_value: Option<BsonDocument>,
    err_info: Option<BsonDocument>,
}

impl WriteError {
    fn plain(index: usize, code: i32, message: impl Into<String>) -> Self {
        Self { index, code, message: message.into(), key_pattern: None, key_value: None, err_info: None }
    }

    fn validation(index: usize, err_info: BsonDocument) -> Self {
        Self {
            index,
            code: 121,
            message: "Document failed validation".to_string(),
            key_pattern: None,
            key_value: None,
            err_info: Some(err_info),
        }
    }

    fn duplicate(index: usize, message: impl Into<String>, key_pattern: BsonDocument, key_value: BsonDocument) -> Self {
        Self { index, code: 11000, message: message.into(), key_pattern: Some(key_pattern), key_value: Some(key_value), err_info: None }
    }
}

/// Largest BSON object `mongod 8.0.9` accepts, as its own error message states
/// (`Size must be between 0 and 16793600(16MB)`).
const MAX_BSON_OBJECT_SIZE: usize = 16_793_600;

/// Rejects one stored document above the BSON ceiling before it reaches the
/// store, with the code and wording `mongod 8.0.9` uses.
fn ensure_document_size(document: &BsonDocument, collection: &str) -> Result<(), CommandError> {
    let size = encode_bson(document).len();
    if size > MAX_BSON_OBJECT_SIZE {
        return Err(CommandError::new(
            10334,
            format!("BSONObj size: {size} (0x{size:X}) is invalid. Size must be between 0 and 16793600(16MB) First element: insert: \"{collection}\""),
        ));
    }
    Ok(())
}

/// `insert`, with `_id` generation, `11000` duplicate mapping, unique-index
/// enforcement, and ordered / unordered continuation.
pub fn insert(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let namespace = namespace_from_command(command, "insert").ok_or_else(|| CommandError::new(73, "insert command missing collection name"))?;
    let session = SessionContext::new(command, store)?;

    let Some(BsonValue::Array(documents)) = command.get("documents")
    else {
        return Err(CommandError::new(2, "insert requires a 'documents' array"));
    };
    if documents.is_empty() || documents.len() > 100_000 {
        return Err(CommandError::new(16, format!("Write batch sizes must be between 1 and 100000. Got {} operations.", documents.len())));
    }
    let collection = namespace.rsplit_once('.').map(|(_, name)| name).unwrap_or(&namespace);
    for item in documents {
        if let BsonValue::Document(document) = item {
            ensure_document_size(document, collection)?;
        }
    }

    let ordered = match command.get("ordered") {
        Some(BsonValue::Bool(ordered)) => *ordered,
        _ => true,
    };

    let index_specs: Vec<IndexSpec> = store.index_specs(&namespace)?;
    let unique_indexes: Vec<IndexSpec> = index_specs.iter().filter(|index| index.unique).cloned().collect();
    let mut candidates: Vec<(usize, BsonDocument, BsonValue)> = Vec::with_capacity(documents.len());
    let mut write_errors: Vec<WriteError> = Vec::new();

    for (index, item) in documents.iter().enumerate() {
        let BsonValue::Document(sub_document) = item
        else {
            write_errors.push(WriteError::plain(index, 2, "documents must be objects"));
            if ordered {
                break;
            }
            continue;
        };
        let mut stored = sub_document.clone();
        if !stored.fields.iter().any(|(field, _)| field == "_id") {
            stored.push("_id".to_string(), BsonValue::ObjectId(generate_object_id()));
        }
        if let Some(error) = parallel_array_error(&stored, &index_specs) {
            write_errors.push(WriteError::plain(index, error.code, error.message));
            if ordered {
                break;
            }
            continue;
        }
        if let Some(info) = insert_validation_failure(store, &namespace, &stored)? {
            write_errors.push(WriteError::validation(index, info));
            if ordered {
                break;
            }
            continue;
        }
        let Some(identifier) = stored.fields.iter().find(|(field, _)| field == "_id").map(|(_, value)| value.clone())
        else {
            continue;
        };
        candidates.push((index, stored, identifier));
    }

    let mut accepted_documents: Vec<BsonDocument> = Vec::new();
    let inserted = if unique_indexes.is_empty() && session.key().is_none() {
        let entries: Vec<(Vec<u8>, Vec<u8>)> =
            candidates.iter().map(|(_, stored, identifier)| (value_to_key(identifier), encode_bson(stored))).collect();
        let positions: Vec<usize> = candidates.iter().map(|(index, _, _)| *index).collect();
        let identifiers: Vec<BsonValue> = candidates.iter().map(|(_, _, identifier)| identifier.clone()).collect();
        let report = store.insert(&namespace, &entries, ordered).map_err(|error| CommandError::new(2, format!("insert failed: {error}")))?;
        for duplicate in &report.duplicates {
            write_errors.push(id_duplicate_error(&namespace, positions[*duplicate], &identifiers[*duplicate]));
        }
        for (position, (_, stored, _)) in candidates.iter().enumerate() {
            if report.duplicates.contains(&position) {
                if ordered {
                    break;
                }
                continue;
            }
            accepted_documents.push(stored.clone());
        }
        if !index_specs.is_empty() {
            let pairs: Vec<(BsonDocument, Vec<u8>)> = accepted_documents
                .iter()
                .filter_map(|document| document.get("_id").map(|identifier| (document.clone(), value_to_key(identifier))))
                .collect();
            write_index_entries(store, session.key(), &pairs, &index_specs)?;
        }
        report.inserted as usize
    }
    else {
        let (count, accepted) = commit_insert(store, session.key(), &namespace, &candidates, &unique_indexes, ordered, &mut write_errors)?;
        accepted_documents.extend(accepted);
        count
    };
    write_errors.sort_by_key(|error| error.index);
    if session.key().is_none() {
        stage_insert_oplog(store, &namespace, &accepted_documents);
    }

    Ok(write_reply(inserted as i64, None, &write_errors))
}

/// Inserts the candidate documents that satisfy every unique index.
///
/// Documents are evaluated in request order. A rejected document never
/// contributes keys, so an unordered batch may still accept a later document
/// that reuses a rejected one's key, exactly like `mongod`.
///
/// 唯一性判定走已经落盘的索引项：`_id` 用点读，唯一索引按元组前缀探针，不再
/// 为了查重扫全表。只有索引项为空（旧目录还没回填）时才退回整表扫描，宁可慢
/// 也不能因为条目缺失放行重复键。
fn commit_insert(
    store: &dyn MongoStorage,
    session: Option<&str>,
    namespace: &str,
    candidates: &[(usize, BsonDocument, BsonValue)],
    unique_indexes: &[IndexSpec],
    ordered: bool,
    write_errors: &mut Vec<WriteError>,
) -> Result<(usize, Vec<BsonDocument>), CommandError> {
    let mut indexed: Vec<BTreeSet<Vec<u8>>> = vec![BTreeSet::new(); unique_indexes.len()];
    let mut persistent: Vec<bool> = Vec::with_capacity(unique_indexes.len());
    for specification in unique_indexes {
        let entries = store
            .reserved_has_entries(&crate::storage::index_namespace(namespace, &specification.name))
            .map_err(|error| CommandError::new(1, format!("index probe failed: {error}")))?;
        persistent.push(entries);
    }
    let fallback: Vec<usize> = persistent.iter().enumerate().filter(|(_, entries)| !**entries).map(|(position, _)| position).collect();
    if !fallback.is_empty() {
        for (_, payload) in namespace_documents(store, namespace, session)? {
            let Ok(document) = parse_bson(&payload)
            else {
                continue;
            };
            for position in &fallback {
                for tuple in index_entries(&document, &unique_indexes[*position]) {
                    indexed[*position].insert(index_tuple(&tuple));
                }
            }
        }
    }

    let mut staged: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut accepted: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut accepted_documents: Vec<BsonDocument> = Vec::new();
    for (index, document, identifier) in candidates {
        let id_key = value_to_key(identifier);
        let taken = staged.contains(&id_key) || namespace_document(store, namespace, &id_key, session)?.is_some();
        if taken {
            write_errors.push(id_duplicate_error(namespace, *index, identifier));
            if ordered {
                break;
            }
            continue;
        }
        let mut conflict: Option<(&IndexSpec, Vec<BsonValue>)> = None;
        'specification: for (position, specification) in unique_indexes.iter().enumerate() {
            for values in index_entries(document, specification) {
                let tuple = index_tuple(&values);
                if indexed[position].contains(&tuple) {
                    conflict = Some((specification, values));
                    break 'specification;
                }
                if persistent[position] {
                    let probe = store
                        .scan_reserved(&crate::storage::index_namespace(namespace, &specification.name), &tuple)
                        .map_err(|error| CommandError::new(1, format!("index probe failed: {error}")))?;
                    if !probe.is_empty() {
                        conflict = Some((specification, values));
                        break 'specification;
                    }
                }
            }
        }
        if let Some((specification, values)) = conflict {
            write_errors.push(duplicate_index_error(*index, namespace, specification, &values));
            if ordered {
                break;
            }
            continue;
        }
        staged.insert(id_key.clone());
        for (position, specification) in unique_indexes.iter().enumerate() {
            for tuple in index_entries(document, specification) {
                indexed[position].insert(index_tuple(&tuple));
            }
        }
        accepted.push((id_key, encode_bson(document)));
        accepted_documents.push(document.clone());
    }

    if !accepted.is_empty() {
        stage_insert(store, session, namespace, &accepted)?;
    }
    Ok((accepted.len(), accepted_documents))
}

/// Stages the oplog entries for documents an insert actually wrote.
///
/// One document is a plain `i` entry; a batch is a single `applyOps` command
/// entry, which is what `mongod 8.0.9` writes for a multi-document insert.
fn stage_insert_oplog(store: &dyn MongoStorage, namespace: &str, documents: &[BsonDocument]) {
    if documents.is_empty() || !store.oplog_enabled() {
        return;
    }
    let uuid = collection_uuid(namespace);
    let entries: Vec<OplogEntry> = documents.iter().map(|document| OplogEntry::insert(namespace, uuid.clone(), document)).collect();
    if entries.len() == 1 {
        if let Some(entry) = entries.into_iter().next() {
            store.stage_oplog(entry);
        }
    }
    else {
        store.stage_oplog(OplogEntry::apply_ops(entries));
    }
}

fn id_duplicate_error(namespace: &str, index: usize, identifier: &BsonValue) -> WriteError {
    let mut key_pattern = BsonDocument::new();
    key_pattern.push("_id".to_string(), BsonValue::Int32(1));
    let mut key_value = BsonDocument::new();
    key_value.push("_id".to_string(), identifier.clone());
    WriteError::duplicate(
        index,
        format!("E11000 duplicate key error collection: {namespace} index: _id_ dup key: {{ _id: {} }}", describe_id(identifier)),
        key_pattern,
        key_value,
    )
}

/// The index values one field contributes, expanding an array one level.
fn field_values(document: &BsonDocument, field: &str) -> Vec<BsonValue> {
    let candidates = collect_field(document, field);
    if candidates.is_empty() {
        // A missing field is indexed as `null`, which is what makes `{a: null}`
        // and an absent `a` collide in a unique index.
        return vec![BsonValue::Null];
    }
    let mut values = Vec::new();
    for candidate in candidates {
        match candidate {
            BsonValue::Array(items) => {
                if items.is_empty() {
                    // `mongod 8.0.9` indexes an empty array as BSON undefined:
                    // `{a: []}` twice collides as `dup key: { a: undefined }`.
                    values.push(BsonValue::Undefined);
                }
                else {
                    // Only one level is expanded, so a nested array stays one
                    // element: `{a: [[1, 2]]}` keys on the array `[1, 2]` itself.
                    values.extend(items.iter().cloned());
                }
            }
            other => values.push(other.clone()),
        }
    }
    values
}

/// Whether one key field is array-valued, which is what makes the index multikey.
fn field_array_valued(document: &BsonDocument, field: &str) -> bool {
    collect_field(document, field).iter().any(|candidate| matches!(candidate, BsonValue::Array(_)))
        || (field.contains('.') && path_traverses_array(document, field))
}

/// Whether a dotted path descends through an array before reaching its leaf.
fn path_traverses_array(document: &BsonDocument, path: &str) -> bool {
    let segments: Vec<&str> = path.split('.').collect();
    match document.get(segments[0]) {
        Some(value) => traverses_array(value, &segments[1..]),
        None => false,
    }
}

fn traverses_array(value: &BsonValue, segments: &[&str]) -> bool {
    if segments.is_empty() {
        return false;
    }
    match value {
        BsonValue::Array(_) => true,
        BsonValue::Document(document) => document.get(segments[0]).map(|next| traverses_array(next, &segments[1..])).unwrap_or(false),
        _ => false,
    }
}

/// Every index key one document contributes, in key order and element order.
///
/// A compound key is the cartesian product of its fields. Duplicate elements
/// inside one document are collapsed, which is why `{a: [3, 3]}` can be stored
/// under a unique index while a second document with `a: 3` cannot.
fn index_entries(document: &BsonDocument, specification: &IndexSpec) -> Vec<Vec<BsonValue>> {
    if specification.is_sparse() && specification.key.fields.iter().all(|(field, _)| collect_field(document, field).is_empty()) {
        // A sparse index only holds documents that carry at least one of the
        // indexed fields. An explicit `null` counts as present.
        return Vec::new();
    }
    if let Some(filter) = &specification.partial_filter_expression
        && !document_matches(document, filter).unwrap_or(false)
    {
        return Vec::new();
    }
    let mut tuples: Vec<Vec<BsonValue>> = vec![Vec::new()];
    for (field, _) in &specification.key.fields {
        let values = field_values(document, field);
        let mut expanded = Vec::with_capacity(tuples.len() * values.len());
        for tuple in &tuples {
            for value in &values {
                let mut next = tuple.clone();
                next.push(value.clone());
                expanded.push(next);
            }
        }
        tuples = expanded;
    }
    if let Some(collation) = &specification.collation {
        let strength = collation.get("strength").and_then(integer_of).unwrap_or(3);
        if strength <= 2 {
            for tuple in &mut tuples {
                for value in tuple.iter_mut() {
                    if let BsonValue::String(text) = value {
                        *text = text.to_lowercase();
                    }
                }
            }
        }
    }
    let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
    tuples.retain(|tuple| seen.insert(index_tuple(tuple)));
    tuples
}

/// Every encoded key one document contributes under one index.
fn tuples_of(document: &BsonDocument, specification: &IndexSpec) -> BTreeSet<Vec<u8>> {
    index_entries(document, specification).into_iter().map(|tuple| index_tuple(&tuple)).collect()
}

/// Code `171` for two array-valued fields under one compound index.
///
/// `mongod 8.0.9` names the two array fields in reverse key order: index
/// `{a: 1, b: 1}` over `{a: [1], b: [2]}` reports `[b] [a]` even when a third
/// field is also an array.
fn parallel_array_error(document: &BsonDocument, indexes: &[IndexSpec]) -> Option<CommandError> {
    for specification in indexes.iter().filter(|index| index.key.fields.len() >= 2) {
        let array_fields: Vec<&str> =
            specification.key.fields.iter().map(|(field, _)| field.as_str()).filter(|field| field_array_valued(document, field)).collect();
        if array_fields.len() >= 2 {
            return Some(CommandError::new(171, format!("cannot index parallel arrays [{}] [{}]", array_fields[1], array_fields[0])));
        }
    }
    None
}

/// Length-prefixed key so two adjacent tuples can never be confused.
fn index_tuple(values: &[BsonValue]) -> Vec<u8> {
    let mut tuple = Vec::new();
    for value in values {
        let key = value_to_key(value);
        tuple.extend_from_slice(&(key.len() as u32).to_be_bytes());
        tuple.extend_from_slice(&key);
    }
    tuple
}

fn duplicate_index_error(index: usize, namespace: &str, specification: &IndexSpec, values: &[BsonValue]) -> WriteError {
    let mut key_value = BsonDocument::new();
    for ((field, _), value) in specification.key.fields.iter().zip(values) {
        key_value.push(field.clone(), value.clone());
    }
    let rendered: Vec<String> =
        specification.key.fields.iter().zip(values).map(|((field, _), value)| format!("{field}: {}", render_shell_value(value))).collect();
    WriteError::duplicate(
        index,
        format!("E11000 duplicate key error collection: {namespace} index: {} dup key: {{ {} }}", specification.name, rendered.join(", ")),
        specification.key.clone(),
        key_value,
    )
}

/// A `hint` resolved against the catalog.
enum Hint {
    /// No hint, or `hint: {}` which `mongod 8.0.9` ignores.
    None,
    /// `{$natural: 1}` or `{$natural: -1}`.
    Natural(i32),
    /// A named or key-pattern hint that matched one visible index.
    Index(IndexSpec),
}

/// One equality, `$in` or range bound the executor actually probes on the leading key field.
enum KeyBound {
    Full,
    Eq(BsonValue),
    In(Vec<BsonValue>),
    /// 首键字段的数值范围；`bool` 表示闭区间，缺省一侧为无界。
    Range {
        lower: Option<(BsonValue, bool)>,
        upper: Option<(BsonValue, bool)>,
    },
}

/// Bounds rendered as `startKey` / `endKey` documents in plan output.
struct CountRange {
    start: BsonValue,
    start_inclusive: bool,
    end: BsonValue,
    end_inclusive: bool,
}

struct FieldPlan {
    field: String,
    bound: KeyBound,
    count: CountRange,
}

struct HintedPlan {
    spec: IndexSpec,
    fields: Vec<FieldPlan>,
    residual: BsonDocument,
    direction: &'static str,
    sort_covered: bool,
    is_multi_key: bool,
}

/// Resolves `hint` exactly as `mongod 8.0.9` does for `find` and `count`.
///
/// A hint against a namespace that does not exist is ignored (the command
/// simply returns no documents), while the same hint against an existing
/// collection fails. A named index must exist and must not be hidden; a key
/// pattern must match one index key document, including field order.
fn resolve_hint(
    command: &BsonDocument,
    store: &dyn MongoStorage,
    namespace: &str,
    filter: &BsonDocument,
    sort: Option<&BsonValue>,
    projection: Option<&BsonValue>,
) -> Result<Hint, CommandError> {
    let Some(value) = command.get("hint")
    else {
        return Ok(Hint::None);
    };
    match value {
        BsonValue::String(_) | BsonValue::Document(_) => {}
        _ => return Err(CommandError::new(9, "Hint must be a string or an object")),
    }
    if !store.namespace_exists(namespace)? {
        return Ok(Hint::None);
    }
    let planner_error = || hint_planner_error(namespace, filter, sort, projection);
    match value {
        BsonValue::String(name) => store
            .index_specs(namespace)?
            .into_iter()
            .find(|spec| spec.is_visible() && spec.name == *name)
            .map(Hint::Index)
            .ok_or_else(planner_error),
        BsonValue::Document(document) => {
            if document.fields.is_empty() {
                return Ok(Hint::None);
            }
            if let Some(direction) = document.get("$natural") {
                if document.fields.len() == 1
                    && let Some(number) = int_value(direction)
                    && (number == 1 || number == -1)
                {
                    return Ok(Hint::Natural(number as i32));
                }
                return Err(CommandError::new(2, "$natural hint cannot be set to a value other than -1 or 1."));
            }
            store
                .index_specs(namespace)?
                .into_iter()
                .find(|spec| spec.is_visible() && spec.key == *document)
                .map(Hint::Index)
                .ok_or_else(planner_error)
        }
        _ => Ok(Hint::None),
    }
}

fn hint_planner_error(namespace: &str, filter: &BsonDocument, sort: Option<&BsonValue>, projection: Option<&BsonValue>) -> CommandError {
    CommandError::new(
        2,
        format!(
            "error processing query: ns={namespace}Tree: {}\nSort: {}\nProj: {}\n planner returned error :: caused by :: hint provided does not correspond to an existing index",
            render_query_tree(filter),
            render_inline_document(sort),
            render_inline_document(projection)
        ),
    )
}

/// Renders the parsed filter the way the planner's `Tree:` line does.
fn render_query_tree(filter: &BsonDocument) -> String {
    let normalized = normalize_query(filter);
    if filter.fields.is_empty() {
        return "$and".to_string();
    }
    match normalized.get("$and") {
        Some(BsonValue::Array(clauses)) => {
            let mut text = "$and".to_string();
            for clause in clauses {
                if let BsonValue::Document(clause) = clause {
                    for line in render_clause_lines(clause) {
                        text.push_str("\n    ");
                        text.push_str(&line);
                    }
                }
            }
            text
        }
        _ => render_clause_lines(&normalized).join("\n    "),
    }
}

fn render_clause_lines(clause: &BsonDocument) -> Vec<String> {
    let mut lines = Vec::new();
    for (field, value) in &clause.fields {
        match value {
            BsonValue::Document(operators) if has_operator(operators) => {
                for (operator, operand) in &operators.fields {
                    if operator == "$options" {
                        continue;
                    }
                    lines.push(format!("{field} {operator} {}", render_tree_value(operand)));
                }
            }
            other => lines.push(format!("{field} $eq {}", render_tree_value(other))),
        }
    }
    lines
}

fn render_tree_value(value: &BsonValue) -> String {
    match value {
        BsonValue::String(text) => format!("{text:?}"),
        BsonValue::Array(items) => {
            format!("[ {}]", items.iter().map(render_tree_value).collect::<Vec<_>>().join(" "))
        }
        BsonValue::Document(document) => render_inline_document(Some(&BsonValue::Document(document.clone()))),
        _ => scalar_text(value),
    }
}

/// `{ a: -1 }` / `{}` rendering used by the `Sort:` and `Proj:` lines.
fn render_inline_document(value: Option<&BsonValue>) -> String {
    match value {
        Some(BsonValue::Document(document)) if !document.fields.is_empty() => format!(
            "{{ {} }}",
            document.fields.iter().map(|(field, value)| format!("{field}: {}", render_tree_value(value))).collect::<Vec<_>>().join(", ")
        ),
        _ => "{}".to_string(),
    }
}

fn scalar_text(value: &BsonValue) -> String {
    match value {
        BsonValue::Int32(number) => number.to_string(),
        BsonValue::Int64(number) => number.to_string(),
        BsonValue::Double(number) => {
            if number.is_nan() {
                "nan.0".to_string()
            }
            else if number.is_infinite() {
                if number.is_sign_positive() { "inf.0".to_string() } else { "-inf.0".to_string() }
            }
            else {
                format!("{number:?}")
            }
        }
        BsonValue::Decimal128(number) => number.as_str(),
        BsonValue::String(text) => format!("{text:?}"),
        BsonValue::Bool(flag) => flag.to_string(),
        BsonValue::Null => "null".to_string(),
        BsonValue::Undefined => "undefined".to_string(),
        BsonValue::MinKey => "MinKey".to_string(),
        BsonValue::MaxKey => "MaxKey".to_string(),
        BsonValue::ObjectId(bytes) => format!("ObjectId('{}')", hex_text(bytes)),
        BsonValue::Binary(binary) => format!("BinData({}, {})", binary.subtype, hex_text(&binary.bytes)),
        BsonValue::DateTime(milliseconds) => format!("new Date({milliseconds})"),
        BsonValue::Timestamp { t, i } => format!("Timestamp({t}, {i})"),
        BsonValue::Symbol(text) => format!("{text:?}"),
        BsonValue::Regex { pattern, options } => format!("/{pattern}/{options}"),
        BsonValue::Code(code) => code.clone(),
        BsonValue::CodeWithScope { code, scope } => {
            format!("CodeWScope( {code}, {} )", render_shell_value(&BsonValue::Document(scope.clone())))
        }
        BsonValue::DbPointer { namespace, id } => {
            format!("DBPointer( \"{namespace}\", ObjectId('{}') )", hex_text(id))
        }
        other => format!("{other:?}"),
    }
}

fn min_key_value() -> BsonValue {
    let mut document = BsonDocument::new();
    document.push("$minKey".to_string(), BsonValue::Int32(1));
    BsonValue::Document(document)
}

fn max_key_value() -> BsonValue {
    let mut document = BsonDocument::new();
    document.push("$maxKey".to_string(), BsonValue::Int32(1));
    BsonValue::Document(document)
}

fn full_count_range() -> CountRange {
    CountRange { start: min_key_value(), start_inclusive: true, end: max_key_value(), end_inclusive: true }
}

fn field_holds_array(documents: &[BsonDocument], field: &str) -> bool {
    documents.iter().any(|document| match document.get(field) {
        Some(BsonValue::Array(items)) => !items.is_empty(),
        _ => false,
    })
}

/// 可执行的首键字段边界。
///
/// 等值、`$in` 与整数边界的范围可以直接切成索引项区间。范围只接受整数边界：
/// 索引项键把整数值编码成定长保序键，非整数（double 位模式、decimal 系数）在
/// 现有编码里不保序，硬切区间会漏掉落在区间外的元素，因此如实退回 `COLLSCAN`。
fn executable_field_plan(field: &str, predicate: Option<&BsonValue>) -> FieldPlan {
    let full = || FieldPlan { field: field.to_string(), bound: KeyBound::Full, count: full_count_range() };
    let equality = |value: &BsonValue| -> Option<FieldPlan> {
        if matches!(value, BsonValue::Regex { .. }) || matches!(value, BsonValue::Document(document) if has_operator(document)) {
            return None;
        }
        Some(FieldPlan {
            field: field.to_string(),
            bound: KeyBound::Eq(value.clone()),
            count: CountRange { start: value.clone(), start_inclusive: true, end: value.clone(), end_inclusive: true },
        })
    };
    let range = |operators: &BsonDocument| -> Option<FieldPlan> {
        let mut lower: Option<(BsonValue, bool)> = None;
        let mut upper: Option<(BsonValue, bool)> = None;
        for (operator, operand) in &operators.fields {
            let (slot, inclusive) = match operator.as_str() {
                "$gt" => (&mut lower, false),
                "$gte" => (&mut lower, true),
                "$lt" => (&mut upper, false),
                "$lte" => (&mut upper, true),
                _ => return None,
            };
            numeric_range_bound(operand)?;
            *slot = Some((operand.clone(), inclusive));
        }
        if lower.is_none() && upper.is_none() {
            return None;
        }
        // 数值范围缺边即 ±无穷，`COUNT_SCAN` 的 `startKey`/`endKey` 如实渲染。
        let count = CountRange {
            start: lower.as_ref().map(|(value, _)| value.clone()).unwrap_or_else(|| BsonValue::Double(f64::NEG_INFINITY)),
            start_inclusive: lower.as_ref().is_none_or(|(_, inclusive)| *inclusive),
            end: upper.as_ref().map(|(value, _)| value.clone()).unwrap_or_else(|| BsonValue::Double(f64::INFINITY)),
            end_inclusive: upper.as_ref().is_none_or(|(_, inclusive)| *inclusive),
        };
        Some(FieldPlan { field: field.to_string(), bound: KeyBound::Range { lower, upper }, count })
    };
    let Some(predicate) = predicate
    else {
        return full();
    };
    match predicate {
        BsonValue::Document(operators) if has_operator(operators) => {
            if operators.fields.len() == 1 {
                if let Some(value) = operators.get("$eq")
                    && let Some(plan) = equality(value)
                {
                    return plan;
                }
                if let Some(BsonValue::Array(values)) = operators.get("$in")
                    && values.iter().all(|value| {
                        !matches!(value, BsonValue::Regex { .. }) && !matches!(value, BsonValue::Document(document) if has_operator(document))
                    })
                {
                    return FieldPlan { field: field.to_string(), bound: KeyBound::In(values.clone()), count: full_count_range() };
                }
            }
            range(operators).unwrap_or_else(full)
        }
        BsonValue::Regex { .. } => full(),
        other => equality(other).unwrap_or_else(full),
    }
}

/// 范围边界里能精确落到整数键上的值；非整数边界不进索引区间。
fn numeric_range_bound(value: &BsonValue) -> Option<i64> {
    match value {
        BsonValue::Int32(number) => Some(*number as i64),
        BsonValue::Int64(number) => Some(*number),
        BsonValue::Double(number) => integral_value(*number),
        BsonValue::Decimal128(number) => number.as_i64(),
        _ => None,
    }
}

/// 构建执行器真正会消费的索引计划。
///
/// 首个键字段按谓词切探针边界；前面字段全是等值时，后续字段的等值谓词继续
/// 收窄同一前缀，遇到范围或非谓词字段即停。边界已完整消费的谓词从 `residual`
/// 移除，`explain` 的 `FETCH.filter` 只报告回表复核真正还要再查的条件。排序与
/// 索引键完全同向或反向时按索引序交付（反向即 `backward` 扫描），否则内存排序。
/// hint 指向部分索引而查询不被 `partialFilterExpression` 蕴含时，索引域与查询
/// 域不相交，按全范围扫描整个部分索引，与 `mongod` 的 hint 行为一致。
fn build_hinted_plan(documents: &[BsonDocument], spec: &IndexSpec, filter: &BsonDocument, sort: &[(String, i32)]) -> HintedPlan {
    let mut fields = Vec::with_capacity(spec.key.fields.len());
    let mut consumed: Vec<String> = Vec::new();
    let partial_outside = spec.partial_filter_expression.as_ref().is_some_and(|partial| !query_implies_partial(filter, partial));
    if partial_outside {
        for (field, _) in &spec.key.fields {
            fields.push(FieldPlan { field: field.clone(), bound: KeyBound::Full, count: full_count_range() });
        }
    }
    else {
        let mut narrowing = true;
        for (position, (field, _)) in spec.key.fields.iter().enumerate() {
            if position == 0 {
                let plan = executable_field_plan(field, filter.get(field));
                if !matches!(plan.bound, KeyBound::Full) {
                    consumed.push(field.clone());
                }
                narrowing = matches!(plan.bound, KeyBound::Eq(_));
                fields.push(plan);
            }
            else if narrowing {
                match equality_bound(filter.get(field)) {
                    Some(value) => {
                        consumed.push(field.clone());
                        fields.push(FieldPlan {
                            field: field.clone(),
                            bound: KeyBound::Eq(value.clone()),
                            count: equality_count_range(&value),
                        });
                    }
                    None => {
                        narrowing = false;
                        fields.push(FieldPlan { field: field.clone(), bound: KeyBound::Full, count: full_count_range() });
                    }
                }
            }
            else {
                fields.push(FieldPlan { field: field.clone(), bound: KeyBound::Full, count: full_count_range() });
            }
        }
    }
    let mut residual = BsonDocument::new();
    for (field, value) in &filter.fields {
        if !field.starts_with('$') && consumed.iter().any(|consumed| consumed == field) {
            continue;
        }
        // `FETCH.filter` 报告解析后的形态：裸等值渲染成 `$eq`。
        let normalized = match value {
            BsonValue::Document(operators) if has_operator(operators) => value.clone(),
            other => {
                let mut equality = BsonDocument::new();
                equality.push("$eq".to_string(), other.clone());
                BsonValue::Document(equality)
            }
        };
        residual.push(field.clone(), normalized);
    }
    let key_matches = |reverse: bool| {
        sort.len() == spec.key.fields.len()
            && sort.iter().zip(spec.key.fields.iter()).all(|((sort_field, sort_direction), (field, key_direction))| {
                sort_field == field
                    && int_value(key_direction).is_some_and(|key| i64::from(*sort_direction) == if reverse { -key } else { key })
            })
    };
    let multi_paths = spec.key.fields.iter().map(|(field, _)| field_holds_array(documents, field)).collect::<Vec<_>>();
    HintedPlan {
        spec: spec.clone(),
        fields,
        residual,
        direction: if key_matches(true) { "backward" } else { "forward" },
        sort_covered: sort.is_empty() || key_matches(false) || key_matches(true),
        is_multi_key: multi_paths.iter().any(|flag| *flag),
    }
}

/// 复合索引前缀可用的等值谓词值；非等值返回 `None`。
fn equality_bound(predicate: Option<&BsonValue>) -> Option<BsonValue> {
    match predicate? {
        BsonValue::Document(document) if has_operator(document) => {
            if document.fields.len() == 1 {
                return document.get("$eq").cloned();
            }
            None
        }
        BsonValue::Regex { .. } => None,
        other => Some(other.clone()),
    }
}

fn equality_count_range(value: &BsonValue) -> CountRange {
    CountRange { start: value.clone(), start_inclusive: true, end: value.clone(), end_inclusive: true }
}

/// `partialFilterExpression` 的每个字段都要求查询里有一条同字段条件，且该
/// 条件落在部分索引域内（等值直接对表达式求值，复杂条件要求逐字相同）；
/// 不满足就不用这个部分索引。
fn query_implies_partial(filter: &BsonDocument, partial: &BsonDocument) -> bool {
    partial.fields.iter().all(|(field, expected)| {
        let Some(actual) = filter.get(field)
        else {
            return false;
        };
        match plain_equality(actual) {
            Some(plain) => {
                let mut probe = BsonDocument::new();
                probe.push(field.clone(), plain.clone());
                let mut clause = BsonDocument::new();
                clause.push(field.clone(), expected.clone());
                document_matches(&probe, &clause).unwrap_or(false)
            }
            None => actual == expected,
        }
    })
}

fn plain_equality(value: &BsonValue) -> Option<&BsonValue> {
    match value {
        BsonValue::Document(operators) if has_operator(operators) => operators.get("$eq"),
        other => Some(other),
    }
}

/// 首个键字段的探针值；全范围扫描返回 `None`。
fn plan_probes(plan: &HintedPlan) -> Option<Vec<BsonValue>> {
    match plan.fields.first().map(|field| &field.bound) {
        Some(KeyBound::Eq(value)) => Some(vec![value.clone()]),
        Some(KeyBound::In(values)) => Some(values.clone()),
        _ => None,
    }
}

/// 一次索引读取的区间：前缀加上排他游标。
struct IndexSpan {
    prefix: Vec<u8>,
    after: Option<Vec<u8>>,
    before: Option<Vec<u8>>,
}

/// 首键字段要读的索引区间；`None` 表示整段读取（无谓词的 hint 全索引扫描）。
///
/// 前面字段全是等值时，探针前缀沿索引键逐个拼接，复合等值一次落到区间上。
fn plan_spans(plan: &HintedPlan) -> Option<Vec<IndexSpan>> {
    match plan.fields.first().map(|field| &field.bound)? {
        KeyBound::Full => None,
        KeyBound::Eq(_) => {
            let prefix: Vec<BsonValue> = plan
                .fields
                .iter()
                .take_while(|field| matches!(field.bound, KeyBound::Eq(_)))
                .filter_map(|field| match &field.bound {
                    KeyBound::Eq(value) => Some(value.clone()),
                    _ => None,
                })
                .collect();
            Some(vec![IndexSpan { prefix: index_tuple(&prefix), after: None, before: None }])
        }
        KeyBound::In(values) => Some(values.iter().map(equality_span).collect()),
        KeyBound::Range { lower, upper } => Some(range_spans(lower.as_ref(), upper.as_ref())),
    }
}

fn equality_span(value: &BsonValue) -> IndexSpan {
    IndexSpan { prefix: index_tuple(std::slice::from_ref(value)), after: None, before: None }
}

/// 整数范围切出定长保序区间，非整数 double / decimal 整段读取后由回表过滤收敛。
///
/// 索引项键是「键长度（4 字节）+ 键字节 + 文档键」，整数键固定 9 字节，
/// 因此整数区间可以按长度前缀定位；double 位模式与 decimal 系数在现有编码里
/// 不保序，硬切会漏数据，只能整段读。
fn range_spans(lower: Option<&(BsonValue, bool)>, upper: Option<&(BsonValue, bool)>) -> Vec<IndexSpan> {
    let mut spans = Vec::new();
    let start = match lower {
        None => Ok(None),
        Some((value, inclusive)) => match numeric_range_bound(value) {
            Some(number) if *inclusive => Ok(Some(number)),
            Some(number) => number.checked_add(1).map(Some).ok_or(()),
            None => Err(()),
        },
    };
    let end = match upper {
        None => Ok(None),
        Some((value, inclusive)) => match numeric_range_bound(value) {
            Some(number) if *inclusive => Ok(Some(number)),
            Some(number) => number.checked_sub(1).map(Some).ok_or(()),
            None => Err(()),
        },
    };
    if let (Ok(start), Ok(end)) = (start, end)
        && (start.is_none() || end.is_none() || start <= end)
    {
        let entry_length = 9u32;
        let region = numeric_entry_prefix(b'n', entry_length as usize);
        let mut span = IndexSpan { prefix: region, after: None, before: None };
        if let Some(start) = start {
            let mut key = entry_length.to_be_bytes().to_vec();
            key.extend_from_slice(&value_to_key(&BsonValue::Int64(start)));
            span.after = Some(key);
        }
        if let Some(end) = end {
            let mut key = entry_length.to_be_bytes().to_vec();
            key.extend_from_slice(&value_to_key(&BsonValue::Int64(end)));
            span.before = prefix_successor(&key);
        }
        spans.push(span);
    }
    spans.push(IndexSpan { prefix: numeric_entry_prefix(b'd', 9), after: None, before: None });
    spans.push(IndexSpan { prefix: numeric_entry_prefix(b'D', 22), after: None, before: None });
    spans
}

/// 单字段索引项的前缀：键长度（4 字节大端）+ 类型字节。
fn numeric_entry_prefix(type_byte: u8, key_len: usize) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(5);
    prefix.extend_from_slice(&(key_len as u32).to_be_bytes());
    prefix.push(type_byte);
    prefix
}

/// 严格大于所有以 `prefix` 开头的键的最小键；全 `0xFF` 时返回 `None`。
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    for position in (0..successor.len()).rev() {
        if successor[position] == u8::MAX {
            successor.truncate(position);
        }
        else {
            successor[position] += 1;
            return Some(successor);
        }
    }
    None
}

/// 这个索引计划能否被当前执行器如实执行。
///
/// 排序规则、稀疏索引的 null 等值、无法被查询蕴含的部分索引都会让索引项与
/// 匹配语义不再一一对应，遇到这些情况一律退回 `COLLSCAN`。显式 `hint` 指向
/// 部分索引时例外：`mongod` 照样扫描整个部分索引，`build_hinted_plan` 已把
/// 边界收敛为全范围。
fn index_executable(plan: &HintedPlan, filter: &BsonDocument, explicit_hint: bool) -> bool {
    let specification = &plan.spec;
    if !specification.is_visible() || specification.collation.is_some() {
        return false;
    }
    // 数组字段上两侧范围可以分别由不同元素满足，单元素区间会漏掉
    // `{a: [3, 12]}` 这类文档。标记为 multikey 后只允许单侧范围。
    if specification.multikey
        && matches!(plan.fields.first().map(|field| &field.bound), Some(KeyBound::Range { lower: Some(_), upper: Some(_) }))
    {
        return false;
    }
    if specification.is_sparse() {
        let Some(values) = plan_probes(plan)
        else {
            return false;
        };
        if values.iter().any(|value| matches!(value, BsonValue::Null | BsonValue::Undefined)) {
            return false;
        }
    }
    if !explicit_hint
        && let Some(partial) = &specification.partial_filter_expression
        && !query_implies_partial(filter, partial)
    {
        return false;
    }
    true
}

/// 索引项是否已经真实落地。
///
/// 空索引可能来自旧目录未重建或中断的删除；此时无法区分「没有匹配」与
/// 「条目缺失」，查询层退回集合扫描，保证结果完整。
fn index_has_entries(store: &dyn MongoStorage, namespace: &str, specification: &IndexSpec) -> Result<bool, CommandError> {
    store
        .reserved_has_entries(&crate::storage::index_namespace(namespace, &specification.name))
        .map_err(|error| CommandError::new(1, format!("index lookup failed: {error}")))
}

/// 没有显式 hint 时按过滤条件挑一个可执行索引：只看首个键字段上的等值/`$in`。
fn automatic_index(store: &dyn MongoStorage, namespace: &str, filter: &BsonDocument) -> Result<Option<IndexSpec>, CommandError> {
    let candidates: Vec<String> = filter.fields.iter().filter(|(field, _)| !field.starts_with('$')).map(|(field, _)| field.clone()).collect();
    if candidates.is_empty() {
        return Ok(None);
    }
    for specification in store.index_specs(namespace)? {
        let Some((first, _)) = specification.key.fields.first()
        else {
            continue;
        };
        if !candidates.iter().any(|field| field == first) {
            continue;
        }
        let plan = build_hinted_plan(&[], &specification, filter, &[]);
        if plan_spans(&plan).is_some() && index_executable(&plan, filter, false) && index_has_entries(store, namespace, &specification)? {
            return Ok(Some(specification));
        }
    }
    Ok(None)
}

fn bound_text(bound: &KeyBound, backward: bool) -> Vec<String> {
    match bound {
        KeyBound::Full => vec![if backward { "[MaxKey, MinKey]".to_string() } else { "[MinKey, MaxKey]".to_string() }],
        KeyBound::Eq(value) => vec![format!("[{}, {}]", scalar_text(value), scalar_text(value))],
        KeyBound::In(values) => values.iter().map(|value| format!("[{}, {}]", scalar_text(value), scalar_text(value))).collect(),
        KeyBound::Range { lower, upper } => {
            let (start_value, start_inclusive) = match lower {
                Some((value, inclusive)) => (scalar_text(value), *inclusive),
                None => ("-inf.0".to_string(), true),
            };
            let (end_value, end_inclusive) = match upper {
                Some((value, inclusive)) => (scalar_text(value), *inclusive),
                None => ("inf.0".to_string(), true),
            };
            let (first, first_inclusive, last, last_inclusive) = if backward {
                (end_value, end_inclusive, start_value, start_inclusive)
            }
            else {
                (start_value, start_inclusive, end_value, end_inclusive)
            };
            vec![format!("{}{}, {}{}", if first_inclusive { "[" } else { "(" }, first, last, if last_inclusive { "]" } else { ")" })]
        }
    }
}

fn plan_key_pattern(plan: &HintedPlan) -> BsonDocument {
    plan.spec.key.clone()
}

fn plan_multi_key_paths(plan: &HintedPlan, documents: &[BsonDocument]) -> BsonDocument {
    let mut paths = BsonDocument::new();
    for (field, _) in &plan.spec.key.fields {
        let mut values = Vec::new();
        if field_holds_array(documents, field) {
            values.push(BsonValue::String(field.clone()));
        }
        paths.push(field.clone(), BsonValue::Array(values));
    }
    paths
}

/// `IXSCAN` / `COUNT_SCAN` shared metadata.
fn scan_stage_document(plan: &HintedPlan, documents: &[BsonDocument], count_scan: bool) -> BsonDocument {
    let mut stage = BsonDocument::new();
    stage.push("stage".to_string(), BsonValue::String(if count_scan { "COUNT_SCAN" } else { "IXSCAN" }.to_string()));
    stage.push("keyPattern".to_string(), BsonValue::Document(plan_key_pattern(plan)));
    stage.push("indexName".to_string(), BsonValue::String(plan.spec.name.clone()));
    stage.push("isMultiKey".to_string(), BsonValue::Bool(plan.is_multi_key));
    stage.push("multiKeyPaths".to_string(), BsonValue::Document(plan_multi_key_paths(plan, documents)));
    stage.push("isUnique".to_string(), BsonValue::Bool(plan.spec.unique));
    stage.push("isSparse".to_string(), BsonValue::Bool(plan.spec.is_sparse()));
    stage.push("isPartial".to_string(), BsonValue::Bool(plan.spec.partial_filter_expression.is_some()));
    stage.push("indexVersion".to_string(), BsonValue::Int32(2));
    if count_scan {
        let mut bounds = BsonDocument::new();
        let mut start = BsonDocument::new();
        let mut end = BsonDocument::new();
        let mut start_inclusive = true;
        let mut end_inclusive = true;
        for field in &plan.fields {
            start.push(field.field.clone(), field.count.start.clone());
            end.push(field.field.clone(), field.count.end.clone());
            start_inclusive &= field.count.start_inclusive;
            end_inclusive &= field.count.end_inclusive;
        }
        bounds.push("startKey".to_string(), BsonValue::Document(start));
        bounds.push("startKeyInclusive".to_string(), BsonValue::Bool(start_inclusive));
        bounds.push("endKey".to_string(), BsonValue::Document(end));
        bounds.push("endKeyInclusive".to_string(), BsonValue::Bool(end_inclusive));
        stage.push("indexBounds".to_string(), BsonValue::Document(bounds));
    }
    else {
        stage.push("direction".to_string(), BsonValue::String(plan.direction.to_string()));
        let mut bounds = BsonDocument::new();
        for field in &plan.fields {
            let backward = plan.direction == "backward";
            bounds.push(field.field.clone(), BsonValue::Array(bound_text(&field.bound, backward).into_iter().map(BsonValue::String).collect()));
        }
        stage.push("indexBounds".to_string(), BsonValue::Document(bounds));
    }
    stage
}

fn add_fetch_wrapper(stage: BsonDocument, filter: &BsonDocument) -> BsonDocument {
    let mut fetch = BsonDocument::new();
    fetch.push("stage".to_string(), BsonValue::String("FETCH".to_string()));
    if !filter.fields.is_empty() {
        fetch.push("filter".to_string(), BsonValue::Document(filter.clone()));
    }
    fetch.push("inputStage".to_string(), BsonValue::Document(stage));
    fetch
}

fn hinted_find_plan(plan: &HintedPlan, documents: &[BsonDocument], skip: usize, limit: usize, sort: &[(String, i32)]) -> BsonDocument {
    let pushed = skip > 0 && plan.residual.fields.is_empty() && (sort.is_empty() || plan.sort_covered);
    let mut scan = scan_stage_document(plan, documents, false);
    if pushed {
        // `skip` 下推到索引层：`SKIP` 夹在 `IXSCAN` 与 `FETCH` 之间，跳过的命中不回表。
        let mut skipped = BsonDocument::new();
        skipped.push("stage".to_string(), BsonValue::String("SKIP".to_string()));
        skipped.push("skipAmount".to_string(), count_value(skip as i64));
        skipped.push("inputStage".to_string(), BsonValue::Document(scan));
        scan = skipped;
    }
    let mut stage = add_fetch_wrapper(scan, &plan.residual);
    if !plan.sort_covered {
        let mut pattern = BsonDocument::new();
        for (field, direction) in sort {
            pattern.push(field.clone(), BsonValue::Int32(*direction));
        }
        let mut sorted = BsonDocument::new();
        sorted.push("stage".to_string(), BsonValue::String("SORT".to_string()));
        sorted.push("sortPattern".to_string(), BsonValue::Document(pattern));
        sorted.push("memLimit".to_string(), BsonValue::Int32(104857600));
        sorted.push("type".to_string(), BsonValue::String("simple".to_string()));
        sorted.push("inputStage".to_string(), BsonValue::Document(stage));
        stage = sorted;
    }
    if skip > 0 && !pushed {
        let mut skipped = BsonDocument::new();
        skipped.push("stage".to_string(), BsonValue::String("SKIP".to_string()));
        skipped.push("skipAmount".to_string(), count_value(skip as i64));
        skipped.push("inputStage".to_string(), BsonValue::Document(stage));
        stage = skipped;
    }
    if limit > 0 {
        let mut limited = BsonDocument::new();
        limited.push("stage".to_string(), BsonValue::String("LIMIT".to_string()));
        limited.push("limitAmount".to_string(), count_value(limit as i64));
        limited.push("inputStage".to_string(), BsonValue::Document(stage));
        stage = limited;
    }
    let mut cached = BsonDocument::new();
    cached.push("isCached".to_string(), BsonValue::Bool(false));
    for (field, value) in stage.fields {
        cached.push(field, value);
    }
    cached
}

fn hinted_count_plan(plan: &HintedPlan, documents: &[BsonDocument]) -> BsonDocument {
    // 边界完整消费查询时计数不回表，`mongod` 用 `COUNT_SCAN` 直接数索引项。
    let stage = if plan.residual.fields.is_empty() {
        scan_stage_document(plan, documents, true)
    }
    else {
        add_fetch_wrapper(scan_stage_document(plan, documents, false), &plan.residual)
    };
    let mut counted = BsonDocument::new();
    counted.push("stage".to_string(), BsonValue::String("COUNT".to_string()));
    counted.push("inputStage".to_string(), BsonValue::Document(stage));
    let mut cached = BsonDocument::new();
    cached.push("isCached".to_string(), BsonValue::Bool(false));
    for (field, value) in counted.fields {
        cached.push(field, value);
    }
    cached
}

fn execution_base(stage: &str, advanced: i64, works: i64, eof: bool) -> BsonDocument {
    let mut document = BsonDocument::new();
    document.push("stage".to_string(), BsonValue::String(stage.to_string()));
    document.push("nReturned".to_string(), count_value(advanced));
    document.push("executionTimeMillisEstimate".to_string(), BsonValue::Int32(0));
    document.push("works".to_string(), count_value(works));
    document.push("advanced".to_string(), count_value(advanced));
    document.push("needTime".to_string(), count_value(if eof { (works - advanced - 1).max(0) } else { 0 }));
    document.push("needYield".to_string(), BsonValue::Int32(0));
    document.push("saveState".to_string(), BsonValue::Int32(0));
    document.push("restoreState".to_string(), BsonValue::Int32(0));
    document.push("isEOF".to_string(), BsonValue::Int32(if eof { 1 } else { 0 }));
    document
}

/// 用执行器真实消费的账目渲染 `IXSCAN`。
///
/// `keys_examined` 与实际扫描的索引项数一致，`advanced` 是交付给上层的命中数
/// （含在索引层被 `skip` 跳过的项）；早停时不计读到区间末尾的那次额外 work。
fn index_scan_stage(plan: &HintedPlan, documents: &[BsonDocument], keys_examined: i64, advanced: i64, exhausted: bool) -> BsonDocument {
    let works = keys_examined + i64::from(exhausted);
    let mut stage = execution_base("IXSCAN", advanced, works, exhausted);
    for (field, value) in scan_stage_document(plan, documents, false).fields.into_iter().skip(1) {
        stage.push(field, value);
    }
    stage.push("keysExamined".to_string(), count_value(keys_examined));
    stage.push("seeks".to_string(), BsonValue::Int32(if keys_examined > 0 { 1 } else { 0 }));
    stage.push("dupsTested".to_string(), BsonValue::Int32(0));
    stage.push("dupsDropped".to_string(), BsonValue::Int32(0));
    stage
}

/// 用执行器真实消费的账目渲染 `COUNT_SCAN`。
///
/// 计数不回表；`keysExamined` 连同收尾探针一并计入，与 `mongod` 数索引项的
/// 账目一致。
fn count_scan_stage(plan: &HintedPlan, documents: &[BsonDocument], keys_examined: i64, advanced: i64, exhausted: bool) -> BsonDocument {
    let works = keys_examined + i64::from(exhausted);
    let mut stage = execution_base("COUNT_SCAN", advanced, works, exhausted);
    for (field, value) in scan_stage_document(plan, documents, true).fields.into_iter().skip(1) {
        stage.push(field, value);
    }
    stage.push("keysExamined".to_string(), count_value(works));
    stage
}

fn reported_find_stats(
    plan: &HintedPlan,
    documents: &[BsonDocument],
    outcome: &ScanOutcome,
    matched: usize,
    skip: usize,
    limit: usize,
    all_plans: bool,
) -> BsonDocument {
    let keys_examined = outcome.keys_examined;
    let documents_examined = outcome.documents_examined;
    let skipped = outcome.skipped_at_index as i64;
    let works = keys_examined + i64::from(outcome.exhausted);
    let mut stage = index_scan_stage(plan, documents, keys_examined, documents_examined + skipped, outcome.exhausted);
    if skipped > 0 {
        // `skip` 下推到索引层：`SKIP` 夹在 `IXSCAN` 与 `FETCH` 之间，跳过的命中不回表。
        let mut skipped_stage = execution_base("SKIP", documents_examined, works, outcome.exhausted);
        skipped_stage.push("skipAmount".to_string(), count_value(skip as i64));
        skipped_stage.push("inputStage".to_string(), BsonValue::Document(stage));
        stage = skipped_stage;
    }
    let mut fetch = execution_base("FETCH", documents_examined, works, outcome.exhausted);
    if !plan.residual.fields.is_empty() {
        fetch.push("filter".to_string(), BsonValue::Document(plan.residual.clone()));
    }
    fetch.push("docsExamined".to_string(), count_value(documents_examined));
    fetch.push("alreadyHasObj".to_string(), BsonValue::Int32(0));
    fetch.push("inputStage".to_string(), BsonValue::Document(stage));
    stage = fetch;
    if !plan.sort_covered {
        let mut sorted = execution_base("SORT", matched as i64, keys_examined + 1, true);
        sorted.push("inputStage".to_string(), BsonValue::Document(stage));
        stage = sorted;
    }
    let after_skip = matched.saturating_sub(skip);
    let returned = if limit > 0 { after_skip.min(limit) } else { after_skip };
    if skip > 0 && skipped == 0 {
        let mut skipped_stage = execution_base("SKIP", after_skip as i64, keys_examined + 1, true);
        skipped_stage.push("skipAmount".to_string(), count_value(skip as i64));
        skipped_stage.push("inputStage".to_string(), BsonValue::Document(stage));
        stage = skipped_stage;
    }
    if limit > 0 {
        let mut limited = execution_base("LIMIT", returned as i64, keys_examined + 1, true);
        limited.push("limitAmount".to_string(), count_value(limit as i64));
        limited.push("inputStage".to_string(), BsonValue::Document(stage));
        stage = limited;
    }
    let mut cached = BsonDocument::new();
    cached.push("isCached".to_string(), BsonValue::Bool(false));
    for (field, value) in stage.fields {
        cached.push(field, value);
    }
    let mut stats = BsonDocument::new();
    stats.push("executionSuccess".to_string(), BsonValue::Bool(true));
    stats.push("nReturned".to_string(), count_value(returned as i64));
    stats.push("executionTimeMillis".to_string(), BsonValue::Int32(0));
    stats.push("totalKeysExamined".to_string(), count_value(keys_examined));
    stats.push("totalDocsExamined".to_string(), count_value(documents_examined));
    stats.push("executionStages".to_string(), BsonValue::Document(cached));
    if all_plans {
        stats.push("allPlansExecution".to_string(), BsonValue::Array(Vec::new()));
    }
    stats
}

fn reported_count_stats(
    plan: &HintedPlan,
    documents: &[BsonDocument],
    outcome: &ScanOutcome,
    matched: usize,
    skip: usize,
    limit: usize,
    all_plans: bool,
) -> BsonDocument {
    let keys_examined = outcome.keys_examined;
    let documents_examined = outcome.documents_examined;
    let advanced = documents_examined + outcome.skipped_at_index as i64;
    let exhausted = outcome.exhausted;
    let index_only = plan.residual.fields.is_empty();
    let input = if index_only {
        count_scan_stage(plan, documents, keys_examined, advanced, exhausted)
    }
    else {
        let works = keys_examined + i64::from(exhausted);
        let stage = index_scan_stage(plan, documents, keys_examined, advanced, exhausted);
        let mut fetch = execution_base("FETCH", advanced, works, exhausted);
        fetch.push("filter".to_string(), BsonValue::Document(plan.residual.clone()));
        fetch.push("docsExamined".to_string(), count_value(documents_examined));
        fetch.push("alreadyHasObj".to_string(), BsonValue::Int32(0));
        fetch.push("inputStage".to_string(), BsonValue::Document(stage));
        fetch
    };
    let skipped_matches = matched.min(skip);
    let available = matched - skipped_matches;
    let counted = if limit > 0 { available.min(limit) } else { available };
    let mut counted_stage = execution_base("COUNT", 0, keys_examined + 1, true);
    counted_stage.push("nCounted".to_string(), count_value(counted as i64));
    counted_stage.push("nSkipped".to_string(), count_value(skipped_matches as i64));
    counted_stage.push("inputStage".to_string(), BsonValue::Document(input));
    let mut cached = BsonDocument::new();
    cached.push("isCached".to_string(), BsonValue::Bool(false));
    for (field, value) in counted_stage.fields {
        cached.push(field, value);
    }
    let mut stats = BsonDocument::new();
    stats.push("executionSuccess".to_string(), BsonValue::Bool(true));
    stats.push("nReturned".to_string(), BsonValue::Int32(0));
    stats.push("executionTimeMillis".to_string(), BsonValue::Int32(0));
    stats.push("totalKeysExamined".to_string(), count_value(keys_examined + i64::from(exhausted && index_only)));
    stats.push("totalDocsExamined".to_string(), count_value(if index_only { 0 } else { documents_examined }));
    stats.push("executionStages".to_string(), BsonValue::Document(cached));
    if all_plans {
        stats.push("allPlansExecution".to_string(), BsonValue::Array(Vec::new()));
    }
    stats
}

/// 执行器真实使用的访问路径。
///
/// `find`、`count`、`explain` 都先调用同一个 [`choose_access`]，因此
/// `explain` 报告的结构就是执行器实际消费的结构，不存在「计划说 IXSCAN、
/// 实际全表扫描」的第二份判断。
enum QueryAccess {
    /// 按编码 `_id` 顺序扫描整个集合，`direction` 为自然序方向。
    Collection { direction: i32 },
    /// 按索引项前缀探针读取候选文档，再回表复核完整过滤条件。
    Index(HintedPlan),
}

/// 一次扫描的真实账目。
struct ScanOutcome {
    documents: Vec<BsonDocument>,
    keys_examined: i64,
    documents_examined: i64,
    /// 扫描是否读到区间末尾；`limit` 早停后为 `false`。
    exhausted: bool,
    /// 在索引层被跳过、没有回表的命中数；只有边界完整消费查询时才可能非零。
    skipped_at_index: usize,
}

/// 边界完整消费查询时，`skip` 可以下推到索引层：跳过的命中不回表。
///
/// 只有结果顺序就是扫描顺序（无排序，或排序由索引序覆盖）时才允许下推，
/// 否则「先跳」与「先排」会给出不同的窗口。
fn pushed_skip(access: &QueryAccess, sort: &[(String, i32)], skip: usize) -> usize {
    match access {
        QueryAccess::Index(plan) if plan.residual.fields.is_empty() && (sort.is_empty() || plan.sort_covered) => skip,
        _ => 0,
    }
}

/// 决定一次查询走集合扫描还是索引探针。
///
/// 事务内的写入只暂存在文档命名空间，索引项在提交前不对外可见，因此事务内
/// 一律走集合扫描，保证读到会话自己的未提交写入。
fn choose_access(
    store: &dyn MongoStorage,
    namespace: &str,
    filter: &BsonDocument,
    hint: &Hint,
    sort: &[(String, i32)],
    documents: &[BsonDocument],
    session: Option<&str>,
) -> Result<QueryAccess, CommandError> {
    if session.is_some() {
        return Ok(QueryAccess::Collection { direction: 1 });
    }
    match hint {
        Hint::Natural(direction) => Ok(QueryAccess::Collection { direction: *direction }),
        Hint::Index(specification) => {
            let plan = build_hinted_plan(documents, specification, filter, sort);
            if index_executable(&plan, filter, true) && index_has_entries(store, namespace, specification)? {
                Ok(QueryAccess::Index(plan))
            }
            else {
                Ok(QueryAccess::Collection { direction: 1 })
            }
        }
        Hint::None => match automatic_index(store, namespace, filter)? {
            Some(specification) => Ok(QueryAccess::Index(build_hinted_plan(documents, &specification, filter, sort))),
            None => Ok(QueryAccess::Collection { direction: 1 }),
        },
    }
}

/// 按访问路径执行一次真实扫描。
///
/// `stop_after` 是命中数边界：达到之后立即停止读取，用于 `limit` 早停。只有
/// 不受排序约束的查询才能给出这个边界，否则「先停」与「先排」会给出不同结果。
/// `skip` 是索引层跳过的命中数（仅在边界完整消费查询时下推），跳过的命中不
/// 回表，账目与 `mongod` 的 `FETCH{SKIP{IXSCAN}}` 一致。反向扫描读完整个区间
/// 再倒序交付，索引项读取数如实计入。
#[allow(clippy::too_many_arguments)]
fn scan_with_access(
    store: &dyn MongoStorage,
    namespace: &str,
    filter: &BsonDocument,
    access: &QueryAccess,
    session: Option<&str>,
    need_total: bool,
    stop_after: Option<usize>,
    skip: usize,
) -> Result<ScanOutcome, CommandError> {
    match access {
        QueryAccess::Collection { .. } => {
            let (documents, documents_examined) = collect_matching_bounded(store, namespace, filter, session, need_total, stop_after)?;
            Ok(ScanOutcome { documents, keys_examined: 0, documents_examined, exhausted: true, skipped_at_index: 0 })
        }
        QueryAccess::Index(plan) => {
            let entry_namespace = crate::storage::index_namespace(namespace, &plan.spec.name);
            let spans = plan_spans(plan).unwrap_or_else(|| vec![IndexSpan { prefix: Vec::new(), after: None, before: None }]);
            let backward = plan.direction == "backward";
            let mut keys_examined = 0i64;
            let mut documents_examined = 0i64;
            let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
            let mut documents = Vec::new();
            let mut skipped_at_index = 0usize;
            let mut exhausted = true;
            let deliver = |document_key: Vec<u8>,
                           store: &dyn MongoStorage,
                           seen: &mut BTreeSet<Vec<u8>>,
                           documents: &mut Vec<BsonDocument>,
                           documents_examined: &mut i64,
                           skipped_at_index: &mut usize|
             -> Result<bool, CommandError> {
                if !seen.insert(document_key.clone()) {
                    return Ok(false);
                }
                if *skipped_at_index < skip {
                    // `skip` 下推到索引层：跳过的命中不回表，与 `mongod` 的 `FETCH{SKIP{IXSCAN}}` 账目一致。
                    *skipped_at_index += 1;
                    return Ok(true);
                }
                // 回表尝试如实计数：悬挂索引项也算一次 `docsExamined`，与 `mongod` 一致。
                *documents_examined += 1;
                if let Some(payload) = namespace_document(store, namespace, &document_key, session)?
                    && let Ok(document) = parse_bson(&payload)
                    && document_matches(&document, filter)?
                {
                    documents.push(document);
                    return Ok(true);
                }
                Ok(false)
            };
            'scan: for span in spans {
                let mut cursor = span.after.clone();
                if backward {
                    // 反向扫描没有按游标倒读的能力：整段读完再倒序交付，索引项读取数如实计入。
                    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                    loop {
                        let batch = store
                            .scan_reserved_range(&entry_namespace, &span.prefix, cursor.as_deref(), span.before.as_deref(), 0)
                            .map_err(|error| CommandError::new(1, format!("index scan failed: {error}")))?;
                        if batch.is_empty() {
                            break;
                        }
                        entries.extend(batch);
                        if let Some(last) = entries.last() {
                            cursor = Some(last.0.clone());
                        }
                    }
                    keys_examined += entries.len() as i64;
                    entries.reverse();
                    for (_entry_key, document_key) in entries {
                        deliver(document_key, store, &mut seen, &mut documents, &mut documents_examined, &mut skipped_at_index)?;
                    }
                    continue 'scan;
                }
                // 区间内部分页：每页只读还差的命中数，避免为了早停把整段前缀取回。
                loop {
                    let page = match stop_after {
                        Some(bound) => (bound - documents.len() - skipped_at_index).max(1),
                        None => 0,
                    };
                    let batch = store
                        .scan_reserved_range(&entry_namespace, &span.prefix, cursor.as_deref(), span.before.as_deref(), page)
                        .map_err(|error| CommandError::new(1, format!("index scan failed: {error}")))?;
                    if batch.is_empty() {
                        break;
                    }
                    let fetched = batch.len();
                    keys_examined += fetched as i64;
                    for (entry_key, document_key) in batch {
                        cursor = Some(entry_key);
                        if deliver(document_key, store, &mut seen, &mut documents, &mut documents_examined, &mut skipped_at_index)?
                            && stop_after.is_some_and(|bound| documents.len() + skipped_at_index >= bound)
                        {
                            exhausted = false;
                            break 'scan;
                        }
                    }
                    if stop_after.is_none() || fetched < page {
                        break;
                    }
                }
            }
            Ok(ScanOutcome { documents, keys_examined, documents_examined, exhausted, skipped_at_index })
        }
    }
}

/// 只读前 `skip + limit` 篇命中即可结束扫描的边界。
///
/// 排序非空时结果顺序由排序决定，不能提前停；`limit` 为 `0` 表示不限量。
fn scan_stop_after(sort: &[(String, i32)], skip: usize, limit: usize) -> Option<usize> {
    if sort.is_empty() && limit > 0 { Some(skip.saturating_add(limit)) } else { None }
}

/// 首段 `$match` 之后紧跟 `$limit`（或 `$skip` + `$limit`）时的扫描边界。
///
/// 只看紧邻的这两段：中间还有别的阶段时，`$limit` 前面的文档数无法从访问路径
/// 直接推出，不能提前停。
fn aggregate_stop_after(stages: &[BsonValue]) -> Option<usize> {
    fn pair(stage: Option<&BsonValue>) -> Option<(&str, &BsonValue)> {
        let BsonValue::Document(document) = stage?
        else {
            return None;
        };
        if document.fields.len() != 1 {
            return None;
        }
        let (name, body) = document.fields.first()?;
        Some((name.as_str(), body))
    }
    let (name, body) = pair(stages.first())?;
    match name {
        "$limit" => positive_stage_integer("$limit", body).ok(),
        "$skip" => {
            let skip = usize::try_from(int_value(body)?).ok()?;
            let (next, next_body) = pair(stages.get(1))?;
            if next == "$limit" { Some(skip.saturating_add(positive_stage_integer("$limit", next_body).ok()?)) } else { None }
        }
        _ => None,
    }
}

/// 值里是否读取了聚合变量（`$$名称`）。
///
/// 只做保守判断：任何以 `$$` 开头的字符串都算引用，宁可不下推也不改语义。
fn references_variables(value: &BsonValue) -> bool {
    match value {
        BsonValue::String(text) => text.starts_with("$$"),
        BsonValue::Array(items) => items.iter().any(references_variables),
        BsonValue::Document(document) => document.fields.iter().any(|(_, value)| references_variables(value)),
        _ => false,
    }
}

/// `find` with filter, sort, skip, limit and cursor batching.
pub fn find(command: &BsonDocument, store: &dyn MongoStorage, cursors: &mut CursorStore) -> Result<BsonDocument, CommandError> {
    let namespace = namespace_from_command(command, "find").ok_or_else(|| CommandError::new(73, "find command missing collection name"))?;
    let session = SessionContext::new(command, store)?;

    let filter = match command.get("filter") {
        Some(BsonValue::Document(filter)) => filter.clone(),
        _ => BsonDocument::new(),
    };
    validate_filter(&filter, "find")?;
    let sort = parse_sort(command.get("sort"));
    let skip = parse_count(command.get("skip"));
    let limit = parse_count(command.get("limit"));
    // `None` keeps the absence of `batchSize`, which means the default 101
    // documents; an explicit `0` opens a cursor with an empty first batch.
    let batch_size = command.get("batchSize").and_then(int_value).map(|count| count.max(0) as usize);
    let projection = match command.get("projection") {
        Some(BsonValue::Document(projection)) => Some(projection.clone()),
        _ => None,
    };
    let hint = resolve_hint(command, store, &namespace, &filter, command.get("sort"), command.get("projection"))?;

    let access = choose_access(store, &namespace, &filter, &hint, &sort, &[], session.key())?;
    // 无排序时结果顺序就是扫描顺序，命中前 `skip + limit` 篇即可停手。
    let stop_after = scan_stop_after(&sort, skip, limit);
    let pushed = pushed_skip(&access, &sort, skip);
    let mut documents = scan_with_access(store, &namespace, &filter, &access, session.key(), false, stop_after, pushed)?.documents;
    if matches!(access, QueryAccess::Collection { direction: -1 }) {
        documents.reverse();
    }
    sort_documents(&mut documents, &sort);

    let start = if pushed > 0 { 0 } else { skip.min(documents.len()) };
    let end = if limit > 0 { (start + limit).min(documents.len()) } else { documents.len() };
    let mut window: Vec<BsonValue> = Vec::new();
    for document in documents.into_iter().skip(start).take(end - start) {
        match &projection {
            Some(projection) => {
                if let Some(projected) = project_stage(&[document], projection)?.into_iter().next() {
                    window.push(BsonValue::Document(projected));
                }
            }
            None => window.push(BsonValue::Document(document)),
        }
    }

    let (cursor_id, first_batch) = cursors.open(&namespace, window, batch_size);
    Ok(cursor_reply(&namespace, "firstBatch", first_batch, cursor_id))
}

/// `count`, honouring `query`, `skip` and `limit`.
pub fn count(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let namespace = namespace_from_command(command, "count").ok_or_else(|| CommandError::new(73, "count command missing collection name"))?;
    let session = SessionContext::new(command, store)?;

    let filter = match command.get("query") {
        Some(BsonValue::Document(filter)) => filter.clone(),
        _ => BsonDocument::new(),
    };
    validate_filter(&filter, "count")?;
    let hint = resolve_hint(command, store, &namespace, &filter, None, None)?;
    let skip = match command.get("skip") {
        Some(value) => match int_value(value) {
            Some(number) if number >= 0 => number as usize,
            Some(number) => {
                return Err(CommandError::new(2, format!("BSON field 'skip' value must be >= 0, actual value '{number}'")));
            }
            None => 0,
        },
        None => 0,
    };
    // A negative `limit` counts backwards from the end of the match set, which
    // is the same window size as its absolute value.
    let limit = match command.get("limit") {
        Some(value) => int_value(value).map(|number| number.unsigned_abs() as usize).unwrap_or(0),
        None => 0,
    };

    let matched = {
        let access = choose_access(store, &namespace, &filter, &hint, &[], &[], session.key())?;
        let stop_after = scan_stop_after(&[], skip, limit);
        let pushed = pushed_skip(&access, &[], skip);
        let outcome = scan_with_access(store, &namespace, &filter, &access, session.key(), false, stop_after, pushed)?;
        outcome.documents.len() + outcome.skipped_at_index
    };
    let start = skip.min(matched);
    let end = if limit > 0 { (start + limit).min(matched) } else { matched };

    Ok(write_reply((end - start) as i64, None, &[]))
}

/// `explain` for `find` and `count`, covering all three verbosity levels.
///
/// Hash fields (`queryHash`, `planCacheShapeHash`, `planCacheKey`,
/// `queryShapeHash`) are derived deterministically from the command because the
/// real planner hashes are internal to `mongod`. Every structural field mirrors
/// the `mongod 8.0.9` reply.
pub fn explain(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let verbosity = match command.get("verbosity") {
        Some(BsonValue::String(verbosity)) => {
            if !["queryPlanner", "executionStats", "allPlansExecution"].contains(&verbosity.as_str()) {
                return Err(CommandError::new(
                    2,
                    format!("Enumeration value '{verbosity}' for field 'explain.verbosity' is not a valid value."),
                ));
            }
            verbosity.clone()
        }
        Some(_) => return Err(CommandError::new(2, "explain verbosity must be a string")),
        None => "executionStats".to_string(),
    };
    let Some(BsonValue::Document(inner)) = command.get("explain")
    else {
        return Err(CommandError::new(2, "explain requires a document"));
    };
    let Some(name) = inner.command_name()
    else {
        return Err(CommandError::new(2, "explain requires a command"));
    };
    if name != "find" && name != "count" {
        return Err(CommandError::new(2, format!("explain is not supported for `{name}`")));
    }

    let database = match command.get("$db") {
        Some(BsonValue::String(database)) => database.clone(),
        _ => "test".to_string(),
    };
    let collection = match inner.get(name) {
        Some(BsonValue::String(collection)) => collection.clone(),
        _ => return Err(CommandError::new(2, format!("{name} requires a collection name"))),
    };
    let namespace = format!("{database}.{collection}");
    let filter = match inner.get(if name == "find" { "filter" } else { "query" }) {
        Some(BsonValue::Document(filter)) => filter.clone(),
        Some(_) => return Err(CommandError::new(2, format!("{name} query must be a document"))),
        None => BsonDocument::new(),
    };
    validate_filter(&filter, "explain")?;
    let hint = resolve_hint(inner, store, &namespace, &filter, inner.get("sort"), inner.get("projection"))?;
    let (skip, limit) = explain_skip_limit(inner)?;
    let normalized = normalize_query(&filter);
    let sort = if name == "find" { parse_sort(inner.get("sort")) } else { Vec::new() };

    let all_documents = collect_matching(store, &namespace, &BsonDocument::new(), None)?;
    let total = all_documents.len();
    // 与 find / count 走同一个访问路径决策函数：explain 不再单独推一份计划。
    let access = choose_access(store, &namespace, &filter, &hint, &sort, &all_documents, None)?;
    // 与 find / count 用同一个早停边界，explain 报的扫描量就是执行器真实读到的量。
    let stop_after = scan_stop_after(&sort, skip, limit);
    let pushed = pushed_skip(&access, &sort, skip);
    let outcome = scan_with_access(store, &namespace, &filter, &access, None, true, stop_after, pushed)?;
    let matched = outcome.documents.len() + outcome.skipped_at_index;
    let counted = if limit > 0 { matched.saturating_sub(skip).min(limit) } else { matched.saturating_sub(skip) };
    let direction = match &access {
        QueryAccess::Collection { direction: -1 } => "backward",
        QueryAccess::Index(plan) => plan.direction,
        _ => "forward",
    };

    let mut query_planner = BsonDocument::new();
    query_planner.push("namespace".to_string(), BsonValue::String(namespace.clone()));
    query_planner.push("parsedQuery".to_string(), BsonValue::Document(normalized.clone()));
    query_planner.push("indexFilterSet".to_string(), BsonValue::Bool(false));
    let hash_seed = format!("{namespace}|{name}|{}", json::document_to_json(inner));
    let query_hash = stable_hash(&hash_seed, 0);
    query_planner.push("queryHash".to_string(), BsonValue::String(query_hash.clone()));
    query_planner.push("planCacheShapeHash".to_string(), BsonValue::String(query_hash.clone()));
    query_planner.push("planCacheKey".to_string(), BsonValue::String(stable_hash(&format!("{hash_seed}|plan"), 1)));
    query_planner.push("optimizationTimeMillis".to_string(), BsonValue::Int32(0));
    query_planner.push("maxIndexedOrSolutionsReached".to_string(), BsonValue::Bool(false));
    query_planner.push("maxIndexedAndSolutionsReached".to_string(), BsonValue::Bool(false));
    query_planner.push("maxScansToExplodeReached".to_string(), BsonValue::Bool(false));
    query_planner.push("prunedSimilarIndexes".to_string(), BsonValue::Bool(false));
    let plan = match (&access, name) {
        (QueryAccess::Index(plan), "find") => hinted_find_plan(plan, &all_documents, skip, limit, &sort),
        (QueryAccess::Index(plan), _) => hinted_count_plan(plan, &all_documents),
        (QueryAccess::Collection { .. }, _) => winning_plan(name, &normalized, skip, limit, direction),
    };
    query_planner.push("winningPlan".to_string(), BsonValue::Document(plan));
    query_planner.push("rejectedPlans".to_string(), BsonValue::Array(Vec::new()));

    let mut reply = BsonDocument::new();
    reply.push("explainVersion".to_string(), BsonValue::String("1".to_string()));
    reply.push("queryPlanner".to_string(), BsonValue::Document(query_planner));
    // `count` explain omits `queryShapeHash`; `find` carries it.
    if name == "find" {
        reply.push("queryShapeHash".to_string(), BsonValue::String(stable_shape_hash(&hash_seed)));
    }
    if verbosity != "queryPlanner" {
        let stats = match (&access, name) {
            (QueryAccess::Index(plan), "find") => {
                reported_find_stats(plan, &all_documents, &outcome, matched, skip, limit, verbosity == "allPlansExecution")
            }
            (QueryAccess::Index(plan), _) => {
                reported_count_stats(plan, &all_documents, &outcome, matched, skip, limit, verbosity == "allPlansExecution")
            }
            (QueryAccess::Collection { .. }, _) => explain_execution_stats(ExecutionStats {
                command: name,
                filter: &normalized,
                total,
                examined: outcome.documents_examined,
                matched,
                skip,
                limit,
                counted,
                all_plans: verbosity == "allPlansExecution",
                direction,
            }),
        };
        reply.push("executionStats".to_string(), BsonValue::Document(stats));
    }
    let mut echoed = inner.clone();
    if echoed.get("$db").is_none() {
        echoed.push("$db".to_string(), BsonValue::String(database));
    }
    reply.push("command".to_string(), BsonValue::Document(echoed));
    reply.push("serverInfo".to_string(), BsonValue::Document(server_info()));
    reply.push("serverParameters".to_string(), BsonValue::Document(server_parameters()));
    // `explain` replies carry `ok` as a double, unlike the plain command replies.
    reply.push("ok".to_string(), BsonValue::Double(1.0));
    Ok(reply)
}

fn explain_skip_limit(command: &BsonDocument) -> Result<(usize, usize), CommandError> {
    let skip = match command.get("skip") {
        Some(value) => match int_value(value) {
            Some(number) if number >= 0 => number as usize,
            Some(number) => {
                return Err(CommandError::new(2, format!("BSON field 'skip' value must be >= 0, actual value '{number}'")));
            }
            None => 0,
        },
        None => 0,
    };
    let limit = match command.get("limit") {
        Some(value) => int_value(value).map(|number| number.unsigned_abs() as usize).unwrap_or(0),
        None => 0,
    };
    Ok((skip, limit))
}

/// The planner reports every equality as `{$eq: value}` and joins multiple
/// fields with `$and` in field-name order.
fn normalize_query(filter: &BsonDocument) -> BsonDocument {
    if filter.fields.is_empty() {
        return BsonDocument::new();
    }
    let mut clauses = Vec::new();
    for (field, expected) in &filter.fields {
        if field.starts_with('$') {
            let mut clause = BsonDocument::new();
            clause.push(field.clone(), expected.clone());
            clauses.push((field.clone(), clause));
            continue;
        }
        let mut clause = BsonDocument::new();
        let normalized = match expected {
            BsonValue::Document(operators) if has_operator(operators) => expected.clone(),
            other => {
                let mut equality = BsonDocument::new();
                equality.push("$eq".to_string(), other.clone());
                BsonValue::Document(equality)
            }
        };
        clause.push(field.clone(), normalized);
        clauses.push((field.clone(), clause));
    }
    if clauses.len() == 1 {
        return clauses.pop().unwrap().1;
    }
    clauses.sort_by(|left, right| left.0.cmp(&right.0));
    let mut combined = BsonDocument::new();
    combined.push("$and".to_string(), BsonValue::Array(clauses.into_iter().map(|(_, clause)| BsonValue::Document(clause)).collect()));
    combined
}

fn winning_plan(command: &str, filter: &BsonDocument, skip: usize, limit: usize, direction: &str) -> BsonDocument {
    let mut scan = BsonDocument::new();
    scan.push("stage".to_string(), BsonValue::String("COLLSCAN".to_string()));
    if !filter.fields.is_empty() {
        scan.push("filter".to_string(), BsonValue::Document(filter.clone()));
    }
    scan.push("direction".to_string(), BsonValue::String(direction.to_string()));
    let mut stage = if command == "count" {
        let mut counted = BsonDocument::new();
        counted.push("stage".to_string(), BsonValue::String("COUNT".to_string()));
        counted.push("inputStage".to_string(), BsonValue::Document(scan));
        counted
    }
    else {
        scan
    };
    if command == "find" {
        if skip > 0 {
            let mut skipped = BsonDocument::new();
            skipped.push("stage".to_string(), BsonValue::String("SKIP".to_string()));
            skipped.push("skipAmount".to_string(), count_value(skip as i64));
            skipped.push("inputStage".to_string(), BsonValue::Document(stage));
            stage = skipped;
        }
        if limit > 0 {
            let mut limited = BsonDocument::new();
            limited.push("stage".to_string(), BsonValue::String("LIMIT".to_string()));
            limited.push("limitAmount".to_string(), count_value(limit as i64));
            limited.push("inputStage".to_string(), BsonValue::Document(stage));
            stage = limited;
        }
    }
    // `isCached` marks only the outermost stage; nested input stages omit it.
    let mut cached = BsonDocument::new();
    cached.push("isCached".to_string(), BsonValue::Bool(false));
    for (field, value) in stage.fields {
        cached.push(field, value);
    }
    cached
}

struct ExecutionStats<'a> {
    command: &'a str,
    filter: &'a BsonDocument,
    total: usize,
    /// 本次扫描实际读到的文档数：`limit` 早停后不再是整个集合。
    examined: i64,
    matched: usize,
    skip: usize,
    limit: usize,
    counted: usize,
    all_plans: bool,
    direction: &'a str,
}

fn explain_execution_stats(request: ExecutionStats<'_>) -> BsonDocument {
    let ExecutionStats { command, filter, total, examined, matched, skip, limit, counted, all_plans, direction } = request;
    let works = (total + 1) as i64;
    let returned = if command == "count" { 0 } else { counted };
    let scan = execution_stage("COLLSCAN", matched as i64, works, examined, filter, direction);
    let stage = if command == "count" {
        let mut counted_stage = BsonDocument::new();
        for (field, value) in execution_stage("COUNT", 0, works, examined, &BsonDocument::new(), direction).fields {
            counted_stage.push(field, value);
        }
        counted_stage.push("nCounted".to_string(), count_value(counted as i64));
        counted_stage.push("nSkipped".to_string(), count_value(skip as i64));
        counted_stage.push("inputStage".to_string(), BsonValue::Document(scan));
        counted_stage
    }
    else {
        let mut stage = scan;
        if skip > 0 {
            let mut skipped = execution_stage("SKIP", returned as i64, works, examined, &BsonDocument::new(), direction);
            skipped.push("skipAmount".to_string(), count_value(skip as i64));
            skipped.push("inputStage".to_string(), BsonValue::Document(stage));
            stage = skipped;
        }
        if limit > 0 {
            let mut limited = BsonDocument::new();
            for (field, value) in execution_stage("LIMIT", returned as i64, works, examined, &BsonDocument::new(), direction).fields {
                limited.push(field, value);
            }
            limited.push("limitAmount".to_string(), count_value(limit as i64));
            limited.push("inputStage".to_string(), BsonValue::Document(stage));
            stage = limited;
        }
        stage
    };
    // `isCached` marks only the outermost execution stage; nested stages omit it.
    let mut cached = BsonDocument::new();
    cached.push("isCached".to_string(), BsonValue::Bool(false));
    for (field, value) in stage.fields {
        cached.push(field, value);
    }
    let stage = cached;
    let mut stats = BsonDocument::new();
    stats.push("executionSuccess".to_string(), BsonValue::Bool(true));
    stats.push("nReturned".to_string(), count_value(returned as i64));
    stats.push("executionTimeMillis".to_string(), BsonValue::Int32(0));
    stats.push("totalKeysExamined".to_string(), BsonValue::Int32(0));
    stats.push("totalDocsExamined".to_string(), count_value(examined));
    stats.push("executionStages".to_string(), BsonValue::Document(stage));
    if all_plans {
        stats.push("allPlansExecution".to_string(), BsonValue::Array(Vec::new()));
    }
    stats
}

fn execution_stage(stage: &str, advanced: i64, works: i64, examined: i64, filter: &BsonDocument, direction: &str) -> BsonDocument {
    let mut document = BsonDocument::new();
    document.push("stage".to_string(), BsonValue::String(stage.to_string()));
    if !filter.fields.is_empty() {
        document.push("filter".to_string(), BsonValue::Document(filter.clone()));
    }
    document.push("nReturned".to_string(), count_value(advanced));
    document.push("executionTimeMillisEstimate".to_string(), BsonValue::Int32(0));
    document.push("works".to_string(), count_value(works));
    document.push("advanced".to_string(), count_value(advanced));
    document.push("needTime".to_string(), count_value(works - advanced - 1));
    document.push("needYield".to_string(), BsonValue::Int32(0));
    document.push("saveState".to_string(), BsonValue::Int32(0));
    document.push("restoreState".to_string(), BsonValue::Int32(0));
    document.push("isEOF".to_string(), BsonValue::Int32(1));
    if stage == "COLLSCAN" {
        document.push("direction".to_string(), BsonValue::String(direction.to_string()));
        document.push("docsExamined".to_string(), count_value(examined));
    }
    document
}

fn server_info() -> BsonDocument {
    let mut info = BsonDocument::new();
    info.push("host".to_string(), BsonValue::String("127.0.0.1".to_string()));
    info.push("port".to_string(), BsonValue::Int32(0));
    info.push("version".to_string(), BsonValue::String("8.0.9".to_string()));
    info.push("gitVersion".to_string(), BsonValue::String("f882ef816d531ecfbb593843e4c554fda90ca416".to_string()));
    info
}

fn server_parameters() -> BsonDocument {
    let mut parameters = BsonDocument::new();
    for name in [
        "internalQueryFacetBufferSizeBytes",
        "internalQueryFacetMaxOutputDocSizeBytes",
        "internalLookupStageIntermediateDocumentMaxSizeBytes",
        "internalDocumentSourceGroupMaxMemoryBytes",
        "internalQueryMaxBlockingSortMemoryUsageBytes",
    ] {
        parameters.push(name.to_string(), BsonValue::Int32(104857600));
    }
    parameters.push("internalQueryProhibitBlockingMergeOnMongoS".to_string(), BsonValue::Int32(0));
    parameters.push("internalQueryMaxAddToSetBytes".to_string(), BsonValue::Int32(104857600));
    parameters.push("internalDocumentSourceSetWindowFieldsMaxMemoryBytes".to_string(), BsonValue::Int32(104857600));
    parameters.push("internalQueryFrameworkControl".to_string(), BsonValue::String("trySbeRestricted".to_string()));
    parameters.push("internalQueryPlannerIgnoreIndexWithCollationForRegex".to_string(), BsonValue::Int32(1));
    parameters
}

fn stable_hash(seed: &str, round: usize) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in seed.as_bytes().iter().chain(std::iter::once(&(round as u8))) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016X}")[..8].to_string()
}

fn stable_shape_hash(seed: &str) -> String {
    (0..4)
        .map(|round| {
            format!("{:016X}", {
                let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
                for byte in seed.as_bytes().iter().chain(std::iter::once(&(round as u8))) {
                    hash ^= u64::from(*byte);
                    hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
                }
                hash
            })
        })
        .collect::<String>()
        .to_uppercase()
}

/// `aggregate` with the documented stages this gateway implements.
///
/// Every other stage answers `59`.
pub fn aggregate(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let namespace =
        namespace_from_command(command, "aggregate").ok_or_else(|| CommandError::new(73, "aggregate command missing collection name"))?;
    let session = SessionContext::new(command, store)?;

    let stages = match command.get("pipeline") {
        Some(BsonValue::Array(stages)) => stages,
        _ => return Err(CommandError::new(2, "aggregate requires a pipeline array")),
    };

    // 首个 `$match` 直接下推到访问路径：能走索引就走索引，后接 `$limit` 时还能
    // 提前停。`let` 绑定的变量只通过 `$$名称` 读取：`$match` 里没有 `$$` 引用时
    // 语义与无 `let` 完全一致，仍然下推；有引用就留在管道里按绑定求值。
    let documents;
    let mut remaining: &[BsonValue] = stages;
    if let Some(BsonValue::Document(stage)) = stages.first()
        && let Some((name, body)) = stage.fields.first()
        && name == "$match"
        && let BsonValue::Document(filter) = body
        && (command.get("let").is_none() || !references_variables(body))
    {
        validate_filter(filter, "aggregate $match")?;
        let stop_after = aggregate_stop_after(&stages[1..]);
        let access = choose_access(store, &namespace, filter, &Hint::None, &[], &[], session.key())?;
        documents = scan_with_access(store, &namespace, filter, &access, session.key(), false, stop_after, 0)?.documents;
        remaining = &stages[1..];
    }
    else {
        documents = collect_matching(store, &namespace, &BsonDocument::new(), session.key())?;
    }
    let documents = run_pipeline(documents, remaining, store, &namespace, session.key(), &empty_variables())?;

    Ok(cursor_reply(&namespace, "firstBatch", documents.into_iter().map(BsonValue::Document).collect(), 0))
}

/// Applies one pipeline to an in-memory document set.
///
/// `$lookup` sub-pipelines and `$facet` branches call this recursively, so the
/// stage implementations never carry their own execution loop.
fn run_pipeline(
    mut documents: Vec<BsonDocument>,
    stages: &[BsonValue],
    store: &dyn MongoStorage,
    namespace: &str,
    session: Option<&str>,
    variables: &ExpressionVariables,
) -> Result<Vec<BsonDocument>, CommandError> {
    for (position, stage) in stages.iter().enumerate() {
        let BsonValue::Document(stage_document) = stage
        else {
            return Err(CommandError::new(2, "aggregate stage must be a document"));
        };
        if stage_document.fields.len() != 1 {
            return Err(CommandError::new(2, "aggregate stage must contain exactly one field"));
        }
        let Some((name, body)) = stage_document.fields.first()
        else {
            return Err(CommandError::new(2, "aggregate stage is empty"));
        };
        if matches!(name.as_str(), "$out" | "$merge") && position + 1 != stages.len() {
            return Err(CommandError::new(40601, format!("{name} can only be the final stage in the pipeline")));
        }
        // Each stage consumes the previous stage's output: the pipeline is applied
        // in order, never collected and matched in one pass.
        match name.as_str() {
            "$match" => {
                let BsonValue::Document(filter) = body
                else {
                    return Err(CommandError::new(2, "$match requires a document"));
                };
                validate_filter(filter, "aggregate $match")?;
                let mut kept = Vec::new();
                for candidate in documents {
                    if document_matches_vars(&candidate, filter, variables)? {
                        kept.push(candidate);
                    }
                }
                documents = kept;
            }
            "$sort" => {
                let BsonValue::Document(specification) = body
                else {
                    return Err(CommandError::new(2, "$sort requires a document"));
                };
                let sort = parse_sort_stage(specification)?;
                sort_documents(&mut documents, &sort);
            }
            "$limit" => {
                let limit = positive_stage_integer("$limit", body)?;
                documents.truncate(limit);
            }
            "$skip" => {
                let skip = positive_stage_integer("$skip", body)?;
                documents = documents.into_iter().skip(skip).collect();
            }
            "$count" => {
                let BsonValue::String(name) = body
                else {
                    return Err(CommandError::new(2, "$count requires a field name string"));
                };
                validate_count_name(name)?;
                // "If the input dataset is empty, $count doesn't return a result."
                if documents.is_empty() {
                    documents.clear();
                }
                else {
                    let mut counted = BsonDocument::new();
                    counted.push(name.clone(), count_value(documents.len() as i64));
                    documents = vec![counted];
                }
            }
            "$unwind" => documents = unwind_stage(&documents, body)?,
            "$lookup" => documents = lookup_stage(documents, body, store, namespace, session, variables)?,
            "$facet" => documents = facet_stage(documents, body, store, namespace, session, variables)?,
            "$bucket" => documents = bucket_stage(&documents, body, variables)?,
            "$bucketAuto" => documents = bucket_auto_stage(&documents, body, variables)?,
            "$setWindowFields" => documents = set_window_fields_stage(&documents, body, variables)?,
            "$sortByCount" => documents = sort_by_count_stage(&documents, body)?,
            "$graphLookup" => documents = graph_lookup_stage(documents, body, store, namespace, session, variables)?,
            "$replaceWith" => documents = replace_with_stage(documents, body, variables)?,
            "$unionWith" => {
                documents = union_with_stage(documents, body, store, namespace, session, variables)?;
            }
            "$sample" => documents = sample_stage(documents, body)?,
            "$densify" => documents = densify_stage(documents, body, variables)?,
            "$fill" => documents = fill_stage(documents, body, variables)?,
            "$redact" => documents = redact_stage(documents, body, variables)?,
            "$documents" => {
                return Err(CommandError::new(73, "'$documents' can only be run with {aggregate: 1}"));
            }
            "$merge" => {
                documents = merge_stage(documents, body, store, namespace, session, variables)?;
            }
            "$out" => {
                documents = out_stage(documents, body, store, namespace, session)?;
            }
            "$set" => {
                let BsonValue::Document(specification) = body
                else {
                    return Err(CommandError::new(2, "$set requires a document"));
                };
                documents = add_fields_stage(&documents, specification)?;
            }
            "$group" => {
                let BsonValue::Document(specification) = body
                else {
                    return Err(CommandError::new(2, "$group requires a document"));
                };
                documents = group_stage(&documents, specification)?;
            }
            "$addFields" => {
                let BsonValue::Document(specification) = body
                else {
                    return Err(CommandError::new(2, "$addFields requires a document"));
                };
                documents = add_fields_stage(&documents, specification)?;
            }
            "$project" => {
                let BsonValue::Document(specification) = body
                else {
                    return Err(CommandError::new(2, "$project requires a document"));
                };
                documents = project_stage(&documents, specification)?;
            }
            other => return Err(unsupported_stage(other)),
        }
    }
    Ok(documents)
}

/// `delete` with `limit` (`0` deletes every match).
pub fn delete(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let namespace = namespace_from_command(command, "delete").ok_or_else(|| CommandError::new(73, "delete command missing collection name"))?;
    let session = SessionContext::new(command, store)?;

    let specifications = match command.get("deletes") {
        Some(BsonValue::Array(deletes)) => deletes.clone(),
        _ => {
            let mut single = BsonDocument::new();
            single.push("q".to_string(), BsonValue::Document(BsonDocument::new()));
            single.push("limit".to_string(), BsonValue::Int32(0));
            vec![BsonValue::Document(single)]
        }
    };

    let mut deleted: i64 = 0;
    let mut deleted_identifiers: Vec<BsonValue> = Vec::new();
    for specification in &specifications {
        let BsonValue::Document(specification) = specification
        else {
            continue;
        };
        let empty = BsonDocument::new();
        let filter = match specification.get("q") {
            Some(BsonValue::Document(filter)) => filter,
            _ => &empty,
        };
        let limit = match specification.get("limit") {
            Some(BsonValue::Int32(limit)) if *limit >= 0 => *limit as usize,
            _ => 0,
        };
        validate_filter(filter, "delete")?;

        if let Some(identifier) = single_id_lookup(filter) {
            let key = value_to_key(identifier);
            let removed = stage_delete(store, session.key(), &namespace, std::slice::from_ref(&key))?;
            deleted += removed as i64;
            if removed > 0 {
                deleted_identifiers.push(identifier.clone());
            }
        }
        else {
            let mut keys = Vec::new();
            let mut identifiers = Vec::new();
            for (key, value) in namespace_documents(store, &namespace, session.key())? {
                if limit > 0 && keys.len() >= limit {
                    break;
                }
                let Ok(candidate) = parse_bson(&value)
                else {
                    continue;
                };
                if document_matches(&candidate, filter)? {
                    if let Some(identifier) = candidate.get("_id") {
                        identifiers.push(identifier.clone());
                    }
                    keys.push(key);
                }
            }
            if !keys.is_empty() {
                deleted += stage_delete(store, session.key(), &namespace, &keys)? as i64;
                deleted_identifiers.extend(identifiers);
            }
        }
    }
    if session.key().is_none() {
        stage_delete_oplog(store, &namespace, &deleted_identifiers);
    }

    Ok(write_reply(deleted, None, &[]))
}

/// Stages one `d` entry per deleted document.
fn stage_delete_oplog(store: &dyn MongoStorage, namespace: &str, identifiers: &[BsonValue]) {
    if identifiers.is_empty() || !store.oplog_enabled() {
        return;
    }
    let uuid = collection_uuid(namespace);
    for identifier in identifiers {
        store.stage_oplog(OplogEntry::delete(namespace, uuid.clone(), identifier));
    }
}

/// Stages one `u` entry per document an update actually changed.
fn stage_update_oplog(store: &dyn MongoStorage, session: Option<&str>, namespace: &str, pairs: &[(BsonDocument, BsonDocument)]) {
    if pairs.is_empty() || session.is_some() || !store.oplog_enabled() {
        return;
    }
    let uuid = collection_uuid(namespace);
    for (old, new) in pairs {
        store.stage_oplog(OplogEntry::update(namespace, uuid.clone(), old, new));
    }
}

/// `update` with the documented update operators, replacement documents and
/// `upsert`; `multi` toggles updateMany.
pub fn update(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let namespace = namespace_from_command(command, "update").ok_or_else(|| CommandError::new(73, "update command missing collection name"))?;
    let session = SessionContext::new(command, store)?;
    let Some(BsonValue::Array(specifications)) = command.get("updates")
    else {
        return Err(CommandError::new(2, "update requires an 'updates' array"));
    };

    let mut matched: i64 = 0;
    let mut modified: i64 = 0;
    let index_specs: Vec<IndexSpec> = store.index_specs(&namespace)?;
    let unique_indexes: Vec<IndexSpec> = index_specs.iter().filter(|index| index.unique).cloned().collect();
    // 唯一索引项全部已落盘且不在事务里时，冲突检查走前缀探针，不再为每条更新命令整表建 map。
    // 条目缺失（旧目录还没回填）或事务内暂存对探针不可见时，退回整表 map，宁可慢也不放行重复键。
    let probe_namespaces: Option<Vec<String>> = if session.key().is_some() {
        None
    } else {
        let mut namespaces = Vec::with_capacity(unique_indexes.len());
        let mut ready = true;
        for specification in &unique_indexes {
            let entries_namespace = crate::storage::index_namespace(&namespace, &specification.name);
            let entries = store
                .reserved_has_entries(&entries_namespace)
                .map_err(|error| CommandError::new(1, format!("index probe failed: {error}")))?;
            if !entries {
                ready = false;
                break;
            }
            namespaces.push(entries_namespace);
        }
        ready.then_some(namespaces)
    };
    let mut indexed: Option<BTreeMap<Vec<u8>, Vec<BTreeSet<Vec<u8>>>>> = None;
    for (index, specification) in specifications.iter().enumerate() {
        let BsonValue::Document(specification) = specification
        else {
            return Err(CommandError::new(2, "update specification must be a document"));
        };
        let empty = BsonDocument::new();
        let filter = match specification.get("q") {
            Some(BsonValue::Document(filter)) => filter,
            _ => &empty,
        };
        validate_filter(filter, "update")?;

        let Some(BsonValue::Document(update_document)) = specification.get("u")
        else {
            return Err(CommandError::new(2, "update specification requires a 'u' document"));
        };
        let operator_update = update_document.fields.iter().any(|(field, _)| field.starts_with('$'));
        let replacement = !operator_update;
        if operator_update {
            if let Some((field, _)) = update_document.fields.iter().find(|(field, _)| !field.starts_with('$')) {
                return Err(CommandError::new(
                    9,
                    format!("Unknown modifier: {field}. Expected a valid update modifier or pipeline-style update specified as an array"),
                ));
            }
            validate_update_document(update_document)?;
        }
        let upsert = matches!(specification.get("upsert"), Some(BsonValue::Bool(true)));
        let multi = matches!(specification.get("multi"), Some(BsonValue::Bool(true)));

        let mut changes = Vec::new();
        let mut oplog_pairs: Vec<(BsonDocument, BsonDocument)> = Vec::new();
        // `_id` 等值过滤最多命中一篇，点读定位，不再整命名空间扫描匹配；探针不可用时退回扫描，
        // 扫描路径需要唯一 map 时惰性建一次，后续规格复用并就地刷新。
        let point_read = probe_namespaces.is_some() && single_id_lookup(filter).is_some();
        let candidates: Vec<(Vec<u8>, Vec<u8>)> = if let Some(identifier) = single_id_lookup(filter).filter(|_| point_read) {
            let key = value_to_key(identifier);
            match namespace_document(store, &namespace, &key, session.key())? {
                Some(payload) => vec![(key, payload)],
                None => Vec::new(),
            }
        }
        else {
            if !unique_indexes.is_empty() && indexed.is_none() {
                let mut map = BTreeMap::new();
                for (key, value) in namespace_documents(store, &namespace, session.key())? {
                    if let Ok(document) = parse_bson(&value) {
                        map.insert(key, unique_indexes.iter().map(|spec| tuples_of(&document, spec)).collect());
                    }
                }
                indexed = Some(map);
            }
            namespace_documents(store, &namespace, session.key())?
        };
        for (key, value) in candidates {
            let Ok(document) = parse_bson(&value)
            else {
                continue;
            };
            if !document_matches(&document, filter)? {
                continue;
            }
            matched += 1;
            let identifier_value = document.get("_id").cloned();
            let identifier = identifier_value.as_ref().map(describe_id).unwrap_or_else(|| "<missing>".to_string());
            let applied = if replacement {
                build_replacement(update_document, identifier_value.as_ref())
            }
            else {
                apply_update(&document, update_document, &identifier, filter, false)
            };
            let updated = match applied {
                Ok(updated) => updated,
                Err(error) => {
                    // The real server keeps the writes already applied, reports a
                    // zeroed counter pair plus `writeErrors`, and aborts the rest
                    // of the command.
                    if !changes.is_empty() {
                        stage_replace(store, session.key(), &namespace, &changes)?;
                    }
                    stage_update_oplog(store, session.key(), &namespace, &oplog_pairs);
                    return Ok(write_reply(0, Some(0), &[WriteError::plain(index, error.code, error.message.clone())]));
                }
            };
            if let Some(info) = update_validation_failure(store, &namespace, &document, &updated)? {
                if !changes.is_empty() {
                    stage_replace(store, session.key(), &namespace, &changes)?;
                }
                stage_update_oplog(store, session.key(), &namespace, &oplog_pairs);
                return Ok(write_reply(0, Some(0), &[WriteError::validation(index, info)]));
            }
            if let Some(error) = parallel_array_error(&updated, &index_specs) {
                // A parallel-array key cannot be indexed at all, so the real
                // server reports a zeroed counter pair plus `writeErrors`.
                if !changes.is_empty() {
                    stage_replace(store, session.key(), &namespace, &changes)?;
                }
                stage_update_oplog(store, session.key(), &namespace, &oplog_pairs);
                return Ok(write_reply(0, Some(0), &[WriteError::plain(index, error.code, error.message)]));
            }
            if !unique_indexes.is_empty() {
                let conflict = match (point_read, &probe_namespaces, &indexed) {
                    (true, Some(probes), _) => unique_conflict_by_probe(store, probes, &unique_indexes, &updated, &key)?,
                    (false, _, Some(map)) => find_unique_conflict(&unique_indexes, &updated, &key, map),
                    _ => None,
                };
                if let Some((specification, values)) = conflict {
                    // Same shape as a failing multi-update: already applied writes
                    // stay, counters report zero, and the command stops here.
                    if !changes.is_empty() {
                        stage_replace(store, session.key(), &namespace, &changes)?;
                    }
                    stage_update_oplog(store, session.key(), &namespace, &oplog_pairs);
                    return Ok(write_reply(0, Some(0), &[duplicate_index_error(index, &namespace, specification, &values)]));
                }
                if let Some(entry) = indexed.as_mut().and_then(|map| map.get_mut(&key)) {
                    *entry = unique_indexes.iter().map(|spec| tuples_of(&updated, spec)).collect();
                }
            }
            let payload = encode_bson(&updated);
            if payload != value {
                modified += 1;
                changes.push((key, payload));
                oplog_pairs.push((document, updated));
            }
            if !multi {
                break;
            }
        }
        if !changes.is_empty() {
            stage_replace(store, session.key(), &namespace, &changes)?;
        }
        stage_update_oplog(store, session.key(), &namespace, &oplog_pairs);
        if matched == 0 && upsert {
            let applied = if replacement {
                let query_identifier = match filter.get("_id") {
                    Some(BsonValue::Document(operators)) if has_operator(operators) => None,
                    Some(value) => Some(value.clone()),
                    None => None,
                };
                build_replacement(update_document, query_identifier.as_ref())
            }
            else {
                let base = upsert_base(filter);
                let identifier = base.get("_id").map(describe_id).unwrap_or_else(|| "<missing>".to_string());
                apply_update(&base, update_document, &identifier, filter, true)
            };
            let mut inserted = match applied {
                Ok(document) => document,
                Err(error) => {
                    return Ok(write_reply(0, Some(0), &[WriteError::plain(index, error.code, error.message)]));
                }
            };
            if inserted.get("_id").is_none() {
                inserted.fields.insert(0, ("_id".to_string(), BsonValue::ObjectId(generate_object_id())));
            }
            if let Some(info) = insert_validation_failure(store, &namespace, &inserted)? {
                return Ok(write_reply(0, Some(0), &[WriteError::validation(index, info)]));
            }
            let Some(identifier) = inserted.get("_id").cloned()
            else {
                return Ok(write_reply(0, Some(0), &[]));
            };
            if let Some(error) = parallel_array_error(&inserted, &index_specs) {
                return Ok(write_reply(0, Some(0), &[WriteError::plain(index, error.code, error.message)]));
            }
            let candidates = vec![(index, inserted, identifier.clone())];
            let mut write_errors = Vec::new();
            let (inserted_count, accepted) =
                commit_insert(store, session.key(), &namespace, &candidates, &unique_indexes, true, &mut write_errors)?;
            if inserted_count == 1 {
                if session.key().is_none() {
                    stage_insert_oplog(store, &namespace, &accepted);
                }
                return Ok(upsert_reply(index, &identifier));
            }
            return Ok(write_reply(0, Some(0), &write_errors));
        }
    }

    Ok(write_reply(matched, Some(modified), &[]))
}

/// Base document for an operator upsert: the equality clauses of the filter,
/// in filter order. Operator clauses and logical clauses are skipped, matching
/// the server's upsert construction.
fn upsert_base(filter: &BsonDocument) -> BsonDocument {
    let mut base = BsonDocument::new();
    for (field, value) in &filter.fields {
        if field.starts_with('$') {
            continue;
        }
        if let BsonValue::Document(operators) = value
            && has_operator(operators)
        {
            continue;
        }
        let segments: Vec<&str> = field.split('.').collect();
        let _ = set_segments(&mut base, &segments, value.clone());
    }
    base
}

/// Replacement document rule: `_id` is kept from the matched or queried
/// document unless the replacement repeats it, and an attempt to change it is
/// rejected with code `66`. Other fields are replaced wholesale.
fn build_replacement(specification: &BsonDocument, identifier: Option<&BsonValue>) -> Result<BsonDocument, CommandError> {
    let replacement_identifier = specification.get("_id");
    if let (Some(current), Some(replacement)) = (identifier, replacement_identifier)
        && !values_equal(Some(current), replacement)
    {
        return Err(CommandError::new(
            66,
            format!(
                "After applying the update, the (immutable) field '_id' was found to have been altered to _id: {}",
                describe_id(replacement)
            ),
        ));
    }
    let chosen = identifier.cloned().or_else(|| replacement_identifier.cloned());
    let mut result = BsonDocument::new();
    if let Some(identifier) = chosen {
        result.push("_id".to_string(), identifier);
    }
    for (field, value) in &specification.fields {
        if field != "_id" {
            result.push(field.clone(), value.clone());
        }
    }
    Ok(result)
}

/// Reply for an upsert that inserted one document: `n`, `upserted` and a zero
/// `nModified`, in the order the server emits them.
fn upsert_reply(index: usize, identifier: &BsonValue) -> BsonDocument {
    let mut response = BsonDocument::new();
    response.push("n".to_string(), count_value(1));
    let mut entry = BsonDocument::new();
    entry.push("index".to_string(), BsonValue::Int32(index as i32));
    entry.push("_id".to_string(), identifier.clone());
    response.push("upserted".to_string(), BsonValue::Array(vec![BsonValue::Document(entry)]));
    response.push("nModified".to_string(), count_value(0));
    response.push("ok".to_string(), BsonValue::Double(1.0));
    response
}

/// Finds the first unique index key of `document` already held by a different
/// stored document.
fn find_unique_conflict<'a>(
    indexes: &'a [IndexSpec],
    document: &BsonDocument,
    key: &[u8],
    current: &BTreeMap<Vec<u8>, Vec<BTreeSet<Vec<u8>>>>,
) -> Option<(&'a IndexSpec, Vec<BsonValue>)> {
    for (position, specification) in indexes.iter().enumerate() {
        for values in index_entries(document, specification) {
            let tuple = index_tuple(&values);
            for (other, sets) in current {
                if other.as_slice() != key && sets.get(position).is_some_and(|set| set.contains(&tuple)) {
                    return Some((specification, values));
                }
            }
        }
    }
    None
}

/// 按已落盘索引项探针检查 `document` 更新后的唯一键是否与**其它**文档冲突。
///
/// 每个唯一索引项的键是 `index_tuple(values) + 文档键`，条目按索引命名空间隔离，
/// 定长元组自定界，所以前缀 `tuple` 命中的条目尾部就是持有该元组的文档键；只要存在
/// 一条不等于本篇自身条目，即为冲突。探针只读，不整表扫描。
fn unique_conflict_by_probe<'a>(
    store: &dyn MongoStorage,
    namespaces: &[String],
    indexes: &'a [IndexSpec],
    document: &BsonDocument,
    key: &[u8],
) -> Result<Option<(&'a IndexSpec, Vec<BsonValue>)>, CommandError> {
    for (specification, entries_namespace) in indexes.iter().zip(namespaces) {
        for values in index_entries(document, specification) {
            let tuple = index_tuple(&values);
            let own = index_entry_key(&values, key);
            let probe = store
                .scan_reserved(&entries_namespace, &tuple)
                .map_err(|error| CommandError::new(1, format!("index probe failed: {error}")))?;
            if probe.iter().any(|(entry_key, _)| entry_key.as_slice() != own.as_slice()) {
                return Ok(Some((specification, values)));
            }
        }
    }
    Ok(None)
}

/// Reply shape shared by `insert` / `update` / `delete` / `count`, matching
/// `mongod 8.0.9`: `ok` is a double and every counter is int32.
///
/// The field order is also the real server's: `n`, `writeErrors`, `nModified`,
/// `ok`. A successful update therefore stays `n`, `nModified`, `ok`.
fn write_reply(count: i64, modified: Option<i64>, errors: &[WriteError]) -> BsonDocument {
    let mut response = BsonDocument::new();
    response.push("n".to_string(), count_value(count));
    if !errors.is_empty() {
        let entries: Vec<BsonValue> = errors
            .iter()
            .map(|error| {
                let mut entry = BsonDocument::new();
                entry.push("index".to_string(), BsonValue::Int32(error.index as i32));
                entry.push("code".to_string(), BsonValue::Int32(error.code));
                entry.push("errmsg".to_string(), BsonValue::String(error.message.clone()));
                if let Some(pattern) = &error.key_pattern {
                    entry.push("keyPattern".to_string(), BsonValue::Document(pattern.clone()));
                }
                if let Some(value) = &error.key_value {
                    entry.push("keyValue".to_string(), BsonValue::Document(value.clone()));
                }
                if let Some(info) = &error.err_info {
                    entry.push("errInfo".to_string(), BsonValue::Document(info.clone()));
                }
                BsonValue::Document(entry)
            })
            .collect();
        response.push("writeErrors".to_string(), BsonValue::Array(entries));
    }
    if let Some(modified) = modified {
        response.push("nModified".to_string(), count_value(modified));
    }
    response.push("ok".to_string(), BsonValue::Double(1.0));
    response
}

fn validate_update_document(document: &BsonDocument) -> Result<(), CommandError> {
    if document.fields.is_empty() {
        return Err(CommandError::new(2, "update specification is empty"));
    }
    let mut paths: Vec<&str> = Vec::new();
    for (operator_index, (operator, operand)) in document.fields.iter().enumerate() {
        if !operator.starts_with('$') {
            return Err(CommandError::new(59, "replacement updates are not supported; `u` must use an update operator"));
        }
        if !UPDATE_OPERATORS.contains(&operator.as_str()) {
            return Err(CommandError::new(9, format!("Unknown modifier: {operator} (operatorIndex: {operator_index})")));
        }
        let BsonValue::Document(operand) = operand
        else {
            return Err(CommandError::new(9, format!("update operator {operator} requires a document")));
        };
        for (path, value) in &operand.fields {
            if path == "_id" || path.starts_with("_id.") {
                return Err(CommandError::new(66, format!("Performing an update on the path '{path}' would modify the immutable field '_id'")));
            }
            if paths.contains(&path.as_str()) {
                return Err(CommandError::new(40, format!("Updating the path '{path}' would create a conflict at '{path}'")));
            }
            paths.push(path.as_str());
            if operator == "$rename" {
                let BsonValue::String(target) = value
                else {
                    return Err(CommandError::new(2, format!("The 'to' field for $rename must be a string: {path}")));
                };
                if target == "_id" || target.starts_with("_id.") {
                    return Err(CommandError::new(
                        66,
                        format!("Performing an update on the path '{target}' would modify the immutable field '_id'"),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn apply_update(
    document: &BsonDocument,
    specification: &BsonDocument,
    identifier: &str,
    filter: &BsonDocument,
    on_insert: bool,
) -> Result<BsonDocument, CommandError> {
    let mut updated = document.clone();
    for (operator, operand) in &specification.fields {
        let BsonValue::Document(operand) = operand
        else {
            continue;
        };
        if matches!(operator.as_str(), "$set" | "$inc" | "$mul" | "$min" | "$max") {
            for path in operand.fields.iter().map(|(path, _)| path) {
                let segments = resolve_segments(path, document, filter)?;
                let path_refs: Vec<&str> = segments.iter().map(String::as_str).collect();
                validate_negative_index(document, &path_refs)?;
            }
        }
        for (path, value) in &operand.fields {
            let segments = resolve_segments(path, &updated, filter)?;
            let path_refs: Vec<&str> = segments.iter().map(String::as_str).collect();
            match operator.as_str() {
                "$set" => set_segments(&mut updated, &path_refs, value.clone())?,
                "$unset" => unset_segments(&mut updated, &path_refs),
                "$inc" => numeric_segments(&mut updated, &path_refs, value, path, identifier, false)?,
                "$mul" => numeric_segments(&mut updated, &path_refs, value, path, identifier, true)?,
                // `$min` / `$max` keep the smaller / larger value under the BSON
                // ordering, and write the operand when the field is missing.
                "$min" => keep_extreme(&mut updated, &path_refs, value, true)?,
                "$max" => keep_extreme(&mut updated, &path_refs, value, false)?,
                // `$setOnInsert` is a no-op on a matched document and applies only
                // while constructing the document an upsert inserts.
                "$setOnInsert" => {
                    if on_insert {
                        set_segments(&mut updated, &path_refs, value.clone())?;
                    }
                }
                "$rename" => rename_path(&mut updated, &path_refs, value, path)?,
                "$push" => push_path(&mut updated, &path_refs, value, path, identifier)?,
                "$pop" => pop_path(&mut updated, &path_refs, value, path)?,
                "$addToSet" => add_to_set_path(&mut updated, &path_refs, value, path, identifier)?,
                "$pull" => pull_path(&mut updated, &path_refs, value, path)?,
                _ => {}
            }
        }
    }
    Ok(updated)
}

/// Resolves the positional `$` segment against the document and filter that
/// produced the match. Only the first matching array element is supported.
fn resolve_segments(path: &str, document: &BsonDocument, filter: &BsonDocument) -> Result<Vec<String>, CommandError> {
    let segments: Vec<String> = path.split('.').map(str::to_string).collect();
    if !segments.iter().any(|segment| segment == "$") {
        return Ok(segments);
    }
    let mut resolved = Vec::with_capacity(segments.len());
    for (position, segment) in segments.iter().enumerate() {
        if segment == "$" {
            let index = positional_index(document, &resolved, filter);
            resolved.push(index.to_string());
        }
        else {
            resolved.push(segment.clone());
        }
        let _ = position;
    }
    Ok(resolved)
}

/// Index of the first array element selected by the filter clause for the array
/// that precedes the positional operator.
fn positional_index(document: &BsonDocument, prefix: &[String], filter: &BsonDocument) -> usize {
    let prefix_refs: Vec<&str> = prefix.iter().map(String::as_str).collect();
    let Some(BsonValue::Array(items)) = value_at(document, &prefix_refs).cloned()
    else {
        return 0;
    };
    let key = prefix.join(".");
    if let Some(predicate) = filter.get(&key) {
        if let BsonValue::Document(operators) = predicate
            && has_operator(operators)
        {
            if let Some(BsonValue::Document(specification)) = operators.get("$elemMatch") {
                for (index, item) in items.iter().enumerate() {
                    if let BsonValue::Document(element) = item
                        && document_matches(element, specification).unwrap_or(false)
                    {
                        return index;
                    }
                }
            }
            else {
                for (index, item) in items.iter().enumerate() {
                    if operator_matches(&[item], operators, &key, 0).unwrap_or(false) {
                        return index;
                    }
                }
            }
            return 0;
        }
        for (index, item) in items.iter().enumerate() {
            if values_equal(Some(item), predicate) {
                return index;
            }
        }
    }
    let mut clauses = BsonDocument::new();
    let mut has_clause = false;
    for (field, expected) in &filter.fields {
        if let Some(rest) = field.strip_prefix(&format!("{key}.")) {
            clauses.push(rest.to_string(), expected.clone());
            has_clause = true;
        }
    }
    if has_clause {
        for (index, item) in items.iter().enumerate() {
            if let BsonValue::Document(element) = item
                && document_matches(element, &clauses).unwrap_or(false)
            {
                return index;
            }
        }
    }
    0
}

fn validate_negative_index(document: &BsonDocument, segments: &[&str]) -> Result<(), CommandError> {
    for position in 1..segments.len() {
        let Some(BsonValue::Array(items)) = value_at(document, &segments[..position])
        else {
            continue;
        };
        let Ok(number) = segments[position].parse::<i64>()
        else {
            continue;
        };
        if number < 0 {
            return Err(cannot_create_field_in_array(items, segments[position - 1], segments[position]));
        }
    }
    Ok(())
}

/// Every value reachable by `segments`, traversing arrays field-wise so
/// `a.b` over `[{b: 1}, {b: 2}]` yields both values.
fn collect_values_at<'a>(document: &'a BsonDocument, segments: &[&str], out: &mut Vec<&'a BsonValue>) {
    let Some((head, rest)) = segments.split_first()
    else {
        return;
    };
    if let Some(value) = document.get(head) {
        collect_descend_values(value, rest, out);
    }
}

fn collect_descend_values<'a>(value: &'a BsonValue, segments: &[&str], out: &mut Vec<&'a BsonValue>) {
    let Some((head, rest)) = segments.split_first()
    else {
        out.push(value);
        return;
    };
    match value {
        BsonValue::Document(document) => {
            if let Some(next) = document.get(head) {
                collect_descend_values(next, rest, out);
            }
        }
        BsonValue::Array(items) => {
            if let Ok(index) = head.parse::<usize>() {
                if let Some(next) = items.get(index) {
                    collect_descend_values(next, rest, out);
                }
            }
            else {
                for item in items {
                    collect_descend_values(item, segments, out);
                }
            }
        }
        _ => {}
    }
}

fn value_at<'a>(document: &'a BsonDocument, segments: &[&str]) -> Option<&'a BsonValue> {
    let head = segments.first()?;
    let value = document.get(head)?;
    if segments.len() == 1 {
        return Some(value);
    }
    descend_value(value, &segments[1..])
}

fn descend_value<'a>(value: &'a BsonValue, segments: &[&str]) -> Option<&'a BsonValue> {
    if segments.is_empty() {
        return Some(value);
    }
    match value {
        BsonValue::Document(document) => document.get(segments[0]).and_then(|next| descend_value(next, &segments[1..])),
        BsonValue::Array(items) => {
            let index = segments[0].parse::<usize>().ok()?;
            items.get(index).and_then(|next| descend_value(next, &segments[1..]))
        }
        _ => None,
    }
}

fn value_at_mut<'a>(document: &'a mut BsonDocument, segments: &[&str]) -> Option<&'a mut BsonValue> {
    let head = *segments.first()?;
    let position = document.fields.iter().position(|(field, _)| field == head)?;
    if segments.len() == 1 {
        return Some(&mut document.fields[position].1);
    }
    descend_value_mut(&mut document.fields[position].1, &segments[1..])
}

fn descend_value_mut<'a>(value: &'a mut BsonValue, segments: &[&str]) -> Option<&'a mut BsonValue> {
    if segments.is_empty() {
        return Some(value);
    }
    match value {
        BsonValue::Document(document) => {
            let position = document.fields.iter().position(|(field, _)| field == segments[0])?;
            descend_value_mut(&mut document.fields[position].1, &segments[1..])
        }
        BsonValue::Array(items) => {
            let index = segments[0].parse::<usize>().ok()?;
            let next = items.get_mut(index)?;
            descend_value_mut(next, &segments[1..])
        }
        _ => None,
    }
}

/// `$min` / `$max`: keep the operand only when it is smaller / larger than the
/// current value under the BSON ordering. A missing field always takes the
/// operand, matching `mongod` where `{ $min: { a: 1 } }` creates `a: 1`.
fn keep_extreme(document: &mut BsonDocument, segments: &[&str], value: &BsonValue, minimum: bool) -> Result<(), CommandError> {
    let replace = match value_at(document, segments) {
        None => true,
        Some(current) => {
            let ordering = compare_values(value, current);
            if minimum { ordering == Ordering::Less } else { ordering == Ordering::Greater }
        }
    };
    if replace {
        set_segments(document, segments, value.clone())?;
    }
    Ok(())
}

fn set_segments(document: &mut BsonDocument, segments: &[&str], value: BsonValue) -> Result<(), CommandError> {
    let head = segments[0];
    if segments.len() == 1 {
        if let Some(existing) = document.fields.iter_mut().find(|(field, _)| field == head) {
            existing.1 = value;
        }
        else {
            document.push(head.to_string(), value);
        }
        return Ok(());
    }
    let tail = &segments[1..];
    if !document.fields.iter().any(|(field, _)| field == head) {
        let mut child = BsonDocument::new();
        set_segments(&mut child, tail, value)?;
        document.push(head.to_string(), BsonValue::Document(child));
        return Ok(());
    }
    let position = document.fields.iter().position(|(field, _)| field == head).expect("head exists");
    match &mut document.fields[position].1 {
        BsonValue::Document(child) => set_segments(child, tail, value),
        BsonValue::Array(items) => set_array_segments(items, head, tail, value),
        other => {
            let rendered = render_shell_value(other);
            Err(CommandError::new(28, format!("Cannot create field '{}' in element {{{head}: {rendered}}}", tail[0])))
        }
    }
}

/// `$addFields` / `$set`: like `set_segments`, but a scalar along the path is
/// replaced by an embedded document (aggregation semantics), while arrays keep
/// the update-style error.
fn set_aggregation_segments(document: &mut BsonDocument, segments: &[&str], value: BsonValue) -> Result<(), CommandError> {
    let head = segments[0];
    if segments.len() == 1 {
        if let Some(existing) = document.fields.iter_mut().find(|(field, _)| field == head) {
            existing.1 = value;
        }
        else {
            document.push(head.to_string(), value);
        }
        return Ok(());
    }
    let tail = &segments[1..];
    let Some(position) = document.fields.iter().position(|(field, _)| field == head)
    else {
        let mut child = BsonDocument::new();
        set_aggregation_segments(&mut child, tail, value)?;
        document.push(head.to_string(), BsonValue::Document(child));
        return Ok(());
    };
    if matches!(&document.fields[position].1, BsonValue::Array(_)) {
        let rendered = render_shell_value(&document.fields[position].1);
        return Err(CommandError::new(28, format!("Cannot create field '{}' in element {{{head}: {rendered}}}", tail[0])));
    }
    match &mut document.fields[position].1 {
        BsonValue::Document(child) => set_aggregation_segments(child, tail, value),
        _ => {
            let mut child = BsonDocument::new();
            set_aggregation_segments(&mut child, tail, value)?;
            document.fields[position].1 = BsonValue::Document(child);
            Ok(())
        }
    }
}

fn set_array_segments(items: &mut Vec<BsonValue>, field: &str, segments: &[&str], value: BsonValue) -> Result<(), CommandError> {
    let part = segments[0];
    let Ok(number) = part.parse::<i64>()
    else {
        return Err(cannot_create_field_in_array(items, field, part));
    };
    if number < 0 {
        return Err(cannot_create_field_in_array(items, field, part));
    }
    let index = number as usize;
    if index >= items.len() {
        items.resize(index + 1, BsonValue::Null);
    }
    if segments.len() == 1 {
        items[index] = value;
        return Ok(());
    }
    if matches!(items[index], BsonValue::Null) {
        items[index] = BsonValue::Document(BsonDocument::new());
    }
    match &mut items[index] {
        BsonValue::Document(child) => set_segments(child, &segments[1..], value),
        BsonValue::Array(inner) => set_array_segments(inner, part, &segments[1..], value),
        other => {
            let rendered = render_shell_value(other);
            Err(CommandError::new(28, format!("Cannot create field '{}' in element {rendered}", segments[1])))
        }
    }
}

fn cannot_create_field_in_array(items: &[BsonValue], field: &str, child: &str) -> CommandError {
    let rendered = render_shell_value(&BsonValue::Array(items.to_vec()));
    CommandError::new(28, format!("Cannot create field '{child}' in element {{{field}: {rendered}}}"))
}

fn unset_segments(document: &mut BsonDocument, segments: &[&str]) {
    let head = segments[0];
    if segments.len() == 1 {
        document.fields.retain(|(field, _)| field != head);
        return;
    }
    let position = match document.fields.iter().position(|(field, _)| field == head) {
        Some(position) => position,
        None => return,
    };
    match &mut document.fields[position].1 {
        BsonValue::Document(child) => unset_segments(child, &segments[1..]),
        BsonValue::Array(items) => unset_array_segments(items, &segments[1..]),
        _ => {}
    }
}

fn unset_array_segments(items: &mut [BsonValue], segments: &[&str]) {
    let Ok(index) = segments[0].parse::<usize>()
    else {
        return;
    };
    if index >= items.len() {
        return;
    }
    if segments.len() == 1 {
        items[index] = BsonValue::Null;
        return;
    }
    match &mut items[index] {
        BsonValue::Document(child) => unset_segments(child, &segments[1..]),
        BsonValue::Array(inner) => unset_array_segments(inner, &segments[1..]),
        _ => {}
    }
}

fn numeric_segments(
    document: &mut BsonDocument,
    segments: &[&str],
    amount: &BsonValue,
    path: &str,
    identifier: &str,
    multiply: bool,
) -> Result<(), CommandError> {
    let current = value_at(document, segments).cloned();
    let result = match current {
        Some(value) => {
            if multiply {
                multiply_value(&value, amount).ok_or_else(|| {
                    CommandError::new(
                        14,
                        format!(
                            "Cannot apply $mul to a value of non-numeric type. {{_id: {identifier}}} has the field '{path}' of non-numeric type {}",
                            bson_type_name(&value)
                        ),
                    )
                })?
            }
            else {
                increment_value(&value, amount).ok_or_else(|| {
                    CommandError::new(14, format!("Cannot apply $inc to a value of non-numeric type (field: \"{path}\", _id: {identifier})"))
                })?
            }
        }
        None => {
            if multiply {
                zero_of(amount)
            }
            else {
                if number_of(amount).is_none() {
                    return Err(CommandError::new(
                        14,
                        format!("Cannot apply $inc to a value of non-numeric type (field: \"{path}\", _id: {identifier})"),
                    ));
                }
                amount.clone()
            }
        }
    };
    set_segments(document, segments, result)
}

fn rename_path(document: &mut BsonDocument, source: &[&str], value: &BsonValue, path: &str) -> Result<(), CommandError> {
    let BsonValue::String(target) = value
    else {
        return Ok(());
    };
    if path == target {
        return Err(CommandError::new(2, format!("The source and target field for $rename must differ: {path}: \"{target}\"")));
    }
    let Some(source_value) = take_path(document, source)
    else {
        return Ok(());
    };
    let target_segments: Vec<&str> = target.split('.').collect();
    unset_segments(document, &target_segments);
    set_segments(document, &target_segments, source_value)
}

fn take_path(document: &mut BsonDocument, segments: &[&str]) -> Option<BsonValue> {
    let head = *segments.first()?;
    let position = document.fields.iter().position(|(field, _)| field == head)?;
    if segments.len() == 1 {
        return Some(document.fields.remove(position).1);
    }
    match &mut document.fields[position].1 {
        BsonValue::Document(child) => take_path(child, &segments[1..]),
        _ => None,
    }
}

fn push_path(document: &mut BsonDocument, segments: &[&str], value: &BsonValue, path: &str, identifier: &str) -> Result<(), CommandError> {
    let items = ensure_array(document, segments, path, identifier)?;
    if let BsonValue::Document(modifiers) = value
        && has_operator(modifiers)
    {
        let values = match modifiers.get("$each") {
            Some(BsonValue::Array(values)) => values.clone(),
            Some(_) => return Err(CommandError::new(2, "$each requires an array")),
            None => return Err(CommandError::new(2, "$slice and $position require $each")),
        };
        for (key, _) in &modifiers.fields {
            if !matches!(key.as_str(), "$each" | "$slice" | "$position") {
                return Err(CommandError::new(9, format!("Unrecognized clause in $push: {key}")));
            }
        }
        let position = match modifiers.get("$position") {
            Some(operand) => match integer_of(operand) {
                Some(position) => {
                    if position < 0 {
                        let from_end = items.len() as i128 + position;
                        Some(from_end.max(0) as usize)
                    }
                    else {
                        Some((position as usize).min(items.len()))
                    }
                }
                None => return Err(CommandError::new(2, "$position must be an integer")),
            },
            None => None,
        };
        match position {
            Some(position) => {
                let tail = items.split_off(position);
                items.extend(values);
                items.extend(tail);
            }
            None => items.extend(values),
        }
        if let Some(operand) = modifiers.get("$slice") {
            let Some(slice) = integer_of(operand)
            else {
                return Err(CommandError::new(2, "$slice must be an integer"));
            };
            if slice >= 0 {
                items.truncate(slice as usize);
            }
            else {
                let keep = (-slice) as usize;
                if items.len() > keep {
                    let drop = items.len() - keep;
                    items.drain(0..drop);
                }
            }
        }
        return Ok(());
    }
    items.push(value.clone());
    Ok(())
}

fn add_to_set_path(
    document: &mut BsonDocument,
    segments: &[&str],
    value: &BsonValue,
    path: &str,
    identifier: &str,
) -> Result<(), CommandError> {
    ensure_array_with_message(document, segments, path, identifier, |field, kind| {
        format!("Cannot apply $addToSet to non-array field. Field named '{field}' has non-array type {kind}")
    })
    .and_then(|items| {
        if let BsonValue::Document(modifiers) = value
            && has_operator(modifiers)
        {
            for (key, _) in &modifiers.fields {
                if key != "$each" {
                    return Err(CommandError::new(9, format!("Found unexpected fields after $each in $addToSet: {key}")));
                }
            }
            let Some(BsonValue::Array(values)) = modifiers.get("$each")
            else {
                return Err(CommandError::new(2, "$each requires an array"));
            };
            for item in values {
                if !items.iter().any(|existing| values_equal(Some(existing), item)) {
                    items.push(item.clone());
                }
            }
            return Ok(());
        }
        if !items.iter().any(|existing| values_equal(Some(existing), value)) {
            items.push(value.clone());
        }
        Ok(())
    })
}

fn pop_path(document: &mut BsonDocument, segments: &[&str], value: &BsonValue, path: &str) -> Result<(), CommandError> {
    let direction = match value {
        BsonValue::Int32(number) => *number,
        BsonValue::Int64(number) => *number as i32,
        _ => 0,
    };
    if direction != 1 && direction != -1 {
        return Err(CommandError::new(9, format!("$pop expects 1 or -1, found: {}", render_shell_value(value))));
    }
    let Some(current) = value_at(document, segments).cloned()
    else {
        return Ok(());
    };
    let BsonValue::Array(items) = current
    else {
        return Err(CommandError::new(14, format!("Path '{path}' contains an element of non-array type '{}'", bson_type_name(&current))));
    };
    if items.is_empty() {
        return Ok(());
    }
    let mut items = items;
    if direction == 1 {
        items.pop();
    }
    else {
        items.remove(0);
    }
    set_segments(document, segments, BsonValue::Array(items))
}

fn pull_path(document: &mut BsonDocument, segments: &[&str], value: &BsonValue, path: &str) -> Result<(), CommandError> {
    let Some(current) = value_at(document, segments).cloned()
    else {
        return Ok(());
    };
    let BsonValue::Array(items) = current
    else {
        return Err(CommandError::new(2, "Cannot apply $pull to a non-array value"));
    };
    let mut kept = Vec::with_capacity(items.len());
    for item in items {
        if !pull_matches(&item, value, path)? {
            kept.push(item);
        }
    }
    set_segments(document, segments, BsonValue::Array(kept))
}

fn pull_matches(item: &BsonValue, operand: &BsonValue, _path: &str) -> Result<bool, CommandError> {
    if let BsonValue::Document(operand) = operand {
        if has_operator(operand) {
            return operator_matches(&[item], operand, "pull", 0);
        }
        return match item {
            BsonValue::Document(element) => document_matches(element, operand),
            _ => Ok(false),
        };
    }
    Ok(values_equal(Some(item), operand))
}

fn ensure_array<'a>(
    document: &'a mut BsonDocument,
    segments: &[&str],
    path: &str,
    identifier: &str,
) -> Result<&'a mut Vec<BsonValue>, CommandError> {
    ensure_array_with_message(document, segments, path, identifier, |field, kind| {
        format!("The field '{field}' must be an array but is of type {kind} in document {{_id: {identifier}}}")
    })
}

fn ensure_array_with_message<'a>(
    document: &'a mut BsonDocument,
    segments: &[&str],
    path: &str,
    identifier: &str,
    message: impl Fn(&str, &str) -> String,
) -> Result<&'a mut Vec<BsonValue>, CommandError> {
    let _ = identifier;
    if value_at(document, segments).is_none() {
        set_segments(document, segments, BsonValue::Array(Vec::new()))?;
    }
    let value = value_at_mut(document, segments).expect("array path was created");
    match value {
        BsonValue::Array(items) => Ok(items),
        other => {
            let kind = bson_type_name(other);
            Err(CommandError::new(2, message(path, kind)))
        }
    }
}

fn zero_of(amount: &BsonValue) -> BsonValue {
    match amount {
        BsonValue::Int32(_) => BsonValue::Int32(0),
        BsonValue::Int64(_) => BsonValue::Int64(0),
        BsonValue::Double(_) => BsonValue::Double(0.0),
        other => other.clone(),
    }
}

fn multiply_value(current: &BsonValue, amount: &BsonValue) -> Option<BsonValue> {
    let left = number_of(current)?;
    let right = number_of(amount)?;
    let product = left * right;
    if matches!(current, BsonValue::Double(_)) || matches!(amount, BsonValue::Double(_)) {
        return Some(BsonValue::Double(product));
    }
    if matches!(current, BsonValue::Int64(_)) || matches!(amount, BsonValue::Int64(_)) {
        return Some(BsonValue::Int64(product as i64));
    }
    if let Ok(value) = i32::try_from(product as i64) { Some(BsonValue::Int32(value)) } else { Some(BsonValue::Int64(product as i64)) }
}

/// Renders one value the way `mongod` prints it inside update error messages.
///
/// Every shape below was read back from a `mongod 8.0.9` duplicate-key `errmsg`;
/// dates print as `new Date(<milliseconds>)`, not as `ISODate("...")`.
fn render_shell_value(value: &BsonValue) -> String {
    match value {
        BsonValue::Document(document) => {
            if document.fields.is_empty() {
                "{}".to_string()
            }
            else {
                let fields: Vec<String> =
                    document.fields.iter().map(|(field, value)| format!("{field}: {}", render_shell_value(value))).collect();
                format!("{{ {} }}", fields.join(", "))
            }
        }
        BsonValue::Array(items) => {
            if items.is_empty() {
                "[]".to_string()
            }
            else {
                let items: Vec<String> = items.iter().map(render_shell_value).collect();
                format!("[ {} ]", items.join(", "))
            }
        }
        BsonValue::String(text) => format!("\"{text}\""),
        BsonValue::Int32(number) => number.to_string(),
        BsonValue::Int64(number) => number.to_string(),
        BsonValue::Double(number) => number.to_string(),
        BsonValue::Decimal128(number) => number.as_str(),
        BsonValue::Bool(flag) => if *flag { "true" } else { "false" }.to_string(),
        BsonValue::Null => "null".to_string(),
        BsonValue::Undefined => "undefined".to_string(),
        BsonValue::ObjectId(bytes) => format!("ObjectId('{}')", hex_text(bytes)),
        BsonValue::Binary(binary) => format!("BinData({}, {})", binary.subtype, hex_text(&binary.bytes)),
        BsonValue::DateTime(milliseconds) => format!("new Date({milliseconds})"),
        BsonValue::Timestamp { t, i } => format!("Timestamp({t}, {i})"),
        BsonValue::MinKey => "MinKey".to_string(),
        BsonValue::MaxKey => "MaxKey".to_string(),
        BsonValue::Symbol(text) => format!("\"{text}\""),
        BsonValue::Regex { pattern, options } => format!("/{pattern}/{options}"),
        BsonValue::Code(code) => code.clone(),
        BsonValue::CodeWithScope { code, scope } => {
            format!("CodeWScope( {code}, {} )", render_shell_value(&BsonValue::Document(scope.clone())))
        }
        // DBPointer is obsolete and `mongod 8.0.9` no longer emits it in error
        // messages, so this shape follows the classic shell form.
        BsonValue::DbPointer { namespace, id } => {
            format!("DBPointer( \"{namespace}\", ObjectId('{}') )", hex_text(id))
        }
    }
}

fn hex_text(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Whether a requested key pattern names the same index key as `specification`.
///
/// Field order is significant and each value uses the numeric-equivalence key
/// encoding, so `{ a: 1 }` matches `{ a: 1.0 }` but not `{ a: "1" }` and not
/// a reordered compound key.
fn key_pattern_matches(requested: &BsonDocument, specification: &BsonDocument) -> bool {
    requested.fields.len() == specification.fields.len()
        && requested
            .fields
            .iter()
            .zip(&specification.fields)
            .all(|((want, left), (have, right))| want == have && value_to_key(left) == value_to_key(right))
}

/// BSON type name `mongod` prints in a wrong-type error message.
fn bson_type_name(value: &BsonValue) -> &'static str {
    match value {
        BsonValue::Document(_) => "object",
        BsonValue::Array(_) => "array",
        BsonValue::String(_) => "string",
        BsonValue::Int32(_) => "int",
        BsonValue::Int64(_) => "long",
        BsonValue::Double(_) => "double",
        BsonValue::Bool(_) => "bool",
        BsonValue::Null => "null",
        BsonValue::Undefined => "undefined",
        BsonValue::ObjectId(_) => "objectId",
        BsonValue::Binary(_) => "binData",
        BsonValue::DateTime(_) => "date",
        BsonValue::Timestamp { .. } => "timestamp",
        BsonValue::Decimal128(_) => "decimal",
        BsonValue::Regex { .. } => "regex",
        BsonValue::Code(_) => "javascript",
        BsonValue::CodeWithScope { .. } => "javascriptWithScope",
        BsonValue::Symbol(_) => "symbol",
        BsonValue::DbPointer { .. } => "dbPointer",
        BsonValue::MinKey => "minKey",
        BsonValue::MaxKey => "maxKey",
    }
}

fn increment_value(current: &BsonValue, amount: &BsonValue) -> Option<BsonValue> {
    match (current, amount) {
        (BsonValue::Int32(left), BsonValue::Int32(right)) => {
            let sum = i128::from(*left) + i128::from(*right);
            if let Ok(value) = i32::try_from(sum) { Some(BsonValue::Int32(value)) } else { i64::try_from(sum).ok().map(BsonValue::Int64) }
        }
        (BsonValue::Int32(_) | BsonValue::Int64(_), BsonValue::Int32(_) | BsonValue::Int64(_)) => {
            let sum = integer_of(current)? + integer_of(amount)?;
            i64::try_from(sum).ok().map(BsonValue::Int64)
        }
        (BsonValue::Int32(_) | BsonValue::Int64(_) | BsonValue::Double(_), BsonValue::Double(_))
        | (BsonValue::Double(_), BsonValue::Int32(_) | BsonValue::Int64(_)) => {
            Some(BsonValue::Double(number_of(current)? + number_of(amount)?))
        }
        _ => None,
    }
}

/// `getMore`: drains the caller-owned cursor table.
///
/// A cursor is bound to the namespace it was opened on. `mongod` requires the
/// `collection` field and refuses a request that names another namespace, so the
/// lookup is scoped before any document is read.
pub fn get_more(command: &BsonDocument, cursors: &mut CursorStore) -> Result<BsonDocument, CommandError> {
    let Some(cursor_id) = command.get("getMore").and_then(int_value)
    else {
        return Err(CommandError::new(2, "getMore requires a cursor id"));
    };
    let Some(collection) = command.get("collection").and_then(|value| match value {
        BsonValue::String(collection) => Some(collection.as_str()),
        _ => None,
    })
    else {
        return Err(CommandError::new(40415, "BSON field 'getMore.collection' is missing but a required field"));
    };
    let namespace = format!("{}.{collection}", command.database());
    // Absent and explicit `0` both mean "fill the batch", which `mongod 8.0.9`
    // answers by draining everything that fits under the BSON ceiling.
    let batch_size = command.get("batchSize").and_then(int_value).filter(|count| *count > 0).map(|count| count as usize);
    let (batch, namespace) = cursors.get_more(cursor_id, &namespace, batch_size).map_err(|error| match error {
        GetMoreError::NotFound(id) => CommandError::new(43, format!("cursor id {id} not found")),
        // `mongod` refuses a cross-namespace request with `Unauthorized` before
        // it looks at the cursor's state.
        GetMoreError::NamespaceMismatch { requested, cursor } => {
            CommandError::new(13, format!("Requested getMore on namespace '{requested}', but cursor belongs to a different namespace {cursor}"))
        }
        // `mongod 8.0.9` kills the cursor with the collection and answers one
        // `QueryPlanKilled` naming the collection UUID.
        GetMoreError::CollectionDropped { namespace, .. } => {
            CommandError::new(175, format!("collection dropped. UUID {}", collection_uuid_text(&namespace)))
        }
    })?;
    let remaining = if cursors.contains(cursor_id) { cursor_id } else { 0 };
    Ok(cursor_reply(&namespace, "nextBatch", batch, remaining))
}

/// `killCursors`.
pub fn kill_cursors(command: &BsonDocument, cursors: &mut CursorStore) -> Result<BsonDocument, CommandError> {
    let identifiers = match command.get("cursors") {
        Some(BsonValue::Array(identifiers)) => identifiers.clone(),
        _ => Vec::new(),
    };
    let requested: Vec<i64> = identifiers.iter().filter_map(int_value).collect();
    let (killed, not_found) = cursors.kill(&requested);

    let mut response = BsonDocument::new();
    response.push("cursorsKilled".to_string(), BsonValue::Array(killed.into_iter().map(BsonValue::Int64).collect()));
    response.push("cursorsNotFound".to_string(), BsonValue::Array(not_found.into_iter().map(BsonValue::Int64).collect()));
    response.push("cursorsAlive".to_string(), BsonValue::Array(Vec::new()));
    response.push("cursorsUnknown".to_string(), BsonValue::Array(Vec::new()));
    response.push("ok".to_string(), BsonValue::Double(1.0));
    Ok(response)
}

/// `listCollections` with `nameOnly`, `authorizedCollections`, and a general
/// `filter` applied to the rendered collection descriptions.
pub fn list_collections(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let database = command.database();
    let prefix = format!("{database}.");
    let name_only = match command.get("nameOnly") {
        Some(BsonValue::Bool(flag)) => *flag,
        Some(other) => {
            return Err(CommandError::new(
                14,
                format!("BSON field 'listCollections.nameOnly' is the wrong type '{}', expected type 'bool'", bson_type_name(other)),
            ));
        }
        None => false,
    };
    if let Some(other) = command.get("authorizedCollections")
        && !matches!(other, BsonValue::Bool(_))
    {
        return Err(CommandError::new(
            14,
            format!("BSON field 'listCollections.authorizedCollections' is the wrong type '{}', expected type 'bool'", bson_type_name(other)),
        ));
    }
    let filter = match command.get("filter") {
        Some(BsonValue::Document(filter)) => {
            validate_filter(filter, "listCollections")?;
            Some(filter)
        }
        _ => None,
    };

    let mut entries = Vec::new();
    for namespace in store.namespaces()? {
        let Some(collection) = namespace.strip_prefix(&prefix)
        else {
            continue;
        };
        let mut entry = BsonDocument::new();
        entry.push("name".to_string(), BsonValue::String(collection.to_string()));
        entry.push("type".to_string(), BsonValue::String("collection".to_string()));
        if name_only {
            if let Some(filter) = filter
                && !document_matches(&entry, filter)?
            {
                continue;
            }
            entries.push(BsonValue::Document(entry));
            continue;
        }
        let is_oplog = store.oplog_namespace().as_deref() == Some(namespace.as_str());
        let mut options = BsonDocument::new();
        if is_oplog {
            if let Some((_, size, _, _)) = store.oplog_status() {
                options.push("capped".to_string(), BsonValue::Bool(true));
                options.push("size".to_string(), BsonValue::Int64(size));
                options.push("autoIndexId".to_string(), BsonValue::Bool(false));
            }
        }
        else {
            if let Some(validator) = collection_validator_value(store, &namespace)? {
                options.push("validator".to_string(), validator);
            }
            let stored = store.collection_options(&namespace)?;
            if let Some(level) = &stored.validation_level {
                options.push("validationLevel".to_string(), BsonValue::String(level.clone()));
            }
            if let Some(action) = &stored.validation_action {
                options.push("validationAction".to_string(), BsonValue::String(action.clone()));
            }
        }
        entry.push("options".to_string(), BsonValue::Document(options));
        let mut info = BsonDocument::new();
        info.push("readOnly".to_string(), BsonValue::Bool(false));
        info.push("uuid".to_string(), collection_uuid(&namespace));
        entry.push("info".to_string(), BsonValue::Document(info));
        if !is_oplog {
            let mut id_index = BsonDocument::new();
            id_index.push("v".to_string(), BsonValue::Int32(2));
            let mut key = BsonDocument::new();
            key.push("_id".to_string(), BsonValue::Int32(1));
            id_index.push("key".to_string(), BsonValue::Document(key));
            id_index.push("name".to_string(), BsonValue::String("_id_".to_string()));
            entry.push("idIndex".to_string(), BsonValue::Document(id_index));
        }
        if let Some(filter) = filter
            && !document_matches(&entry, filter)?
        {
            continue;
        }
        entries.push(BsonValue::Document(entry));
    }

    Ok(cursor_reply(&format!("{database}.$cmd.listCollections"), "firstBatch", entries, 0))
}

/// A stable collection identifier derived from the namespace, so reopening a
/// store keeps the `info.uuid` the catalog reported before.
fn collection_uuid_bytes(namespace: &str) -> [u8; 16] {
    let mut high: u64 = 0xcbf2_9ce4_8422_2325;
    let mut low: u64 = 0x9e37_79b9_7f4a_7c15;
    for byte in namespace.bytes() {
        high ^= u64::from(byte);
        high = high.wrapping_mul(0x0000_0100_0000_01b3);
        low ^= u64::from(byte).wrapping_add(0x9e37_79b9);
        low = low.rotate_left(7).wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&high.to_be_bytes());
    bytes[8..].copy_from_slice(&low.to_be_bytes());
    bytes
}

fn collection_uuid(namespace: &str) -> BsonValue {
    BsonValue::Binary(BinaryData::new(4, collection_uuid_bytes(namespace).to_vec()))
}

/// UUID in the `8-4-4-4-12` textual form `mongod` uses inside error messages.
fn collection_uuid_text(namespace: &str) -> String {
    let bytes = collection_uuid_bytes(namespace);
    format!(
        "{}-{}-{}-{}-{}",
        bytes[..4].iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        bytes[4..6].iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        bytes[6..8].iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        bytes[8..10].iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        bytes[10..].iter().map(|byte| format!("{byte:02x}")).collect::<String>()
    )
}

/// `mongod 8.0.9` refuses a second `create` whose options differ from the
/// stored ones and quotes the stored options in the message, uuid first.
fn create_conflict_message(namespace: &str, options: &CollectionOptions) -> String {
    let mut rendered = vec![format!("uuid: UUID(\"{}\")", collection_uuid_text(namespace))];
    if let Some(validator) = &options.validator
        && let Ok(value) = json::from_json(validator)
    {
        rendered.push(format!("validator: {}", render_shell_value(&value)));
    }
    if let Some(level) = &options.validation_level {
        rendered.push(format!("validationLevel: \"{level}\""));
    }
    if let Some(action) = &options.validation_action {
        rendered.push(format!("validationAction: \"{action}\""));
    }
    format!("namespace {namespace} already exists, but with different options: {{ {} }}", rendered.join(", "))
}

fn collection_validator_value(store: &dyn MongoStorage, namespace: &str) -> Result<Option<BsonValue>, CommandError> {
    Ok(store.collection_options(namespace)?.validator.and_then(|validator| json::from_json(&validator).ok()))
}

/// The validator schema, level, and action in force for a namespace. A missing
/// `$jsonSchema` validator means no schema validation happens.
fn collection_schema(store: &dyn MongoStorage, namespace: &str) -> Result<Option<(BsonDocument, String, String)>, CommandError> {
    let options = store.collection_options(namespace)?;
    let Some(validator) = options.validator.and_then(|value| json::from_json(&value).ok())
    else {
        return Ok(None);
    };
    let BsonValue::Document(validator) = validator
    else {
        return Ok(None);
    };
    let schema = match validator.get("$jsonSchema") {
        Some(BsonValue::Document(schema)) => schema.clone(),
        _ => return Ok(None),
    };
    let level = options.validation_level.unwrap_or_else(|| "strict".to_string());
    let action = options.validation_action.unwrap_or_else(|| "error".to_string());
    Ok(Some((schema, level, action)))
}

fn schema_rules(schema: &BsonDocument, document: &BsonDocument) -> Vec<BsonValue> {
    let mut rules = Vec::new();
    if let Some(BsonValue::String(expected)) = schema.get("bsonType")
        && expected != "object"
    {
        let mut specified = BsonDocument::new();
        specified.push("$jsonSchema".to_string(), BsonValue::Document(schema.clone()));
        let mut rule = BsonDocument::new();
        rule.push("operatorName".to_string(), BsonValue::String("$jsonSchema".to_string()));
        rule.push("specifiedAs".to_string(), BsonValue::Document(specified));
        rule.push("reason".to_string(), BsonValue::String("expression did not evaluate to true".to_string()));
        rules.push(BsonValue::Document(rule));
        return rules;
    }
    if let Some(BsonValue::Bool(false)) = schema.get("additionalProperties") {
        let properties = match schema.get("properties") {
            Some(BsonValue::Document(properties)) => properties,
            _ => &BsonDocument::new(),
        };
        let extra: Vec<String> =
            document.fields.iter().filter(|(field, _)| properties.get(field).is_none()).map(|(field, _)| field.clone()).collect();
        if !extra.is_empty() {
            let mut specified = BsonDocument::new();
            specified.push("additionalProperties".to_string(), BsonValue::Bool(false));
            let mut rule = BsonDocument::new();
            rule.push("operatorName".to_string(), BsonValue::String("additionalProperties".to_string()));
            rule.push("specifiedAs".to_string(), BsonValue::Document(specified));
            rule.push("additionalProperties".to_string(), BsonValue::Array(extra.into_iter().map(BsonValue::String).collect()));
            rules.push(BsonValue::Document(rule));
        }
    }
    if let Some(BsonValue::Array(required)) = schema.get("required") {
        let missing: Vec<BsonValue> = required
            .iter()
            .filter_map(|requirement| match requirement {
                BsonValue::String(field) if document.get(field).is_none() => Some(BsonValue::String(field.clone())),
                _ => None,
            })
            .collect();
        if !missing.is_empty() {
            let mut specified = BsonDocument::new();
            specified.push("required".to_string(), BsonValue::Array(required.clone()));
            let mut rule = BsonDocument::new();
            rule.push("operatorName".to_string(), BsonValue::String("required".to_string()));
            rule.push("specifiedAs".to_string(), BsonValue::Document(specified));
            rule.push("missingProperties".to_string(), BsonValue::Array(missing));
            rules.push(BsonValue::Document(rule));
        }
    }
    if let Some(BsonValue::Document(properties)) = schema.get("properties") {
        let mut unsatisfied = Vec::new();
        for (field, property_schema) in &properties.fields {
            let BsonValue::Document(property_schema) = property_schema
            else {
                continue;
            };
            let Some(value) = document.get(field)
            else {
                continue;
            };
            let details = schema_details(property_schema, value);
            if details.is_empty() {
                continue;
            }
            let mut entry = BsonDocument::new();
            entry.push("propertyName".to_string(), BsonValue::String(field.clone()));
            entry.push("details".to_string(), BsonValue::Array(details.into_iter().map(BsonValue::Document).collect()));
            unsatisfied.push(BsonValue::Document(entry));
        }
        if !unsatisfied.is_empty() {
            let mut rule = BsonDocument::new();
            rule.push("operatorName".to_string(), BsonValue::String("properties".to_string()));
            rule.push("propertiesNotSatisfied".to_string(), BsonValue::Array(unsatisfied));
            rules.push(BsonValue::Document(rule));
        }
    }
    rules
}

fn schema_details(schema: &BsonDocument, value: &BsonValue) -> Vec<BsonDocument> {
    let mut details = Vec::new();
    if let Some(BsonValue::String(expected)) = schema.get("bsonType") {
        let actual = bson_type_name(value);
        if actual != expected {
            let mut specified = BsonDocument::new();
            specified.push("bsonType".to_string(), BsonValue::String(expected.clone()));
            let mut rule = BsonDocument::new();
            rule.push("operatorName".to_string(), BsonValue::String("bsonType".to_string()));
            rule.push("specifiedAs".to_string(), BsonValue::Document(specified));
            rule.push("reason".to_string(), BsonValue::String("type did not match".to_string()));
            rule.push("consideredValue".to_string(), value.clone());
            rule.push("consideredType".to_string(), BsonValue::String(actual.to_string()));
            details.push(rule);
            return details;
        }
    }
    if let Some(minimum) = schema.get("minimum")
        && let (Some(current), Some(limit)) = (number_of(value), number_of(minimum))
        && current < limit
    {
        let mut specified = BsonDocument::new();
        specified.push("minimum".to_string(), minimum.clone());
        let mut rule = BsonDocument::new();
        rule.push("operatorName".to_string(), BsonValue::String("minimum".to_string()));
        rule.push("specifiedAs".to_string(), BsonValue::Document(specified));
        rule.push("reason".to_string(), BsonValue::String("comparison failed".to_string()));
        rule.push("consideredValue".to_string(), value.clone());
        details.push(rule);
    }
    if let Some(maximum) = schema.get("maximum")
        && let (Some(current), Some(limit)) = (number_of(value), number_of(maximum))
        && current > limit
    {
        let mut specified = BsonDocument::new();
        specified.push("maximum".to_string(), maximum.clone());
        let mut rule = BsonDocument::new();
        rule.push("operatorName".to_string(), BsonValue::String("maximum".to_string()));
        rule.push("specifiedAs".to_string(), BsonValue::Document(specified));
        rule.push("reason".to_string(), BsonValue::String("comparison failed".to_string()));
        rule.push("consideredValue".to_string(), value.clone());
        details.push(rule);
    }
    if let (Some(BsonValue::Document(items)), BsonValue::Array(values)) = (schema.get("items"), value) {
        for (index, item) in values.iter().enumerate() {
            let item_details = schema_details(items, item);
            if item_details.is_empty() {
                continue;
            }
            let mut rule = BsonDocument::new();
            rule.push("operatorName".to_string(), BsonValue::String("items".to_string()));
            rule.push("reason".to_string(), BsonValue::String("At least one item did not match the sub-schema".to_string()));
            rule.push("itemIndex".to_string(), BsonValue::Int32(index as i32));
            rule.push("details".to_string(), BsonValue::Array(item_details.into_iter().map(BsonValue::Document).collect()));
            details.push(rule);
        }
    }
    details
}

fn validation_err_info(document: &BsonDocument, rules: Vec<BsonValue>) -> BsonDocument {
    let mut details = BsonDocument::new();
    details.push("operatorName".to_string(), BsonValue::String("$jsonSchema".to_string()));
    details.push("schemaRulesNotSatisfied".to_string(), BsonValue::Array(rules));
    let mut info = BsonDocument::new();
    info.push("failingDocumentId".to_string(), document.get("_id").cloned().unwrap_or(BsonValue::Null));
    info.push("details".to_string(), BsonValue::Document(details));
    info
}

/// Insert-path validation: `off` and `warn` never fail a write.
fn insert_validation_failure(store: &dyn MongoStorage, namespace: &str, document: &BsonDocument) -> Result<Option<BsonDocument>, CommandError> {
    let Some((schema, level, action)) = collection_schema(store, namespace)?
    else {
        return Ok(None);
    };
    if level == "off" || action == "warn" {
        return Ok(None);
    }
    let rules = schema_rules(&schema, document);
    if rules.is_empty() {
        return Ok(None);
    }
    Ok(Some(validation_err_info(document, rules)))
}

/// Update-path validation: `moderate` skips documents that were already
/// invalid before the update, matching the server.
fn update_validation_failure(
    store: &dyn MongoStorage,
    namespace: &str,
    original: &BsonDocument,
    updated: &BsonDocument,
) -> Result<Option<BsonDocument>, CommandError> {
    let Some((schema, level, action)) = collection_schema(store, namespace)?
    else {
        return Ok(None);
    };
    if level == "off" || action == "warn" {
        return Ok(None);
    }
    if level == "moderate" {
        let before = schema_rules(&schema, original);
        if !before.is_empty() {
            return Ok(None);
        }
    }
    let rules = schema_rules(&schema, updated);
    if rules.is_empty() {
        return Ok(None);
    }
    Ok(Some(validation_err_info(updated, rules)))
}

fn validation_options(command: &BsonDocument) -> Result<CollectionOptions, CommandError> {
    let mut options = CollectionOptions::default();
    if let Some(validator) = command.get("validator") {
        let BsonValue::Document(validator) = validator
        else {
            return Err(CommandError::new(2, "The 'validator' field must be an object".to_string()));
        };
        options.validator = Some(json::to_json(&BsonValue::Document(validator.clone())));
    }
    if let Some(level) = command.get("validationLevel") {
        let BsonValue::String(level) = level
        else {
            return Err(CommandError::new(2, "The 'validationLevel' field must be a string".to_string()));
        };
        if !matches!(level.as_str(), "off" | "strict" | "moderate") {
            return Err(CommandError::new(2, format!("Enumeration value '{level}' for field 'validationLevel' is not a valid value")));
        }
        options.validation_level = Some(level.clone());
    }
    if let Some(action) = command.get("validationAction") {
        let BsonValue::String(action) = action
        else {
            return Err(CommandError::new(2, "The 'validationAction' field must be a string".to_string()));
        };
        if !matches!(action.as_str(), "error" | "warn") {
            return Err(CommandError::new(2, format!("Enumeration value '{action}' for field 'validationAction' is not a valid value")));
        }
        options.validation_action = Some(action.clone());
    }
    Ok(options)
}

/// `create` with an optional validator, validation level, and action.
pub fn create(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let Some(collection) = command.get("create").and_then(|value| match value {
        BsonValue::String(name) => Some(name.clone()),
        _ => None,
    })
    else {
        return Err(CommandError::new(73, "create command missing collection name"));
    };
    let namespace = format!("{}.{collection}", command.database());
    let options = validation_options(command)?;
    // `mongod 8.0.9` treats `create` as idempotent when the stored options
    // match, and refuses only when they differ.
    if store.namespace_exists(&namespace)? {
        let existing = store.collection_options(&namespace)?;
        if existing != options {
            return Err(CommandError::new(48, create_conflict_message(&namespace, &existing)));
        }
        let mut response = BsonDocument::new();
        response.push("ok".to_string(), BsonValue::Double(1.0));
        return Ok(response);
    }
    store.create_collection(&namespace, options).map_err(|error| CommandError::new(2, format!("create failed: {error}")))?;
    if store.oplog_enabled() {
        let mut object = BsonDocument::new();
        object.push("create".to_string(), BsonValue::String(collection.clone()));
        for (field, value) in &command.fields {
            if field == "create" || is_control_field(field) {
                continue;
            }
            object.push(field.clone(), value.clone());
        }
        let mut id_index = BsonDocument::new();
        id_index.push("v".to_string(), BsonValue::Int32(2));
        let mut id_key = BsonDocument::new();
        id_key.push("_id".to_string(), BsonValue::Int32(1));
        id_index.push("key".to_string(), BsonValue::Document(id_key));
        id_index.push("name".to_string(), BsonValue::String("_id_".to_string()));
        object.push("idIndex".to_string(), BsonValue::Document(id_index));
        store.stage_oplog(OplogEntry::command(&command_namespace(&namespace), Some(collection_uuid(&namespace)), object, None));
    }
    let mut response = BsonDocument::new();
    response.push("ok".to_string(), BsonValue::Double(1.0));
    Ok(response)
}

/// Command fields that describe the request rather than the write.
fn is_control_field(field: &str) -> bool {
    matches!(field, "$db" | "lsid" | "writeConcern" | "$clusterTime" | "apiVersion" | "comment" | "txnNumber" | "$readPreference")
}

/// `<db>.$cmd` for a collection namespace, which is where `mongod` logs
/// collection and index commands.
fn command_namespace(namespace: &str) -> String {
    match namespace.split_once('.') {
        Some((database, _)) => format!("{database}.$cmd"),
        None => format!("{namespace}.$cmd"),
    }
}

/// Stages one `c` entry per index a `createIndexes` really built.
fn stage_create_indexes_oplog(store: &dyn MongoStorage, namespace: &str, collection: &str, created: &[IndexSpec]) {
    if created.is_empty() || !store.oplog_enabled() {
        return;
    }
    let uuid = collection_uuid(namespace);
    for specification in created {
        let mut object = BsonDocument::new();
        object.push("createIndexes".to_string(), BsonValue::String(collection.to_string()));
        object.push("v".to_string(), BsonValue::Int32(2));
        object.push("key".to_string(), BsonValue::Document(specification.key.clone()));
        object.push("name".to_string(), BsonValue::String(specification.name.clone()));
        if specification.unique {
            object.push("unique".to_string(), BsonValue::Bool(true));
        }
        if specification.sparse == Some(true) {
            object.push("sparse".to_string(), BsonValue::Bool(true));
        }
        store.stage_oplog(OplogEntry::command(&command_namespace(namespace), Some(uuid.clone()), object, None));
    }
}

/// `drop` removing one collection, its documents and its user indexes.
///
/// `mongod 8.0.9` answers `{nIndexesWas, ns, ok}` for an existing collection
/// and `{ok}` for a missing one; a view reply has no `nIndexesWas`, but this
/// server has no views, so that branch does not exist here.
pub fn drop(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let database = command.database();
    let collection = match command.get("drop") {
        Some(BsonValue::String(name)) => name.clone(),
        Some(other) => {
            return Err(CommandError::new(14, format!("collection name has invalid type {}", bson_type_name(other))));
        }
        None => return Err(CommandError::new(14, "collection name has invalid type int")),
    };
    if collection.is_empty() {
        return Err(CommandError::new(73, format!("Invalid namespace specified: {database}")));
    }
    let namespace = format!("{database}.{collection}");
    if store.oplog_enabled() && store.oplog_namespace().as_deref() == Some(namespace.as_str()) {
        return Err(CommandError::new(5255000, "can't drop live oplog while replicating"));
    }
    if matches!(database.as_str(), "admin" | "local" | "config") && collection.starts_with("system.") {
        return Err(CommandError::new(20, format!("cannot drop system collection {namespace}")));
    }
    if !store.namespace_exists(&namespace)? {
        let mut response = BsonDocument::new();
        response.push("ok".to_string(), BsonValue::Double(1.0));
        return Ok(response);
    }
    let records = store.scan(&namespace)?.len() as i32;
    let specifications = store.index_specs(&namespace)?;
    let user_indexes = specifications.len() as i32;
    if store.oplog_enabled() {
        let mut object = BsonDocument::new();
        object.push("drop".to_string(), BsonValue::String(collection.clone()));
        let mut old = BsonDocument::new();
        old.push("numRecords".to_string(), BsonValue::Int32(records));
        store.stage_oplog(OplogEntry::command(&command_namespace(&namespace), Some(collection_uuid(&namespace)), object, Some(old)));
    }
    for specification in &specifications {
        store
            .clear_reserved(&crate::storage::index_namespace(&namespace, &specification.name))
            .map_err(|error| CommandError::new(2, format!("drop failed: {error}")))?;
    }
    store.drop_namespace(&namespace).map_err(|error| CommandError::new(2, format!("drop failed: {error}")))?;
    let mut response = BsonDocument::new();
    response.push("nIndexesWas".to_string(), BsonValue::Int32(1 + user_indexes));
    response.push("ns".to_string(), BsonValue::String(namespace.clone()));
    response.push("ok".to_string(), BsonValue::Double(1.0));
    Ok(response)
}

/// `collMod` updating validator, validation level, and validation action in
/// place.
pub fn coll_mod(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let Some(collection) = command.get("collMod").and_then(|value| match value {
        BsonValue::String(name) => Some(name.clone()),
        _ => None,
    })
    else {
        return Err(CommandError::new(73, "collMod command missing collection name"));
    };
    let namespace = format!("{}.{collection}", command.database());
    if !store.namespace_exists(&namespace)? {
        return Err(CommandError::new(26, "ns does not exist".to_string()));
    }
    let mut options = store.collection_options(&namespace)?;
    let changes = validation_options(command)?;
    if changes.validator.is_some() {
        options.validator = changes.validator;
    }
    if changes.validation_level.is_some() {
        options.validation_level = changes.validation_level;
    }
    if changes.validation_action.is_some() {
        options.validation_action = changes.validation_action;
    }
    store.set_collection_options(&namespace, options).map_err(|error| CommandError::new(2, format!("collMod failed: {error}")))?;
    if store.oplog_enabled() {
        let mut object = BsonDocument::new();
        object.push("collMod".to_string(), BsonValue::String(collection.clone()));
        for (field, value) in &command.fields {
            if field == "collMod" || is_control_field(field) {
                continue;
            }
            object.push(field.clone(), value.clone());
        }
        let mut old = BsonDocument::new();
        let mut old_options = BsonDocument::new();
        old_options.push("uuid".to_string(), collection_uuid(&namespace));
        old.push("collectionOptions_old".to_string(), BsonValue::Document(old_options));
        store.stage_oplog(OplogEntry::command(&command_namespace(&namespace), Some(collection_uuid(&namespace)), object, Some(old)));
    }
    let mut response = BsonDocument::new();
    response.push("ok".to_string(), BsonValue::Double(1.0));
    Ok(response)
}

/// Logical session carried by `lsid`. `key` stays `None` while the session has
/// no open transaction, so the command reads and writes committed data.
struct SessionContext {
    key: Option<String>,
}

impl SessionContext {
    fn new(command: &BsonDocument, store: &dyn MongoStorage) -> Result<Self, CommandError> {
        let Some(session) = transaction_key(command)
        else {
            return Ok(Self { key: None });
        };
        let Some(txn_number) = transaction_number(command)?
        else {
            return Ok(Self { key: None });
        };
        if command_bool(command, "startTransaction", false) {
            return match store.begin_transaction(&session, txn_number) {
                TransactionBegin::Started => Ok(Self { key: Some(session) }),
                TransactionBegin::AlreadyStarted => Err(conflicting_transaction_error()),
                TransactionBegin::TooOld { newer } => Err(transaction_too_old_error(command, txn_number, newer)),
            };
        }
        // Only operations inside a multi-document transaction carry
        // `autocommit: false`; every other `txnNumber` is a retryable write and
        // runs against committed data, as `mongod` does.
        if command_bool(command, "autocommit", true) {
            return Ok(Self { key: None });
        }
        match store.session_write_state(&session, txn_number) {
            SessionWriteState::Active => Ok(Self { key: Some(session) }),
            SessionWriteState::TooOld { newer } => Err(transaction_too_old_error(command, txn_number, newer)),
            SessionWriteState::AlreadyCommitted => Err(transaction_committed_error(txn_number)),
            SessionWriteState::AlreadyAborted => Err(no_such_transaction_error(txn_number)),
            SessionWriteState::NoTransaction { active } => Err(transaction_mismatch_error(txn_number, active)),
        }
    }

    fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }
}

/// Stable textual form of the `lsid` document, used as the session key.
fn transaction_key(command: &BsonDocument) -> Option<String> {
    match command.get("lsid") {
        Some(BsonValue::Document(document)) => Some(json::document_to_json(document)),
        _ => None,
    }
}

/// `txnNumber` must be a 64-bit integer, exactly as `mongod 8.0.9` requires.
fn transaction_number(command: &BsonDocument) -> Result<Option<i64>, CommandError> {
    match command.get("txnNumber") {
        Some(BsonValue::Int64(value)) => Ok(Some(*value)),
        Some(BsonValue::Int32(_)) => Err(CommandError::new(14, "BSON field 'request.txnNumber' is the wrong type 'int', expected type 'long'")),
        Some(_) => Err(CommandError::new(14, "BSON field 'request.txnNumber' is the wrong type, expected type 'long'")),
        None => Ok(None),
    }
}

/// Session text in the `TransactionTooOld` message: the UUID followed by the
/// empty user id hash `mongod 8.0.9` prints for a session without `uid`.
fn session_text(command: &BsonDocument) -> String {
    let uuid = match command.get("lsid") {
        Some(BsonValue::Document(lsid)) => match lsid.get("id") {
            Some(BsonValue::Binary(binary)) if binary.bytes.len() == 16 => format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                binary.bytes[0],
                binary.bytes[1],
                binary.bytes[2],
                binary.bytes[3],
                binary.bytes[4],
                binary.bytes[5],
                binary.bytes[6],
                binary.bytes[7],
                binary.bytes[8],
                binary.bytes[9],
                binary.bytes[10],
                binary.bytes[11],
                binary.bytes[12],
                binary.bytes[13],
                binary.bytes[14],
                binary.bytes[15]
            ),
            _ => String::new(),
        },
        _ => String::new(),
    };
    format!("{uuid} - 47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU= -  -  ")
}

fn transaction_too_old_error(command: &BsonDocument, txn_number: i64, newer: i64) -> CommandError {
    CommandError::new(
        225,
        format!(
            "Cannot start transaction with {{ txnNumber: {txn_number} }} on session {}because a newer transaction with txnNumberAndRetryCounter {{ txnNumber: {newer}, txnRetryCounter: 0 }} has already started on this session.",
            session_text(command)
        ),
    )
}

fn conflicting_transaction_error() -> CommandError {
    CommandError::new(117, "Only servers in a sharded cluster can start a new transaction at the active transaction number")
}

fn transaction_committed_error(txn_number: i64) -> CommandError {
    CommandError::new(256, format!("Transaction with {{ txnNumber: {txn_number} }} has been committed."))
}

fn transaction_mismatch_error(txn_number: i64, active: i64) -> CommandError {
    CommandError::new(
        251,
        format!("Given transaction number {txn_number} does not match any in-progress transactions. The active transaction number is {active}"),
    )
    .with_detail("errorLabels", BsonValue::Array(vec![BsonValue::String("TransientTransactionError".to_string())]))
}

fn ok_reply() -> BsonDocument {
    let mut response = BsonDocument::new();
    response.push("ok".to_string(), BsonValue::Double(1.0));
    response
}

/// `mongod 8.0.9` reports a write conflict during plan execution with the
/// `TransientTransactionError` label and aborts the transaction.
fn write_conflict_error() -> CommandError {
    CommandError::new(
        112,
        "Caused by :: Write conflict during plan execution and yielding is disabled. :: Please retry your operation or multi-document transaction.",
    )
    .with_detail("errorLabels", BsonValue::Array(vec![BsonValue::String("TransientTransactionError".to_string())]))
}

fn no_such_transaction_error(txn_number: i64) -> CommandError {
    CommandError::new(251, format!("Transaction with {{ txnNumber: {txn_number} }} has been aborted."))
        .with_detail("errorLabels", BsonValue::Array(vec![BsonValue::String("TransientTransactionError".to_string())]))
}

/// 索引项键：索引键元组后接文档键。
///
/// 元组本身带长度前缀，所以「单字段元组」天然是「复合元组」的前缀，等值探针
/// 可以直接按前缀扫描，不需要再拼分隔符。
fn index_entry_key(values: &[BsonValue], document_key: &[u8]) -> Vec<u8> {
    let mut key = index_tuple(values);
    key.extend_from_slice(document_key);
    key
}

/// 一个文档在一组索引下应当存在的全部条目。
fn document_index_entries(document: &BsonDocument, document_key: &[u8], specifications: &[IndexSpec]) -> BTreeSet<(String, Vec<u8>)> {
    let mut entries = BTreeSet::new();
    for specification in specifications {
        let namespace = crate::storage::index_namespace(&specification.namespace, &specification.name);
        for values in index_entries(document, specification) {
            entries.insert((namespace.clone(), index_entry_key(&values, document_key)));
        }
    }
    entries
}

/// 按命名空间写入索引项；事务内与文档写一样走暂存，提交时一起落地。
fn put_index_keys(
    store: &dyn MongoStorage,
    session: Option<&str>,
    entries: &BTreeMap<String, Vec<(Vec<u8>, Vec<u8>)>>,
) -> Result<(), CommandError> {
    for (namespace, entries) in entries {
        match session {
            Some(session) => {
                for (entry_key, value) in entries {
                    if store.transaction_write(session, namespace, entry_key, Some(value.clone())) == TransactionWrite::Conflict {
                        return Err(write_conflict_error());
                    }
                }
            }
            None => {
                store.put_reserved(namespace, entries).map_err(|error| CommandError::new(2, format!("index write failed: {error}")))?;
            }
        }
    }
    Ok(())
}

/// 按命名空间删除索引项；事务内暂存删除，提交时一起落地。
fn delete_index_keys(store: &dyn MongoStorage, session: Option<&str>, keys: &BTreeMap<String, Vec<Vec<u8>>>) -> Result<(), CommandError> {
    for (namespace, keys) in keys {
        if keys.is_empty() {
            continue;
        }
        match session {
            Some(session) => {
                for key in keys {
                    if store.transaction_write(session, namespace, key, None) == TransactionWrite::Conflict {
                        return Err(write_conflict_error());
                    }
                }
            }
            None => {
                store.delete(namespace, keys).map_err(|error| CommandError::new(2, format!("index delete failed: {error}")))?;
            }
        }
    }
    Ok(())
}

/// 索引键字段出现数组值时把索引标记为 multikey，供范围计划保守回退。
///
/// 标记只会从关变开：数组元素删除后仍按 multikey 处理，宁可少用索引也不漏结果。
fn note_multikey(store: &dyn MongoStorage, documents: &[(BsonDocument, Vec<u8>)], specifications: &[IndexSpec]) -> Result<(), CommandError> {
    for specification in specifications {
        if specification.multikey {
            continue;
        }
        let array_valued =
            documents.iter().any(|(document, _)| specification.key.fields.iter().any(|(field, _)| field_array_valued(document, field)));
        if array_valued {
            store
                .mark_index_multikey(&specification.namespace, &specification.name)
                .map_err(|error| CommandError::new(1, format!("index metadata update failed: {error}")))?;
        }
    }
    Ok(())
}

/// 为一个文档集合写入索引项；每个条目值是文档键，读取时据此回表。
fn write_index_entries(
    store: &dyn MongoStorage,
    session: Option<&str>,
    documents: &[(BsonDocument, Vec<u8>)],
    specifications: &[IndexSpec],
) -> Result<(), CommandError> {
    if specifications.is_empty() || documents.is_empty() {
        return Ok(());
    }
    note_multikey(store, documents, specifications)?;
    let mut by_namespace: BTreeMap<String, Vec<(Vec<u8>, Vec<u8>)>> = BTreeMap::new();
    for (document, document_key) in documents {
        for (namespace, entry_key) in document_index_entries(document, document_key, specifications) {
            by_namespace.entry(namespace).or_default().push((entry_key, document_key.clone()));
        }
    }
    put_index_keys(store, session, &by_namespace)
}

/// 为 `mongodump` 导入的文档补写索引项；导入走覆盖写，调用方在写完后立即调用。
///
/// 覆盖写会替换掉同 `_id` 的旧文档，旧文档的索引项必须一并清掉，否则旧索引值
/// 会留下指向已变更文档的条目。先删旧条目再写新条目，删除期间读到的旧条目只
/// 会取到旧文档并被完整过滤条件拒绝，不会产生错误命中。
pub(crate) fn maintain_imported_indexes(store: &dyn MongoStorage, namespace: &str, documents: &[(Vec<u8>, Vec<u8>)]) -> Result<(), String> {
    let specifications = store.index_specs(namespace).map_err(|error| error.to_string())?;
    if specifications.is_empty() {
        return Ok(());
    }
    let parsed: Vec<(BsonDocument, Vec<u8>)> =
        documents.iter().filter_map(|(key, payload)| parse_bson(payload).ok().map(|document| (document, key.clone()))).collect();
    let mut overwritten: Vec<(BsonDocument, Vec<u8>)> = Vec::new();
    for (_, key) in &parsed {
        match store.get(namespace, key) {
            Ok(Some(payload)) => {
                if let Ok(document) = parse_bson(&payload) {
                    overwritten.push((document, key.clone()));
                }
            }
            Ok(None) => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    remove_index_entries(store, None, &overwritten, &specifications).map_err(|error| error.message)?;
    write_index_entries(store, None, &parsed, &specifications).map_err(|error| error.message)?;
    Ok(())
}

/// 删除一个文档集合的全部索引项。
fn remove_index_entries(
    store: &dyn MongoStorage,
    session: Option<&str>,
    documents: &[(BsonDocument, Vec<u8>)],
    specifications: &[IndexSpec],
) -> Result<(), CommandError> {
    if specifications.is_empty() || documents.is_empty() {
        return Ok(());
    }
    let mut by_namespace: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
    for (document, document_key) in documents {
        for (namespace, entry_key) in document_index_entries(document, document_key, specifications) {
            by_namespace.entry(namespace).or_default().push(entry_key);
        }
    }
    delete_index_keys(store, session, &by_namespace)
}

/// Stages a batch of new documents in the session's transaction, or writes it
/// straight to the shard outside one.
fn stage_insert(
    store: &dyn MongoStorage,
    session: Option<&str>,
    namespace: &str,
    documents: &[(Vec<u8>, Vec<u8>)],
) -> Result<(), CommandError> {
    let specifications = store.index_specs(namespace)?;
    if !specifications.is_empty() {
        let parsed: Vec<(BsonDocument, Vec<u8>)> =
            documents.iter().filter_map(|(key, payload)| parse_bson(payload).ok().map(|document| (document, key.clone()))).collect();
        // 先写索引项再写文档：文档写失败时最多留下指向不存在文档的条目，读取会跳过；
        // 反序则会静默漏掉文档，查询结果少一条且没有任何报错。
        write_index_entries(store, session, &parsed, &specifications)?;
    }
    match session {
        Some(key) => {
            for (document_key, payload) in documents {
                if store.transaction_write(key, namespace, document_key, Some(payload.clone())) == TransactionWrite::Conflict {
                    return Err(write_conflict_error());
                }
            }
            Ok(())
        }
        None => store.import(namespace, documents).map(|_| ()).map_err(|error| CommandError::new(2, format!("insert failed: {error}"))),
    }
}

/// Stages replacements in the session's transaction, or writes them straight
/// to the shard outside one.
fn stage_replace(
    store: &dyn MongoStorage,
    session: Option<&str>,
    namespace: &str,
    documents: &[(Vec<u8>, Vec<u8>)],
) -> Result<usize, CommandError> {
    let specifications = store.index_specs(namespace)?;
    let mut removed: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
    let mut added: BTreeMap<String, Vec<(Vec<u8>, Vec<u8>)>> = BTreeMap::new();
    if !specifications.is_empty() {
        for (document_key, payload) in documents {
            let Ok(replacement) = parse_bson(payload)
            else {
                continue;
            };
            // 替换出来的文档可能第一次把某个索引字段变成数组，标记后两侧范围退回集合扫描。
            if specifications.iter().any(|specification| {
                !specification.multikey && specification.key.fields.iter().any(|(field, _)| field_array_valued(&replacement, field))
            }) {
                note_multikey(store, &[(replacement.clone(), document_key.clone())], &specifications)?;
            }
            let current_keys = document_index_entries(&replacement, document_key, &specifications);
            let previous_keys = match namespace_document(store, namespace, document_key, session)? {
                Some(previous) => match parse_bson(&previous) {
                    Ok(previous) => document_index_entries(&previous, document_key, &specifications),
                    Err(_) => BTreeSet::new(),
                },
                None => BTreeSet::new(),
            };
            for (entry_namespace, entry_key) in current_keys.difference(&previous_keys) {
                added.entry(entry_namespace.clone()).or_default().push((entry_key.clone(), document_key.clone()));
            }
            for (entry_namespace, entry_key) in previous_keys.difference(&current_keys) {
                removed.entry(entry_namespace.clone()).or_default().push(entry_key.clone());
            }
        }
        // 新增项先落地、旧项最后删除：中途失败只会多出可被完整过滤条件淘汰的条目。
        put_index_keys(store, session, &added)?;
    }
    let written = match session {
        Some(key) => {
            for (document_key, payload) in documents {
                if store.transaction_write(key, namespace, document_key, Some(payload.clone())) == TransactionWrite::Conflict {
                    return Err(write_conflict_error());
                }
            }
            Ok(documents.len())
        }
        None => store.replace(namespace, documents).map_err(|error| CommandError::new(2, error.to_string())),
    };
    if !specifications.is_empty() {
        delete_index_keys(store, session, &removed)?;
    }
    written
}

/// Stages deletions in the session's transaction, or writes them straight to
/// the shard outside one.
fn stage_delete(store: &dyn MongoStorage, session: Option<&str>, namespace: &str, keys: &[Vec<u8>]) -> Result<usize, CommandError> {
    let specifications = store.index_specs(namespace)?;
    let mut removed: Vec<(BsonDocument, Vec<u8>)> = Vec::new();
    if !specifications.is_empty() {
        for key in keys {
            if let Some(payload) = namespace_document(store, namespace, key, session)?
                && let Ok(document) = parse_bson(&payload)
            {
                removed.push((document, key.clone()));
            }
        }
    }
    let written = match session {
        Some(key) => {
            for document_key in keys {
                if store.transaction_write(key, namespace, document_key, None) == TransactionWrite::Conflict {
                    return Err(write_conflict_error());
                }
            }
            Ok(keys.len())
        }
        None => store.delete(namespace, keys).map_err(|error| CommandError::new(2, error.to_string())),
    };
    if !specifications.is_empty() {
        remove_index_entries(store, session, &removed, &specifications)?;
    }
    written
}

/// The namespace as seen by the session: staged writes layered over committed
/// data when a transaction is open, committed data otherwise.
fn namespace_documents(store: &dyn MongoStorage, namespace: &str, session: Option<&str>) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CommandError> {
    if let Some(session) = session
        && let Some(documents) = store.transaction_scan(session, namespace)?
    {
        return Ok(documents);
    }
    Ok(store.scan(namespace)?)
}

/// One document as seen by the session, including staged inserts, updates, and
/// deletes.
fn namespace_document(store: &dyn MongoStorage, namespace: &str, key: &[u8], session: Option<&str>) -> Result<Option<Vec<u8>>, CommandError> {
    if let Some(session) = session
        && let Some(staged) = store.transaction_get(session, namespace, key)?
    {
        return Ok(staged);
    }
    Ok(store.get(namespace, key)?)
}

fn command_bool(command: &BsonDocument, field: &str, default: bool) -> bool {
    match command.get(field) {
        Some(BsonValue::Bool(flag)) => *flag,
        _ => default,
    }
}

fn find_projection(documents: Vec<BsonDocument>, fields: Option<&BsonValue>) -> Result<Vec<BsonDocument>, CommandError> {
    match fields {
        Some(BsonValue::Document(specification)) => project_stage(&documents, specification),
        _ => Ok(documents),
    }
}

/// `findAndModify`: `remove`, `new`, `upsert`, `fields`, and `sort`.
pub fn find_and_modify(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let namespace = namespace_from_command(command, "findAndModify")
        .ok_or_else(|| CommandError::new(73, "findAndModify command missing collection name"))?;
    let session = SessionContext::new(command, store)?;
    let empty = BsonDocument::new();
    let filter = match command.get("query") {
        Some(BsonValue::Document(filter)) => filter,
        _ => &empty,
    };
    validate_filter(filter, "findAndModify")?;
    let remove = command_bool(command, "remove", false);
    let return_new = command_bool(command, "new", false);
    let upsert = command_bool(command, "upsert", false);
    let update = match command.get("update") {
        Some(BsonValue::Document(update)) => Some(update),
        _ => None,
    };
    if remove && update.is_some() {
        return Err(CommandError::new(9, "Cannot specify both an update and remove=true".to_string()));
    }
    if !remove && update.is_none() {
        return Err(CommandError::new(9, "Either an update or remove=true must be specified".to_string()));
    }
    if remove && return_new {
        return Err(CommandError::new(
            9,
            "Cannot specify both new=true and remove=true; 'remove' always returns the deleted document".to_string(),
        ));
    }

    let sort = parse_sort(command.get("sort"));
    let mut documents = collect_matching(store, &namespace, filter, session.key())?;
    sort_documents(&mut documents, &sort);
    let matched = documents.first().cloned();

    if remove {
        let mut last_error = BsonDocument::new();
        if let Some(document) = &matched {
            last_error.push("n".to_string(), BsonValue::Int32(1));
            if let Some(identifier) = document.get("_id") {
                stage_delete(store, session.key(), &namespace, &[value_to_key(identifier)])?;
                if session.key().is_none() && store.oplog_enabled() {
                    store.stage_oplog(OplogEntry::delete(&namespace, collection_uuid(&namespace), identifier));
                }
            }
            let value = find_projection(vec![document.clone()], command.get("fields"))?;
            return Ok(find_and_modify_reply(last_error, value.into_iter().next()));
        }
        last_error.push("n".to_string(), BsonValue::Int32(0));
        return Ok(find_and_modify_reply(last_error, None));
    }

    let update_document = update.expect("update checked above");
    let operator_update = update_document.fields.iter().any(|(field, _)| field.starts_with('$'));
    let index_specs = store.index_specs(&namespace)?;
    if let Some(document) = &matched {
        let identifier_value = document.get("_id").cloned();
        let identifier = identifier_value.as_ref().map(describe_id).unwrap_or_else(|| "<missing>".to_string());
        let updated = if operator_update {
            apply_update(document, update_document, &identifier, filter, false)?
        }
        else {
            build_replacement(update_document, identifier_value.as_ref())?
        };
        if let Some(info) = update_validation_failure(store, &namespace, document, &updated)? {
            return Ok(find_and_modify_validation_failure(info));
        }
        if let Some(error) = parallel_array_error(&updated, &index_specs) {
            return Err(CommandError::new(error.code, error.message));
        }
        let unique_indexes: Vec<IndexSpec> = index_specs.iter().filter(|index| index.unique).cloned().collect();
        if !unique_indexes.is_empty() {
            let key = value_to_key(identifier_value.as_ref().unwrap_or(&BsonValue::Null));
            let mut indexed: BTreeMap<Vec<u8>, Vec<BTreeSet<Vec<u8>>>> = BTreeMap::new();
            for (other_key, payload) in namespace_documents(store, &namespace, session.key())? {
                if let Ok(candidate) = parse_bson(&payload) {
                    indexed.insert(other_key, unique_indexes.iter().map(|spec| tuples_of(&candidate, spec)).collect());
                }
            }
            if let Some((specification, values)) = find_unique_conflict(&unique_indexes, &updated, &key, &indexed) {
                let failure = duplicate_index_error(0, &namespace, specification, &values);
                let mut error =
                    CommandError::new(11000, format!("Plan executor error during findAndModify :: caused by :: {}", failure.message));
                if let Some(pattern) = failure.key_pattern {
                    error = error.with_detail("keyPattern", BsonValue::Document(pattern));
                }
                if let Some(value) = failure.key_value {
                    error = error.with_detail("keyValue", BsonValue::Document(value));
                }
                return Err(error);
            }
        }
        let key = value_to_key(identifier_value.as_ref().unwrap_or(&BsonValue::Null));
        stage_replace(store, session.key(), &namespace, &[(key, encode_bson(&updated))])?;
        stage_update_oplog(store, session.key(), &namespace, &[(document.clone(), updated.clone())]);
        let mut last_error = BsonDocument::new();
        last_error.push("n".to_string(), BsonValue::Int32(1));
        last_error.push("updatedExisting".to_string(), BsonValue::Bool(true));
        let selected = if return_new { updated } else { document.clone() };
        let value = find_projection(vec![selected], command.get("fields"))?;
        return Ok(find_and_modify_reply(last_error, value.into_iter().next()));
    }

    if upsert {
        let mut inserted = if operator_update {
            let base = upsert_base(filter);
            let identifier = base.get("_id").map(describe_id).unwrap_or_else(|| "<missing>".to_string());
            apply_update(&base, update_document, &identifier, filter, true)?
        }
        else {
            let query_identifier = match filter.get("_id") {
                Some(BsonValue::Document(operators)) if has_operator(operators) => None,
                Some(value) => Some(value.clone()),
                None => None,
            };
            build_replacement(update_document, query_identifier.as_ref())?
        };
        if inserted.get("_id").is_none() {
            inserted.fields.insert(0, ("_id".to_string(), BsonValue::ObjectId(generate_object_id())));
        }
        if let Some(info) = insert_validation_failure(store, &namespace, &inserted)? {
            return Ok(find_and_modify_validation_failure(info));
        }
        let identifier = inserted.get("_id").cloned().unwrap_or(BsonValue::Null);
        let candidates = vec![(0_usize, inserted.clone(), identifier.clone())];
        let mut write_errors = Vec::new();
        let (inserted_count, accepted) = commit_insert(store, session.key(), &namespace, &candidates, &Vec::new(), true, &mut write_errors)?;
        if let Some(error) = write_errors.first() {
            return Err(CommandError::new(error.code, error.message.clone()));
        }
        if inserted_count == 1 {
            stage_insert_oplog(store, &namespace, &accepted);
        }
        let mut last_error = BsonDocument::new();
        last_error.push("n".to_string(), BsonValue::Int32(1));
        last_error.push("updatedExisting".to_string(), BsonValue::Bool(false));
        last_error.push("upserted".to_string(), identifier);
        let selected = if return_new { Some(inserted) } else { None };
        let value = match selected {
            Some(document) => find_projection(vec![document], command.get("fields"))?.into_iter().next(),
            None => None,
        };
        return Ok(find_and_modify_reply(last_error, value));
    }

    let mut last_error = BsonDocument::new();
    last_error.push("n".to_string(), BsonValue::Int32(0));
    last_error.push("updatedExisting".to_string(), BsonValue::Bool(false));
    Ok(find_and_modify_reply(last_error, None))
}

fn find_and_modify_reply(last_error: BsonDocument, value: Option<BsonDocument>) -> BsonDocument {
    let mut response = BsonDocument::new();
    response.push("lastErrorObject".to_string(), BsonValue::Document(last_error));
    response.push("value".to_string(), value.map(BsonValue::Document).unwrap_or(BsonValue::Null));
    response.push("ok".to_string(), BsonValue::Double(1.0));
    response
}

fn find_and_modify_validation_failure(info: BsonDocument) -> BsonDocument {
    let mut last_error = BsonDocument::new();
    last_error.push("n".to_string(), BsonValue::Int32(0));
    last_error.push("updatedExisting".to_string(), BsonValue::Bool(false));
    let mut response = BsonDocument::new();
    response.push("lastErrorObject".to_string(), BsonValue::Document(last_error));
    response.push("value".to_string(), BsonValue::Null);
    response.push("code".to_string(), BsonValue::Int32(121));
    response.push("errmsg".to_string(), BsonValue::String("Document failed validation".to_string()));
    response.push("errInfo".to_string(), BsonValue::Document(info));
    response.push("ok".to_string(), BsonValue::Double(1.0));
    response
}

/// `commitTransaction`: applies the session's staged writes, reporting a write
/// conflict when another transaction changed one of those documents first.
pub fn commit_transaction(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let Some(session) = transaction_key(command)
    else {
        return Err(CommandError::new(40414, "commitTransaction must be run within a session"));
    };
    let txn_number = transaction_number(command)?.unwrap_or(0);
    match store.transaction_commit(&session, txn_number) {
        TransactionCommit::Committed | TransactionCommit::AlreadyCommitted => Ok(ok_reply()),
        TransactionCommit::Conflict => Err(write_conflict_error()),
        TransactionCommit::AlreadyAborted => Err(no_such_transaction_error(txn_number)),
        TransactionCommit::TooOld { newer } => Err(transaction_too_old_error(command, txn_number, newer)),
        TransactionCommit::NoTransaction { active } => Err(transaction_mismatch_error(txn_number, active)),
    }
}

/// `abortTransaction`: discards the session's staged writes.
pub fn abort_transaction(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let Some(session) = transaction_key(command)
    else {
        return Err(CommandError::new(40414, "abortTransaction must be run within a session"));
    };
    let txn_number = transaction_number(command)?.unwrap_or(0);
    match store.transaction_abort(&session, txn_number) {
        TransactionAbort::Aborted => Ok(ok_reply()),
        TransactionAbort::AlreadyCommitted => Err(transaction_committed_error(txn_number)),
        TransactionAbort::AlreadyAborted => Err(no_such_transaction_error(txn_number)),
        TransactionAbort::TooOld { newer } => Err(transaction_too_old_error(command, txn_number, newer)),
        TransactionAbort::NoTransaction { active } => Err(transaction_mismatch_error(txn_number, active)),
    }
}

/// `distinct`: nested paths, one-level array flattening, query filtering, and
/// first-occurrence ordering.
pub fn distinct(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let namespace =
        namespace_from_command(command, "distinct").ok_or_else(|| CommandError::new(73, "distinct command missing collection name"))?;
    let session = SessionContext::new(command, store)?;
    let Some(key) = command.get("key")
    else {
        return Err(CommandError::new(40414, "BSON field 'distinct.key' is missing but a required field"));
    };
    let BsonValue::String(key) = key
    else {
        return Err(CommandError::new(
            14,
            format!("BSON field 'distinctCommandRequest.key' is the wrong type '{}', expected type 'string'", bson_type_name(key)),
        ));
    };
    let empty = BsonDocument::new();
    let filter = match command.get("query") {
        Some(BsonValue::Document(filter)) => filter,
        _ => &empty,
    };
    validate_filter(filter, "distinct")?;

    let segments: Vec<&str> = key.split('.').collect();
    let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut values = Vec::new();
    for (_key, payload) in namespace_documents(store, &namespace, session.key())? {
        let Ok(document) = parse_bson(&payload)
        else {
            continue;
        };
        if !document_matches(&document, filter)? {
            continue;
        }
        let mut matched = Vec::new();
        collect_values_at(&document, &segments, &mut matched);
        for value in matched {
            collect_distinct(value, &mut seen, &mut values);
        }
    }
    // `mongod` collects distinct values in a sorted BSON set, so the reply is
    // ordered by canonical type and value instead of by scan order.
    values.sort_by(compare_values);
    let mut response = BsonDocument::new();
    response.push("values".to_string(), BsonValue::Array(values));
    response.push("ok".to_string(), BsonValue::Double(1.0));
    Ok(response)
}

fn collect_distinct(value: &BsonValue, seen: &mut BTreeSet<Vec<u8>>, values: &mut Vec<BsonValue>) {
    match value {
        BsonValue::Array(items) => {
            for item in items {
                if seen.insert(value_to_key(item)) {
                    values.push(item.clone());
                }
            }
        }
        other => {
            if seen.insert(value_to_key(other)) {
                values.push(other.clone());
            }
        }
    }
}

/// `bulkWrite` over `admin`, with ordered / unordered execution and the
/// aggregate counters the server reports alongside per-operation results.
pub fn bulk_write(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    if command.database() != "admin" {
        return Err(CommandError::new(13, "bulkWrite may only be run against the admin database.".to_string()));
    }
    let Some(BsonValue::Array(operations)) = command.get("ops")
    else {
        return Err(CommandError::new(40414, "BSON field 'bulkWrite.ops' is missing but a required field"));
    };
    if operations.is_empty() || operations.len() > 100_000 {
        return Err(CommandError::new(16, format!("Write batch sizes must be between 1 and 100000. Got {} operations.", operations.len())));
    }
    let Some(BsonValue::Array(namespaces)) = command.get("nsInfo")
    else {
        return Err(CommandError::new(40414, "BSON field 'bulkWrite.nsInfo' is missing but a required field"));
    };
    let names: Vec<String> = namespaces
        .iter()
        .map(|entry| match entry {
            BsonValue::Document(entry) => match entry.get("ns") {
                Some(BsonValue::String(name)) => Ok(name.clone()),
                _ => Err(CommandError::new(2, "nsInfo entries require a string 'ns' field".to_string())),
            },
            _ => Err(CommandError::new(2, "nsInfo entries must be documents".to_string())),
        })
        .collect::<Result<_, _>>()?;
    let ordered = command_bool(command, "ordered", true);
    let errors_only = command_bool(command, "errorsOnly", false);

    let mut results = Vec::new();
    let mut n_errors = 0_i64;
    let mut n_inserted = 0_i64;
    let mut n_matched = 0_i64;
    let mut n_modified = 0_i64;
    let mut n_upserted = 0_i64;
    let mut n_deleted = 0_i64;

    for (index, operation) in operations.iter().enumerate() {
        let BsonValue::Document(operation) = operation
        else {
            return Err(CommandError::new(2, "BulkWrite ops entry must be a document".to_string()));
        };
        let kind = ["insert", "update", "delete"]
            .into_iter()
            .find(|field| operation.get(field).is_some())
            .ok_or_else(|| CommandError::new(2, "BulkWrite ops entry must be an insert, update, or delete".to_string()))?;
        let Some(BsonValue::Int32(namespace_index)) = operation.get(kind)
        else {
            return Err(CommandError::new(
                2,
                format!("BulkWrite ops entry {} has an invalid nsInfo index.", render_shell_value(&BsonValue::Document(operation.clone()))),
            ));
        };
        let namespace_index = usize::try_from(*namespace_index).ok();
        let Some(namespace_index) = namespace_index.filter(|index| *index < names.len())
        else {
            return Err(CommandError::new(
                2,
                format!("BulkWrite ops entry {} has an invalid nsInfo index.", render_shell_value(&BsonValue::Document(operation.clone()))),
            ));
        };
        let namespace = &names[namespace_index];
        let (database, collection) =
            namespace.split_once('.').ok_or_else(|| CommandError::new(2, format!("nsInfo namespace '{namespace}' is not fully qualified")))?;

        let reply = match kind {
            "insert" => {
                let Some(document) = operation.get("document")
                else {
                    return Err(CommandError::new(2, "BulkWrite insert entry requires a 'document' field".to_string()));
                };
                let mut request = BsonDocument::new();
                request.push("insert".to_string(), BsonValue::String(collection.to_string()));
                request.push("documents".to_string(), BsonValue::Array(vec![document.clone()]));
                request.push("$db".to_string(), BsonValue::String(database.to_string()));
                insert(&request, store)?
            }
            "update" => {
                let Some(filter) = operation.get("filter")
                else {
                    return Err(CommandError::new(2, "BulkWrite update entry requires a 'filter' field".to_string()));
                };
                let Some(update_mods) = operation.get("updateMods")
                else {
                    return Err(CommandError::new(2, "BulkWrite update entry requires an 'updateMods' field".to_string()));
                };
                let mut specification = BsonDocument::new();
                specification.push("q".to_string(), filter.clone());
                specification.push("u".to_string(), update_mods.clone());
                if let Some(value) = operation.get("multi") {
                    specification.push("multi".to_string(), value.clone());
                }
                if let Some(value) = operation.get("upsert") {
                    specification.push("upsert".to_string(), value.clone());
                }
                let mut request = BsonDocument::new();
                request.push("update".to_string(), BsonValue::String(collection.to_string()));
                request.push("updates".to_string(), BsonValue::Array(vec![BsonValue::Document(specification)]));
                request.push("$db".to_string(), BsonValue::String(database.to_string()));
                update(&request, store)?
            }
            _ => {
                let Some(filter) = operation.get("filter")
                else {
                    return Err(CommandError::new(2, "BulkWrite delete entry requires a 'filter' field".to_string()));
                };
                let limit = if command_bool(operation, "multi", false) { 0 } else { 1 };
                let mut specification = BsonDocument::new();
                specification.push("q".to_string(), filter.clone());
                specification.push("limit".to_string(), BsonValue::Int32(limit));
                let mut request = BsonDocument::new();
                request.push("delete".to_string(), BsonValue::String(collection.to_string()));
                request.push("deletes".to_string(), BsonValue::Array(vec![BsonValue::Document(specification)]));
                request.push("$db".to_string(), BsonValue::String(database.to_string()));
                delete(&request, store)?
            }
        };

        let write_errors = match reply.get("writeErrors") {
            Some(BsonValue::Array(errors)) => errors,
            _ => &Vec::new(),
        };
        let failed = !write_errors.is_empty();
        let mut result = BsonDocument::new();
        result.push("ok".to_string(), BsonValue::Double(if failed { 0.0 } else { 1.0 }));
        result.push("idx".to_string(), BsonValue::Int32(index as i32));
        if let Some(first) = write_errors.first() {
            let BsonValue::Document(first) = first
            else {
                return Err(CommandError::new(2, "writeErrors entries must be documents".to_string()));
            };
            for (field, value) in &first.fields {
                if field == "index" {
                    continue;
                }
                result.push(field.clone(), value.clone());
            }
            result.push("n".to_string(), BsonValue::Int32(0));
            n_errors += 1;
        }
        else {
            let n = reply_count(&reply, "n");
            match kind {
                "insert" => {
                    n_inserted += n;
                    result.push("n".to_string(), count_value(n));
                }
                "update" => {
                    if let Some(upserted) = reply.get("upserted") {
                        n_upserted += 1;
                        result.push("n".to_string(), count_value(n));
                        let modified = reply_count(&reply, "nModified");
                        n_modified += modified;
                        result.push("nModified".to_string(), count_value(modified));
                        let upserted = match upserted {
                            BsonValue::Array(items) => items.first().and_then(|item| match item {
                                BsonValue::Document(entry) => entry.get("_id").cloned(),
                                _ => None,
                            }),
                            other => Some(other.clone()),
                        };
                        if let Some(identifier) = upserted {
                            let mut entry = BsonDocument::new();
                            entry.push("_id".to_string(), identifier);
                            result.push("upserted".to_string(), BsonValue::Document(entry));
                        }
                    }
                    else {
                        n_matched += n;
                        let modified = reply_count(&reply, "nModified");
                        n_modified += modified;
                        result.push("n".to_string(), count_value(n));
                        result.push("nModified".to_string(), count_value(modified));
                    }
                }
                _ => {
                    n_deleted += n;
                    result.push("n".to_string(), count_value(n));
                }
            }
        }

        if !errors_only || failed {
            results.push(BsonValue::Document(result));
        }
        if failed && ordered {
            break;
        }
    }

    let mut cursor = BsonDocument::new();
    cursor.push("id".to_string(), BsonValue::Int64(0));
    cursor.push("firstBatch".to_string(), BsonValue::Array(results));
    cursor.push("ns".to_string(), BsonValue::String("admin.$cmd.bulkWrite".to_string()));
    let mut response = BsonDocument::new();
    response.push("cursor".to_string(), BsonValue::Document(cursor));
    response.push("nErrors".to_string(), count_value(n_errors));
    response.push("nInserted".to_string(), count_value(n_inserted));
    response.push("nMatched".to_string(), count_value(n_matched));
    response.push("nModified".to_string(), count_value(n_modified));
    response.push("nUpserted".to_string(), count_value(n_upserted));
    response.push("nDeleted".to_string(), count_value(n_deleted));
    response.push("ok".to_string(), BsonValue::Double(1.0));
    Ok(response)
}

/// `listDatabases`, derived from the stored namespaces.
pub fn list_databases(store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let mut names: Vec<String> =
        store.namespaces()?.iter().filter_map(|namespace| namespace.split_once('.').map(|(db, _)| db.to_string())).collect();
    names.sort();
    names.dedup();

    let databases: Vec<BsonValue> = names
        .into_iter()
        .map(|name| {
            let mut entry = BsonDocument::new();
            entry.push("name".to_string(), BsonValue::String(name));
            entry.push("sizeOnDisk".to_string(), BsonValue::Int64(0));
            entry.push("empty".to_string(), BsonValue::Bool(false));
            BsonValue::Document(entry)
        })
        .collect();

    let mut response = BsonDocument::new();
    response.push("databases".to_string(), BsonValue::Array(databases));
    response.push("totalSize".to_string(), BsonValue::Int64(0));
    response.push("totalSizeMb".to_string(), BsonValue::Int64(0));
    response.push("ok".to_string(), BsonValue::Double(1.0));
    Ok(response)
}

/// `dropDatabase` reply.
///
/// `mongod 8.0.9` answers `{ ok: 1 }` and nothing else; the `dropped` field
/// printed by `mongosh`'s `db.dropDatabase()` helper is added client-side.
/// The command drops every collection of the target database together with its
/// indexes, collection options and the stored users, and the store guarantees a
/// failure leaves the database either fully present or fully absent.
pub fn drop_database(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let value = match command.get("dropDatabase") {
        None | Some(BsonValue::Null) => {
            return Err(CommandError::new(40414, "BSON field 'dropDatabase.dropDatabase' is missing but a required field"));
        }
        Some(value) => value,
    };
    let accepted = match value {
        BsonValue::Int32(number) => *number == 1,
        BsonValue::Int64(number) => *number == 1,
        // `mongod` accepts any double inside `[1, 2)` and rejects 2.0.
        BsonValue::Double(number) => (1.0..2.0).contains(number),
        BsonValue::Decimal128(number) => number.as_str().parse::<f64>().map(|value| value == 1.0).unwrap_or(false),
        other => {
            return Err(CommandError::new(
                14,
                format!(
                    "BSON field 'dropDatabase.dropDatabase' is the wrong type '{}', expected types '[int, double, long, decimal']",
                    bson_type_name(other)
                ),
            ));
        }
    };
    if !accepted {
        return Err(CommandError::new(5255100, "Have to pass 1 as 'drop' parameter"));
    }
    let database = command.database();
    if database == "admin" {
        return Err(CommandError::new(20, "Dropping the 'admin' database is prohibited."));
    }
    let prefix = format!("{database}.");
    let namespaces: Vec<String> = store.namespaces()?.into_iter().filter(|namespace| namespace.starts_with(&prefix)).collect();
    let had_namespaces = !namespaces.is_empty();
    for namespace in &namespaces {
        for specification in store.index_specs(namespace)? {
            store
                .clear_reserved(&crate::storage::index_namespace(namespace, &specification.name))
                .map_err(|error| CommandError::new(2, format!("dropDatabase failed: {error}")))?;
        }
    }
    if had_namespaces && store.oplog_enabled() {
        let mut object = BsonDocument::new();
        object.push("dropDatabase".to_string(), BsonValue::Int32(1));
        store.stage_oplog(OplogEntry::database_command(&format!("{database}.$cmd"), object));
    }
    store.drop_database(&database).map_err(|error| CommandError::new(2, format!("dropDatabase failed: {error}")))?;
    let mut response = BsonDocument::new();
    response.push("ok".to_string(), BsonValue::Double(1.0));
    Ok(response)
}

/// Bytes one index entry occupies in the catalog and in the key material it
/// owns: the `listIndexes` definition plus the length-prefixed key tuple and the
/// 8-byte record id every entry stores next to its key.
const INDEX_RECORD_ID_BYTES: i64 = 8;

/// Key bytes one document contributes to one index.
fn index_record_bytes(values: &[BsonValue]) -> i64 {
    index_tuple(values).len() as i64 + INDEX_RECORD_ID_BYTES
}

/// Per-index footprint in `listIndexes` order, `_id_` first.
///
/// The definition bytes come from the same document `listIndexes` reports, and
/// the entry bytes come from the key material every stored document really
/// contributes (`index_entries` expands multikey values exactly like the write
/// path does). The oplog is a capped collection without an `_id` index, so it
/// reports no indexes.
///
/// The values are logical key bytes, not `mongod`'s WiredTiger page allocation;
/// `KNOWN-DIFFERENCES.md` records the measured difference.
pub fn collection_index_sizes(store: &dyn MongoStorage, namespace: &str) -> Result<Vec<(String, i64)>, CommandError> {
    if store.oplog_namespace().as_deref() == Some(namespace) {
        return Ok(Vec::new());
    }
    let specifications = store.index_specs(namespace)?;
    let mut sizes: Vec<(String, i64)> = Vec::with_capacity(1 + specifications.len());
    sizes.push(("_id_".to_string(), encode_bson(&id_index_listing()).len() as i64));
    for specification in &specifications {
        sizes.push((specification.name.clone(), encode_bson(&stored_index_listing(specification)).len() as i64));
    }
    for (_, payload) in store.scan(namespace)? {
        let Ok(document) = parse_bson(&payload)
        else {
            continue;
        };
        if let Some(identifier) = document.get("_id") {
            sizes[0].1 += index_record_bytes(std::slice::from_ref(identifier));
        }
        for (position, specification) in specifications.iter().enumerate() {
            for values in index_entries(&document, specification) {
                sizes[position + 1].1 += index_record_bytes(&values);
            }
        }
    }
    Ok(sizes)
}

/// `dbStats` reply, computed from the documents and keys the shard really holds.
///
/// `objects` counts the stored documents, `dataSize` sums their encoded BSON
/// lengths and `avgObjSize` is `dataSize / objects`; all three are exact.
/// `storageSize` is the byte count of the records this database owns in the
/// `.yykv` shard (key plus payload). `indexSize` sums the per-index footprints
/// `collStats` reports (definitions plus key material), so the two commands and
/// `collection.stats()` cannot disagree.
///
/// `filesystem` carries host-measured `(used, total)` bytes for the data
/// directory; `None` means the database does not exist, which is the case where
/// `mongod 8.0.9` also reports zero.
pub fn db_stats(command: &BsonDocument, store: &dyn MongoStorage, filesystem: Option<(i64, i64)>) -> Result<BsonDocument, CommandError> {
    let scale = db_stats_scale(command)?;
    let database = command.database();
    let prefix = format!("{database}.");

    let mut collections = 0_i64;
    let mut objects = 0_i64;
    let mut data_size = 0_i64;
    let mut storage_size = 0_i64;
    let mut index_size = 0_i64;
    let mut indexes = 0_i64;
    for namespace in store.namespaces()? {
        let Some(collection) = namespace.strip_prefix(&prefix)
        else {
            continue;
        };
        // `mongod 8.0.9` leaves `system.*` collections out of `dbStats`.
        if collection.starts_with("system.") {
            continue;
        }
        collections += 1;
        let sizes = collection_index_sizes(store, &namespace)?;
        indexes += sizes.len() as i64;
        index_size += sizes.iter().map(|(_, bytes)| *bytes).sum::<i64>();
        for (key, value) in store.scan(&namespace)? {
            objects += 1;
            data_size += value.len() as i64;
            storage_size += (key.len() + value.len()) as i64;
        }
    }

    let scaled = |bytes: i64| -> BsonValue { if scale == 1 { count_value(bytes) } else { count_value(bytes / scale) } };
    let mut document = BsonDocument::new();
    document.push("db".to_string(), BsonValue::String(database));
    document.push("collections".to_string(), count_value(collections));
    document.push("views".to_string(), BsonValue::Int32(0));
    document.push("objects".to_string(), count_value(objects));
    document.push(
        "avgObjSize".to_string(),
        if objects == 0 {
            BsonValue::Int32(0)
        }
        else if data_size % objects == 0 {
            count_value(data_size / objects)
        }
        else {
            BsonValue::Double(data_size as f64 / objects as f64)
        },
    );
    document.push("dataSize".to_string(), if scale == 1 { count_value(data_size) } else { BsonValue::Double(data_size as f64 / scale as f64) });
    document.push("storageSize".to_string(), scaled(storage_size));
    document.push("indexes".to_string(), count_value(indexes));
    document.push("indexSize".to_string(), scaled(index_size));
    document.push("totalSize".to_string(), scaled(storage_size + index_size));
    document.push("scaleFactor".to_string(), count_value(scale));
    let (used, total) = filesystem.unwrap_or((0, 0));
    document.push("fsUsedSize".to_string(), filesystem_value(used, scale));
    document.push("fsTotalSize".to_string(), filesystem_value(total, scale));
    document.push("ok".to_string(), BsonValue::Double(1.0));
    Ok(document)
}

/// `scale` of `dbStats`, validated the way the real command does.
///
/// A missing field means factor 1, a factor below 1 is `BadValue`, and a
/// fractional double is ignored (`mongod 8.0.9` reports `scaleFactor: 1` for
/// `scale: 1.5` instead of rejecting it). Non-numeric values are `TypeMismatch`.
fn db_stats_scale(command: &BsonDocument) -> Result<i64, CommandError> {
    let Some(value) = command.get("scale")
    else {
        return Ok(1);
    };
    let scale = match value {
        BsonValue::Int32(number) => i64::from(*number),
        BsonValue::Int64(number) => *number,
        BsonValue::Double(number) => integral_value(*number).unwrap_or(1),
        BsonValue::Decimal128(number) => number.as_str().parse::<f64>().ok().and_then(integral_value).unwrap_or(1),
        other => {
            return Err(CommandError::new(
                14,
                format!(
                    "BSON field 'dbStats.scale' is the wrong type '{}', expected types '[decimal, double, int, long']",
                    bson_type_name(other)
                ),
            ));
        }
    };
    if scale < 1 {
        return Err(CommandError::new(2, "Scale factor must be greater than zero"));
    }
    Ok(scale)
}

/// Filesystem bytes keep `mongod`'s `long` type at factor 1 and shrink with it otherwise.
fn filesystem_value(bytes: i64, scale: i64) -> BsonValue {
    if scale == 1 { BsonValue::Int64(bytes) } else { count_value(bytes / scale) }
}

/// `collStats`, the per-collection half of `dbStats`.
///
/// The counts and bytes come from the same walk `dbStats` does, and
/// `indexSizes` comes from `collection_index_sizes`, so `dbStats.indexSize` is
/// exactly the sum of the `totalIndexSize` values this command reports.
///
/// `mongod 8.0.9` reports a missing collection with zero counts and empty
/// `indexSizes`; the storage-engine fields (`wiredTiger`, `indexDetails`,
/// `indexBuilds`) do not exist for `.yykv` and are omitted.
pub fn coll_stats(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let scale = coll_stats_scale(command)?;
    let collection = match command.get("collStats") {
        Some(BsonValue::String(name)) => name.clone(),
        Some(other) => {
            return Err(CommandError::new(14, format!("collection name has invalid type {}", bson_type_name(other))));
        }
        None => return Err(CommandError::new(14, "collection name has invalid type int")),
    };
    let namespace = format!("{}.{collection}", command.database());
    let scaled = |bytes: i64| -> BsonValue { if scale == 1 { count_value(bytes) } else { count_value(bytes / scale) } };

    let mut document = BsonDocument::new();
    document.push("ns".to_string(), BsonValue::String(namespace.clone()));
    if !store.namespace_exists(&namespace)? {
        document.push("size".to_string(), count_value(0));
        document.push("count".to_string(), count_value(0));
        document.push("numOrphanDocs".to_string(), count_value(0));
        document.push("storageSize".to_string(), count_value(0));
        document.push("totalSize".to_string(), count_value(0));
        document.push("nindexes".to_string(), count_value(0));
        document.push("totalIndexSize".to_string(), count_value(0));
        document.push("indexSizes".to_string(), BsonValue::Document(BsonDocument::new()));
        document.push("scaleFactor".to_string(), count_value(scale));
        document.push("ok".to_string(), BsonValue::Double(1.0));
        return Ok(document);
    }

    let mut count = 0_i64;
    let mut data_size = 0_i64;
    let mut storage_size = 0_i64;
    for (key, value) in store.scan(&namespace)? {
        count += 1;
        data_size += value.len() as i64;
        storage_size += (key.len() + value.len()) as i64;
    }
    let sizes = collection_index_sizes(store, &namespace)?;
    let total_index_size: i64 = sizes.iter().map(|(_, bytes)| *bytes).sum();

    document.push("size".to_string(), scaled(data_size));
    document.push("count".to_string(), count_value(count));
    if count > 0 {
        // `mongod 8.0.9` truncates this one to an integer, unlike `dbStats`.
        document.push("avgObjSize".to_string(), count_value(data_size / count));
    }
    document.push("numOrphanDocs".to_string(), count_value(0));
    document.push("storageSize".to_string(), scaled(storage_size));
    document.push("freeStorageSize".to_string(), scaled(0));
    document.push("capped".to_string(), BsonValue::Bool(false));
    document.push("nindexes".to_string(), count_value(sizes.len() as i64));
    document.push("totalIndexSize".to_string(), scaled(total_index_size));
    let mut index_sizes = BsonDocument::new();
    for (name, bytes) in &sizes {
        index_sizes.push(name.clone(), scaled(*bytes));
    }
    document.push("indexSizes".to_string(), BsonValue::Document(index_sizes));
    document.push("totalSize".to_string(), scaled(storage_size + total_index_size));
    document.push("scaleFactor".to_string(), count_value(scale));
    document.push("ok".to_string(), BsonValue::Double(1.0));
    Ok(document)
}

/// `collStats` scale, with the wording `mongod 8.0.9` uses for this command.
///
/// A missing field means factor 1, a fractional double is ignored, a factor
/// below 1 is `BadValue` and a non-numeric value is `TypeMismatch`.
fn coll_stats_scale(command: &BsonDocument) -> Result<i64, CommandError> {
    let Some(value) = command.get("scale")
    else {
        return Ok(1);
    };
    let scale = match value {
        BsonValue::Int32(number) => i64::from(*number),
        BsonValue::Int64(number) => *number,
        BsonValue::Double(number) => integral_value(*number).unwrap_or(1),
        BsonValue::Decimal128(number) => number.as_str().parse::<f64>().ok().and_then(integral_value).unwrap_or(1),
        other => {
            return Err(CommandError::new(
                14,
                format!(
                    "BSON field 'collStats.scale' is the wrong type '{}', expected types '[decimal, double, int, long']",
                    bson_type_name(other)
                ),
            ));
        }
    };
    if scale < 1 {
        return Err(CommandError::new(2, format!("BSON field 'scale' value must be >= 1, actual value '{scale}'")));
    }
    Ok(scale)
}

/// Index options read from one `createIndexes` specification, already validated.
fn index_type_error(specification: &BsonDocument, detail: String) -> CommandError {
    CommandError::new(
        14,
        format!("Error in specification {} :: caused by :: {detail}", render_shell_value(&BsonValue::Document(specification.clone()))),
    )
}

fn requested_bool(specification: &BsonDocument, field: &str, quirky: bool) -> Result<Option<bool>, CommandError> {
    match specification.get(field) {
        None => Ok(None),
        Some(BsonValue::Bool(value)) => Ok(Some(*value)),
        Some(other) => {
            let detail = if quirky {
                format!("The field '{field} has value {field}: {}, which is not convertible to bool", render_shell_value(other))
            }
            else {
                format!("The field 'name' must be a bool, but got {}", type_label(other))
            };
            Err(index_type_error(specification, detail))
        }
    }
}

fn requested_wildcard_projection(specification: &BsonDocument) -> Result<Option<BsonDocument>, CommandError> {
    match specification.get("wildcardProjection") {
        None => Ok(None),
        Some(BsonValue::Document(projection)) => Ok(Some(projection.clone())),
        Some(other) => Err(CommandError::new(
            14,
            format!(
                "BSON field 'createIndexes.indexes.wildcardProjection' is the wrong type '{}', expected type 'object'",
                bson_type_name(other)
            ),
        )),
    }
}

fn index_specification_display(specification: &BsonDocument) -> BsonValue {
    // `mongod` echoes the specification in the order the caller sent it.
    let mut display = specification.clone();
    // `v: 2` appears only in the wildcard-specific validation messages.
    let wildcard = match specification.get("key") {
        Some(BsonValue::Document(key)) => key.fields.iter().any(|(field, _)| field == "$**" || field.ends_with(".$**")),
        _ => false,
    };
    if wildcard {
        display.push("v".to_string(), BsonValue::Int32(2));
    }
    BsonValue::Document(display)
}

/// `mongod` reports hashed-index validation failures from the index builder, so
/// the message carries the build wrapper and the normalized specification.
fn index_build_failure(code: i32, namespace: &str, specification: &BsonDocument, reason: &str) -> CommandError {
    let mut normalized = BsonDocument::new();
    normalized.push("v".to_string(), BsonValue::Int32(2));
    if matches!(specification.get("unique"), Some(BsonValue::Bool(true))) {
        normalized.push("unique".to_string(), BsonValue::Bool(true));
    }
    if let Some(key) = specification.get("key") {
        normalized.push("key".to_string(), key.clone());
    }
    if let Some(name) = specification.get("name") {
        normalized.push("name".to_string(), name.clone());
    }
    let uuid = collection_uuid_text(namespace);
    CommandError::new(
        code,
        format!(
            "Index build failed: {uuid}: Collection {namespace} ( {uuid} ) :: caused by :: Caught exception during index builder ((None)) initialization on namespace{namespace} ({uuid}). 1 index specs provided. First index spec: {} :: caused by :: {reason}",
            render_shell_value(&BsonValue::Document(normalized))
        ),
    )
}

/// Validate the index key pattern the way `mongod` does: numeric directions
/// must be non-zero, string values are index plugins, and wildcard keys are
/// `$**` or `path.$**`. Text indexes are not implemented yet and are refused
/// explicitly rather than silently stored under the wrong catalog shape.
fn validate_index_key(
    key: &BsonDocument,
    specification: &BsonDocument,
    namespace: &str,
    unique: bool,
    sparse: Option<bool>,
) -> Result<(), CommandError> {
    let mut hashed = 0usize;
    let mut text = false;
    let mut wildcard = false;
    for (field, value) in &key.fields {
        if field == "$**" || field.ends_with(".$**") {
            wildcard = true;
        }
        match value {
            BsonValue::Int32(number) if *number == 0 => {
                return Err(CommandError::new(
                    67,
                    format!(
                        "Error in specification {} :: caused by :: Values in the index key pattern cannot be 0.",
                        render_shell_value(&index_specification_display(specification))
                    ),
                ));
            }
            BsonValue::Int64(number) if *number == 0 => {
                return Err(CommandError::new(
                    67,
                    format!(
                        "Error in specification {} :: caused by :: Values in the index key pattern cannot be 0.",
                        render_shell_value(&index_specification_display(specification))
                    ),
                ));
            }
            BsonValue::Double(number) if *number == 0.0 => {
                return Err(CommandError::new(
                    67,
                    format!(
                        "Error in specification {} :: caused by :: Values in the index key pattern cannot be 0.",
                        render_shell_value(&index_specification_display(specification))
                    ),
                ));
            }
            BsonValue::Int32(_) | BsonValue::Int64(_) | BsonValue::Double(_) => {}
            BsonValue::String(plugin) => match plugin.as_str() {
                "hashed" => hashed += 1,
                "text" => text = true,
                "2dsphere" | "2d" | "geoHaystack" => {}
                other => {
                    return Err(CommandError::new(
                        67,
                        format!(
                            "Error in specification {} :: caused by :: Unknown index plugin '{other}'",
                            render_shell_value(&index_specification_display(specification))
                        ),
                    ));
                }
            },
            other => {
                return Err(CommandError::new(
                    67,
                    format!(
                        "Error in specification {} :: caused by :: Values in the index key pattern must be 1 or -1 or a supported index plugin name, but found {}",
                        render_shell_value(&index_specification_display(specification)),
                        bson_type_name(other)
                    ),
                ));
            }
        }
    }
    if text {
        return Err(CommandError::new(59, "text indexes are not supported yet"));
    }
    if hashed > 1 {
        let mut key_text = String::new();
        for (field, value) in &key.fields {
            if !key_text.is_empty() {
                key_text.push_str(", ");
            }
            key_text.push_str(field);
            key_text.push_str(": ");
            key_text.push_str(&render_shell_value(value));
        }
        return Err(index_build_failure(
            31303,
            namespace,
            specification,
            &format!("A maximum of one index field is allowed to be hashed but found {hashed} for 'key' {{ {key_text} }}"),
        ));
    }
    if wildcard {
        if unique {
            return Err(CommandError::new(
                67,
                format!(
                    "Error in specification {} :: caused by :: Index type 'wildcard' does not support the 'unique' option",
                    render_shell_value(&index_specification_display(specification))
                ),
            ));
        }
        if sparse == Some(true) {
            return Err(CommandError::new(
                67,
                format!(
                    "Error in specification {} :: caused by :: Index type 'wildcard' does not support the 'sparse' option",
                    render_shell_value(&index_specification_display(specification))
                ),
            ));
        }
    }
    if hashed == 1 && unique {
        return Err(index_build_failure(
            16764,
            namespace,
            specification,
            "Currently hashed indexes cannot guarantee uniqueness. Use a regular index.",
        ));
    }
    Ok(())
}

fn requested_ttl(specification: &BsonDocument) -> Result<Option<i32>, CommandError> {
    let Some(value) = specification.get("expireAfterSeconds")
    else {
        return Ok(None);
    };
    let numeric = match value {
        BsonValue::Int32(seconds) => i64::from(*seconds),
        BsonValue::Int64(seconds) => *seconds,
        BsonValue::Double(seconds) => *seconds as i64,
        other => {
            return Err(CommandError::new(
                67,
                format!(
                    ". Index spec: {} :: caused by :: TTL index 'expireAfterSeconds' option must be numeric, but received a type of '{}",
                    render_shell_value(&BsonValue::Document(specification.clone())),
                    type_label(other)
                ),
            ));
        }
    };
    if !(i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(&numeric) {
        return Err(CommandError::new(
            67,
            format!(
                ". Index spec: {} :: caused by :: TTL index 'expireAfterSeconds' must be within the range of a 32-bit integer",
                render_shell_value(&BsonValue::Document(specification.clone()))
            ),
        ));
    }
    if numeric < 0 {
        return Err(CommandError::new(
            67,
            format!(
                ". Index spec: {} :: caused by :: TTL index 'expireAfterSeconds' option cannot be less than 0",
                render_shell_value(&BsonValue::Document(specification.clone()))
            ),
        ));
    }
    Ok(Some(numeric as i32))
}

fn type_label(value: &BsonValue) -> &'static str {
    match value {
        BsonValue::Document(_) => "object",
        BsonValue::Array(_) => "array",
        BsonValue::String(_) => "string",
        BsonValue::Int32(_) | BsonValue::Int64(_) | BsonValue::Double(_) => "number",
        BsonValue::Bool(_) => "bool",
        BsonValue::Null => "null",
        BsonValue::Undefined => "undefined",
        BsonValue::ObjectId(_) => "objectId",
        BsonValue::Binary(_) => "binData",
        BsonValue::DateTime(_) => "date",
        BsonValue::Timestamp { .. } => "timestamp",
        BsonValue::Decimal128(_) => "decimal",
        BsonValue::Regex { .. } => "regex",
        BsonValue::Code(_) => "javascript",
        BsonValue::CodeWithScope { .. } => "javascriptWithScope",
        BsonValue::Symbol(_) => "symbol",
        BsonValue::DbPointer { .. } => "dbPointer",
        BsonValue::MinKey => "minKey",
        BsonValue::MaxKey => "maxKey",
    }
}

/// Normalizes one `collation` document the way `listIndexes` reports it.
fn requested_collation(specification: &BsonDocument) -> Result<Option<BsonDocument>, CommandError> {
    let Some(value) = specification.get("collation")
    else {
        return Ok(None);
    };
    let BsonValue::Document(collation) = value
    else {
        return Err(index_type_error(specification, format!("The field 'collation' must be an object, but got {}", type_label(value))));
    };
    let mut definition = specification.clone();
    definition.push("v".to_string(), BsonValue::Int32(2));
    let failed = |code: i32, detail: String| {
        CommandError::new(
            code,
            format!(
                "failed to add collation information to index spec for index creation: {} :: caused by :: {detail}",
                render_shell_value(&BsonValue::Document(definition.clone()))
            ),
        )
    };
    let Some(BsonValue::String(locale)) = collation.get("locale")
    else {
        if collation.get("locale").is_none() {
            return Err(failed(40414, "BSON field 'collation.locale' is missing but a required field".to_string()));
        }
        return Err(failed(2, format!("Field 'locale' is invalid in: {}", render_shell_value(&BsonValue::Document(collation.clone())))));
    };
    if !known_locale(locale) {
        return Err(failed(2, format!("Field 'locale' is invalid in: {}", render_shell_value(&BsonValue::Document(collation.clone())))));
    }
    let strength = match collation.get("strength") {
        None => 3,
        Some(BsonValue::Int32(value)) => i64::from(*value),
        Some(BsonValue::Int64(value)) => *value,
        Some(BsonValue::Double(value)) => *value as i64,
        Some(other) => {
            return Err(failed(2, format!("BSON field 'strength' value must be a number, actual value '{}'", render_shell_value(other))));
        }
    };
    if !(1..=5).contains(&strength) {
        let detail = if strength > 5 {
            format!("BSON field 'strength' value must be <= 5, actual value '{strength}'")
        }
        else {
            format!("BSON field 'strength' value must be >= 1, actual value '{strength}'")
        };
        return Err(failed(2, detail));
    }
    let flag = |field: &str| matches!(collation.get(field), Some(BsonValue::Bool(true)));
    let case_first = match collation.get("caseFirst") {
        Some(BsonValue::String(value)) if ["off", "upper", "lower"].contains(&value.as_str()) => value.clone(),
        _ => "off".to_string(),
    };
    let alternate = match collation.get("alternate") {
        Some(BsonValue::String(value)) if ["non-ignorable", "shifted"].contains(&value.as_str()) => value.clone(),
        _ => "non-ignorable".to_string(),
    };
    let max_variable = match collation.get("maxVariable") {
        Some(BsonValue::String(value)) if ["punct", "space"].contains(&value.as_str()) => value.clone(),
        _ => "punct".to_string(),
    };
    let mut normalized = BsonDocument::new();
    normalized.push("locale".to_string(), BsonValue::String(locale.clone()));
    normalized.push("caseLevel".to_string(), BsonValue::Bool(flag("caseLevel")));
    normalized.push("caseFirst".to_string(), BsonValue::String(case_first));
    normalized.push("strength".to_string(), BsonValue::Int32(strength as i32));
    normalized.push("numericOrdering".to_string(), BsonValue::Bool(flag("numericOrdering")));
    normalized.push("alternate".to_string(), BsonValue::String(alternate));
    normalized.push("maxVariable".to_string(), BsonValue::String(max_variable));
    normalized.push("normalization".to_string(), BsonValue::Bool(flag("normalization")));
    normalized.push("backwards".to_string(), BsonValue::Bool(flag("backwards")));
    normalized.push("version".to_string(), BsonValue::String("57.1".to_string()));
    Ok(Some(normalized))
}

/// ICU knows far more locales than this gateway needs; the corpus uses `en`, and
/// an obviously synthetic token such as `bogus` must not silently pass.
fn known_locale(locale: &str) -> bool {
    matches!(
        locale,
        "en" | "en_US"
            | "fr"
            | "fr_FR"
            | "es"
            | "de"
            | "it"
            | "pt"
            | "nl"
            | "sv"
            | "da"
            | "fi"
            | "no"
            | "zh"
            | "zh_Hans"
            | "zh_Hant"
            | "ja"
            | "ko"
            | "ru"
            | "ar"
            | "hi"
            | "th"
            | "vi"
            | "tr"
            | "pl"
            | "cs"
            | "hu"
            | "ro"
            | "el"
            | "he"
            | "id"
            | "ms"
            | "uk"
            | "simple"
    )
}

fn partial_filter_from(specification: &BsonDocument) -> Result<Option<BsonDocument>, CommandError> {
    let Some(value) = specification.get("partialFilterExpression")
    else {
        return Ok(None);
    };
    let BsonValue::Document(filter) = value
    else {
        return Err(index_type_error(
            specification,
            format!("The field 'partialFilterExpression' must be an object, but got {}", type_label(value)),
        ));
    };
    if let Err(error) = validate_filter(filter, "createIndexes") {
        // `createIndexes` reports the bare operator message; the field/operator
        // indices `validate_filter` appends belong to query commands.
        let message = error.message.split(" (operation:").next().unwrap_or(&error.message);
        return Err(CommandError::new(
            error.code,
            format!("Error in specification {} :: caused by :: {message}", render_shell_value(&BsonValue::Document(specification.clone()))),
        ));
    }
    Ok(Some(filter.clone()))
}

fn is_id_key(key: &BsonDocument) -> bool {
    key.fields.len() == 1
        && key.fields[0].0 == "_id"
        && matches!(key.fields[0].1, BsonValue::Int32(1) | BsonValue::Int64(1) | BsonValue::Double(1.0))
}

fn same_index_options(left: &IndexSpec, right: &IndexSpec) -> bool {
    // `hidden` is not part of index equivalence on `mongod 8.0.9`: a visible
    // index and a hidden index over the same key are the same index.
    left.key == right.key
        && left.unique == right.unique
        && left.is_sparse() == right.is_sparse()
        && left.expire_after_seconds == right.expire_after_seconds
        && left.collation == right.collation
        && left.partial_filter_expression == right.partial_filter_expression
        && left.wildcard_projection == right.wildcard_projection
}

fn render_index_definition(specification: &IndexSpec) -> String {
    let mut definition = BsonDocument::new();
    definition.push("v".to_string(), BsonValue::Int32(2));
    if specification.unique {
        definition.push("unique".to_string(), BsonValue::Bool(true));
    }
    if specification.hidden {
        definition.push("hidden".to_string(), BsonValue::Bool(true));
    }
    definition.push("key".to_string(), BsonValue::Document(specification.key.clone()));
    definition.push("name".to_string(), BsonValue::String(specification.name.clone()));
    if let Some(sparse) = specification.sparse {
        definition.push("sparse".to_string(), BsonValue::Bool(sparse));
    }
    if let Some(seconds) = specification.expire_after_seconds {
        definition.push("expireAfterSeconds".to_string(), BsonValue::Int32(seconds));
    }
    if let Some(collation) = &specification.collation {
        definition.push("collation".to_string(), BsonValue::Document(collation.clone()));
    }
    if let Some(filter) = &specification.partial_filter_expression {
        definition.push("partialFilterExpression".to_string(), BsonValue::Document(filter.clone()));
    }
    if let Some(projection) = &specification.wildcard_projection {
        definition.push("wildcardProjection".to_string(), BsonValue::Document(projection.clone()));
    }
    render_shell_value(&BsonValue::Document(definition))
}

/// `createIndexes` for single-field and compound keys, `unique`, and explicit
/// names.
///
/// `name` is required: `mongod 8.0.9` rejects a specification without it, so
/// this gateway does not invent a default name either.
pub fn create_indexes(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let namespace = namespace_from_command(command, "createIndexes")
        .ok_or_else(|| CommandError::new(73, "createIndexes command missing collection name"))?;
    let Some(BsonValue::Array(specifications)) = command.get("indexes")
    else {
        return Err(CommandError::new(40414, "BSON field 'createIndexes.indexes' is missing but a required field"));
    };
    if specifications.is_empty() {
        return Err(CommandError::new(2, "Must specify at least one index to create"));
    }

    let existing = store.index_specs(&namespace)?;
    let existed = store.namespace_exists(&namespace)?;
    let mut known = existing.clone();
    let mut accepted: Vec<IndexSpec> = Vec::new();
    let mut skipped = 0_usize;

    for item in specifications {
        let BsonValue::Document(specification) = item
        else {
            return Err(CommandError::new(2, "index specification must be a document"));
        };
        let Some(BsonValue::Document(key)) = specification.get("key")
        else {
            return Err(CommandError::new(
                9,
                format!(
                    "Error in specification {} :: caused by :: The 'key' field is a required property of an index specification",
                    render_shell_value(&BsonValue::Document(specification.clone()))
                ),
            ));
        };
        if key.fields.is_empty() {
            return Err(CommandError::new(
                67,
                format!(
                    "Error in specification {} :: caused by :: Index keys cannot be empty.",
                    render_shell_value(&BsonValue::Document(specification.clone()))
                ),
            ));
        }
        let Some(BsonValue::String(name)) = specification.get("name")
        else {
            return Err(CommandError::new(
                9,
                format!(
                    "Error in specification {} :: caused by :: The 'name' field is a required property of an index specification",
                    render_shell_value(&BsonValue::Document(specification.clone()))
                ),
            ));
        };
        if name.is_empty() {
            let mut rendered = specification.clone();
            rendered.push("v".to_string(), BsonValue::Int32(2));
            return Err(CommandError::new(
                67,
                format!(
                    "Error in specification {} :: caused by :: index name cannot be empty",
                    render_shell_value(&BsonValue::Document(rendered))
                ),
            ));
        }
        let unique = requested_bool(specification, "unique", true)?.unwrap_or(false);
        let sparse = requested_bool(specification, "sparse", true)?;
        let hidden = requested_bool(specification, "hidden", false)?.unwrap_or(false);
        let expire_after_seconds = requested_ttl(specification)?;
        let collation = requested_collation(specification)?;
        let partial_filter_expression = partial_filter_from(specification)?;
        let wildcard_projection = requested_wildcard_projection(specification)?;
        validate_index_key(key, specification, &namespace, unique, sparse)?;

        let mut candidate = IndexSpec {
            namespace: namespace.clone(),
            name: name.clone(),
            key: key.clone(),
            unique,
            sparse,
            hidden,
            expire_after_seconds,
            collation,
            partial_filter_expression,
            wildcard_projection,
            multikey: false,
        };

        if is_id_key(key) {
            // `_id` is always indexed under the implicit `_id_` name. A second
            // specification for it never creates a second index.
            if candidate.expire_after_seconds.is_some() {
                let mut definition = specification.clone();
                definition.push("v".to_string(), BsonValue::Int32(2));
                return Err(CommandError::new(
                    197,
                    format!(
                        "The field 'expireAfterSeconds' is not valid for an _id index specification. Specification: {}",
                        render_shell_value(&BsonValue::Document(definition))
                    ),
                ));
            }
            skipped += 1;
            continue;
        }
        if candidate.expire_after_seconds.is_some() && candidate.key.fields.len() != 1 {
            return Err(CommandError::new(
                67,
                format!(
                    "TTL indexes are single-field indexes, compound indexes do not support TTL. Index spec: {}",
                    render_shell_value(&BsonValue::Document(specification.clone()))
                ),
            ));
        }

        if let Some(hit) = known.iter().find(|index| index.name == *name) {
            if same_index_options(hit, &candidate) {
                skipped += 1;
                continue;
            }
            return Err(CommandError::new(
                86,
                format!(
                    "An existing index has the same name as the requested index. When index names are not specified, they are auto generated and can cause conflicts. Please refer to our documentation. Requested index: {}, existing index: {}",
                    render_index_definition(&candidate),
                    render_index_definition(hit)
                ),
            ));
        }
        if let Some(hit) = known.iter().find(|index| same_index_options(index, &candidate)) {
            if hit.hidden == candidate.hidden {
                return Err(CommandError::new(85, format!("Index already exists with a different name: {}", hit.name)));
            }
            return Err(CommandError::new(
                85,
                format!(
                    "Index build failed: 00000000-0000-0000-0000-000000000000: Collection {namespace} ( 00000000-0000-0000-0000-000000000000 ) :: caused by :: An equivalent index already exists with a different name and options. Requested index: {}, existing index: {}",
                    render_index_definition(&candidate),
                    render_index_definition(hit)
                ),
            ));
        }

        candidate.name = name.clone();
        if candidate.unique {
            verify_unique_build(store, &namespace, &candidate)?;
        }
        known.push(candidate.clone());
        accepted.push(candidate);
    }

    if !accepted.is_empty() {
        store.add_indexes(&namespace, &accepted).map_err(|error| CommandError::new(2, format!("createIndexes failed: {error}")))?;
        // 逐个数：新建索引需要回填，旧目录里缺条目的索引也在这里补齐，避免
        // 查询把「条目缺失」当成「没有匹配」。
        for specification in store.index_specs(&namespace)? {
            if !store.reserved_has_entries(&crate::storage::index_namespace(&namespace, &specification.name))? {
                backfill_index(store, &namespace, &specification)?;
            }
        }
        if let Some(BsonValue::String(collection)) = command.get("createIndexes") {
            stage_create_indexes_oplog(store, &namespace, collection, &accepted);
        }
    }

    let before = 1 + existing.len() as i64;
    let after = before + accepted.len() as i64;
    let mut reply = BsonDocument::new();
    reply.push("numIndexesBefore".to_string(), count_value(before));
    reply.push("numIndexesAfter".to_string(), count_value(after));
    if accepted.is_empty() {
        reply.push("note".to_string(), BsonValue::String("all indexes already exist".to_string()));
    }
    else {
        reply.push("createdCollectionAutomatically".to_string(), BsonValue::Bool(!existed));
        if skipped > 0 {
            reply.push("note".to_string(), BsonValue::String("index already exists".to_string()));
        }
    }
    reply.push("ok".to_string(), BsonValue::Double(1.0));
    Ok(reply)
}

/// 用集合现有文档重建一个索引的全部条目。
///
/// 先清空同名索引项，避免上一次删除中断或崩溃留下的旧条目与新定义混在一起。
fn backfill_index(store: &dyn MongoStorage, namespace: &str, specification: &IndexSpec) -> Result<(), CommandError> {
    let entry_namespace = crate::storage::index_namespace(namespace, &specification.name);
    store.clear_reserved(&entry_namespace)?;
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut multikey = false;
    for (document_key, payload) in store.scan(namespace)? {
        let Ok(document) = parse_bson(&payload)
        else {
            continue;
        };
        if !multikey {
            multikey = specification.key.fields.iter().any(|(field, _)| field_array_valued(&document, field));
        }
        for values in index_entries(&document, specification) {
            entries.push((index_entry_key(&values, &document_key), document_key.clone()));
        }
    }
    store.put_reserved(&entry_namespace, &entries).map_err(|error| CommandError::new(2, format!("createIndexes backfill failed: {error}")))?;
    if multikey && !specification.multikey {
        store
            .mark_index_multikey(namespace, &specification.name)
            .map_err(|error| CommandError::new(2, format!("createIndexes multikey marking failed: {error}")))?;
    }
    Ok(())
}

/// Refuses to build a unique index over documents that already collide.
fn verify_unique_build(store: &dyn MongoStorage, namespace: &str, specification: &IndexSpec) -> Result<(), CommandError> {
    let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
    for (_, payload) in store.scan(namespace)? {
        let Ok(document) = parse_bson(&payload)
        else {
            continue;
        };
        for values in index_entries(&document, specification) {
            if !seen.insert(index_tuple(&values)) {
                let key_value =
                    specification.key.fields.iter().zip(values.iter()).fold(BsonDocument::new(), |mut document, ((field, _), value)| {
                        document.push(field.clone(), value.clone());
                        document
                    });
                return Err(CommandError::new(
                    11000,
                    format!(
                        "Index build failed: {}: Collection {} ( {} ) :: caused by :: {}",
                        seeded_uuid(&format!("{namespace}|{}|build", specification.name)),
                        namespace,
                        seeded_uuid(namespace),
                        duplicate_message(namespace, specification, &values)
                    ),
                )
                .with_detail("keyPattern", BsonValue::Document(specification.key.clone()))
                .with_detail("keyValue", BsonValue::Document(key_value)));
            }
        }
    }
    Ok(())
}

/// Deterministic UUID-shaped identifier.
///
/// `mongod` prints its random collection UUID and index-build operation UUID in
/// this message. Their concrete values are opaque and change per build, so this
/// gateway derives stable ones from the namespace instead: the message template
/// is byte-identical while the two UUID slots stay reproducible for tests.
fn seeded_uuid(seed: &str) -> String {
    let high = fnv1a64(0xcbf2_9ce4_8422_2325, seed);
    let low = fnv1a64(0x8422_2325_cbf2_9ce4, seed);
    let high = (high & 0xffff_ffff_ffff_0fff) | 0x0000_0000_0000_4000;
    let low = (low & 0x3fff_ffff_ffff_ffff) | 0x8000_0000_0000_0000;
    format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}", high >> 32, (high >> 16) & 0xffff, high & 0xffff, low >> 48, low & 0xffff_ffff_ffff)
}

fn fnv1a64(offset: u64, text: &str) -> u64 {
    let mut hash = offset;
    for byte in text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn duplicate_message(namespace: &str, specification: &IndexSpec, values: &[BsonValue]) -> String {
    let rendered: Vec<String> =
        specification.key.fields.iter().zip(values).map(|((field, _), value)| format!("{field}: {}", render_shell_value(value))).collect();
    format!("E11000 duplicate key error collection: {namespace} index: {} dup key: {{ {} }}", specification.name, rendered.join(", "))
}

/// `listIndexes` document for the implicit `_id_` index, which `mongod` reports
/// first and never stores in the catalog.
fn id_index_listing() -> BsonDocument {
    let mut identifier = BsonDocument::new();
    identifier.push("_id".to_string(), BsonValue::Int32(1));
    let mut entry = BsonDocument::new();
    entry.push("v".to_string(), BsonValue::Int32(2));
    entry.push("key".to_string(), BsonValue::Document(identifier));
    entry.push("name".to_string(), BsonValue::String("_id_".to_string()));
    entry
}

/// `listIndexes` document for one stored index, options included.
fn stored_index_listing(specification: &IndexSpec) -> BsonDocument {
    let mut entry = BsonDocument::new();
    entry.push("v".to_string(), BsonValue::Int32(2));
    entry.push("key".to_string(), BsonValue::Document(specification.key.clone()));
    entry.push("name".to_string(), BsonValue::String(specification.name.clone()));
    if let Some(projection) = &specification.wildcard_projection {
        entry.push("wildcardProjection".to_string(), BsonValue::Document(projection.clone()));
    }
    if specification.unique {
        entry.push("unique".to_string(), BsonValue::Bool(true));
    }
    if specification.hidden {
        entry.push("hidden".to_string(), BsonValue::Bool(true));
    }
    if let Some(sparse) = specification.sparse {
        entry.push("sparse".to_string(), BsonValue::Bool(sparse));
    }
    if let Some(collation) = &specification.collation {
        entry.push("collation".to_string(), BsonValue::Document(collation.clone()));
    }
    if let Some(filter) = &specification.partial_filter_expression {
        entry.push("partialFilterExpression".to_string(), BsonValue::Document(filter.clone()));
    }
    if let Some(seconds) = specification.expire_after_seconds {
        entry.push("expireAfterSeconds".to_string(), BsonValue::Int32(seconds));
    }
    entry
}

/// `listIndexes`: `_id_` first, then user indexes in creation order.
pub fn list_indexes(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let namespace =
        namespace_from_command(command, "listIndexes").ok_or_else(|| CommandError::new(73, "listIndexes command missing collection name"))?;
    if !store.namespace_exists(&namespace)? {
        return Err(CommandError::new(26, format!("ns does not exist: {namespace}")));
    }

    let mut first_batch: Vec<BsonValue> = Vec::new();
    first_batch.push(BsonValue::Document(id_index_listing()));
    for specification in store.index_specs(&namespace)? {
        first_batch.push(BsonValue::Document(stored_index_listing(&specification)));
    }

    let mut cursor = BsonDocument::new();
    cursor.push("id".to_string(), BsonValue::Int64(0));
    cursor.push("ns".to_string(), BsonValue::String(namespace));
    cursor.push("firstBatch".to_string(), BsonValue::Array(first_batch));
    let mut reply = BsonDocument::new();
    reply.push("cursor".to_string(), BsonValue::Document(cursor));
    reply.push("ok".to_string(), BsonValue::Double(1.0));
    Ok(reply)
}

/// `dropIndexes` by name, `*`, or key pattern; `_id_` is never droppable.
pub fn drop_indexes(command: &BsonDocument, store: &dyn MongoStorage) -> Result<BsonDocument, CommandError> {
    let namespace =
        namespace_from_command(command, "dropIndexes").ok_or_else(|| CommandError::new(73, "dropIndexes command missing collection name"))?;
    if !store.namespace_exists(&namespace)? {
        return Err(CommandError::new(26, format!("ns not found {namespace}")));
    }
    let existing = store.index_specs(&namespace)?;
    let name = match command.get("index") {
        Some(BsonValue::String(name)) => name.clone(),
        Some(BsonValue::Document(key)) => {
            let mut identifier = BsonDocument::new();
            identifier.push("_id".to_string(), BsonValue::Int32(1));
            if key_pattern_matches(key, &identifier) {
                return Err(CommandError::new(72, "cannot drop _id index"));
            }
            match existing.iter().find(|index| key_pattern_matches(key, &index.key)) {
                Some(index) => index.name.clone(),
                None => {
                    return Err(CommandError::new(
                        27,
                        format!("can't find index with key: {}", render_shell_value(&BsonValue::Document(key.clone()))),
                    ));
                }
            }
        }
        Some(other) => {
            return Err(CommandError::new(
                14,
                format!("BSON field 'dropIndexes.index' is the wrong type '{}', expected types '[string']", bson_type_name(other)),
            ));
        }
        None => return Err(CommandError::new(40414, "BSON field 'dropIndexes.index' is missing but a required field")),
    };

    let before = 1 + existing.len() as i64;
    if name == "_id_" {
        return Err(CommandError::new(72, "cannot drop _id index"));
    }
    if name != "*" && !existing.iter().any(|index| index.name == name) {
        return Err(CommandError::new(27, format!("index not found with name [{name}]")));
    }

    let dropped: Vec<IndexSpec> =
        if name == "*" { existing.clone() } else { existing.iter().filter(|index| index.name == name).cloned().collect() };
    store.drop_indexes(&namespace, &name).map_err(|error| CommandError::new(2, format!("dropIndexes failed: {error}")))?;
    for specification in &dropped {
        store
            .clear_reserved(&crate::storage::index_namespace(&namespace, &specification.name))
            .map_err(|error| CommandError::new(2, format!("dropIndexes cleanup failed: {error}")))?;
    }
    if store.oplog_enabled() {
        let uuid = collection_uuid(&namespace);
        for specification in &dropped {
            let mut object = BsonDocument::new();
            if let Some(BsonValue::String(collection)) = command.get("dropIndexes") {
                object.push("dropIndexes".to_string(), BsonValue::String(collection.clone()));
            }
            object.push("index".to_string(), BsonValue::String(name.clone()));
            let mut old = BsonDocument::new();
            old.push("v".to_string(), BsonValue::Int32(2));
            old.push("key".to_string(), BsonValue::Document(specification.key.clone()));
            old.push("name".to_string(), BsonValue::String(specification.name.clone()));
            store.stage_oplog(OplogEntry::command(&command_namespace(&namespace), Some(uuid.clone()), object, Some(old)));
        }
    }

    let mut reply = BsonDocument::new();
    reply.push("nIndexesWas".to_string(), count_value(before));
    if name == "*" {
        reply.push("msg".to_string(), BsonValue::String("non-_id indexes dropped for collection".to_string()));
    }
    reply.push("ok".to_string(), BsonValue::Double(1.0));
    Ok(reply)
}

/// Outcome of one TTL monitor pass.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TtlReport {
    /// Whether the sleep window had elapsed, so a pass actually ran.
    pub scanned: bool,
    /// Documents removed by this pass.
    pub deleted: usize,
    /// Per-namespace removal counts, in catalog order.
    pub collections: Vec<(String, usize)>,
}

/// One pass of the TTL monitor.
///
/// `mongod 8.0.9` removes `expireAfterSeconds` documents from a background
/// thread that wakes every `ttlMonitorSleepSecs` (default 60). A document whose
/// deadline falls between two wake-ups is removed by the next wake-up, so the
/// visible delay is `[0, ttlMonitorSleepSecs]` rather than millisecond exact.
/// This primitive keeps that shape: it returns without scanning unless the
/// window since the previous pass has elapsed, so a document never disappears
/// merely because its own deadline passed.
///
/// Hosts call it from their own loop and pass `now_ms`; that caller-supplied
/// clock is also the only clock the scan reads, so tests can compress the
/// window without changing the semantics.
///
/// Only a single-field index expires documents. An indexed value that is not a
/// [`BsonValue::DateTime`] never expires, and array values expire when any one
/// element is a date past the deadline.
pub fn ttl_monitor(store: &dyn MongoStorage, now_ms: i64) -> Result<TtlReport, CommandError> {
    let mut report = TtlReport::default();
    if !store.claim_ttl_pass(now_ms) {
        return Ok(report);
    }
    report.scanned = true;

    for namespace in store.namespaces()? {
        let mut deleted = 0_usize;
        for specification in store.index_specs(&namespace)? {
            let Some(seconds) = specification.expire_after_seconds
            else {
                continue;
            };
            // `mongod` creates a compound TTL index but never expires documents
            // from it. A hidden index still expires: `hidden` only keeps the
            // index out of the planner.
            if specification.key.fields.len() != 1 {
                continue;
            }
            let Some((field, _)) = specification.key.fields.first()
            else {
                continue;
            };
            let threshold = now_ms.saturating_sub(i64::from(seconds) * 1_000);
            let mut candidates: Vec<Vec<u8>> = Vec::new();
            for (key, payload) in store.scan(&namespace)? {
                let Ok(document) = parse_bson(&payload)
                else {
                    continue;
                };
                if let Some(filter) = &specification.partial_filter_expression
                    && !document_matches(&document, filter).unwrap_or(false)
                {
                    continue;
                }
                let expired = field_values(&document, field).iter().any(|value| match value {
                    BsonValue::DateTime(milliseconds) => *milliseconds <= threshold,
                    _ => false,
                });
                if expired {
                    candidates.push(key);
                }
            }
            if candidates.is_empty() {
                continue;
            }
            deleted += stage_delete(store, None, &namespace, &candidates)?;
        }
        if deleted > 0 {
            report.deleted += deleted;
            report.collections.push((namespace, deleted));
        }
    }
    Ok(report)
}

/// Host-facing document for one monitor pass, shared by both bindings.
pub fn ttl_report_document(report: &TtlReport) -> BsonDocument {
    let mut document = BsonDocument::new();
    document.push("scanned".to_string(), BsonValue::Bool(report.scanned));
    document.push("deleted".to_string(), BsonValue::Int32(report.deleted as i32));
    let collections = report
        .collections
        .iter()
        .map(|(namespace, count)| {
            let mut entry = BsonDocument::new();
            entry.push("ns".to_string(), BsonValue::String(namespace.clone()));
            entry.push("deleted".to_string(), BsonValue::Int32(*count as i32));
            BsonValue::Document(entry)
        })
        .collect();
    document.push("collections".to_string(), BsonValue::Array(collections));
    document
}

fn namespace_from_command(document: &BsonDocument, command_key: &str) -> Option<String> {
    let database = document.database();
    let collection = match document.get(command_key) {
        Some(BsonValue::String(collection)) if valid_collection_name(collection) => collection.clone(),
        _ => return None,
    };
    Some(format!("{database}.{collection}"))
}

fn valid_collection_name(name: &str) -> bool {
    if name.is_empty() || name.contains('$') || name.contains('\0') {
        return false;
    }
    if name.starts_with('.') || name.ends_with('.') {
        return false;
    }
    !name.contains("..")
}

fn cursor_reply(namespace: &str, batch_key: &str, batch: Vec<BsonValue>, cursor_id: i64) -> BsonDocument {
    let mut cursor = BsonDocument::new();
    cursor.push(batch_key.to_string(), BsonValue::Array(batch));
    cursor.push("id".to_string(), BsonValue::Int64(cursor_id));
    cursor.push("ns".to_string(), BsonValue::String(namespace.to_string()));

    let mut response = BsonDocument::new();
    response.push("ok".to_string(), BsonValue::Double(1.0));
    response.push("cursor".to_string(), BsonValue::Document(cursor));
    response
}

fn collect_matching(
    store: &dyn MongoStorage,
    namespace: &str,
    filter: &BsonDocument,
    session: Option<&str>,
) -> Result<Vec<BsonDocument>, CommandError> {
    if let Some(identifier) = single_id_lookup(filter) {
        let key = value_to_key(identifier);
        return match namespace_document(store, namespace, &key, session)? {
            Some(bytes) => match parse_bson(&bytes) {
                Ok(document) if document_matches(&document, filter)? => Ok(vec![document]),
                _ => Ok(Vec::new()),
            },
            None => Ok(Vec::new()),
        };
    }

    let mut documents = Vec::new();
    for (_key, value) in namespace_documents(store, namespace, session)? {
        if let Ok(candidate) = parse_bson(&value)
            && document_matches(&candidate, filter)?
        {
            documents.push(candidate);
        }
    }
    Ok(documents)
}

/// 与 [`collect_matching`] 相同，但命中数达到 `stop_after` 后停止读取。
///
/// 第二个返回值是实际读到的文档数，供 `explain` 报告 `docsExamined`：提前停
/// 之后不能再报整个集合的大小。`need_total` 为假时调用方不看这个数字。
fn collect_matching_bounded(
    store: &dyn MongoStorage,
    namespace: &str,
    filter: &BsonDocument,
    session: Option<&str>,
    need_total: bool,
    stop_after: Option<usize>,
) -> Result<(Vec<BsonDocument>, i64), CommandError> {
    if stop_after.is_none() {
        let documents = collect_matching(store, namespace, filter, session)?;
        let total = if need_total { namespace_documents(store, namespace, session)?.len() as i64 } else { 0 };
        return Ok((documents, total));
    }
    if let Some(identifier) = single_id_lookup(filter) {
        let key = value_to_key(identifier);
        return Ok(match namespace_document(store, namespace, &key, session)? {
            Some(bytes) => match parse_bson(&bytes) {
                Ok(document) if document_matches(&document, filter)? => (vec![document], 1),
                _ => (Vec::new(), 1),
            },
            None => (Vec::new(), 0),
        });
    }
    let mut documents = Vec::new();
    let mut examined = 0i64;
    for (_key, value) in namespace_documents(store, namespace, session)? {
        examined += 1;
        if let Ok(candidate) = parse_bson(&value)
            && document_matches(&candidate, filter)?
        {
            documents.push(candidate);
            if stop_after.is_some_and(|bound| documents.len() >= bound) {
                break;
            }
        }
    }
    Ok((documents, examined))
}

fn single_id_lookup(filter: &BsonDocument) -> Option<&BsonValue> {
    if filter.fields.len() != 1 {
        return None;
    }
    let (key, value) = filter.fields.first()?;
    if key != "_id" {
        return None;
    }
    match value {
        BsonValue::Document(operators) if operators.fields.iter().any(|(operator, _)| operator.starts_with('$')) => None,
        _ => Some(value),
    }
}

/// Thrown for a pipeline stage this gateway does not implement, naming both the
/// stage and the supported surface.
fn unsupported_stage(stage: &str) -> CommandError {
    CommandError::new(
        59,
        format!(
            "unknown aggregation stage: {stage} (supported: $match, $sort, $limit, $skip, $count, $unwind, $lookup, $graphLookup, $facet, $bucket, $bucketAuto, $setWindowFields, $sortByCount, $group, $addFields, $set, $project, $replaceWith, $merge, $out, $unionWith, $sample, $densify, $fill, $redact)"
        ),
    )
}

/// `$limit` / `$skip` take a positive integer.
fn positive_stage_integer(stage: &str, value: &BsonValue) -> Result<usize, CommandError> {
    match int_value(value) {
        Some(number) if number > 0 => Ok(number as usize),
        _ => Err(CommandError::new(2, format!("{stage} requires a positive integer"))),
    }
}

/// `$count` output field name: non-empty, no leading `$`, no `.`.
fn validate_count_name(name: &str) -> Result<(), CommandError> {
    if name.is_empty() || name.starts_with('$') || name.contains('.') {
        return Err(CommandError::new(
            2,
            format!("$count output field name `{name}` must be non-empty, must not start with `$` and must not contain `.`"),
        ));
    }
    Ok(())
}

/// `$sort` accepts 1/-1 directions, rejects duplicates and caps the key count at 32.
fn parse_sort_stage(specification: &BsonDocument) -> Result<Vec<(String, i32)>, CommandError> {
    if specification.fields.is_empty() {
        return Err(CommandError::new(2, "$sort requires at least one field"));
    }
    if specification.fields.len() > 32 {
        return Err(CommandError::new(2, "$sort accepts at most 32 keys"));
    }
    let mut sort: Vec<(String, i32)> = Vec::new();
    for (field, direction) in specification.fields.iter() {
        if sort.iter().any(|(existing, _)| existing == field) {
            return Err(CommandError::new(2, format!("$sort field `{field}` appears more than once")));
        }
        match int_value(direction) {
            Some(value) if value == 1 || value == -1 => sort.push((field.clone(), value as i32)),
            _ => return Err(CommandError::new(2, format!("$sort direction for `{field}` must be 1 or -1"))),
        }
    }
    Ok(sort)
}

/// Sorts one stage input. Array fields use the lowest element ascending and the
/// highest element descending, as the `$sort` page specifies.
fn sort_key(document: &BsonDocument, path: &str, direction: i32) -> BsonValue {
    let value = lookup_field(document, path);
    let Some(value) = value
    else {
        // Missing fields compare like null; the manual states two documents that
        // both lack the field sort on "the same" value.
        return BsonValue::Null;
    };
    match value {
        BsonValue::Array(items) if !items.is_empty() => {
            let mut keys = items.iter();
            let mut chosen = keys.next().unwrap().clone();
            for item in keys {
                let ordering = compare_values(&chosen, item);
                if (direction >= 0 && ordering == Ordering::Greater) || (direction < 0 && ordering == Ordering::Less) {
                    chosen = item.clone();
                }
            }
            chosen
        }
        other => other.clone(),
    }
}

/// `$unwind`: string form drops null/missing/empty-array documents; the document
/// form adds `preserveNullAndEmptyArrays` and `includeArrayIndex`.
fn unwind_stage(documents: &[BsonDocument], body: &BsonValue) -> Result<Vec<BsonDocument>, CommandError> {
    let (path, preserve, index_field) = match body {
        BsonValue::String(path) => (unwind_path(path)?, false, None),
        BsonValue::Document(specification) => {
            let BsonValue::String(path) = specification.get("path").unwrap_or(&BsonValue::Null)
            else {
                return Err(CommandError::new(2, "$unwind requires a `path` string"));
            };
            let path = unwind_path(path)?;
            let preserve = match specification.get("preserveNullAndEmptyArrays") {
                None => false,
                Some(BsonValue::Bool(flag)) => *flag,
                Some(_) => return Err(CommandError::new(2, "$unwind `preserveNullAndEmptyArrays` must be a boolean")),
            };
            let index_field = match specification.get("includeArrayIndex") {
                None => None,
                Some(BsonValue::String(field)) if !field.is_empty() && !field.starts_with('$') => Some(field.clone()),
                Some(_) => {
                    return Err(CommandError::new(2, "$unwind `includeArrayIndex` must be a non-empty string that does not start with `$`"));
                }
            };
            (path, preserve, index_field)
        }
        _ => return Err(CommandError::new(2, "$unwind requires a path string or a document")),
    };

    let mut output = Vec::new();
    for document in documents {
        let value = lookup_field(document, &path).cloned();
        match value {
            Some(BsonValue::Array(items)) if !items.is_empty() => {
                for (index, item) in items.into_iter().enumerate() {
                    let mut expanded = document.clone();
                    set_field_path(&mut expanded, &path, item);
                    if let Some(field) = &index_field {
                        expanded.push(field.clone(), BsonValue::Int64(index as i64));
                    }
                    output.push(expanded);
                }
            }
            Some(BsonValue::Array(_)) => {
                // Empty array: preserved only with the flag, and then the path is omitted.
                if preserve {
                    let mut expanded = document.clone();
                    remove_field_path(&mut expanded, &path);
                    if let Some(field) = &index_field {
                        expanded.push(field.clone(), BsonValue::Null);
                    }
                    output.push(expanded);
                }
            }
            Some(BsonValue::Null) => {
                if preserve {
                    let mut expanded = document.clone();
                    if let Some(field) = &index_field {
                        expanded.push(field.clone(), BsonValue::Null);
                    }
                    output.push(expanded);
                }
            }
            None => {
                if preserve {
                    let mut expanded = document.clone();
                    if let Some(field) = &index_field {
                        expanded.push(field.clone(), BsonValue::Null);
                    }
                    output.push(expanded);
                }
            }
            Some(scalar) => {
                let mut expanded = document.clone();
                set_field_path(&mut expanded, &path, scalar);
                if let Some(field) = &index_field {
                    expanded.push(field.clone(), BsonValue::Null);
                }
                output.push(expanded);
            }
        }
    }
    Ok(output)
}

fn unwind_path(path: &str) -> Result<String, CommandError> {
    match path.strip_prefix('$') {
        Some(rest) if !rest.is_empty() => Ok(rest.to_string()),
        _ => Err(CommandError::new(2, "$unwind path must start with `$`")),
    }
}

/// `$lookup` equality form (`from` / `localField` / `foreignField` / `as`) and
/// the pipeline form (`let` variables read through `$$name`).
///
/// A local array is expanded element by element and the matching foreign
/// documents are unioned in storage order, which is what `mongod 8.0.9` returns
/// for the equality form. A sub-pipeline runs against the foreign collection
/// after the implicit equality match, exactly as the manual describes.
fn lookup_stage(
    documents: Vec<BsonDocument>,
    body: &BsonValue,
    store: &dyn MongoStorage,
    namespace: &str,
    session: Option<&str>,
    variables: &ExpressionVariables,
) -> Result<Vec<BsonDocument>, CommandError> {
    let BsonValue::Document(specification) = body
    else {
        return Err(CommandError::new(9, "$lookup requires a document"));
    };
    let from = match specification.get("from") {
        Some(BsonValue::String(name)) if !name.is_empty() => name.clone(),
        Some(BsonValue::String(_)) | None => {
            return Err(CommandError::new(9, "must specify 'pipeline' when 'from' is empty"));
        }
        Some(other) => {
            return Err(CommandError::new(9, format!("$lookup 'from' field must be a string, but found {}", bson_type_name(other))));
        }
    };
    let as_field = match specification.get("as") {
        Some(BsonValue::String(field)) if !field.is_empty() => field.clone(),
        Some(other) => {
            return Err(CommandError::new(
                9,
                format!("$lookup argument 'as' must be a string, found as: {}: {}", render_shell_value(other), bson_type_name(other)),
            ));
        }
        None => return Err(CommandError::new(9, "must specify 'as' field for a $lookup")),
    };
    let pipeline = match specification.get("pipeline") {
        Some(BsonValue::Array(stages)) => Some(stages.clone()),
        Some(_) => return Err(CommandError::new(9, "$lookup 'pipeline' field must be an array")),
        None => None,
    };
    let let_variables = match specification.get("let") {
        Some(BsonValue::Document(bindings)) => bindings.clone(),
        Some(_) => return Err(CommandError::new(9, "$lookup 'let' field must be a document")),
        None => BsonDocument::new(),
    };
    let local_field = match specification.get("localField") {
        Some(BsonValue::String(field)) => Some(field.clone()),
        Some(other) => {
            return Err(CommandError::new(
                9,
                format!(
                    "$lookup argument 'localField' must be a string, found localField: {}: {}",
                    render_shell_value(other),
                    bson_type_name(other)
                ),
            ));
        }
        None => None,
    };
    let foreign_field = match specification.get("foreignField") {
        Some(BsonValue::String(field)) => Some(field.clone()),
        Some(other) => {
            return Err(CommandError::new(
                9,
                format!(
                    "$lookup argument 'foreignField' must be a string, found foreignField: {}: {}",
                    render_shell_value(other),
                    bson_type_name(other)
                ),
            ));
        }
        None => None,
    };
    if pipeline.is_none() && (local_field.is_none() || foreign_field.is_none()) {
        return Err(CommandError::new(9, "$lookup requires either 'pipeline' or both 'localField' and 'foreignField' to be specified"));
    }

    let database = namespace.split_once('.').map(|(database, _)| database).unwrap_or("admin");
    let foreign_namespace = format!("{database}.{from}");
    let foreign_documents = collect_matching(store, &foreign_namespace, &BsonDocument::new(), session)?;

    let mut output = Vec::with_capacity(documents.len());
    for document in documents {
        let mut bound = variables.clone();
        for (name, expression) in &let_variables.fields {
            bound.insert(name.clone(), evaluate_expression_vars(&document, expression, variables)?);
        }

        let mut candidates = foreign_documents.clone();
        if let (Some(local_field), Some(foreign_field)) = (&local_field, &foreign_field) {
            let value = lookup_field(&document, local_field).cloned().unwrap_or(BsonValue::Null);
            let local_values: Vec<&BsonValue> = match &value {
                BsonValue::Array(items) if !items.is_empty() => items.iter().collect(),
                // An empty local array matches the same foreign values as a missing
                // or null local field does, which is what `mongod 8.0.9` returns.
                BsonValue::Array(_) => vec![&BsonValue::Null],
                other => vec![other],
            };
            let mut matched = Vec::new();
            for foreign_document in candidates.into_iter() {
                let fields = collect_field(&foreign_document, foreign_field);
                if local_values.iter().any(|local| equality_candidates(&fields, local)) {
                    matched.push(foreign_document);
                }
            }
            candidates = matched;
        }
        if let Some(stages) = &pipeline {
            reject_stage_outside(stages, 51047, "is not allowed within a $lookup's sub-pipeline")?;
            candidates = run_pipeline(candidates, stages, store, &foreign_namespace, session, &bound)?;
        }

        let joined: Vec<BsonValue> = candidates.into_iter().map(BsonValue::Document).collect();
        let mut expanded = document;
        set_field_path(&mut expanded, &as_field, BsonValue::Array(joined));
        output.push(expanded);
    }
    Ok(output)
}

/// `$graphLookup`: breadth-first traversal of `connectFromField` →
/// `connectToField` starting from `startWith`.
///
/// The result array order is not defined by `mongod 8.0.9`, so callers that
/// need a stable shape project `$size` or a single-element chain.
fn graph_lookup_stage(
    documents: Vec<BsonDocument>,
    body: &BsonValue,
    store: &dyn MongoStorage,
    namespace: &str,
    session: Option<&str>,
    variables: &ExpressionVariables,
) -> Result<Vec<BsonDocument>, CommandError> {
    let BsonValue::Document(specification) = body
    else {
        return Err(CommandError::new(40105, "$graphLookup requires a document"));
    };
    let required = ["from", "as", "startWith", "connectFromField", "connectToField"];
    if required.iter().any(|field| specification.get(field).is_none()) {
        return Err(CommandError::new(
            40105,
            "$graphLookup requires 'from', 'as', 'startWith', 'connectFromField', and 'connectToField' to be specified.",
        ));
    }
    let from = match specification.get("from") {
        Some(BsonValue::String(name)) if !name.is_empty() => name.clone(),
        Some(other) => {
            return Err(CommandError::new(40105, format!("$graphLookup 'from' must be a string, found {}", bson_type_name(other))));
        }
        None => unreachable!(),
    };
    let as_field = match specification.get("as") {
        Some(BsonValue::String(field)) if !field.is_empty() => field.clone(),
        Some(other) => {
            return Err(CommandError::new(40105, format!("$graphLookup 'as' must be a non-empty string, found {}", bson_type_name(other))));
        }
        None => unreachable!(),
    };
    let start_with = specification.get("startWith").cloned().unwrap_or(BsonValue::Null);
    let connect_from = match specification.get("connectFromField") {
        Some(BsonValue::String(field)) => field.clone(),
        Some(other) => {
            return Err(CommandError::new(40105, format!("$graphLookup 'connectFromField' must be a string, found {}", bson_type_name(other))));
        }
        None => unreachable!(),
    };
    let connect_to = match specification.get("connectToField") {
        Some(BsonValue::String(field)) => field.clone(),
        Some(other) => {
            return Err(CommandError::new(40105, format!("$graphLookup 'connectToField' must be a string, found {}", bson_type_name(other))));
        }
        None => unreachable!(),
    };
    let max_depth = match specification.get("maxDepth") {
        None => None,
        Some(BsonValue::Int32(depth)) => {
            if *depth < 0 {
                return Err(CommandError::new(40101, format!("maxDepth requires a nonnegative argument, found: {depth}")));
            }
            Some(i64::from(*depth))
        }
        Some(BsonValue::Int64(depth)) => {
            if *depth < 0 {
                return Err(CommandError::new(40101, format!("maxDepth requires a nonnegative argument, found: {depth}")));
            }
            Some(*depth)
        }
        Some(BsonValue::Double(depth)) => {
            if *depth < 0.0 {
                return Err(CommandError::new(40101, format!("maxDepth requires a nonnegative argument, found: {depth}")));
            }
            if depth.fract() != 0.0 {
                return Err(CommandError::new(40102, format!("maxDepth could not be represented as a long long: {}", *depth as i64)));
            }
            Some(*depth as i64)
        }
        Some(other) => {
            return Err(CommandError::new(40102, format!("maxDepth could not be represented as a long long: {}", render_shell_value(other))));
        }
    };
    let depth_field = match specification.get("depthField") {
        Some(BsonValue::String(field)) => Some(field.clone()),
        Some(other) => {
            return Err(CommandError::new(40105, format!("$graphLookup 'depthField' must be a string, found {}", bson_type_name(other))));
        }
        None => None,
    };
    let restrict = match specification.get("restrictSearchWithMatch") {
        Some(BsonValue::Document(filter)) => {
            if filter.fields.iter().any(|(field, _)| field == "$where") {
                return Err(CommandError::new(
                    2,
                    "Failed to parse 'restrictSearchWithMatch' option to $graphLookup :: caused by :: $where is not allowed in this context",
                ));
            }
            validate_filter(filter, "$graphLookup restrictSearchWithMatch")?;
            Some(filter.clone())
        }
        Some(other) => {
            return Err(CommandError::new(
                2,
                format!("$graphLookup 'restrictSearchWithMatch' must be an object, found {}", bson_type_name(other)),
            ));
        }
        None => None,
    };

    let database = namespace.split_once('.').map(|(database, _)| database).unwrap_or("admin");
    let foreign_namespace = format!("{database}.{from}");
    let foreign_documents = collect_matching(store, &foreign_namespace, &BsonDocument::new(), session)?;

    let mut output = Vec::with_capacity(documents.len());
    for document in documents {
        let start = evaluate_expression_vars(&document, &start_with, variables)?;
        let mut queue: Vec<(BsonValue, i64)> = match start {
            BsonValue::Array(items) => items.into_iter().map(|item| (item, 0)).collect(),
            BsonValue::Null => Vec::new(),
            other => vec![(other, 0)],
        };
        let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut collected: Vec<BsonDocument> = Vec::new();
        while let Some((value, depth)) = queue.first().cloned() {
            queue.remove(0);
            for candidate in &foreign_documents {
                let fields = collect_field(candidate, &connect_to);
                if !equality_candidates(&fields, &value) {
                    continue;
                }
                if let Some(filter) = &restrict
                    && !document_matches_vars(candidate, filter, variables)?
                {
                    continue;
                }
                let identity = candidate.get("_id").map(value_to_key).unwrap_or_else(|| encode_bson(candidate));
                if !seen.insert(identity) {
                    continue;
                }
                let mut result = candidate.clone();
                if let Some(field) = &depth_field {
                    set_field_path(&mut result, field, BsonValue::Int64(depth));
                }
                collected.push(result);
                if max_depth.is_some_and(|limit| depth >= limit) {
                    continue;
                }
                for next in collect_field(candidate, &connect_from) {
                    match next {
                        BsonValue::Array(items) => {
                            for item in items {
                                queue.push((item.clone(), depth + 1));
                            }
                        }
                        BsonValue::Null => {}
                        other => queue.push((other.clone(), depth + 1)),
                    }
                }
            }
        }
        let mut expanded = document;
        set_field_path(&mut expanded, &as_field, BsonValue::Array(collected.into_iter().map(BsonValue::Document).collect()));
        output.push(expanded);
    }
    Ok(output)
}

/// `$replaceWith`: the input document is replaced by the evaluated document.
fn replace_with_stage(
    documents: Vec<BsonDocument>,
    body: &BsonValue,
    variables: &ExpressionVariables,
) -> Result<Vec<BsonDocument>, CommandError> {
    let mut output = Vec::with_capacity(documents.len());
    for document in documents {
        match evaluate_expression_optional_vars(&document, body, variables)? {
            Some(BsonValue::Document(replacement)) => output.push(replacement),
            Some(other) => {
                return Err(CommandError::new(
                    40228,
                    format!(
                        "PlanExecutor error during aggregation :: caused by :: 'replacement document'  must evaluate to an object, but resulting value was: {}. Type of resulting value: '{}'. Input document: {{}}",
                        render_shell_value(&other),
                        bson_type_name(&other)
                    ),
                ));
            }
            None => {
                return Err(CommandError::new(
                    40228,
                    "PlanExecutor error during aggregation :: caused by :: 'replacement document'  must evaluate to an object, but resulting value was: MISSING. Type of resulting value: 'missing'. Input document: {}",
                ));
            }
        }
    }
    Ok(output)
}

/// `$out` target namespace resolution, shared by the error shapes and by
/// `$merge`.
fn stage_target_namespace(body: &BsonValue, namespace: &str, stage: &str) -> Result<String, CommandError> {
    let database = namespace.split_once('.').map(|(database, _)| database).unwrap_or("admin");
    match body {
        BsonValue::String(collection) => {
            if collection.is_empty() {
                return Err(CommandError::new(73, format!("Invalid $out target namespace, {database}")));
            }
            Ok(format!("{database}.{collection}"))
        }
        BsonValue::Document(specification) => {
            let target_database = match specification.get("db") {
                Some(BsonValue::String(name)) => name.clone(),
                _ => {
                    return Err(CommandError::new(40414, format!("BSON field '{stage}.db' is missing but a required field")));
                }
            };
            let collection = match specification.get("coll") {
                Some(BsonValue::String(name)) => name.clone(),
                _ => {
                    return Err(CommandError::new(40414, format!("BSON field '{stage}.coll' is missing but a required field")));
                }
            };
            for (field, _) in &specification.fields {
                if field != "db" && field != "coll" {
                    return Err(CommandError::new(40415, format!("BSON field '{stage}.{field}' is an unknown field.")));
                }
            }
            if collection.is_empty() {
                return Err(CommandError::new(73, format!("Invalid $out target namespace, {target_database}")));
            }
            Ok(format!("{target_database}.{collection}"))
        }
        other => Err(CommandError::new(16990, format!("$out only supports a string or object argument, but found {}", bson_type_name(other)))),
    }
}

fn temporary_aggregation_namespace(namespace: &str) -> String {
    let database = namespace.split_once('.').map(|(database, _)| database).unwrap_or("admin");
    let mut state =
        SystemTime::now().duration_since(UNIX_EPOCH).map(|duration| duration.as_nanos() as u64).unwrap_or(0x9e37_79b9_7f4a_7c15) | 1;
    let mut hex = String::with_capacity(32);
    for _ in 0..32 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        hex.push(char::from_digit((state & 0xf) as u32, 16).unwrap());
    }
    format!("{database}.tmp.agg_out.{}-{}-4{}-{}-{}", &hex[0..8], &hex[8..12], &hex[13..16], &hex[16..20], &hex[20..32])
}

fn out_duplicate_error(namespace: &str, index_name: &str, key_value: &BsonDocument) -> CommandError {
    let mut key_text = String::new();
    for (field, value) in &key_value.fields {
        if !key_text.is_empty() {
            key_text.push_str(", ");
        }
        key_text.push_str(field);
        key_text.push_str(": ");
        key_text.push_str(&render_shell_value(value));
    }
    CommandError::new(
        11000,
        format!(
            "PlanExecutor error during aggregation :: caused by :: E11000 duplicate key error collection: {namespace} index: {index_name} dup key: {{ {key_text} }}"
        ),
    )
    .with_detail("keyValue", BsonValue::Document(key_value.clone()))
}

fn reject_stage_outside(stages: &[BsonValue], code: i32, within: &str) -> Result<(), CommandError> {
    for stage in stages {
        let BsonValue::Document(document) = stage
        else {
            continue;
        };
        if let Some((name, _)) = document.fields.first()
            && matches!(name.as_str(), "$out" | "$merge")
        {
            return Err(CommandError::new(code, format!("{name} {within}")));
        }
    }
    Ok(())
}

/// `$unionWith`: appends the documents of another collection, optionally
/// filtered by a sub-pipeline. The sub-pipeline runs in the target collection's
/// namespace, exactly like `$lookup`'s `from`.
fn union_with_stage(
    documents: Vec<BsonDocument>,
    body: &BsonValue,
    store: &dyn MongoStorage,
    namespace: &str,
    session: Option<&str>,
    variables: &ExpressionVariables,
) -> Result<Vec<BsonDocument>, CommandError> {
    let (collection, pipeline) = match body {
        BsonValue::String(collection) => (Some(collection.clone()), None),
        BsonValue::Document(specification) => {
            let mut collection = None;
            let mut pipeline = None;
            for (key, value) in &specification.fields {
                match key.as_str() {
                    "coll" => match value {
                        BsonValue::String(name) => collection = Some(name.clone()),
                        other => {
                            return Err(CommandError::new(
                                14,
                                format!("BSON field '$unionWith.coll' is the wrong type '{}', expected type 'string'", bson_type_name(other)),
                            ));
                        }
                    },
                    "pipeline" => match value {
                        BsonValue::Array(stages) => pipeline = Some(stages.clone()),
                        _ => return Err(CommandError::new(14, "'pipeline' option must be specified as an array")),
                    },
                    other => {
                        return Err(CommandError::new(40415, format!("BSON field '$unionWith.{other}' is an unknown field.")));
                    }
                }
            }
            (collection, pipeline)
        }
        other => {
            return Err(CommandError::new(
                9,
                format!("the $unionWith stage specification must be an object or string, but found {}", bson_type_name(other)),
            ));
        }
    };

    let database = namespace.split_once('.').map(|(database, _)| database).unwrap_or("admin");
    let documents_required =
        || CommandError::new(9, "$unionWith stage without explicit collection must have a pipeline with $documents as first stage");
    let mut output = documents;
    match collection {
        Some(collection) if !collection.is_empty() => {
            let target = format!("{database}.{collection}");
            let mut candidates = collect_matching(store, &target, &BsonDocument::new(), session)?;
            if let Some(stages) = &pipeline {
                candidates = run_pipeline(candidates, stages, store, &target, session, variables)?;
            }
            output.extend(candidates);
        }
        Some(_) => {
            return Err(CommandError::new(
                73,
                format!("PlanExecutor error during aggregation :: caused by :: Namespace {database} is not a valid collection name"),
            ));
        }
        None => {
            let Some(stages) = pipeline
            else {
                return Err(documents_required());
            };
            let Some(BsonValue::Document(first)) = stages.first()
            else {
                return Err(documents_required());
            };
            let Some((first_name, first_body)) = first.fields.first()
            else {
                return Err(documents_required());
            };
            if first_name != "$documents" || first.fields.len() != 1 {
                return Err(documents_required());
            }
            let BsonValue::Array(items) = first_body
            else {
                return Err(CommandError::new(
                    14,
                    format!("BSON field '$documents' is the wrong type '{}', expected type 'array'", bson_type_name(first_body)),
                ));
            };
            let mut candidates = Vec::with_capacity(items.len());
            for item in items {
                let BsonValue::Document(document) = item
                else {
                    return Err(CommandError::new(
                        14,
                        format!("$documents requires an array of documents, but found {}", bson_type_name(item)),
                    ));
                };
                candidates.push(document.clone());
            }
            if stages.len() > 1 {
                candidates = run_pipeline(candidates, &stages[1..], store, namespace, session, variables)?;
            }
            output.extend(candidates);
        }
    }
    Ok(output)
}

/// `$sample`: at most `size` documents in arbitrary order. `mongod` truncates a
/// fractional size toward zero and treats anything at or above the input count
/// as "all documents".
fn sample_stage(mut documents: Vec<BsonDocument>, body: &BsonValue) -> Result<Vec<BsonDocument>, CommandError> {
    let BsonValue::Document(specification) = body
    else {
        return Err(CommandError::new(28745, "the $sample stage specification must be an object"));
    };
    for (key, _) in &specification.fields {
        if key != "size" {
            return Err(CommandError::new(28748, format!("unrecognized option to $sample: {key}")));
        }
    }
    let Some(value) = specification.get("size")
    else {
        return Err(CommandError::new(28749, "$sample stage must specify a size"));
    };
    let Some(number) = number_of(value)
    else {
        return Err(CommandError::new(28746, "size argument to $sample must be a number"));
    };
    let truncated = number.trunc();
    if truncated < 0.0 {
        return Err(CommandError::new(28747, "size argument to $sample must not be negative"));
    }
    let size = if truncated >= documents.len() as f64 { documents.len() } else { truncated as usize };

    // A xorshift shuffle keeps `$sample` random without pulling a dependency in.
    let mut state =
        SystemTime::now().duration_since(UNIX_EPOCH).map(|duration| duration.as_nanos() as u64).unwrap_or(0x9e37_79b9_7f4a_7c15) | 1;
    for index in (1..documents.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let swap = (state % (index as u64 + 1)) as usize;
        documents.swap(index, swap);
    }
    documents.truncate(size);
    Ok(documents)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DensifyKind {
    Numeric,
    Date,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeUnit {
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl TimeUnit {
    fn millis(self) -> Option<i64> {
        match self {
            TimeUnit::Millisecond => Some(1),
            TimeUnit::Second => Some(1_000),
            TimeUnit::Minute => Some(60_000),
            TimeUnit::Hour => Some(3_600_000),
            TimeUnit::Day => Some(86_400_000),
            TimeUnit::Week => Some(604_800_000),
            TimeUnit::Month | TimeUnit::Year => None,
        }
    }
}

#[derive(Debug, Clone)]
enum DensifyBounds {
    Full,
    Partition,
    Array(BsonValue, BsonValue),
}

struct DensifySpecification {
    field: String,
    partition_fields: Vec<String>,
    bounds: DensifyBounds,
    step: f64,
    unit: Option<TimeUnit>,
}

fn parse_densify_unit(value: &BsonValue) -> Result<TimeUnit, CommandError> {
    let BsonValue::String(name) = value
    else {
        return Err(CommandError::new(
            14,
            format!("BSON field '$densify.range.unit' is the wrong type '{}', expected type 'string'", bson_type_name(value)),
        ));
    };
    match name.as_str() {
        "millisecond" | "milliseconds" => Ok(TimeUnit::Millisecond),
        "second" | "seconds" => Ok(TimeUnit::Second),
        "minute" | "minutes" => Ok(TimeUnit::Minute),
        "hour" | "hours" => Ok(TimeUnit::Hour),
        "day" | "days" => Ok(TimeUnit::Day),
        "week" | "weeks" => Ok(TimeUnit::Week),
        "month" | "months" => Ok(TimeUnit::Month),
        "year" | "years" => Ok(TimeUnit::Year),
        other => Err(CommandError::new(9, format!("unknown time unit value: {other}"))),
    }
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_index = (month + 9) % 12;
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 { shifted } else { shifted - 146_096 } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era = (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 { month_index + 3 } else { month_index - 9 };
    (if month <= 2 { year + 1 } else { year }, month, day)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            }
            else {
                28
            }
        }
    }
}

fn add_calendar_units(millis: i64, count: i64, unit: TimeUnit) -> i64 {
    let day_millis = 86_400_000;
    let remainder = millis.rem_euclid(day_millis);
    let days = (millis - remainder) / day_millis;
    let (year, month, day) = civil_from_days(days);
    let (new_year, new_month) = match unit {
        TimeUnit::Month => {
            let total = year * 12 + (month - 1) + count;
            (total.div_euclid(12), total.rem_euclid(12) + 1)
        }
        TimeUnit::Year => (year + count, month),
        _ => (year, month),
    };
    let clamped_day = day.min(days_in_month(new_year, new_month));
    days_from_civil(new_year, new_month, clamped_day) * day_millis + remainder
}

fn densify_value_as_number(value: &BsonValue) -> Option<f64> {
    match value {
        BsonValue::Int32(number) => Some(f64::from(*number)),
        BsonValue::Int64(number) => Some(*number as f64),
        BsonValue::Double(number) => Some(*number),
        _ => None,
    }
}

fn densify_number_value(value: f64, hint: &BsonValue) -> BsonValue {
    match hint {
        BsonValue::Int32(_) => {
            if let Ok(integer) = i32::try_from(value as i64)
                && value == integer as f64
            {
                return BsonValue::Int32(integer);
            }
            BsonValue::Double(value)
        }
        BsonValue::Int64(_) => {
            let integer = value as i64;
            if value == integer as f64 { BsonValue::Int64(integer) } else { BsonValue::Double(value) }
        }
        _ => BsonValue::Double(value),
    }
}

fn parse_densify(body: &BsonValue) -> Result<DensifySpecification, CommandError> {
    let BsonValue::Document(specification) = body
    else {
        return Err(CommandError::new(9, "the $densify stage specification must be an object"));
    };
    let mut field = None;
    let mut partition_fields = Vec::new();
    let mut range = None;
    for (key, value) in &specification.fields {
        match key.as_str() {
            "field" => match value {
                BsonValue::String(name) => field = Some(name.clone()),
                other => {
                    return Err(CommandError::new(
                        14,
                        format!("BSON field '$densify.field' is the wrong type '{}', expected type 'string'", bson_type_name(other)),
                    ));
                }
            },
            "partitionByFields" => match value {
                BsonValue::Array(items) => {
                    for item in items {
                        let BsonValue::String(name) = item
                        else {
                            return Err(CommandError::new(
                                14,
                                format!(
                                    "BSON field '$densify.partitionByFields' is the wrong type '{}', expected type 'string'",
                                    bson_type_name(item)
                                ),
                            ));
                        };
                        partition_fields.push(name.clone());
                    }
                }
                other => {
                    return Err(CommandError::new(
                        14,
                        format!("BSON field '$densify.partitionByFields' is the wrong type '{}', expected type 'array'", bson_type_name(other)),
                    ));
                }
            },
            "range" => range = Some(value),
            other => {
                return Err(CommandError::new(40415, format!("BSON field '$densify.{other}' is an unknown field.")));
            }
        }
    }
    let Some(field) = field
    else {
        return Err(CommandError::new(40414, "BSON field '$densify.field' is missing but a required field"));
    };
    if field.starts_with('$') {
        return Err(CommandError::new(16410, "FieldPath field names may not start with '$'. Consider using $getField or $setField."));
    }
    for name in &partition_fields {
        if name.starts_with('$') {
            return Err(CommandError::new(16410, "FieldPath field names may not start with '$'. Consider using $getField or $setField."));
        }
    }
    let Some(range) = range
    else {
        return Err(CommandError::new(40414, "BSON field '$densify.range' is missing but a required field"));
    };
    let BsonValue::Document(range) = range
    else {
        return Err(CommandError::new(
            14,
            format!("BSON field '$densify.range' is the wrong type '{}', expected type 'object'", bson_type_name(range)),
        ));
    };
    let mut step = None;
    let mut unit = None;
    let mut bounds = None;
    for (key, value) in &range.fields {
        match key.as_str() {
            "step" => match value {
                BsonValue::Int32(_) | BsonValue::Int64(_) | BsonValue::Double(_) => step = Some(number_of(value).unwrap_or(0.0)),
                other => {
                    return Err(CommandError::new(
                        14,
                        format!(
                            "BSON field '$densify.range.step' is the wrong type '{}', expected types '[int, double, decimal, long']",
                            bson_type_name(other)
                        ),
                    ));
                }
            },
            "unit" => unit = Some(parse_densify_unit(value)?),
            "bounds" => bounds = Some(value),
            other => {
                return Err(CommandError::new(40415, format!("BSON field '$densify.range.{other}' is an unknown field.")));
            }
        }
    }
    let Some(step) = step
    else {
        return Err(CommandError::new(40414, "BSON field '$densify.range.step' is missing but a required field"));
    };
    if step <= 0.0 {
        return Err(CommandError::new(5733401, "The step parameter in a range statement must be a strictly positive numeric value"));
    }
    let Some(bounds) = bounds
    else {
        return Err(CommandError::new(40414, "BSON field '$densify.range.bounds' is missing but a required field"));
    };
    let bounds = match bounds {
        BsonValue::String(name) => match name.as_str() {
            "full" => DensifyBounds::Full,
            "partition" => {
                if partition_fields.is_empty() {
                    return Err(CommandError::new(
                        5733408,
                        "One cannot specify the bounds as 'partition' without specifying a non-empty array of partitionByFields. You may have meant to specify 'full' bounds.",
                    ));
                }
                DensifyBounds::Partition
            }
            _ => return Err(CommandError::new(5946802, "Bounds string must either be 'full' or 'partition'")),
        },
        BsonValue::Array(items) if items.len() == 2 => {
            let lower = &items[0];
            let upper = &items[1];
            let numeric = densify_value_as_number(lower).is_some() && densify_value_as_number(upper).is_some();
            let dated = matches!(lower, BsonValue::DateTime(_)) && matches!(upper, BsonValue::DateTime(_));
            if numeric {
                if unit.is_some() {
                    return Err(CommandError::new(5733409, "Numeric bounds may not have unit parameter"));
                }
                if densify_value_as_number(lower).unwrap() >= densify_value_as_number(upper).unwrap() {
                    return Err(CommandError::new(5733402, "A bounding array must be an ascending array of either two dates or two numbers"));
                }
            }
            else if !dated || unit.is_none() {
                return Err(CommandError::new(5733402, "A bounding array must be an ascending array of either two dates or two numbers"));
            }
            else if let (BsonValue::DateTime(lower), BsonValue::DateTime(upper)) = (lower, upper)
                && *lower >= *upper
            {
                return Err(CommandError::new(5733402, "A bounding array must be an ascending array of either two dates or two numbers"));
            }
            DensifyBounds::Array(lower.clone(), upper.clone())
        }
        _ => {
            return Err(CommandError::new(5733402, "A bounding array must be an ascending array of either two dates or two numbers"));
        }
    };
    Ok(DensifySpecification { field, partition_fields, bounds, step, unit })
}

fn densify_partition_fields(specification: &DensifySpecification, document: &BsonDocument) -> Vec<BsonValue> {
    specification.partition_fields.iter().map(|field| lookup_field(document, field).cloned().unwrap_or(BsonValue::Null)).collect()
}

fn densify_stage(documents: Vec<BsonDocument>, body: &BsonValue, variables: &ExpressionVariables) -> Result<Vec<BsonDocument>, CommandError> {
    let specification = parse_densify(body)?;
    let _ = variables;

    let mut kind: Option<DensifyKind> = None;
    for document in &documents {
        let Some(value) = lookup_field(document, &specification.field)
        else {
            continue;
        };
        match value {
            BsonValue::Null | BsonValue::Undefined => {}
            BsonValue::Int32(_) | BsonValue::Int64(_) | BsonValue::Double(_) => {
                if kind == Some(DensifyKind::Date) {
                    return Err(CommandError::new(
                        5733201,
                        "PlanExecutor error during aggregation :: caused by :: Densify field type must be numeric or a date",
                    ));
                }
                kind = Some(DensifyKind::Numeric);
            }
            BsonValue::DateTime(_) => {
                if kind == Some(DensifyKind::Numeric) {
                    return Err(CommandError::new(
                        5733201,
                        "PlanExecutor error during aggregation :: caused by :: Densify field type must be numeric or a date",
                    ));
                }
                kind = Some(DensifyKind::Date);
            }
            _ => {
                return Err(CommandError::new(
                    5733201,
                    "PlanExecutor error during aggregation :: caused by :: Densify field type must be numeric or a date",
                ));
            }
        }
    }
    let Some(kind) = kind
    else {
        return Ok(documents);
    };
    if kind == DensifyKind::Date && specification.unit.is_none() {
        return Err(CommandError::new(
            6053600,
            "PlanExecutor error during aggregation :: caused by :: Encountered date densify value in collection when step does not have a date unit.",
        ));
    }
    if kind == DensifyKind::Numeric && specification.unit.is_some() {
        return Err(CommandError::new(
            6053600,
            "PlanExecutor error during aggregation :: caused by :: Encountered numeric densify value in collection when step has a date unit.",
        ));
    }

    // Sort by partition fields and then the densified field, which is the order
    // `mongod` emits (the gap documents are spliced into that sequence).
    let mut sorted = documents;
    sorted.sort_by(|left, right| {
        for (position, field) in specification.partition_fields.iter().enumerate() {
            let _ = position;
            let left_value = lookup_field(left, field).cloned().unwrap_or(BsonValue::Null);
            let right_value = lookup_field(right, field).cloned().unwrap_or(BsonValue::Null);
            let ordering = compare_values(&left_value, &right_value);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        let left_value = lookup_field(left, &specification.field).cloned().unwrap_or(BsonValue::Null);
        let right_value = lookup_field(right, &specification.field).cloned().unwrap_or(BsonValue::Null);
        compare_values(&left_value, &right_value)
    });

    let numeric_value = |document: &BsonDocument| -> Option<f64> {
        lookup_field(document, &specification.field).and_then(|value| match value {
            BsonValue::Null | BsonValue::Undefined => None,
            other => densify_value_as_number(other),
        })
    };
    let date_value = |document: &BsonDocument| -> Option<i64> {
        lookup_field(document, &specification.field).and_then(|value| match value {
            BsonValue::DateTime(millis) => Some(*millis),
            _ => None,
        })
    };

    let mut global_range: Option<(f64, f64)> = None;
    if matches!(specification.bounds, DensifyBounds::Full) {
        let mut lower = f64::INFINITY;
        let mut upper = f64::NEG_INFINITY;
        for document in &sorted {
            if let Some(value) = numeric_value(document) {
                lower = lower.min(value);
                upper = upper.max(value);
            }
            else if let Some(millis) = date_value(document) {
                lower = lower.min(millis as f64);
                upper = upper.max(millis as f64);
            }
        }
        if lower.is_finite() && upper.is_finite() {
            global_range = Some((lower, upper));
        }
    }
    let mut global_dates: Option<(i64, i64)> = None;
    if matches!(specification.bounds, DensifyBounds::Full) {
        let mut lower = i64::MAX;
        let mut upper = i64::MIN;
        for document in &sorted {
            if let Some(millis) = date_value(document) {
                lower = lower.min(millis);
                upper = upper.max(millis);
            }
        }
        if lower != i64::MAX {
            global_dates = Some((lower, upper));
        }
    }

    let mut output: Vec<BsonDocument> = Vec::with_capacity(sorted.len());
    let mut index = 0;
    while index < sorted.len() {
        let key = densify_partition_fields(&specification, &sorted[index]);
        let start = index;
        while index < sorted.len() && densify_partition_fields(&specification, &sorted[index]) == key {
            index += 1;
        }
        let group = &sorted[start..index];

        let hint = group
            .iter()
            .find_map(|document| match lookup_field(document, &specification.field) {
                Some(value @ (BsonValue::Int32(_) | BsonValue::Int64(_) | BsonValue::Double(_))) => Some(value.clone()),
                _ => None,
            })
            .unwrap_or(BsonValue::Double(0.0));

        let mut existing_numbers: BTreeSet<u64> = BTreeSet::new();
        let mut group_number_range: Option<(f64, f64)> = None;
        let mut existing_dates: BTreeSet<i64> = BTreeSet::new();
        let mut group_date_range: Option<(i64, i64)> = None;
        for document in group {
            if let Some(value) = numeric_value(document) {
                existing_numbers.insert(value.to_bits());
                group_number_range = Some(match group_number_range {
                    Some((lower, upper)) => (lower.min(value), upper.max(value)),
                    None => (value, value),
                });
            }
            if let Some(millis) = date_value(document) {
                existing_dates.insert(millis);
                group_date_range = Some(match group_date_range {
                    Some((lower, upper)) => (lower.min(millis), upper.max(millis)),
                    None => (millis, millis),
                });
            }
        }

        let mut generated: Vec<BsonDocument> = Vec::new();
        let make_document = |value: BsonValue| {
            let mut document = BsonDocument::new();
            for (field, field_value) in specification.partition_fields.iter().zip(&key) {
                let _ = field;
                if let Some(original) = group.iter().find_map(|candidate| lookup_field(candidate, field).cloned()) {
                    document.push(field.clone(), original);
                }
                else {
                    let _ = field_value;
                }
            }
            document.push(specification.field.clone(), value);
            document
        };

        if kind == DensifyKind::Numeric {
            let range = match &specification.bounds {
                DensifyBounds::Full => global_range,
                DensifyBounds::Partition => group_number_range,
                DensifyBounds::Array(lower, upper) => match (densify_value_as_number(lower), densify_value_as_number(upper)) {
                    (Some(lower), Some(upper)) => Some((lower, upper)),
                    _ => None,
                },
            };
            if let Some((lower, upper)) = range {
                let mut value = lower;
                let mut iterations = 0usize;
                while value < upper && iterations < 10_000_000 {
                    if !existing_numbers.contains(&value.to_bits()) {
                        generated.push(make_document(densify_number_value(value, &hint)));
                    }
                    value += specification.step;
                    iterations += 1;
                }
            }
        }
        else {
            let unit = specification.unit.unwrap_or(TimeUnit::Millisecond);
            let range = match &specification.bounds {
                DensifyBounds::Full => global_dates,
                DensifyBounds::Partition => group_date_range,
                DensifyBounds::Array(lower, upper) => match (lower, upper) {
                    (BsonValue::DateTime(lower), BsonValue::DateTime(upper)) => Some((*lower, *upper)),
                    _ => None,
                },
            };
            if let Some((lower, upper)) = range {
                let mut value = lower;
                let mut iterations = 0usize;
                while value < upper && iterations < 10_000_000 {
                    if !existing_dates.contains(&value) {
                        generated.push(make_document(BsonValue::DateTime(value)));
                    }
                    match unit {
                        TimeUnit::Month | TimeUnit::Year => {
                            value = add_calendar_units(value, 1, unit);
                        }
                        _ => {
                            value = value.saturating_add(unit.millis().unwrap_or(1));
                        }
                    }
                    iterations += 1;
                }
            }
        }

        // Merge the group's own documents with the generated gap documents.
        let mut group_cursor = 0;
        let mut generated_cursor = 0;
        while group_cursor < group.len() || generated_cursor < generated.len() {
            let take_original = match (group.get(group_cursor), generated.get(generated_cursor)) {
                (Some(original), Some(gap)) => {
                    let original_value = lookup_field(original, &specification.field).cloned().unwrap_or(BsonValue::Null);
                    let gap_value = lookup_field(gap, &specification.field).cloned().unwrap_or(BsonValue::Null);
                    compare_values(&original_value, &gap_value) != Ordering::Greater
                }
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            if take_original {
                output.push(group[group_cursor].clone());
                group_cursor += 1;
            }
            else {
                output.push(generated[generated_cursor].clone());
                generated_cursor += 1;
            }
        }
    }
    Ok(output)
}

#[derive(Clone)]
enum FillMethod {
    Value(BsonValue),
    Locf,
    Linear,
}

struct FillField {
    field: String,
    method: FillMethod,
}

fn fill_stage(documents: Vec<BsonDocument>, body: &BsonValue, variables: &ExpressionVariables) -> Result<Vec<BsonDocument>, CommandError> {
    let BsonValue::Document(specification) = body
    else {
        return Err(CommandError::new(9, "the $fill stage specification must be an object"));
    };
    let mut sort_by: Option<Vec<(String, i32)>> = None;
    let mut partition_by: Option<BsonValue> = None;
    let mut partition_fields: Option<Vec<String>> = None;
    let mut output = None;
    for (key, value) in &specification.fields {
        match key.as_str() {
            "sortBy" => match value {
                BsonValue::Document(sort) => sort_by = Some(parse_sort_stage(sort)?),
                other => {
                    return Err(CommandError::new(
                        14,
                        format!("BSON field '$fill.sortBy' is the wrong type '{}', expected type 'object'", bson_type_name(other)),
                    ));
                }
            },
            "partitionBy" => partition_by = Some(value.clone()),
            "partitionByFields" => match value {
                BsonValue::Array(items) => {
                    let mut fields = Vec::with_capacity(items.len());
                    for item in items {
                        let BsonValue::String(name) = item
                        else {
                            return Err(CommandError::new(
                                14,
                                format!(
                                    "BSON field '$fill.partitionByFields' is the wrong type '{}', expected type 'string'",
                                    bson_type_name(item)
                                ),
                            ));
                        };
                        fields.push(name.clone());
                    }
                    partition_fields = Some(fields);
                }
                other => {
                    return Err(CommandError::new(
                        14,
                        format!("BSON field '$fill.partitionByFields' is the wrong type '{}', expected type 'array'", bson_type_name(other)),
                    ));
                }
            },
            "output" => output = Some(value),
            other => {
                return Err(CommandError::new(40415, format!("BSON field '$fill.{other}' is an unknown field.")));
            }
        }
    }
    let Some(output) = output
    else {
        return Err(CommandError::new(40414, "BSON field '$fill.output' is missing but a required field"));
    };
    let BsonValue::Document(output) = output
    else {
        return Err(CommandError::new(
            14,
            format!("BSON field '$fill.output' is the wrong type '{}', expected type 'object'", bson_type_name(output)),
        ));
    };
    if output.fields.is_empty() {
        return Err(CommandError::new(6050203, "The output field in '$fill' must contain an object with at least one element"));
    }
    let mut fills: Vec<FillField> = Vec::new();
    for (field, entry) in &output.fields {
        let BsonValue::Document(entry) = entry
        else {
            return Err(CommandError::new(6050200, "Each fill output specification must be an object with exactly one field"));
        };
        for (name, _) in &entry.fields {
            if name != "value" && name != "method" {
                return Err(CommandError::new(40415, format!("BSON field '$fill.{name}' is an unknown field.")));
            }
        }
        let value = entry.get("value");
        let method = entry.get("method");
        if value.is_some() && method.is_some() {
            return Err(CommandError::new(
                6050201,
                "Each fill output specification must have exactly one of 'method' or 'value' fields, not both",
            ));
        }
        let fill_method = match (value, method) {
            (Some(value), None) => FillMethod::Value(value.clone()),
            (None, Some(method)) => match method {
                BsonValue::String(name) if name == "locf" => FillMethod::Locf,
                BsonValue::String(name) if name == "linear" => FillMethod::Linear,
                _ => return Err(CommandError::new(6050202, "Method must be either locf or linear")),
            },
            (None, None) => continue,
            _ => unreachable!(),
        };
        fills.push(FillField { field: field.clone(), method: fill_method });
    }
    if partition_by.is_some() && partition_fields.is_some() {
        return Err(CommandError::new(6050204, "Maximum one of 'partitionBy' and 'partitionByFields can be specified in '$fill'"));
    }
    let linear = fills.iter().any(|fill| matches!(fill.method, FillMethod::Linear));
    if linear {
        match &sort_by {
            Some(sort) if sort.len() == 1 => {}
            _ => {
                return Err(CommandError::new(
                    605001,
                    "$linearFill must be specified with a top level sortBy expression with exactly one element",
                ));
            }
        }
    }

    let partition_key = |document: &BsonDocument| -> Result<Vec<u8>, CommandError> {
        if let Some(fields) = &partition_fields {
            let mut key = Vec::new();
            for field in fields {
                let value = lookup_field(document, field).cloned().unwrap_or(BsonValue::Null);
                key.extend_from_slice(&value_to_key(&value));
            }
            return Ok(key);
        }
        if let Some(expression) = &partition_by {
            let value = evaluate_expression_vars(document, expression, variables)?;
            return Ok(value_to_key(&value));
        }
        Ok(Vec::new())
    };

    let mut working = documents;
    let ordered = fills.iter().any(|fill| !matches!(fill.method, FillMethod::Value(_)));
    if ordered && let Some(sort) = &sort_by {
        sort_documents(&mut working, sort);
    }

    // Group indices by partition while preserving the document order.
    let mut groups: Vec<(Vec<u8>, Vec<usize>)> = Vec::new();
    for (position, document) in working.iter().enumerate() {
        let key = partition_key(document)?;
        match groups.iter_mut().find(|(existing, _)| existing == &key) {
            Some((_, members)) => members.push(position),
            None => groups.push((key, vec![position])),
        }
    }

    for (_, members) in &groups {
        for fill in &fills {
            match &fill.method {
                FillMethod::Value(value) => {
                    for position in members {
                        let document = &mut working[*position];
                        match lookup_field(document, &fill.field) {
                            Some(BsonValue::Null) | Some(BsonValue::Undefined) | None => {
                                set_field_path(document, &fill.field, value.clone());
                            }
                            Some(_) => {}
                        }
                    }
                }
                FillMethod::Locf => {
                    let mut last: Option<BsonValue> = None;
                    for position in members {
                        let document = &mut working[*position];
                        match lookup_field(document, &fill.field).cloned() {
                            Some(BsonValue::Null) | Some(BsonValue::Undefined) | None => {
                                if let Some(value) = &last {
                                    set_field_path(document, &fill.field, value.clone());
                                }
                            }
                            Some(value) => last = Some(value),
                        }
                    }
                }
                FillMethod::Linear => {
                    let mut anchors: Vec<(usize, f64)> = Vec::new();
                    for position in members {
                        match lookup_field(&working[*position], &fill.field) {
                            Some(BsonValue::Null) | Some(BsonValue::Undefined) | None => {}
                            Some(BsonValue::Int32(_) | BsonValue::Int64(_) | BsonValue::Double(_)) => {
                                let value = densify_value_as_number(lookup_field(&working[*position], &fill.field).unwrap()).unwrap();
                                anchors.push((*position, value));
                            }
                            Some(other) => {
                                return Err(CommandError::new(
                                    14,
                                    format!(
                                        "PlanExecutor error during aggregation :: caused by :: Value to be filled must be numeric or nullish, but found {}",
                                        bson_type_name(other)
                                    ),
                                ));
                            }
                        }
                    }
                    for pair in anchors.windows(2) {
                        let (left_position, left_value) = pair[0];
                        let (right_position, right_value) = pair[1];
                        if right_position == left_position + 1 {
                            continue;
                        }
                        let steps = (right_position - left_position) as f64;
                        for offset in 1..(right_position - left_position) {
                            let ratio = offset as f64 / steps;
                            let value = left_value + (right_value - left_value) * ratio;
                            // `mongod` always yields a double for interpolated values.
                            set_field_path(&mut working[left_position + offset], &fill.field, BsonValue::Double(value));
                        }
                    }
                }
            }
        }
    }

    // `locf` and `linear` emit documents in the sort order they used; `value`
    // fills keep the caller's order.
    if ordered {
        let mut ordered_documents = Vec::with_capacity(working.len());
        for (_, members) in &groups {
            for position in members {
                ordered_documents.push(working[*position].clone());
            }
        }
        return Ok(ordered_documents);
    }
    Ok(working)
}

/// `$redact` sentinels. They must not collide with ordinary string values
/// because `"$$KEEP"` produced as a literal is an error in `mongod`.
const REDACT_KEEP: &str = "\u{1}$KEEP";
const REDACT_DESCEND: &str = "\u{1}$DESCEND";
const REDACT_PRUNE: &str = "\u{1}$PRUNE";

fn validate_redact_variables(expression: &BsonValue, variables: &ExpressionVariables) -> Result<(), CommandError> {
    const RESERVED: [&str; 5] = ["ROOT", "CURRENT", "KEEP", "DESCEND", "PRUNE"];
    match expression {
        BsonValue::String(name) if name.starts_with("$$") => {
            let variable = &name[2..];
            if !RESERVED.contains(&variable) && !variables.contains_key(variable) {
                return Err(CommandError::new(17276, format!("Use of undefined variable: {variable}")));
            }
        }
        BsonValue::Document(document) => {
            for (_, value) in &document.fields {
                validate_redact_variables(value, variables)?;
            }
        }
        BsonValue::Array(items) => {
            for item in items {
                validate_redact_variables(item, variables)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn redact_value(
    value: &BsonValue,
    expression: &BsonValue,
    variables: &ExpressionVariables,
    root: &BsonDocument,
) -> Result<Option<BsonValue>, CommandError> {
    match value {
        BsonValue::Document(document) => Ok(redact_document(document, expression, variables, root)?.map(BsonValue::Document)),
        BsonValue::Array(items) => {
            let mut kept = Vec::with_capacity(items.len());
            for item in items {
                if let Some(value) = redact_value(item, expression, variables, root)? {
                    kept.push(value);
                }
            }
            Ok(Some(BsonValue::Array(kept)))
        }
        other => Ok(Some(other.clone())),
    }
}

fn redact_document(
    document: &BsonDocument,
    expression: &BsonValue,
    variables: &ExpressionVariables,
    root: &BsonDocument,
) -> Result<Option<BsonDocument>, CommandError> {
    let mut bound = variables.clone();
    bound.insert("ROOT".to_string(), BsonValue::Document(root.clone()));
    bound.insert("CURRENT".to_string(), BsonValue::Document(document.clone()));
    bound.insert("KEEP".to_string(), BsonValue::String(REDACT_KEEP.to_string()));
    bound.insert("DESCEND".to_string(), BsonValue::String(REDACT_DESCEND.to_string()));
    bound.insert("PRUNE".to_string(), BsonValue::String(REDACT_PRUNE.to_string()));
    let decision = evaluate_expression_optional_vars(document, expression, &bound)?;
    match decision {
        Some(BsonValue::String(name)) if name == REDACT_KEEP => Ok(Some(document.clone())),
        Some(BsonValue::String(name)) if name == REDACT_PRUNE => Ok(None),
        Some(BsonValue::String(name)) if name == REDACT_DESCEND => {
            let mut descended = BsonDocument::new();
            for (field, value) in &document.fields {
                if let Some(value) = redact_value(value, expression, variables, root)? {
                    descended.push(field.clone(), value);
                }
            }
            Ok(Some(descended))
        }
        other => {
            let rendered = match &other {
                Some(value) => render_shell_value(value),
                None => "MISSING".to_string(),
            };
            Err(CommandError::new(
                17053,
                format!(
                    "PlanExecutor error during aggregation :: caused by :: $redact's expression should not return anything aside from the variables $$KEEP, $$DESCEND, and $$PRUNE, but returned {rendered}"
                ),
            ))
        }
    }
}

fn redact_stage(documents: Vec<BsonDocument>, body: &BsonValue, variables: &ExpressionVariables) -> Result<Vec<BsonDocument>, CommandError> {
    validate_redact_variables(body, variables)?;
    let mut output = Vec::with_capacity(documents.len());
    for document in &documents {
        if let Some(redacted) = redact_document(document, body, variables, document)? {
            output.push(redacted);
        }
    }
    Ok(output)
}

fn out_stage(
    documents: Vec<BsonDocument>,
    body: &BsonValue,
    store: &dyn MongoStorage,
    namespace: &str,
    session: Option<&str>,
) -> Result<Vec<BsonDocument>, CommandError> {
    let target = stage_target_namespace(body, namespace, "$out")?;
    // `$out` is atomic in `mongod`: it writes every document first and only
    // replaces the target once the whole output set satisfies the target's
    // unique indexes. Validate that here so a failure cannot leave the old
    // collection half-deleted.
    let mut staged_documents = Vec::with_capacity(documents.len());
    for mut document in documents {
        if document.get("_id").is_none() {
            document.push("_id".to_string(), BsonValue::ObjectId(generate_object_id()));
        }
        staged_documents.push(document);
    }
    let temporary = temporary_aggregation_namespace(&target);
    let mut identifiers: BTreeSet<Vec<u8>> = BTreeSet::new();
    for document in &staged_documents {
        let identifier = document.get("_id").cloned().unwrap_or(BsonValue::Null);
        if !identifiers.insert(value_to_key(&identifier)) {
            let mut key_value = BsonDocument::new();
            key_value.push("_id".to_string(), identifier);
            return Err(out_duplicate_error(&temporary, "_id_", &key_value));
        }
    }
    let indexes = store.index_specs(&target)?;
    for specification in indexes.iter().filter(|index| index.unique) {
        let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
        for document in &staged_documents {
            for tuple in index_entries(document, specification) {
                if !seen.insert(index_tuple(&tuple)) {
                    let mut key_value = BsonDocument::new();
                    for ((field, _), value) in specification.key.fields.iter().zip(&tuple) {
                        key_value.push(field.clone(), value.clone());
                    }
                    return Err(out_duplicate_error(&temporary, &specification.name, &key_value));
                }
            }
        }
    }
    let target_keys: Vec<Vec<u8>> = namespace_documents(store, &target, session)?.into_iter().map(|(key, _)| key).collect();
    if !target_keys.is_empty() {
        stage_delete(store, session, &target, &target_keys)?;
    }
    let entries: Vec<(Vec<u8>, Vec<u8>)> = staged_documents
        .into_iter()
        .map(|document| {
            let identifier = document.get("_id").cloned().unwrap_or(BsonValue::Null);
            (value_to_key(&identifier), encode_bson(&document))
        })
        .collect();
    stage_insert(store, session, &target, &entries)?;
    Ok(Vec::new())
}

/// One `$merge` mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeMode {
    Replace,
    Merge,
    KeepExisting,
    Fail,
    Pipeline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeNotMatched {
    Insert,
    Discard,
    Fail,
}

fn merge_mode(value: Option<&BsonValue>) -> Result<MergeMode, CommandError> {
    match value {
        None => Ok(MergeMode::Merge),
        Some(BsonValue::String(name)) => match name.as_str() {
            "replace" => Ok(MergeMode::Replace),
            "merge" => Ok(MergeMode::Merge),
            "keepExisting" => Ok(MergeMode::KeepExisting),
            "fail" => Ok(MergeMode::Fail),
            other => Err(CommandError::new(2, format!("Enumeration value '{other}' for field 'whenMatched' is not a valid value."))),
        },
        Some(BsonValue::Array(stages)) if stages.is_empty() => Ok(MergeMode::Pipeline),
        Some(BsonValue::Array(_)) => Ok(MergeMode::Pipeline),
        Some(other) => {
            Err(CommandError::new(2, format!("Enumeration value '{}' for field 'whenMatched' is not a valid value.", bson_type_name(other))))
        }
    }
}

fn merge_not_matched(value: Option<&BsonValue>) -> Result<MergeNotMatched, CommandError> {
    match value {
        None => Ok(MergeNotMatched::Insert),
        Some(BsonValue::String(name)) => match name.as_str() {
            "insert" => Ok(MergeNotMatched::Insert),
            "discard" => Ok(MergeNotMatched::Discard),
            "fail" => Ok(MergeNotMatched::Fail),
            other => Err(CommandError::new(2, format!("Enumeration value '{other}' for field '$merge.whenNotMatched' is not a valid value."))),
        },
        Some(other) => Err(CommandError::new(
            2,
            format!("Enumeration value '{}' for field '$merge.whenNotMatched' is not a valid value.", bson_type_name(other)),
        )),
    }
}

/// Applies a `whenMatched` update pipeline to an existing target document.
fn merge_update_pipeline(
    mut target: BsonDocument,
    stages: &[BsonValue],
    variables: &ExpressionVariables,
) -> Result<BsonDocument, CommandError> {
    let original_id = target.get("_id").cloned();
    for stage in stages {
        let BsonValue::Document(stage_document) = stage
        else {
            return Err(CommandError::new(2, "aggregate stage must be a document"));
        };
        let Some((name, body)) = stage_document.fields.first()
        else {
            return Err(CommandError::new(2, "aggregate stage is empty"));
        };
        if stage_document.fields.len() != 1 {
            return Err(CommandError::new(2, "aggregate stage must contain exactly one field"));
        }
        match name.as_str() {
            "$set" | "$addFields" => {
                let BsonValue::Document(specification) = body
                else {
                    return Err(CommandError::new(2, format!("{name} requires a document")));
                };
                for (field, expression) in &specification.fields {
                    let value = evaluate_expression_vars(&target, expression, variables)?;
                    let segments: Vec<&str> = field.split('.').collect();
                    set_aggregation_segments(&mut target, &segments, value)?;
                }
            }
            "$unset" => {
                let fields = match body {
                    BsonValue::String(field) => vec![field.clone()],
                    BsonValue::Array(items) => items
                        .iter()
                        .filter_map(|item| match item {
                            BsonValue::String(field) => Some(field.clone()),
                            _ => None,
                        })
                        .collect(),
                    _ => return Err(CommandError::new(2, "$unset requires a string or an array of strings")),
                };
                for field in fields {
                    remove_field_path(&mut target, &field);
                }
            }
            "$replaceWith" => match evaluate_expression_optional_vars(&target, body, variables)? {
                Some(BsonValue::Document(replacement)) => target = replacement,
                Some(other) => {
                    return Err(CommandError::new(
                        40228,
                        format!(
                            "PlanExecutor error during aggregation :: caused by :: 'replacement document'  must evaluate to an object, but resulting value was: {}. Type of resulting value: '{}'. Input document: {{}}",
                            render_shell_value(&other),
                            bson_type_name(&other)
                        ),
                    ));
                }
                None => {
                    return Err(CommandError::new(
                        40228,
                        "PlanExecutor error during aggregation :: caused by :: 'replacement document'  must evaluate to an object, but resulting value was: MISSING. Type of resulting value: 'missing'. Input document: {}",
                    ));
                }
            },
            "$project" => {
                let BsonValue::Document(specification) = body
                else {
                    return Err(CommandError::new(2, "$project requires a document"));
                };
                target = project_stage(std::slice::from_ref(&target), specification)?.remove(0);
            }
            "$match" => {
                return Err(CommandError::new(
                    72,
                    "PlanExecutor error during aggregation :: caused by :: $match is not allowed to be used within an update",
                ));
            }
            other => {
                return Err(CommandError::new(
                    59,
                    format!("unsupported update pipeline stage: {other} (supported: $set, $addFields, $unset, $replaceWith, $project)"),
                ));
            }
        }
    }
    if let Some(identifier) = original_id {
        match target.get("_id") {
            Some(updated) if value_to_key(updated) == value_to_key(&identifier) => {}
            Some(updated) => {
                return Err(merge_immutable_id_error(updated));
            }
            None => target.push("_id".to_string(), identifier),
        }
    }
    Ok(target)
}

fn merge_immutable_id_error(identifier: &BsonValue) -> CommandError {
    CommandError::new(
        66,
        format!(
            "PlanExecutor error during aggregation :: caused by :: $merge failed to update the matching document, did you attempt to modify the _id or the shard key? :: caused by :: After applying the update, the (immutable) field '_id' was found to have been altered to _id: {}",
            render_shell_value(identifier)
        ),
    )
}

fn merge_immutable_field_error() -> CommandError {
    CommandError::new(
        66,
        "PlanExecutor error during aggregation :: caused by :: $merge failed to update the matching document, did you attempt to modify the _id or the shard key? :: caused by :: Performing an update on the path '_id' would modify the immutable field '_id'",
    )
}

fn merge_duplicate_error(
    namespace: &str,
    index_name: &str,
    key_pattern: &BsonDocument,
    key_value: &BsonDocument,
    when_matched_fail: bool,
) -> CommandError {
    let mut key_text = String::new();
    for (field, value) in &key_value.fields {
        if !key_text.is_empty() {
            key_text.push_str(", ");
        }
        key_text.push_str(field);
        key_text.push_str(": ");
        key_text.push_str(&render_shell_value(value));
    }
    let prefix = if when_matched_fail {
        "PlanExecutor error during aggregation :: caused by :: $merge with whenMatched: fail found an existing document with the same values for the 'on' fields :: caused by :: "
    }
    else {
        "PlanExecutor error during aggregation :: caused by :: $merge failed due to a DuplicateKey error :: caused by :: "
    };
    CommandError::new(
        11000,
        format!("{prefix}E11000 duplicate key error collection: {namespace} index: {index_name} dup key: {{ {key_text} }}"),
    )
    .with_detail("keyPattern", BsonValue::Document(key_pattern.clone()))
    .with_detail("keyValue", BsonValue::Document(key_value.clone()))
}

fn merge_key_value(document: &BsonDocument, key_pattern: &BsonDocument) -> BsonDocument {
    let mut key_value = BsonDocument::new();
    for (field, _) in &key_pattern.fields {
        key_value.push(field.clone(), lookup_field(document, field).cloned().unwrap_or(BsonValue::Null));
    }
    key_value
}

/// `$merge`: upserts the pipeline output into `into` using `on` as the join
/// key, with the documented `whenMatched` / `whenNotMatched` modes.
fn merge_stage(
    documents: Vec<BsonDocument>,
    body: &BsonValue,
    store: &dyn MongoStorage,
    namespace: &str,
    session: Option<&str>,
    variables: &ExpressionVariables,
) -> Result<Vec<BsonDocument>, CommandError> {
    let BsonValue::Document(specification) = body
    else {
        return Err(CommandError::new(2, "$merge requires a document"));
    };
    let target = merge_target_namespace(specification, namespace)?;
    let on_fields = merge_on_fields(specification)?;
    let mode = merge_mode(specification.get("whenMatched"))?;
    let not_matched = merge_not_matched(specification.get("whenNotMatched"))?;
    if matches!(mode, MergeMode::KeepExisting | MergeMode::Fail) && not_matched != MergeNotMatched::Insert {
        let matched = if matches!(mode, MergeMode::KeepExisting) { "keepExisting" } else { "fail" };
        let missing = match not_matched {
            MergeNotMatched::Insert => "insert",
            MergeNotMatched::Discard => "discard",
            MergeNotMatched::Fail => "fail",
        };
        return Err(CommandError::new(
            51181,
            format!("Combination of $merge modes 'whenMatched: {matched}' and 'whenNotMatched: {missing}' is not supported"),
        ));
    }
    let when_matched_pipeline = match specification.get("whenMatched") {
        Some(BsonValue::Array(stages)) => stages.clone(),
        _ => Vec::new(),
    };
    let let_variables = match specification.get("let") {
        Some(BsonValue::Document(bindings)) => bindings.clone(),
        _ => BsonDocument::new(),
    };

    // A missing target is created before the index check, exactly like `mongod`.
    if !store.namespace_exists(&target)? {
        store.import(&target, &[]).map_err(|error| CommandError::new(2, format!("$merge failed: {error}")))?;
    }
    let Some((index_name, key_pattern)) = merge_unique_index(store, &target, &on_fields)?
    else {
        return Err(CommandError::new(51183, "Cannot find index to verify that join fields will be unique"));
    };

    let mut target_documents: Vec<BsonDocument> = collect_matching(store, &target, &BsonDocument::new(), session)?;

    // `mongod` validates every document's `on` values before applying any
    // write, so a later document can reject the whole aggregation.
    let mut on_values_by_document: Vec<Vec<BsonValue>> = Vec::with_capacity(documents.len());
    for document in &documents {
        let mut on_values = Vec::new();
        for field in &on_fields {
            let value = lookup_field(document, field).cloned().unwrap_or(BsonValue::Null);
            if matches!(value, BsonValue::Null | BsonValue::Undefined | BsonValue::Array(_)) {
                return Err(CommandError::new(
                    51132,
                    format!(
                        "PlanExecutor error during aggregation :: caused by :: $merge write error: 'on' field '{field}' cannot be missing, null, undefined or an array"
                    ),
                ));
            }
            on_values.push(value);
        }
        on_values_by_document.push(on_values);
    }

    for (document, on_values) in documents.into_iter().zip(on_values_by_document) {
        let matched_position = target_documents.iter().position(|candidate| {
            on_fields.iter().zip(&on_values).all(|(field, value)| {
                lookup_field(candidate, field).map(|candidate_value| compare_values(candidate_value, value) == Ordering::Equal).unwrap_or(false)
            })
        });

        if let Some(position) = matched_position {
            let existing = target_documents[position].clone();
            let mut bound = variables.clone();
            for (name, expression) in &let_variables.fields {
                bound.insert(name.clone(), evaluate_expression_vars(&existing, expression, variables)?);
            }
            let updated = match mode {
                MergeMode::KeepExisting => continue,
                MergeMode::Fail => {
                    let same_identifier = match (existing.get("_id"), document.get("_id")) {
                        (Some(left), Some(right)) => value_to_key(left) == value_to_key(right),
                        _ => false,
                    };
                    if index_name == "_id_" || same_identifier {
                        let mut id_pattern = BsonDocument::new();
                        id_pattern.push("_id".to_string(), BsonValue::Int32(1));
                        let id_value = merge_key_value(&existing, &id_pattern);
                        return Err(merge_duplicate_error(&target, "_id_", &id_pattern, &id_value, index_name == "_id_"));
                    }
                    let on_value = merge_key_value(&existing, &key_pattern);
                    return Err(merge_duplicate_error(&target, &index_name, &key_pattern, &on_value, true));
                }
                MergeMode::Replace => {
                    let mut replacement = document.clone();
                    if let Some(identifier) = existing.get("_id") {
                        match document.get("_id") {
                            Some(new_identifier) if value_to_key(new_identifier) != value_to_key(identifier) => {
                                return Err(merge_immutable_id_error(new_identifier));
                            }
                            Some(_) => {}
                            None => replacement.push("_id".to_string(), identifier.clone()),
                        }
                    }
                    replacement
                }
                MergeMode::Merge => merge_document(&existing, &document)?,
                MergeMode::Pipeline => merge_update_pipeline(existing, &when_matched_pipeline, &bound)?,
            };
            target_documents[position] = updated.clone();
            let identifier = updated.get("_id").cloned().unwrap_or(BsonValue::Null);
            // `$merge` applies each document as it goes: a later failure does not
            // roll back the writes already made, which is what `mongod` does.
            stage_replace(store, session, &target, &[(value_to_key(&identifier), encode_bson(&updated))])?;
        }
        else {
            match not_matched {
                MergeNotMatched::Discard => continue,
                MergeNotMatched::Fail => {
                    return Err(CommandError::new(
                        13113,
                        "PlanExecutor error during aggregation :: caused by :: $merge could not find a matching document in the target collection for at least one document in the source collection",
                    ));
                }
                MergeNotMatched::Insert => {}
            }
            let mut inserted = document.clone();
            let identifier = match inserted.get("_id").cloned() {
                Some(identifier) => identifier,
                None => {
                    let identifier = BsonValue::ObjectId(generate_object_id());
                    inserted.push("_id".to_string(), identifier.clone());
                    identifier
                }
            };
            let identifier_key = value_to_key(&identifier);
            if let Some((conflict_name, conflict_pattern, conflict_value)) = merge_conflict(&target_documents, &inserted, &identifier_key) {
                return Err(merge_duplicate_error(&target, &conflict_name, &conflict_pattern, &conflict_value, false));
            }
            target_documents.push(inserted.clone());
            stage_insert(store, session, &target, &[(identifier_key, encode_bson(&inserted))])?;
        }
    }

    Ok(Vec::new())
}

fn merge_target_namespace(specification: &BsonDocument, namespace: &str) -> Result<String, CommandError> {
    match specification.get("into") {
        Some(BsonValue::String(collection)) if !collection.is_empty() => {
            let database = namespace.split_once('.').map(|(database, _)| database).unwrap_or("admin");
            Ok(format!("{database}.{collection}"))
        }
        Some(BsonValue::Document(specification)) => {
            let database = match specification.get("db") {
                Some(BsonValue::String(name)) if !name.is_empty() => name.clone(),
                _ => {
                    return Err(CommandError::new(51178, "BSON field '$merge.into.db' is missing but a required field"));
                }
            };
            let collection = match specification.get("coll") {
                Some(BsonValue::String(name)) if !name.is_empty() => name.clone(),
                _ => {
                    return Err(CommandError::new(51178, "BSON field '$merge.into.coll' is missing but a required field"));
                }
            };
            Ok(format!("{database}.{collection}"))
        }
        Some(other) => Err(CommandError::new(
            51178,
            format!("$merge 'into' field  must be either a string or an object, but found {}", bson_type_name(other)),
        )),
        None => Err(CommandError::new(51178, "$merge requires an 'into' field")),
    }
}

fn merge_on_fields(specification: &BsonDocument) -> Result<Vec<String>, CommandError> {
    match specification.get("on") {
        None => Ok(vec!["_id".to_string()]),
        Some(BsonValue::String(field)) => Ok(vec![field.clone()]),
        Some(BsonValue::Array(items)) => {
            if items.is_empty() {
                return Err(CommandError::new(51187, "If explicitly specifying $merge 'on', must include at least one field"));
            }
            let mut fields = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    BsonValue::String(field) => fields.push(field.clone()),
                    other => {
                        return Err(CommandError::new(
                            51186,
                            format!("$merge 'on' field  must be either a string or an array of strings, but found {}", bson_type_name(other)),
                        ));
                    }
                }
            }
            Ok(fields)
        }
        Some(other) => Err(CommandError::new(
            51186,
            format!("$merge 'on' field  must be either a string or an array of strings, but found {}", bson_type_name(other)),
        )),
    }
}

/// The unique index backing the `on` fields, as `(name, key pattern)`.
fn merge_unique_index(store: &dyn MongoStorage, namespace: &str, on_fields: &[String]) -> Result<Option<(String, BsonDocument)>, CommandError> {
    if on_fields.len() == 1 && on_fields[0] == "_id" {
        let mut pattern = BsonDocument::new();
        pattern.push("_id".to_string(), BsonValue::Int32(1));
        return Ok(Some(("_id_".to_string(), pattern)));
    }
    for spec in store.index_specs(namespace)? {
        if !spec.unique || spec.key.fields.len() != on_fields.len() {
            continue;
        }
        if spec.key.fields.iter().zip(on_fields).all(|((field, _), wanted)| field == wanted) {
            return Ok(Some((spec.name.clone(), spec.key.clone())));
        }
    }
    Ok(None)
}

fn merge_document(existing: &BsonDocument, incoming: &BsonDocument) -> Result<BsonDocument, CommandError> {
    if let (Some(left), Some(right)) = (existing.get("_id"), incoming.get("_id"))
        && value_to_key(left) != value_to_key(right)
    {
        return Err(merge_immutable_field_error());
    }
    let mut merged = existing.clone();
    for (field, value) in &incoming.fields {
        if let Some(entry) = merged.fields.iter_mut().find(|(key, _)| key == field) {
            entry.1 = value.clone();
        }
        else {
            merged.push(field.clone(), value.clone());
        }
    }
    Ok(merged)
}

/// First duplicate-key conflict an insert would create, as
/// `(index name, key pattern, key value)`.
fn merge_conflict(documents: &[BsonDocument], inserted: &BsonDocument, identifier: &[u8]) -> Option<(String, BsonDocument, BsonDocument)> {
    for candidate in documents {
        let candidate_id = candidate.get("_id").map(value_to_key).unwrap_or_default();
        if candidate_id == identifier {
            let mut pattern = BsonDocument::new();
            pattern.push("_id".to_string(), BsonValue::Int32(1));
            let value = merge_key_value(inserted, &pattern);
            return Some(("_id_".to_string(), pattern, value));
        }
    }
    None
}

/// `$facet`: every branch consumes the same input and the stage emits a single
/// document holding one array per branch, in specification order.
fn facet_stage(
    documents: Vec<BsonDocument>,
    body: &BsonValue,
    store: &dyn MongoStorage,
    namespace: &str,
    session: Option<&str>,
    variables: &ExpressionVariables,
) -> Result<Vec<BsonDocument>, CommandError> {
    let BsonValue::Document(specification) = body
    else {
        return Err(CommandError::new(2, "$facet requires a document"));
    };
    let mut faceted = BsonDocument::new();
    for (name, branch) in &specification.fields {
        let BsonValue::Array(stages) = branch
        else {
            return Err(CommandError::new(2, format!("$facet branch `{name}` must be a pipeline array")));
        };
        reject_stage_outside(stages, 40600, "is not allowed to be used within a $facet stage")?;
        let branch_output = run_pipeline(documents.clone(), stages, store, namespace, session, variables)?;
        faceted.push(name.clone(), BsonValue::Array(branch_output.into_iter().map(BsonValue::Document).collect()));
    }
    Ok(vec![faceted])
}

/// `$bucket`: fixed boundaries, an optional `default` bucket, and accumulator
/// output. Empty buckets are omitted and the default bucket is emitted last.
fn bucket_stage(documents: &[BsonDocument], body: &BsonValue, variables: &ExpressionVariables) -> Result<Vec<BsonDocument>, CommandError> {
    let BsonValue::Document(specification) = body
    else {
        return Err(CommandError::new(2, "$bucket requires a document"));
    };
    let Some(group_by) = specification.get("groupBy")
    else {
        return Err(CommandError::new(2, "$bucket requires a `groupBy` expression"));
    };
    let Some(BsonValue::Array(boundaries)) = specification.get("boundaries")
    else {
        return Err(CommandError::new(2, "$bucket requires a `boundaries` array"));
    };
    if boundaries.len() < 2 {
        return Err(CommandError::new(2, "$bucket requires at least two boundaries"));
    }
    for pair in boundaries.windows(2) {
        if compare_values(&pair[0], &pair[1]) != Ordering::Less {
            return Err(CommandError::new(
                2,
                format!(
                    "$bucket boundaries must be in ascending order, found {} before {}",
                    render_shell_value(&pair[0]),
                    render_shell_value(&pair[1])
                ),
            ));
        }
    }
    let default = specification.get("default").cloned();
    let accumulators = bucket_accumulators(specification)?;

    let mut members: Vec<Vec<usize>> = vec![Vec::new(); boundaries.len() - 1];
    let mut default_members: Vec<usize> = Vec::new();
    for (index, document) in documents.iter().enumerate() {
        let value = evaluate_expression_vars(document, group_by, variables)?;
        let mut placed = false;
        for bucket in 0..boundaries.len() - 1 {
            let above_lower = compare_values(&value, &boundaries[bucket]) != Ordering::Less;
            let below_upper = compare_values(&value, &boundaries[bucket + 1]) == Ordering::Less;
            if above_lower && below_upper {
                members[bucket].push(index);
                placed = true;
                break;
            }
        }
        if !placed {
            let Some(default) = &default
            else {
                return Err(CommandError::new(
                    7158303,
                    "PlanExecutor error during aggregation :: caused by :: $switch could not find a matching branch for an input, and no default was specified.",
                ));
            };
            let _ = default;
            default_members.push(index);
        }
    }

    let mut output = Vec::new();
    for (bucket, group) in members.iter().enumerate() {
        if group.is_empty() {
            continue;
        }
        let mut bucketed = BsonDocument::new();
        bucketed.push("_id".to_string(), boundaries[bucket].clone());
        for accumulator in &accumulators {
            bucketed.push(accumulator.field.clone(), accumulate(accumulator, documents, group)?);
        }
        output.push(bucketed);
    }
    if !default_members.is_empty() {
        let mut bucketed = BsonDocument::new();
        bucketed.push("_id".to_string(), default.unwrap_or(BsonValue::Null));
        for accumulator in &accumulators {
            bucketed.push(accumulator.field.clone(), accumulate(accumulator, documents, &default_members)?);
        }
        output.push(bucketed);
    }
    Ok(output)
}

fn bucket_accumulators(specification: &BsonDocument) -> Result<Vec<Accumulator>, CommandError> {
    match specification.get("output") {
        Some(BsonValue::Document(output)) => {
            let mut accumulators = Vec::new();
            for (field, value) in &output.fields {
                accumulators.push(parse_accumulator(field, value)?);
            }
            Ok(accumulators)
        }
        Some(_) => Err(CommandError::new(2, "$bucket `output` must be a document")),
        None => Ok(vec![Accumulator { field: "count".to_string(), kind: AccumulatorKind::Sum, argument: BsonValue::Int32(1) }]),
    }
}

fn parse_accumulator(field: &str, value: &BsonValue) -> Result<Accumulator, CommandError> {
    let BsonValue::Document(operator_document) = value
    else {
        return Err(CommandError::new(2, format!("accumulator `{field}` must be a document")));
    };
    if operator_document.fields.len() != 1 {
        return Err(CommandError::new(2, format!("accumulator `{field}` must contain exactly one operator")));
    }
    let (operator, argument) = &operator_document.fields[0];
    let Some(kind) = AccumulatorKind::parse(operator)
    else {
        return Err(CommandError::new(
            59,
            format!("unknown accumulator: {operator} (supported: $sum, $avg, $min, $max, $push, $addToSet, $first, $last)"),
        ));
    };
    Ok(Accumulator { field: field.to_string(), kind, argument: argument.clone() })
}

/// `$bucketAuto`: at most `buckets` buckets over sorted `groupBy` values.
///
/// Equal values always land in the same bucket, which is why the bucket count
/// can be lower than the request when the input has few distinct values.
fn bucket_auto_stage(documents: &[BsonDocument], body: &BsonValue, variables: &ExpressionVariables) -> Result<Vec<BsonDocument>, CommandError> {
    let BsonValue::Document(specification) = body
    else {
        return Err(CommandError::new(2, "$bucketAuto requires a document"));
    };
    let Some(group_by) = specification.get("groupBy")
    else {
        return Err(CommandError::new(2, "$bucketAuto requires a `groupBy` expression"));
    };
    let buckets = match specification.get("buckets") {
        Some(value) => match int_value(value) {
            Some(count) if count > 0 => count as usize,
            _ => return Err(CommandError::new(2, "$bucketAuto requires a positive `buckets` count")),
        },
        None => return Err(CommandError::new(2, "$bucketAuto requires `buckets`")),
    };
    let output = match specification.get("output") {
        Some(BsonValue::Document(output)) => {
            let mut accumulators = Vec::new();
            for (field, value) in &output.fields {
                accumulators.push(parse_accumulator(field, value)?);
            }
            Some(accumulators)
        }
        Some(_) => return Err(CommandError::new(2, "$bucketAuto `output` must be a document")),
        None => None,
    };

    let mut keys: Vec<(BsonValue, usize)> = Vec::with_capacity(documents.len());
    for (index, document) in documents.iter().enumerate() {
        keys.push((evaluate_expression_vars(document, group_by, variables)?, index));
    }
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    // Stable sort keeps equal values in input order.
    keys.sort_by(|left, right| compare_values(&left.0, &right.0));

    let target = keys.len().div_ceil(buckets);
    let mut output_documents = Vec::new();
    let mut start = 0_usize;
    while start < keys.len() {
        let mut end = start;
        let mut taken = 0_usize;
        while end < keys.len() {
            let group_start = end;
            let value = keys[end].0.clone();
            while end < keys.len() && compare_values(&keys[end].0, &value) == Ordering::Equal {
                end += 1;
            }
            taken += end - group_start;
            if taken >= target {
                break;
            }
        }
        let min = keys[start].0.clone();
        let max = if end < keys.len() { keys[end].0.clone() } else { keys[end - 1].0.clone() };
        let members: Vec<usize> = keys[start..end].iter().map(|(_, index)| *index).collect();
        let mut identifier = BsonDocument::new();
        identifier.push("min".to_string(), min);
        identifier.push("max".to_string(), max);
        let mut bucketed = BsonDocument::new();
        bucketed.push("_id".to_string(), BsonValue::Document(identifier));
        match &output {
            Some(accumulators) => {
                for accumulator in accumulators {
                    bucketed.push(accumulator.field.clone(), accumulate(accumulator, documents, &members)?);
                }
            }
            None => bucketed.push("count".to_string(), count_value(members.len() as i64)),
        }
        output_documents.push(bucketed);
        start = end;
    }
    Ok(output_documents)
}

/// One window output field: `$documentNumber`, `$rank`, `$denseRank` or `$sum`.
struct WindowOutput {
    field: String,
    kind: WindowKind,
    argument: BsonValue,
    window: Option<(WindowBound, WindowBound)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowKind {
    DocumentNumber,
    Rank,
    DenseRank,
    Sum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowBound {
    Unbounded,
    Current,
    Offset(i64),
}

/// `$setWindowFields`: partitions, sorts, and writes the supported window
/// fields. Partitions are emitted in ascending partition-key order and each
/// partition in `sortBy` order, matching `mongod 8.0.9`.
fn set_window_fields_stage(
    documents: &[BsonDocument],
    body: &BsonValue,
    variables: &ExpressionVariables,
) -> Result<Vec<BsonDocument>, CommandError> {
    let BsonValue::Document(specification) = body
    else {
        return Err(CommandError::new(2, "$setWindowFields requires a document"));
    };
    let partition_by = specification.get("partitionBy");
    let sort = match specification.get("sortBy") {
        Some(BsonValue::Document(sort)) => parse_sort_stage(sort)?,
        Some(_) => return Err(CommandError::new(2, "$setWindowFields `sortBy` must be a document")),
        None => Vec::new(),
    };
    let Some(BsonValue::Document(output_specification)) = specification.get("output")
    else {
        return Err(CommandError::new(2, "$setWindowFields requires an `output` document"));
    };

    let mut outputs: Vec<WindowOutput> = Vec::new();
    for (field, value) in &output_specification.fields {
        let BsonValue::Document(operator_document) = value
        else {
            return Err(CommandError::new(2, format!("$setWindowFields field `{field}` must be a document")));
        };
        let mut kind: Option<WindowKind> = None;
        let mut argument = BsonValue::Null;
        let mut window: Option<(WindowBound, WindowBound)> = None;
        for (operator, operand) in &operator_document.fields {
            match operator.as_str() {
                "$documentNumber" => kind = Some(WindowKind::DocumentNumber),
                "$rank" => kind = Some(WindowKind::Rank),
                "$denseRank" => kind = Some(WindowKind::DenseRank),
                "$sum" => {
                    kind = Some(WindowKind::Sum);
                    argument = operand.clone();
                }
                "window" => window = Some(parse_window(operand)?),
                other => {
                    return Err(CommandError::new(
                        59,
                        format!("unknown window operator: {other} (supported: $documentNumber, $rank, $denseRank, $sum)"),
                    ));
                }
            }
        }
        let Some(kind) = kind
        else {
            return Err(CommandError::new(2, format!("$setWindowFields field `{field}` has no operator")));
        };
        let required_sort = match kind {
            WindowKind::DocumentNumber => "$documentNumber",
            WindowKind::Rank => "$rank",
            WindowKind::DenseRank => "$denseRank",
            WindowKind::Sum => "",
        };
        if !required_sort.is_empty() {
            if sort.len() != 1 {
                return Err(CommandError::new(
                    5371602,
                    format!("{required_sort} must be specified with a top level sortBy expression with exactly one element"),
                ));
            }
            if window.is_some() {
                return Err(CommandError::new(2, format!("{required_sort} does not accept a `window`")));
            }
        }
        outputs.push(WindowOutput { field: field.clone(), kind, argument, window });
    }

    let mut partitions: Vec<(BsonValue, Vec<usize>)> = Vec::new();
    for (index, document) in documents.iter().enumerate() {
        let key = match partition_by {
            Some(expression) => evaluate_expression_vars(document, expression, variables)?,
            None => BsonValue::Null,
        };
        match partitions.iter_mut().find(|(existing, _)| values_equal(Some(existing), &key)) {
            Some((_, members)) => members.push(index),
            None => partitions.push((key, vec![index])),
        }
    }
    partitions.sort_by(|left, right| compare_values(&left.0, &right.0));
    for (_, members) in &mut partitions {
        members.sort_by(|left, right| compare_documents(documents, *left, *right, &sort));
    }

    let mut result = Vec::new();
    for (_, members) in &partitions {
        let mut dense_rank = 0_i32;
        let mut previous_key: Option<Vec<BsonValue>> = None;
        for (position, index) in members.iter().enumerate() {
            let key = sort_keys(&documents[*index], &sort);
            let same_as_previous = previous_key.as_ref().map(|previous| previous == &key).unwrap_or(false);
            if position == 0 || !same_as_previous {
                dense_rank += 1;
            }
            let rank = if position == 0 {
                1
            }
            else if same_as_previous {
                // The rank of a tie is the position of the first document in its group.
                let mut first = position;
                while first > 0 && sort_keys(&documents[members[first - 1]], &sort) == key {
                    first -= 1;
                }
                (first + 1) as i32
            }
            else {
                (position + 1) as i32
            };
            previous_key = Some(key);

            let mut expanded = documents[*index].clone();
            for output in &outputs {
                let value = match output.kind {
                    WindowKind::DocumentNumber => count_value((position + 1) as i64),
                    WindowKind::Rank => count_value(i64::from(rank)),
                    WindowKind::DenseRank => count_value(i64::from(dense_rank)),
                    WindowKind::Sum => {
                        let (start, end) = output.window.unwrap_or((WindowBound::Unbounded, WindowBound::Unbounded));
                        let window_members = window_range(members, position, start, end);
                        let accumulator =
                            Accumulator { field: output.field.clone(), kind: AccumulatorKind::Sum, argument: output.argument.clone() };
                        accumulate(&accumulator, documents, &window_members)?
                    }
                };
                set_field_path(&mut expanded, &output.field, value);
            }
            result.push(expanded);
        }
    }
    Ok(result)
}

fn parse_window(value: &BsonValue) -> Result<(WindowBound, WindowBound), CommandError> {
    let BsonValue::Document(specification) = value
    else {
        return Err(CommandError::new(2, "`window` must be a document"));
    };
    if specification.get("range").is_some() {
        return Err(CommandError::new(59, "`window.range` is outside this gateway (supported: `window.documents`)"));
    }
    let Some(BsonValue::Array(bounds)) = specification.get("documents")
    else {
        return Err(CommandError::new(2, "`window` requires a `documents` array"));
    };
    if bounds.len() != 2 {
        return Err(CommandError::new(2, "`window.documents` requires exactly two bounds"));
    }
    Ok((parse_window_bound(&bounds[0])?, parse_window_bound(&bounds[1])?))
}

fn parse_window_bound(value: &BsonValue) -> Result<WindowBound, CommandError> {
    match value {
        BsonValue::String(text) if text == "unbounded" => Ok(WindowBound::Unbounded),
        BsonValue::String(text) if text == "current" => Ok(WindowBound::Current),
        _ => match int_value(value) {
            Some(offset) => Ok(WindowBound::Offset(offset)),
            None => Err(CommandError::new(2, "`window.documents` bounds must be `unbounded`, `current` or an integer")),
        },
    }
}

fn window_range(members: &[usize], position: usize, start: WindowBound, end: WindowBound) -> Vec<usize> {
    let last = members.len().saturating_sub(1);
    let start_index = match start {
        WindowBound::Unbounded => 0,
        WindowBound::Current => position,
        WindowBound::Offset(offset) => offset_position(position, offset, last),
    };
    let end_index = match end {
        WindowBound::Unbounded => last,
        WindowBound::Current => position,
        WindowBound::Offset(offset) => offset_position(position, offset, last),
    };
    if start_index > end_index || start_index > last {
        return Vec::new();
    }
    members[start_index..=end_index.min(last)].to_vec()
}

fn offset_position(position: usize, offset: i64, last: usize) -> usize {
    if offset >= 0 { position.saturating_add(offset as usize).min(last) } else { position.saturating_sub((-offset) as usize) }
}

fn sort_keys(document: &BsonDocument, sort: &[(String, i32)]) -> Vec<BsonValue> {
    sort.iter().map(|(field, direction)| sort_key(document, field, *direction)).collect()
}

fn compare_documents(documents: &[BsonDocument], left: usize, right: usize, sort: &[(String, i32)]) -> Ordering {
    for (field, direction) in sort {
        let ordering = compare_values(&sort_key(&documents[left], field, *direction), &sort_key(&documents[right], field, *direction));
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

/// `$sortByCount` over a `$`-prefixed field path.
///
/// The whole value is one group key, including an array or an empty array, and a
/// missing field groups as `null`, matching `mongod 8.0.9`. Expression objects
/// are a separate implementation and stay outside this gateway.
fn sort_by_count_stage(documents: &[BsonDocument], body: &BsonValue) -> Result<Vec<BsonDocument>, CommandError> {
    let path = match body {
        BsonValue::String(path) if path.starts_with('$') && path.len() > 1 => path[1..].to_string(),
        BsonValue::String(_) => {
            return Err(CommandError::new(
                40148,
                "the sortByCount field must be defined as a $-prefixed path or an expression inside an object",
            ));
        }
        BsonValue::Document(_) => {
            return Err(CommandError::new(
                59,
                "$sortByCount expressions are outside this gateway (coordinate: only `$`-prefixed field paths are supported)",
            ));
        }
        _ => {
            return Err(CommandError::new(40149, "the sortByCount field must be specified as a string or as an object"));
        }
    };

    let mut groups: Vec<(BsonValue, i32)> = Vec::new();
    let mut positions: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
    for document in documents {
        let value = lookup_field(document, &path).cloned().unwrap_or(BsonValue::Null);
        match positions.get(&value_to_key(&value)) {
            Some(position) => groups[*position].1 += 1,
            None => {
                positions.insert(value_to_key(&value), groups.len());
                groups.push((value, 1));
            }
        }
    }
    // Descending count, ties in first-appearance order. `mongod` leaves equal
    // counts in hash order, so equal counts are never asserted.
    groups.sort_by_key(|left| std::cmp::Reverse(left.1));
    Ok(groups
        .into_iter()
        .map(|(value, count)| {
            let mut grouped = BsonDocument::new();
            grouped.push("_id".to_string(), value);
            grouped.push("count".to_string(), count_value(i64::from(count)));
            grouped
        })
        .collect())
}

/// Accumulator kinds backed by a documented behavior sentence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccumulatorKind {
    Sum,
    Avg,
    Min,
    Max,
    Push,
    AddToSet,
    First,
    Last,
}

impl AccumulatorKind {
    fn parse(operator: &str) -> Option<Self> {
        match operator {
            "$sum" => Some(Self::Sum),
            "$avg" => Some(Self::Avg),
            "$min" => Some(Self::Min),
            "$max" => Some(Self::Max),
            "$push" => Some(Self::Push),
            "$addToSet" => Some(Self::AddToSet),
            "$first" => Some(Self::First),
            "$last" => Some(Self::Last),
            _ => None,
        }
    }
}

struct Accumulator {
    field: String,
    kind: AccumulatorKind,
    argument: BsonValue,
}

/// `$group` groups by `_id` (null, constant, or `"$field"`) and applies the
/// supported accumulators to each group. Output order is unspecified by the
/// manual, so callers must sort if they need one.
fn group_stage(documents: &[BsonDocument], specification: &BsonDocument) -> Result<Vec<BsonDocument>, CommandError> {
    let Some(identifier) = specification.get("_id")
    else {
        return Err(CommandError::new(2, "$group requires an `_id` expression"));
    };
    match identifier {
        BsonValue::Document(_) => {
            return Err(CommandError::new(
                59,
                "$group `_id` document expressions are not implemented; use null, a constant, or a \"$field\" path",
            ));
        }
        BsonValue::Array(_) => {
            return Err(CommandError::new(
                59,
                "$group `_id` array expressions are not implemented; use null, a constant, or a \"$field\" path",
            ));
        }
        _ => {}
    }

    let mut accumulators: Vec<Accumulator> = Vec::new();
    for (field, accumulator) in specification.fields.iter().filter(|(field, _)| field != "_id") {
        let BsonValue::Document(operator_document) = accumulator
        else {
            return Err(CommandError::new(2, format!("$group field `{field}` must be an accumulator document")));
        };
        if operator_document.fields.len() != 1 {
            return Err(CommandError::new(2, format!("$group field `{field}` must contain exactly one accumulator")));
        }
        let (operator, argument) = &operator_document.fields[0];
        let Some(kind) = AccumulatorKind::parse(operator)
        else {
            return Err(CommandError::new(
                59,
                format!("unknown accumulator: {operator} (supported: $sum, $avg, $min, $max, $push, $addToSet, $first, $last)"),
            ));
        };
        accumulators.push(Accumulator { field: field.clone(), kind, argument: argument.clone() });
    }

    // "If there are no documents from the prior pipeline stage, the $group stage
    // returns nothing."
    if documents.is_empty() {
        return Ok(Vec::new());
    }

    let mut groups: Vec<(BsonValue, Vec<usize>)> = Vec::new();
    for (index, document) in documents.iter().enumerate() {
        let key = evaluate_group_id(document, identifier);
        match groups.iter_mut().find(|(existing, _)| values_equal(Some(existing), &key)) {
            Some((_, members)) => members.push(index),
            None => groups.push((key, vec![index])),
        }
    }

    let mut output = Vec::new();
    for (key, members) in groups {
        let mut grouped = BsonDocument::new();
        grouped.push("_id".to_string(), key);
        for accumulator in &accumulators {
            grouped.push(accumulator.field.clone(), accumulate(accumulator, documents, &members)?);
        }
        output.push(grouped);
    }
    Ok(output)
}

fn evaluate_group_id(document: &BsonDocument, identifier: &BsonValue) -> BsonValue {
    match identifier {
        BsonValue::String(path) if path.starts_with('$') => lookup_field(document, &path[1..]).cloned().unwrap_or(BsonValue::Null),
        other => other.clone(),
    }
}

fn accumulate(accumulator: &Accumulator, documents: &[BsonDocument], members: &[usize]) -> Result<BsonValue, CommandError> {
    // `$push` and `$addToSet` skip a missing field entirely while still keeping an
    // explicit `null`; every other accumulator treats missing as `null`.
    let optional_values: Vec<Option<BsonValue>> =
        members.iter().map(|index| evaluate_expression_optional(&documents[*index], &accumulator.argument)).collect::<Result<Vec<_>, _>>()?;
    let values: Vec<BsonValue> = optional_values.iter().cloned().map(|value| value.unwrap_or(BsonValue::Null)).collect();

    match accumulator.kind {
        AccumulatorKind::Sum => {
            let mut total: i128 = 0;
            let mut double_total: Option<f64> = None;
            for value in values {
                match value {
                    BsonValue::Int32(number) => total += i128::from(number),
                    BsonValue::Int64(number) => total += i128::from(number),
                    BsonValue::Double(number) => *double_total.get_or_insert(0.0) += number,
                    // "$sum ignores the non-numeric values"
                    _ => {}
                }
            }
            Ok(match double_total {
                Some(double) => BsonValue::Double(double + total as f64),
                None => match i64::try_from(total) {
                    Ok(exact) => count_value(exact),
                    Err(_) => BsonValue::Double(total as f64),
                },
            })
        }
        AccumulatorKind::Avg => {
            let mut total = 0.0_f64;
            let mut count = 0_u64;
            for value in values {
                if let Some(number) = number_of(&value) {
                    total += number;
                    count += 1;
                }
            }
            // "$avg returns null if all input values are non-numeric"
            Ok(if count == 0 { BsonValue::Null } else { BsonValue::Double(total / count as f64) })
        }
        AccumulatorKind::Min | AccumulatorKind::Max => {
            let wanted = if accumulator.kind == AccumulatorKind::Min { Ordering::Less } else { Ordering::Greater };
            let mut chosen: Option<BsonValue> = None;
            for value in values {
                // "$min/$max ignore null or missing values"
                if matches!(value, BsonValue::Null) {
                    continue;
                }
                chosen = Some(match chosen {
                    None => value,
                    Some(current) => {
                        if compare_values(&value, &current) == wanted {
                            value
                        }
                        else {
                            current
                        }
                    }
                });
            }
            Ok(chosen.unwrap_or(BsonValue::Null))
        }
        AccumulatorKind::Push => Ok(BsonValue::Array(optional_values.into_iter().flatten().collect())),
        AccumulatorKind::AddToSet => {
            let mut unique: Vec<BsonValue> = Vec::new();
            for value in optional_values.into_iter().flatten() {
                if !unique.iter().any(|existing| values_equal(Some(existing), &value)) {
                    unique.push(value);
                }
            }
            Ok(BsonValue::Array(unique))
        }
        AccumulatorKind::First => Ok(values.first().cloned().unwrap_or(BsonValue::Null)),
        AccumulatorKind::Last => Ok(values.last().cloned().unwrap_or(BsonValue::Null)),
    }
}

/// `$addFields`: keeps every input field and adds or replaces the specified
/// paths. Expressions are evaluated against the input document, so fields in
/// the same specification never observe each other.
fn add_fields_stage(documents: &[BsonDocument], specification: &BsonDocument) -> Result<Vec<BsonDocument>, CommandError> {
    let mut output = Vec::with_capacity(documents.len());
    for document in documents {
        let mut result = document.clone();
        for (field, expression) in &specification.fields {
            if field.starts_with('$') {
                return Err(CommandError::new(2, "FieldPath field names may not start with '$'. Consider using $getField or $setField."));
            }
            let value = evaluate_expression(document, expression)?;
            let segments: Vec<&str> = field.split('.').collect();
            set_aggregation_segments(&mut result, &segments, value)?;
        }
        output.push(result);
    }
    Ok(output)
}

fn project_stage(documents: &[BsonDocument], specification: &BsonDocument) -> Result<Vec<BsonDocument>, CommandError> {
    // The projection mode is fixed by the first non-`_id` field, and a later
    // field of the other mode is rejected with the same code and message as
    // `mongod 8.0.9`. Boolean/numeric `_id` rules never count as a conflict.
    let mut exclusion_projection: Option<bool> = None;
    for (field, rule) in specification.fields.iter() {
        if field == "_id" && (rule_is_exclusion(rule) || rule_is_inclusion(rule)) {
            continue;
        }
        let is_exclusion = rule_is_exclusion(rule);
        match exclusion_projection {
            None => exclusion_projection = Some(is_exclusion),
            Some(true) if !is_exclusion => {
                if rule_is_inclusion(rule) {
                    return Err(CommandError::new(
                        31253,
                        format!("Invalid $project :: caused by :: Cannot do inclusion on field {field} in exclusion projection"),
                    ));
                }
                return Err(CommandError::new(
                    31252,
                    "Invalid $project :: caused by :: Cannot use expression other than $meta in exclusion projection".to_string(),
                ));
            }
            Some(false) if is_exclusion => {
                return Err(CommandError::new(
                    31254,
                    format!("Invalid $project :: caused by :: Cannot do exclusion on field {field} in inclusion projection"),
                ));
            }
            _ => {}
        }
    }
    let has_exclusion = exclusion_projection.unwrap_or(false);
    if has_exclusion {
        let mut projected = Vec::new();
        for document in documents {
            let mut kept = document.clone();
            for (field, rule) in specification.fields.iter() {
                if rule_is_exclusion(rule) {
                    remove_field_path(&mut kept, field);
                }
            }
            projected.push(kept);
        }
        return Ok(projected);
    }

    let id_rule = specification.get("_id");
    let id_excluded = id_rule.map(rule_is_exclusion).unwrap_or(false);
    let id_expression = id_rule.filter(|rule| !rule_is_exclusion(rule) && !rule_is_inclusion(rule));
    let mut projected = Vec::new();
    for document in documents {
        let mut result = BsonDocument::new();
        if !id_excluded {
            if let Some(expression) = id_expression {
                if let Some(value) = evaluate_expression_optional(document, expression)? {
                    result.push("_id".to_string(), value);
                }
            }
            else if let Some(identifier) = document.get("_id") {
                result.push("_id".to_string(), identifier.clone());
            }
        }
        for (field, rule) in specification.fields.iter() {
            if field == "_id" {
                continue;
            }
            if rule_is_inclusion(rule) {
                if let Some(value) = lookup_field(document, field) {
                    set_field_path(&mut result, field, value.clone());
                }
                continue;
            }
            if rule_is_exclusion(rule) {
                continue;
            }
            let value = evaluate_expression(document, rule)?;
            set_field_path(&mut result, field, value);
        }
        projected.push(result);
    }
    Ok(projected)
}

/// Minimal fixpoint helpers used by `$project`, `$unwind` and `$sort`.
fn set_field_path(document: &mut BsonDocument, path: &str, value: BsonValue) {
    let (head, tail) = match path.split_once('.') {
        Some((head, tail)) => (head, tail),
        None => {
            if let Some(entry) = document.fields.iter_mut().find(|(key, _)| key == path) {
                entry.1 = value;
            }
            else {
                document.push(path.to_string(), value);
            }
            return;
        }
    };
    if let Some(entry) = document.fields.iter_mut().find(|(key, _)| key == head)
        && let BsonValue::Document(inner) = &mut entry.1
    {
        set_field_path(inner, tail, value);
        return;
    }
    let mut nested = BsonDocument::new();
    set_field_path(&mut nested, tail, value);
    document.push(head.to_string(), BsonValue::Document(nested));
}

fn remove_field_path(document: &mut BsonDocument, path: &str) {
    match path.split_once('.') {
        None => document.fields.retain(|(key, _)| key != path),
        Some((head, tail)) => {
            if let Some(entry) = document.fields.iter_mut().find(|(key, _)| key == head)
                && let BsonValue::Document(inner) = &mut entry.1
            {
                remove_field_path(inner, tail);
            }
        }
    }
}

/// Variables bound by `$lookup` `let` (and future stages). A `$$name` expression
/// reads them, a `$name` expression reads the candidate document.
type ExpressionVariables = BTreeMap<String, BsonValue>;

fn empty_variables() -> ExpressionVariables {
    BTreeMap::new()
}

/// A field-path expression evaluates to `None` when the field is missing, so
/// `$push` / `$addToSet` can skip it instead of turning it into `null`.
fn evaluate_expression_optional(document: &BsonDocument, expression: &BsonValue) -> Result<Option<BsonValue>, CommandError> {
    evaluate_expression_optional_vars(document, expression, &empty_variables())
}

fn evaluate_expression(document: &BsonDocument, expression: &BsonValue) -> Result<BsonValue, CommandError> {
    evaluate_expression_vars(document, expression, &empty_variables())
}

fn evaluate_expression_optional_vars(
    document: &BsonDocument,
    expression: &BsonValue,
    variables: &ExpressionVariables,
) -> Result<Option<BsonValue>, CommandError> {
    match expression {
        BsonValue::String(variable) if variable.starts_with("$$") => Ok(variables.get(&variable[2..]).cloned()),
        BsonValue::String(path) if path.starts_with('$') => Ok(lookup_field(document, &path[1..]).cloned()),
        other => evaluate_expression_vars(document, other, variables).map(Some),
    }
}

fn evaluate_expression_vars(
    document: &BsonDocument,
    expression: &BsonValue,
    variables: &ExpressionVariables,
) -> Result<BsonValue, CommandError> {
    match expression {
        BsonValue::String(path) if path.starts_with("$$") => Ok(variables.get(&path[2..]).cloned().unwrap_or(BsonValue::Null)),
        BsonValue::String(path) if path.starts_with('$') => Ok(lookup_field(document, &path[1..]).cloned().unwrap_or(BsonValue::Null)),
        BsonValue::Document(operator_document) if operator_document.fields.len() == 1 && operator_document.fields[0].0.starts_with('$') => {
            let (operator, argument) = &operator_document.fields[0];
            match operator.as_str() {
                "$literal" => Ok(argument.clone()),
                "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" => {
                    let BsonValue::Array(arguments) = argument
                    else {
                        return Err(CommandError::new(2, format!("{operator} requires an array of two expressions")));
                    };
                    if arguments.len() != 2 {
                        return Err(CommandError::new(2, format!("{operator} requires exactly two expressions")));
                    }
                    let left = evaluate_expression_vars(document, &arguments[0], variables)?;
                    let right = evaluate_expression_vars(document, &arguments[1], variables)?;
                    let ordering = compare_values(&left, &right);
                    Ok(BsonValue::Bool(match operator.as_str() {
                        "$eq" => ordering == Ordering::Equal,
                        "$ne" => ordering != Ordering::Equal,
                        "$gt" => ordering == Ordering::Greater,
                        "$gte" => ordering != Ordering::Less,
                        "$lt" => ordering == Ordering::Less,
                        _ => ordering != Ordering::Greater,
                    }))
                }
                "$in" => {
                    let BsonValue::Array(arguments) = argument
                    else {
                        return Err(CommandError::new(2, "$in requires an array of two expressions"));
                    };
                    if arguments.len() != 2 {
                        return Err(CommandError::new(2, "$in requires exactly two expressions"));
                    }
                    let needle = evaluate_expression_vars(document, &arguments[0], variables)?;
                    let haystack = evaluate_expression_vars(document, &arguments[1], variables)?;
                    let BsonValue::Array(values) = haystack
                    else {
                        return Err(CommandError::new(2, "$in second argument must be an array"));
                    };
                    Ok(BsonValue::Bool(values.iter().any(|value| compare_values(&needle, value) == Ordering::Equal)))
                }
                "$and" | "$or" => {
                    let BsonValue::Array(arguments) = argument
                    else {
                        return Err(CommandError::new(2, format!("{operator} requires an array of expressions")));
                    };
                    let mut result = operator == "$and";
                    for expression in arguments {
                        let value = evaluate_expression_vars(document, expression, variables)?;
                        let truthy = expression_truthy(&value);
                        if operator == "$and" {
                            result = result && truthy;
                        }
                        else {
                            result = result || truthy;
                        }
                    }
                    Ok(BsonValue::Bool(result))
                }
                "$not" => {
                    let BsonValue::Array(arguments) = argument
                    else {
                        return Err(CommandError::new(2, "$not requires an array of one expression"));
                    };
                    let first = arguments.first().ok_or_else(|| CommandError::new(2, "$not requires one expression"))?;
                    let value = evaluate_expression_vars(document, first, variables)?;
                    Ok(BsonValue::Bool(!expression_truthy(&value)))
                }
                "$cond" => {
                    let (condition, then_value, else_value) = match argument {
                        BsonValue::Array(arguments) if arguments.len() == 3 => (&arguments[0], &arguments[1], &arguments[2]),
                        BsonValue::Document(options) => {
                            let condition = options.get("if").ok_or_else(|| CommandError::new(17072, "$cond requires an 'if' argument"))?;
                            let then_value = options.get("then").ok_or_else(|| CommandError::new(17072, "$cond requires a 'then' argument"))?;
                            let else_value =
                                options.get("else").ok_or_else(|| CommandError::new(17072, "$cond requires an 'else' argument"))?;
                            (condition, then_value, else_value)
                        }
                        _ => {
                            return Err(CommandError::new(
                                17072,
                                "$cond requires either an array of three expressions or an object with 'if', 'then' and 'else'",
                            ));
                        }
                    };
                    let condition = evaluate_expression_vars(document, condition, variables)?;
                    if expression_truthy(&condition) {
                        evaluate_expression_vars(document, then_value, variables)
                    }
                    else {
                        evaluate_expression_vars(document, else_value, variables)
                    }
                }
                "$add" | "$subtract" | "$multiply" | "$divide" | "$mod" | "$pow" => {
                    evaluate_arithmetic(document, operator, argument, variables)
                }
                "$abs" | "$ceil" | "$floor" | "$sqrt" => {
                    let operand = evaluate_expression_vars(document, argument, variables)?;
                    match numeric_operand(&operand, operator)? {
                        ArithmeticOperand::Null => Ok(BsonValue::Null),
                        ArithmeticOperand::Integer { value, long } => match operator.as_str() {
                            "$abs" => integer_arithmetic_result(value.abs(), long, operator),
                            "$ceil" | "$floor" => integer_arithmetic_result(value, long, operator),
                            _ => {
                                if value < 0 {
                                    return Err(CommandError::new(28714, "$sqrt's argument must be greater than or equal to 0"));
                                }
                                Ok(BsonValue::Double((value as f64).sqrt()))
                            }
                        },
                        ArithmeticOperand::Double(number) => match operator.as_str() {
                            "$abs" => Ok(BsonValue::Double(number.abs())),
                            "$ceil" => Ok(BsonValue::Double(number.ceil())),
                            "$floor" => Ok(BsonValue::Double(number.floor())),
                            _ => {
                                if number < 0.0 {
                                    return Err(CommandError::new(28714, "$sqrt's argument must be greater than or equal to 0"));
                                }
                                Ok(BsonValue::Double(number.sqrt()))
                            }
                        },
                        ArithmeticOperand::DateTime(_) => unreachable!("numeric_operand rejects dates"),
                    }
                }
                "$round" | "$trunc" => {
                    let arguments = match argument {
                        BsonValue::Array(arguments) if !arguments.is_empty() && arguments.len() <= 2 => arguments.as_slice(),
                        _ => {
                            return Err(CommandError::new(2, format!("{operator} requires an array of one or two expressions")));
                        }
                    };
                    let number = evaluate_expression_vars(document, &arguments[0], variables)?;
                    let place = match arguments.get(1) {
                        Some(expression) => evaluate_expression_vars(document, expression, variables)?,
                        None => BsonValue::Int32(0),
                    };
                    let place = match integral_i64(&place) {
                        Some(place) if (-20..=100).contains(&place) => place as i32,
                        Some(_) => {
                            return Err(CommandError::new(2, format!("{operator} places must be between -20 and 100")));
                        }
                        None => {
                            return Err(CommandError::new(2, format!("{operator} places must be an integer")));
                        }
                    };
                    match numeric_operand(&number, operator)? {
                        ArithmeticOperand::Null => Ok(BsonValue::Null),
                        ArithmeticOperand::Integer { value, long } => {
                            let rounded = round_integer_to_place(value, place, operator == "$round");
                            integer_arithmetic_result(rounded, long, operator)
                        }
                        ArithmeticOperand::Double(number) => {
                            let rounded = round_double_to_place(number, place, operator == "$round");
                            Ok(BsonValue::Double(rounded))
                        }
                        ArithmeticOperand::DateTime(_) => unreachable!("numeric_operand rejects dates"),
                    }
                }
                "$concat" => {
                    let BsonValue::Array(arguments) = argument
                    else {
                        return Err(CommandError::new(2, "$concat requires an array of expressions"));
                    };
                    let mut joined = String::new();
                    for expression in arguments {
                        match evaluate_expression_vars(document, expression, variables)? {
                            BsonValue::String(text) => joined.push_str(&text),
                            // "$concat returns null if any argument is null or missing"
                            BsonValue::Null => return Ok(BsonValue::Null),
                            other => {
                                return Err(CommandError::new(2, format!("$concat requires string arguments, got {other:?}")));
                            }
                        }
                    }
                    Ok(BsonValue::String(joined))
                }
                "$toLower" | "$toUpper" | "$strLenCP" | "$strLenBytes" | "$substr" | "$substrBytes" | "$substrCP" | "$split" | "$trim"
                | "$ltrim" | "$rtrim" | "$indexOfCP" | "$indexOfBytes" | "$replaceOne" | "$replaceAll" | "$strcasecmp" => {
                    evaluate_string_operator(document, operator, argument, variables)
                }
                "$isArray" | "$arrayElemAt" | "$first" | "$last" | "$concatArrays" | "$reverseArray" | "$indexOfArray" | "$range"
                | "$slice" => evaluate_array_operator(document, operator, argument, variables),
                "$year" | "$month" | "$dayOfMonth" | "$hour" | "$minute" | "$second" | "$millisecond" | "$dayOfWeek" | "$dayOfYear"
                | "$week" | "$isoWeek" | "$isoWeekYear" | "$isoDayOfWeek" | "$dateToString" | "$dateFromString" | "$dateToParts"
                | "$dateFromParts" | "$dateAdd" | "$dateSubtract" | "$dateDiff" | "$dateTrunc" => {
                    evaluate_date_operator(document, operator, argument, variables)
                }
                "$toString" | "$toInt" | "$toLong" | "$toDouble" | "$toDecimal" | "$toBool" | "$toObjectId" | "$toDate" | "$type"
                | "$convert" | "$isNumber" => evaluate_conversion_operator(document, operator, argument, variables),
                "$size" => {
                    let value = evaluate_expression_vars(document, argument, variables)?;
                    match value {
                        BsonValue::Array(items) => Ok(count_value(items.len() as i64)),
                        other => Err(CommandError::new(
                            17124,
                            format!("The argument to $size must be an array, but was of type: {}", bson_type_name(&other)),
                        )),
                    }
                }
                "$ifNull" => {
                    let BsonValue::Array(arguments) = argument
                    else {
                        return Err(CommandError::new(2, "$ifNull requires an array of expressions"));
                    };
                    if arguments.len() < 2 {
                        return Err(CommandError::new(2, "$ifNull requires at least two arguments"));
                    }
                    for expression in arguments {
                        let value = evaluate_expression_vars(document, expression, variables)?;
                        if !matches!(value, BsonValue::Null) {
                            return Ok(value);
                        }
                    }
                    Ok(BsonValue::Null)
                }
                other => Err(CommandError::new(
                    59,
                    format!(
                        "unknown $project expression: {other} (supported: $add, $subtract, $multiply, $divide, $mod, $pow, $abs, $ceil, $floor, $round, $trunc, $sqrt, $concat, $toLower, $toUpper, $strLenCP, $strLenBytes, $substr, $substrBytes, $substrCP, $split, $trim, $ltrim, $rtrim, $indexOfCP, $indexOfBytes, $replaceOne, $replaceAll, $strcasecmp, $isArray, $arrayElemAt, $first, $last, $concatArrays, $reverseArray, $indexOfArray, $range, $slice, $year, $month, $dayOfMonth, $hour, $minute, $second, $millisecond, $dayOfWeek, $dayOfYear, $week, $isoWeek, $isoWeekYear, $isoDayOfWeek, $dateToString, $dateFromString, $dateToParts, $dateFromParts, $dateAdd, $dateSubtract, $dateDiff, $dateTrunc, $toString, $toInt, $toLong, $toDouble, $toDecimal, $toBool, $toObjectId, $toDate, $type, $convert, $isNumber, $cond, $ifNull, $size, $literal, $eq, $ne, $gt, $gte, $lt, $lte, $in, $and, $or, $not)"
                    ),
                )),
            }
        }
        BsonValue::Document(fields) => {
            // A document whose single field is not an operator is a literal
            // object whose values are themselves expressions; a missing field
            // is omitted rather than rendered as null.
            let mut result = BsonDocument::new();
            for (field, value) in &fields.fields {
                if let Some(evaluated) = evaluate_expression_optional_vars(document, value, variables)? {
                    set_field_path(&mut result, field, evaluated);
                }
            }
            Ok(BsonValue::Document(result))
        }
        other => Ok(other.clone()),
    }
}

/// 算术算子归一化后的操作数。
///
/// `Null` 同时覆盖 `null` 与已废弃的 `undefined`；`DateTime` 只在 `$add` 与
/// `$subtract` 里合法。Decimal128 的精确十进制算术尚未实现，按不支持处理。
enum ArithmeticOperand {
    /// `null` 或 `undefined`，算术结果直接是 `null`。
    Null,
    /// 整数操作数；`long` 标记原始 BSON 类型是否为 64 位。
    Integer {
        /// 以 `i128` 承载，避免 `int` / `long` 相加时中途丢失精度。
        value: i128,
        /// 原始类型是 `long` 时，结果只能落在 `long`。
        long: bool,
    },
    /// 双精度操作数。
    Double(f64),
    /// BSON Date，自 Unix 纪元起的毫秒数。
    DateTime(i64),
}

impl ArithmeticOperand {
    /// 排除 `null` 与日期之后的 `f64` 视图，供 `$divide` / `$mod` / `$pow` 使用。
    fn as_double(&self) -> Option<f64> {
        match self {
            Self::Integer { value, .. } => Some(*value as f64),
            Self::Double(number) => Some(*number),
            Self::Null | Self::DateTime(_) => None,
        }
    }
}

/// 把表达式结果归一化成算术操作数。
///
/// 接受 `int`、`long`、`double` 与 `date`，其余类型按 `mongod` 的
/// `TypeMismatch` 拒绝。错误码与措辞待真实 `mongod 8.0.9` 录制核对。
fn arithmetic_operand(value: &BsonValue, operator: &str) -> Result<ArithmeticOperand, CommandError> {
    Ok(match value {
        BsonValue::Null | BsonValue::Undefined => ArithmeticOperand::Null,
        BsonValue::Int32(number) => ArithmeticOperand::Integer { value: i128::from(*number), long: false },
        BsonValue::Int64(number) => ArithmeticOperand::Integer { value: i128::from(*number), long: true },
        BsonValue::Double(number) => ArithmeticOperand::Double(*number),
        BsonValue::DateTime(milliseconds) => ArithmeticOperand::DateTime(*milliseconds),
        other => {
            return Err(CommandError::new(16554, format!("{operator} only supports numeric or date types, not {}", bson_type_name(other))));
        }
    })
}

/// 只接受数值的算术算子（乘、除、取模、乘方、取整）使用的入口。
fn numeric_operand(value: &BsonValue, operator: &str) -> Result<ArithmeticOperand, CommandError> {
    match value {
        BsonValue::DateTime(_) => Err(CommandError::new(16555, format!("{operator} only supports numeric types, not date"))),
        BsonValue::Null | BsonValue::Undefined | BsonValue::Int32(_) | BsonValue::Int64(_) | BsonValue::Double(_) => {
            arithmetic_operand(value, operator)
        }
        other => Err(CommandError::new(16555, format!("{operator} only supports numeric types, not {}", bson_type_name(other)))),
    }
}

/// 把精确整数结果按最小可用的 BSON 整数类型写出。
///
/// 两侧都曾是 `int` 时优先 `int32`，放不下升到 `int64`；任一侧是 `long` 时
/// 只允许 `int64`，溢出即报错。
fn integer_arithmetic_result(value: i128, long: bool, operator: &str) -> Result<BsonValue, CommandError> {
    if !long && let Ok(small) = i32::try_from(value) {
        return Ok(BsonValue::Int32(small));
    }
    i64::try_from(value).map(BsonValue::Int64).map_err(|_| CommandError::new(2, format!("{operator} result overflows a 64-bit integer")))
}

/// 实现 `$add`、`$subtract`、`$multiply`、`$divide`、`$mod` 与 `$pow`。
///
/// 数值提升规则：任一操作数为 `double` 时结果为 `double`；两侧都是整数时保持
/// 整数，`int32` 放不下才升到 `int64`，`int64` 溢出报错。日期只参与 `$add`
/// 与 `$subtract`，且偏移量必须是整毫秒。所有错误码、错误措辞与 `$round`
/// 的浮点精度均待真实 `mongod 8.0.9` 录制核对。
fn evaluate_arithmetic(
    document: &BsonDocument,
    operator: &str,
    argument: &BsonValue,
    variables: &ExpressionVariables,
) -> Result<BsonValue, CommandError> {
    let BsonValue::Array(arguments) = argument
    else {
        return Err(CommandError::new(2, format!("{operator} requires an array of expressions")));
    };
    let mut values = Vec::with_capacity(arguments.len());
    for expression in arguments {
        values.push(evaluate_expression_vars(document, expression, variables)?);
    }
    match operator {
        "$add" => {
            let mut integer_total: i128 = 0;
            let mut long = false;
            let mut double_total: Option<f64> = None;
            let mut date: Option<i64> = None;
            for value in &values {
                match arithmetic_operand(value, operator)? {
                    ArithmeticOperand::Null => return Ok(BsonValue::Null),
                    ArithmeticOperand::Integer { value, long: operand_long } => {
                        integer_total += value;
                        long |= operand_long;
                    }
                    ArithmeticOperand::Double(number) => *double_total.get_or_insert(0.0) += number,
                    ArithmeticOperand::DateTime(milliseconds) => {
                        if date.replace(milliseconds).is_some() {
                            return Err(CommandError::new(2, "$add cannot add two dates"));
                        }
                    }
                }
            }
            if let Some(milliseconds) = date {
                let offset = match double_total {
                    Some(number) if number.fract() == 0.0 && number >= i64::MIN as f64 && number <= i64::MAX as f64 => {
                        integer_total + number as i128
                    }
                    Some(_) => return Err(CommandError::new(2, "$add cannot add a non-integral value to a date")),
                    None => integer_total,
                };
                let shifted = i64::try_from(offset).ok().and_then(|offset| milliseconds.checked_add(offset));
                return shifted.map(BsonValue::DateTime).ok_or_else(|| CommandError::new(2, "$add date result is out of range"));
            }
            Ok(match double_total {
                Some(total) => BsonValue::Double(total + integer_total as f64),
                None => integer_arithmetic_result(integer_total, long, operator)?,
            })
        }
        "$subtract" => {
            if values.len() != 2 {
                return Err(CommandError::new(2, "$subtract requires exactly two expressions"));
            }
            let left = arithmetic_operand(&values[0], operator)?;
            let right = arithmetic_operand(&values[1], operator)?;
            match (left, right) {
                (ArithmeticOperand::Null, _) | (_, ArithmeticOperand::Null) => Ok(BsonValue::Null),
                (ArithmeticOperand::DateTime(left), ArithmeticOperand::DateTime(right)) => {
                    left.checked_sub(right).map(BsonValue::Int64).ok_or_else(|| CommandError::new(2, "$subtract date result is out of range"))
                }
                (ArithmeticOperand::DateTime(left), ArithmeticOperand::Integer { value, .. }) => i64::try_from(value)
                    .ok()
                    .and_then(|offset| left.checked_sub(offset))
                    .map(BsonValue::DateTime)
                    .ok_or_else(|| CommandError::new(2, "$subtract date result is out of range")),
                (ArithmeticOperand::DateTime(left), ArithmeticOperand::Double(number)) => integral_i64(&BsonValue::Double(number))
                    .and_then(|offset| left.checked_sub(offset))
                    .map(BsonValue::DateTime)
                    .ok_or_else(|| CommandError::new(2, "$subtract cannot subtract a non-integral value from a date")),
                (ArithmeticOperand::Integer { .. } | ArithmeticOperand::Double(_), ArithmeticOperand::DateTime(_)) => {
                    Err(CommandError::new(2, "$subtract cannot subtract a date from a number"))
                }
                (
                    ArithmeticOperand::Integer { value: left, long: left_long },
                    ArithmeticOperand::Integer { value: right, long: right_long },
                ) => integer_arithmetic_result(left - right, left_long || right_long, operator),
                (ArithmeticOperand::Integer { value, .. }, ArithmeticOperand::Double(right)) => Ok(BsonValue::Double(value as f64 - right)),
                (ArithmeticOperand::Double(left), ArithmeticOperand::Integer { value, .. }) => Ok(BsonValue::Double(left - value as f64)),
                (ArithmeticOperand::Double(left), ArithmeticOperand::Double(right)) => Ok(BsonValue::Double(left - right)),
            }
        }
        "$multiply" => {
            if values.is_empty() {
                return Err(CommandError::new(2, "$multiply requires at least one expression"));
            }
            let mut integer_total: i128 = 1;
            let mut long = false;
            let mut double_total: Option<f64> = None;
            for value in &values {
                match numeric_operand(value, operator)? {
                    ArithmeticOperand::Null => return Ok(BsonValue::Null),
                    ArithmeticOperand::Integer { value, long: operand_long } => {
                        integer_total = integer_total
                            .checked_mul(value)
                            .ok_or_else(|| CommandError::new(2, "$multiply result overflows a 64-bit integer"))?;
                        long |= operand_long;
                    }
                    ArithmeticOperand::Double(number) => *double_total.get_or_insert(1.0) *= number,
                    ArithmeticOperand::DateTime(_) => unreachable!("numeric_operand rejects dates"),
                }
            }
            Ok(match double_total {
                Some(total) => BsonValue::Double(total * integer_total as f64),
                None => integer_arithmetic_result(integer_total, long, operator)?,
            })
        }
        "$divide" => {
            if values.len() != 2 {
                return Err(CommandError::new(2, "$divide requires exactly two expressions"));
            }
            let left = numeric_operand(&values[0], operator)?;
            let right = numeric_operand(&values[1], operator)?;
            if matches!(&left, ArithmeticOperand::Null) || matches!(&right, ArithmeticOperand::Null) {
                return Ok(BsonValue::Null);
            }
            let divisor = right.as_double().expect("non-null numeric operand");
            if divisor == 0.0 {
                return Err(CommandError::new(2, "can't $divide by zero"));
            }
            let dividend = left.as_double().expect("non-null numeric operand");
            Ok(BsonValue::Double(dividend / divisor))
        }
        "$mod" => {
            if values.len() != 2 {
                return Err(CommandError::new(2, "$mod requires exactly two expressions"));
            }
            let left = numeric_operand(&values[0], operator)?;
            let right = numeric_operand(&values[1], operator)?;
            match (&left, &right) {
                (ArithmeticOperand::Null, _) | (_, ArithmeticOperand::Null) => Ok(BsonValue::Null),
                (
                    ArithmeticOperand::Integer { value: left, long: left_long },
                    ArithmeticOperand::Integer { value: right, long: right_long },
                ) => {
                    if *right == 0 {
                        return Err(CommandError::new(2, "can't $mod by zero"));
                    }
                    integer_arithmetic_result(left % right, *left_long || *right_long, operator)
                }
                _ => {
                    let divisor = right.as_double().expect("non-null numeric operand");
                    if divisor == 0.0 {
                        return Err(CommandError::new(2, "can't $mod by zero"));
                    }
                    let dividend = left.as_double().expect("non-null numeric operand");
                    Ok(BsonValue::Double(dividend % divisor))
                }
            }
        }
        "$pow" => {
            if values.len() != 2 {
                return Err(CommandError::new(2, "$pow requires exactly two expressions"));
            }
            let base = numeric_operand(&values[0], operator)?;
            let exponent = numeric_operand(&values[1], operator)?;
            match (&base, &exponent) {
                (ArithmeticOperand::Null, _) | (_, ArithmeticOperand::Null) => Ok(BsonValue::Null),
                (ArithmeticOperand::Integer { value: base, long }, ArithmeticOperand::Integer { value: exponent, .. }) if *exponent >= 0 => {
                    integer_power(*base, *exponent, *long, operator)
                }
                _ => {
                    let base = base.as_double().expect("non-null numeric operand");
                    let exponent = exponent.as_double().expect("non-null numeric operand");
                    if base == 0.0 && exponent < 0.0 {
                        return Err(CommandError::new(2, "can't take 0 to a negative exponent"));
                    }
                    Ok(BsonValue::Double(base.powf(exponent)))
                }
            }
        }
        other => Err(CommandError::new(59, format!("unknown arithmetic expression: {other}"))),
    }
}

/// 整数乘方：只处理非负指数，结果按整数类型提升规则写出。
fn integer_power(base: i128, exponent: i128, long: bool, operator: &str) -> Result<BsonValue, CommandError> {
    let result = match base {
        0 if exponent == 0 => 1,
        0 => 0,
        1 => 1,
        -1 if exponent % 2 == 0 => 1,
        -1 => -1,
        _ => {
            // 绝对值大于 1 时，指数超过 63 必然溢出 64 位整数。
            if exponent > 63 {
                return Err(CommandError::new(2, format!("{operator} result overflows a 64-bit integer")));
            }
            let mut result: i128 = 1;
            for _ in 0..exponent {
                result =
                    result.checked_mul(base).ok_or_else(|| CommandError::new(2, format!("{operator} result overflows a 64-bit integer")))?;
            }
            result
        }
    };
    integer_arithmetic_result(result, long, operator)
}

/// 整数按 `10^-place` 取舍；`round` 为 `false` 时向零截断。
///
/// `$round` 用半偶数（银行家）舍入，与 `mongod` 的整数路径一致。
fn round_integer_to_place(value: i128, place: i32, round: bool) -> i128 {
    if place >= 0 {
        return value;
    }
    let factor = 10i128.pow((-place) as u32);
    let quotient = value / factor;
    if !round {
        return quotient * factor;
    }
    let remainder = value % factor;
    let doubled = remainder.abs() * 2;
    let step = if doubled > factor || (doubled == factor && quotient % 2 != 0) { value.signum() } else { 0 };
    (quotient + step) * factor
}

/// 双精度按 `10^-place` 取舍；`$round` 用半偶数舍入。
///
/// 缩放用的是二进制浮点，极端位数上的舍入可能与 `mongod` 不同，待真机核对。
fn round_double_to_place(value: f64, place: i32, round: bool) -> f64 {
    let factor = 10f64.powi(place);
    let scaled = value * factor;
    let rounded = if round { scaled.round_ties_even() } else { scaled.trunc() };
    rounded / factor
}

/// 字符串算子归一化后的输入。
enum StringArgument {
    /// `null` 或缺失；各算子自行决定按空串、`null` 还是报错处理。
    Null,
    /// 已求值的字符串。
    Text(String),
}

/// 把字符串算子的单个输入归一化。
///
/// 非字符串按 `TypeMismatch` 拒绝，各算子传入手册对应的错误码；错误码与措辞
/// 待真实 `mongod 8.0.9` 录制核对。
fn string_argument(value: BsonValue, operator: &str, code: i32) -> Result<StringArgument, CommandError> {
    match value {
        BsonValue::Null | BsonValue::Undefined => Ok(StringArgument::Null),
        BsonValue::String(text) => Ok(StringArgument::Text(text)),
        other => Err(CommandError::new(code, format!("{operator} requires a string argument, found: {}", bson_type_name(&other)))),
    }
}

/// 取算子的定长参数列表。
fn operator_arguments<'a>(argument: &'a BsonValue, operator: &str, minimum: usize, maximum: usize) -> Result<&'a [BsonValue], CommandError> {
    let BsonValue::Array(arguments) = argument
    else {
        return Err(CommandError::new(2, format!("{operator} requires an array of expressions")));
    };
    if arguments.len() < minimum || arguments.len() > maximum {
        return Err(CommandError::new(2, format!("{operator} requires between {minimum} and {maximum} expressions")));
    }
    Ok(arguments)
}

/// 取非负整数参数；浮点必须先有精确整数视图才接受。
fn non_negative_integer(value: &BsonValue, operator: &str, code: i32) -> Result<i64, CommandError> {
    match integral_i64(value) {
        Some(number) if number >= 0 => Ok(number),
        _ => Err(CommandError::new(code, format!("{operator} requires a non-negative integer argument"))),
    }
}

/// `$trim` / `$ltrim` / `$rtrim` 未指定 `chars` 时的默认字符集。
///
/// `mongod` 手册给的默认集合包含空字符、空格、制表、回车、换行、垂直制表与换页。
fn default_trim_chars() -> String {
    "\u{0} \t\n\r\u{b}\u{c}".to_string()
}

/// 求值字符串类聚合算子。
///
/// 语义按 `mongod` 手册实现：`$toLower` / `$toUpper` 把 `null` 当空串，
/// `$strcasecmp` 把 `null` 当空串参与比较，其余长度、截取、拆分、裁剪、查找、
/// 替换类算子遇到 `null` 直接返回 `null`。所有错误码、错误措辞、非 ASCII 大小写
/// 映射与 UTF-8 边界判定均待真实 `mongod 8.0.9` 录制核对。
fn evaluate_string_operator(
    document: &BsonDocument,
    operator: &str,
    argument: &BsonValue,
    variables: &ExpressionVariables,
) -> Result<BsonValue, CommandError> {
    match operator {
        "$toLower" | "$toUpper" => {
            let value = evaluate_expression_vars(document, argument, variables)?;
            let text = match string_argument(value, operator, 16007)? {
                StringArgument::Null => String::new(),
                StringArgument::Text(text) => text,
            };
            Ok(BsonValue::String(if operator == "$toLower" { text.to_lowercase() } else { text.to_uppercase() }))
        }
        "$strLenCP" | "$strLenBytes" => {
            let value = evaluate_expression_vars(document, argument, variables)?;
            let code = if operator == "$strLenCP" { 34471 } else { 34473 };
            match string_argument(value, operator, code)? {
                StringArgument::Null => Ok(BsonValue::Null),
                StringArgument::Text(text) => {
                    let length = if operator == "$strLenCP" { text.chars().count() } else { text.len() };
                    Ok(count_value(length as i64))
                }
            }
        }
        "$substr" | "$substrBytes" | "$substrCP" => {
            let arguments = operator_arguments(argument, operator, 3, 3)?;
            let value = evaluate_expression_vars(document, &arguments[0], variables)?;
            let text = match string_argument(value, operator, 34450)? {
                StringArgument::Null => return Ok(BsonValue::Null),
                StringArgument::Text(text) => text,
            };
            let start = evaluate_expression_vars(document, &arguments[1], variables)?;
            let start =
                integral_i64(&start).ok_or_else(|| CommandError::new(34452, format!("{operator} requires an integer starting index")))?;
            let length = evaluate_expression_vars(document, &arguments[2], variables)?;
            let length = integral_i64(&length).ok_or_else(|| CommandError::new(34453, format!("{operator} requires an integer length")))?;
            if start < 0 {
                // 负起点按手册返回空串。
                return Ok(BsonValue::String(String::new()));
            }
            if operator == "$substrCP" {
                Ok(BsonValue::String(substring_by_code_point(&text, start as usize, length)))
            }
            else {
                substring_by_byte(&text, start as usize, length, operator)
            }
        }
        "$split" => {
            let arguments = operator_arguments(argument, operator, 2, 2)?;
            let input = evaluate_expression_vars(document, &arguments[0], variables)?;
            let separator = evaluate_expression_vars(document, &arguments[1], variables)?;
            let input = string_argument(input, operator, 40085)?;
            let separator = string_argument(separator, operator, 40086)?;
            match (input, separator) {
                (StringArgument::Null, _) | (_, StringArgument::Null) => Ok(BsonValue::Null),
                (StringArgument::Text(input), StringArgument::Text(separator)) => {
                    if separator.is_empty() {
                        return Err(CommandError::new(40086, "$split requires a non-empty separator"));
                    }
                    Ok(BsonValue::Array(input.split(separator.as_str()).map(|part| BsonValue::String(part.to_string())).collect()))
                }
            }
        }
        "$trim" | "$ltrim" | "$rtrim" => {
            let (input_expression, chars_expression) = match argument {
                BsonValue::Document(options) => {
                    for (field, _) in &options.fields {
                        if field != "input" && field != "chars" {
                            return Err(CommandError::new(2, format!("{operator} found an unknown argument: {field}")));
                        }
                    }
                    let input = options.get("input").ok_or_else(|| CommandError::new(2, format!("{operator} requires an 'input' field")))?;
                    (input, options.get("chars"))
                }
                other => (other, None),
            };
            let input = evaluate_expression_vars(document, input_expression, variables)?;
            let input = match string_argument(input, operator, 50699)? {
                StringArgument::Null => return Ok(BsonValue::Null),
                StringArgument::Text(text) => text,
            };
            let chars = match chars_expression {
                Some(expression) => {
                    let chars = evaluate_expression_vars(document, expression, variables)?;
                    match string_argument(chars, operator, 50702)? {
                        // `chars` 为 `null` 时按未提供处理，用默认字符集，待真机核对。
                        StringArgument::Null => default_trim_chars(),
                        StringArgument::Text(text) => text,
                    }
                }
                None => default_trim_chars(),
            };
            let trimmed = match operator {
                "$trim" => input.trim_matches(|character| chars.contains(character)),
                "$ltrim" => input.trim_start_matches(|character| chars.contains(character)),
                _ => input.trim_end_matches(|character| chars.contains(character)),
            };
            Ok(BsonValue::String(trimmed.to_string()))
        }
        "$indexOfCP" | "$indexOfBytes" => {
            let arguments = operator_arguments(argument, operator, 2, 4)?;
            let input = evaluate_expression_vars(document, &arguments[0], variables)?;
            let search = evaluate_expression_vars(document, &arguments[1], variables)?;
            let input = string_argument(input, operator, 40091)?;
            let search = string_argument(search, operator, 40092)?;
            let (input, search) = match (input, search) {
                (StringArgument::Null, _) | (_, StringArgument::Null) => return Ok(BsonValue::Null),
                (StringArgument::Text(input), StringArgument::Text(search)) => (input, search),
            };
            let start = match arguments.get(2) {
                Some(expression) => non_negative_integer(&evaluate_expression_vars(document, expression, variables)?, operator, 40093)?,
                None => 0,
            };
            if operator == "$indexOfCP" {
                index_of_code_point(&input, &search, start, arguments.get(3), document, variables)
            }
            else {
                index_of_byte(&input, &search, start, arguments.get(3), document, variables)
            }
        }
        "$replaceOne" | "$replaceAll" => {
            let arguments = operator_arguments(argument, operator, 3, 3)?;
            let input = evaluate_expression_vars(document, &arguments[0], variables)?;
            let find = evaluate_expression_vars(document, &arguments[1], variables)?;
            let replacement = evaluate_expression_vars(document, &arguments[2], variables)?;
            let input = string_argument(input, operator, 51745)?;
            let find = string_argument(find, operator, 51746)?;
            let replacement = string_argument(replacement, operator, 51747)?;
            match (input, find, replacement) {
                (StringArgument::Null, _, _) | (_, StringArgument::Null, _) | (_, _, StringArgument::Null) => Ok(BsonValue::Null),
                (StringArgument::Text(input), StringArgument::Text(find), StringArgument::Text(replacement)) => {
                    if find.is_empty() {
                        // 空 `find` 按手册原样返回输入串。
                        return Ok(BsonValue::String(input));
                    }
                    Ok(BsonValue::String(if operator == "$replaceOne" {
                        input.replacen(find.as_str(), replacement.as_str(), 1)
                    }
                    else {
                        input.replace(find.as_str(), replacement.as_str())
                    }))
                }
            }
        }
        "$strcasecmp" => {
            let arguments = operator_arguments(argument, operator, 2, 2)?;
            let left = evaluate_expression_vars(document, &arguments[0], variables)?;
            let right = evaluate_expression_vars(document, &arguments[1], variables)?;
            let text = |value: BsonValue| -> Result<String, CommandError> {
                match string_argument(value, "$strcasecmp", 16007)? {
                    StringArgument::Null => Ok(String::new()),
                    StringArgument::Text(text) => Ok(text),
                }
            };
            let left = text(left)?;
            let right = text(right)?;
            Ok(BsonValue::Int32(match left.to_lowercase().cmp(&right.to_lowercase()) {
                Ordering::Less => -1,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            }))
        }
        other => unreachable!("evaluate_string_operator called with {other}"),
    }
}

/// 按字节截取；起点与终点都必须落在 UTF-8 字符边界上。
///
/// 负 `length` 表示取到串尾，越界一律截断到串尾；起始下标落在续字节上属于
/// 手册定义的范围错误，错误码与措辞待真机核对。
fn substring_by_byte(text: &str, start: usize, length: i64, operator: &str) -> Result<BsonValue, CommandError> {
    if start >= text.len() {
        return Ok(BsonValue::String(String::new()));
    }
    if !text.is_char_boundary(start) {
        return Err(CommandError::new(28656, format!("{operator}: invalid range, starting index is a UTF-8 continuation byte")));
    }
    let end = if length < 0 { text.len() } else { start.saturating_add(usize::try_from(length).unwrap_or(usize::MAX)).min(text.len()) };
    if !text.is_char_boundary(end) {
        return Err(CommandError::new(28657, format!("{operator}: invalid range, ending index is in the middle of a UTF-8 character")));
    }
    Ok(BsonValue::String(text[start..end].to_string()))
}

/// 按 Unicode 码点截取；负 `length` 表示取到串尾。
fn substring_by_code_point(text: &str, start: usize, length: i64) -> String {
    let count = text.chars().count();
    if start >= count {
        return String::new();
    }
    let end = if length < 0 { count } else { start.saturating_add(usize::try_from(length).unwrap_or(usize::MAX)).min(count) };
    text.chars().skip(start).take(end - start).collect()
}

/// `$indexOfCP`：在码点窗口内查找，返回码点下标。
fn index_of_code_point(
    input: &str,
    search: &str,
    start: i64,
    end_expression: Option<&BsonValue>,
    document: &BsonDocument,
    variables: &ExpressionVariables,
) -> Result<BsonValue, CommandError> {
    let haystack: Vec<char> = input.chars().collect();
    let needle: Vec<char> = search.chars().collect();
    let count = haystack.len() as i64;
    let end = match end_expression {
        Some(expression) => non_negative_integer(&evaluate_expression_vars(document, expression, variables)?, "$indexOfCP", 40094)?,
        None => count,
    };
    if start > count || start > end || needle.len() as i64 > count {
        return Ok(BsonValue::Int32(-1));
    }
    let end = end.min(count) as usize;
    let start = start as usize;
    let window = &haystack[start..end];
    let position = if needle.is_empty() { Some(0) } else { window.windows(needle.len()).position(|candidate| candidate == needle.as_slice()) };
    Ok(count_value(position.map_or(-1, |offset| (start + offset) as i64)))
}

/// `$indexOfBytes`：在字节窗口内查找，返回字节下标。
///
/// 起点与终点必须落在 UTF-8 字符边界上，否则按手册报范围错误；错误码与措辞
/// 待真机核对。
fn index_of_byte(
    input: &str,
    search: &str,
    start: i64,
    end_expression: Option<&BsonValue>,
    document: &BsonDocument,
    variables: &ExpressionVariables,
) -> Result<BsonValue, CommandError> {
    let count = input.len() as i64;
    let end = match end_expression {
        Some(expression) => non_negative_integer(&evaluate_expression_vars(document, expression, variables)?, "$indexOfBytes", 40094)?,
        None => count,
    };
    if start > count || start > end {
        return Ok(BsonValue::Int32(-1));
    }
    let start = start as usize;
    let end = end.min(count) as usize;
    if !input.is_char_boundary(start) || !input.is_char_boundary(end) {
        return Err(CommandError::new(40095, "$indexOfBytes requires a starting and ending index on UTF-8 character boundaries"));
    }
    Ok(count_value(input[start..end].find(search).map_or(-1, |offset| (start + offset) as i64)))
}

/// 数组算子归一化后的输入。
enum ArrayArgument {
    /// `null` 或缺失，多数数组算子直接返回 `null`。
    Null,
    /// 已求值的数组元素。
    Items(Vec<BsonValue>),
}

/// 把数组算子的单个输入归一化。
///
/// 非数组按 `TypeMismatch` 拒绝，各算子传入手册对应的错误码；错误码与措辞
/// 待真实 `mongod 8.0.9` 录制核对。
fn array_argument(value: BsonValue, operator: &str, code: i32) -> Result<ArrayArgument, CommandError> {
    match value {
        BsonValue::Null | BsonValue::Undefined => Ok(ArrayArgument::Null),
        BsonValue::Array(items) => Ok(ArrayArgument::Items(items)),
        other => Err(CommandError::new(code, format!("{operator} requires an array argument, found: {}", bson_type_name(&other)))),
    }
}

/// 取数组元素；越界返回 `null`。
///
/// `mongod` 对越界返回缺失字段（投影时字段消失），本仓暂以 `null` 表达，
/// 差异待真机录制后决定是否需要独立的缺失值。
fn array_element(items: &[BsonValue], position: i64) -> BsonValue {
    usize::try_from(position).ok().and_then(|index| items.get(index)).cloned().unwrap_or(BsonValue::Null)
}

/// 求值数组类聚合算子。
///
/// 语义按 `mongod` 手册实现：`null` 输入一律返回 `null`，越界取元素与空数组
/// 的 `$first` / `$last` 在 `mongod` 里是缺失字段，这里暂以 `null` 表达。
/// 所有错误码与错误措辞均待真实 `mongod 8.0.9` 录制核对。
fn evaluate_array_operator(
    document: &BsonDocument,
    operator: &str,
    argument: &BsonValue,
    variables: &ExpressionVariables,
) -> Result<BsonValue, CommandError> {
    match operator {
        "$isArray" => {
            let value = evaluate_expression_vars(document, argument, variables)?;
            Ok(BsonValue::Bool(matches!(value, BsonValue::Array(_))))
        }
        "$arrayElemAt" | "$first" | "$last" => {
            let (value, index) = if operator == "$arrayElemAt" {
                let arguments = operator_arguments(argument, operator, 2, 2)?;
                (evaluate_expression_vars(document, &arguments[0], variables)?, Some(&arguments[1]))
            }
            else {
                (evaluate_expression_vars(document, argument, variables)?, None)
            };
            let items = match array_argument(value, operator, 28689)? {
                ArrayArgument::Null => return Ok(BsonValue::Null),
                ArrayArgument::Items(items) => items,
            };
            let position = match index {
                Some(expression) => {
                    let index = evaluate_expression_vars(document, expression, variables)?;
                    integral_i64(&index).ok_or_else(|| CommandError::new(28690, format!("{operator}'s second argument must be an integer")))?
                }
                None if operator == "$first" => 0,
                None => items.len() as i64 - 1,
            };
            let position = if position < 0 { (items.len() as i64).saturating_add(position) } else { position };
            Ok(array_element(&items, position))
        }
        "$concatArrays" => {
            let arguments = operator_arguments(argument, operator, 1, usize::MAX)?;
            let mut combined = Vec::new();
            for expression in arguments {
                let value = evaluate_expression_vars(document, expression, variables)?;
                match array_argument(value, operator, 28664)? {
                    ArrayArgument::Null => return Ok(BsonValue::Null),
                    ArrayArgument::Items(items) => combined.extend(items),
                }
            }
            Ok(BsonValue::Array(combined))
        }
        "$reverseArray" => {
            let value = evaluate_expression_vars(document, argument, variables)?;
            match array_argument(value, operator, 34435)? {
                ArrayArgument::Null => Ok(BsonValue::Null),
                ArrayArgument::Items(mut items) => {
                    items.reverse();
                    Ok(BsonValue::Array(items))
                }
            }
        }
        "$indexOfArray" => {
            let arguments = operator_arguments(argument, operator, 2, 4)?;
            let value = evaluate_expression_vars(document, &arguments[0], variables)?;
            let search = evaluate_expression_vars(document, &arguments[1], variables)?;
            let items = match array_argument(value, operator, 40090)? {
                ArrayArgument::Null => return Ok(BsonValue::Null),
                ArrayArgument::Items(items) => items,
            };
            let start = match arguments.get(2) {
                Some(expression) => non_negative_integer(&evaluate_expression_vars(document, expression, variables)?, operator, 40096)?,
                None => 0,
            };
            let end = match arguments.get(3) {
                Some(expression) => non_negative_integer(&evaluate_expression_vars(document, expression, variables)?, operator, 40097)?,
                None => items.len() as i64,
            };
            let end = end.min(items.len() as i64);
            if start > end {
                return Ok(BsonValue::Int32(-1));
            }
            let start = start as usize;
            let end = end as usize;
            let position = items[start..end].iter().position(|item| compare_values(item, &search) == Ordering::Equal);
            Ok(count_value(position.map_or(-1, |offset| (start + offset) as i64)))
        }
        "$range" => {
            let arguments = operator_arguments(argument, operator, 2, 3)?;
            let values: Vec<BsonValue> =
                arguments.iter().map(|expression| evaluate_expression_vars(document, expression, variables)).collect::<Result<_, _>>()?;
            let numbers = values
                .iter()
                .map(|value| integral_i64(value).ok_or_else(|| CommandError::new(34443, format!("{operator} requires integer arguments"))))
                .collect::<Result<Vec<_>, _>>()?;
            let start = numbers[0];
            let end = numbers[1];
            let step = numbers.get(2).copied().unwrap_or(1);
            if step == 0 {
                return Err(CommandError::new(34449, "$range requires a non-zero step value"));
            }
            let long = values.iter().any(|value| matches!(value, BsonValue::Int64(_)));
            let mut items = Vec::new();
            let mut current = start;
            while (step > 0 && current < end) || (step < 0 && current > end) {
                // 元素上限防止一条表达式把内存打满，具体上限待真机核对。
                if items.len() >= 1_000_000 {
                    return Err(CommandError::new(2, "$range result exceeds 1000000 elements"));
                }
                items.push(if long {
                    BsonValue::Int64(current)
                }
                else {
                    i32::try_from(current).map(BsonValue::Int32).unwrap_or(BsonValue::Int64(current))
                });
                current = current.checked_add(step).ok_or_else(|| CommandError::new(2, "$range step overflows a 64-bit integer"))?;
            }
            Ok(BsonValue::Array(items))
        }
        "$slice" => {
            let arguments = operator_arguments(argument, operator, 2, 3)?;
            let value = evaluate_expression_vars(document, &arguments[0], variables)?;
            let items = match array_argument(value, operator, 28724)? {
                ArrayArgument::Null => return Ok(BsonValue::Null),
                ArrayArgument::Items(items) => items,
            };
            let length = items.len() as i64;
            let integer = |expression: &BsonValue| -> Result<i64, CommandError> {
                let value = evaluate_expression_vars(document, expression, variables)?;
                integral_i64(&value).ok_or_else(|| CommandError::new(28729, format!("{operator} requires integer arguments")))
            };
            if arguments.len() == 2 {
                let count = integer(&arguments[1])?;
                let (start, end) = if count >= 0 { (0, count.min(length)) } else { ((length.saturating_add(count)).max(0), length) };
                Ok(BsonValue::Array(items[start as usize..end as usize].to_vec()))
            }
            else {
                let position = integer(&arguments[1])?;
                let count = integer(&arguments[2])?;
                if count <= 0 {
                    return Err(CommandError::new(28729, "$slice third argument must be positive"));
                }
                let start = if position < 0 { (length.saturating_add(position)).max(0) } else { position.min(length) };
                let end = start.saturating_add(count).min(length);
                Ok(BsonValue::Array(items[start as usize..end as usize].to_vec()))
            }
        }
        other => unreachable!("evaluate_array_operator called with {other}"),
    }
}

/// 日期算子解释时使用的固定偏移。
///
/// 本仓不内置 IANA 时区数据库：只支持 `UTC` / `GMT` / `Z` 与 `±HH[:MM]` 形式的
/// 固定偏移。`Asia/Shanghai` 一类的 Olson 名称一律拒绝，夏令时与历史时区规则
/// 的边界语义待真实 `mongod 8.0.9` 录制核对。
#[derive(Clone, Copy)]
struct FixedOffset {
    seconds: i64,
}

impl FixedOffset {
    fn utc() -> Self {
        Self { seconds: 0 }
    }

    fn parse(value: &str) -> Result<Self, CommandError> {
        let trimmed = value.trim();
        let upper = trimmed.to_ascii_uppercase();
        if matches!(upper.as_str(), "UTC" | "GMT" | "Z") {
            return Ok(Self::utc());
        }
        let sign = match upper.as_bytes().first() {
            Some(b'+') => 1,
            Some(b'-') => -1,
            _ => return Err(invalid_timezone(value)),
        };
        let rest = &upper[1..];
        let (hours, minutes) = match rest.len() {
            2 => (rest, "00"),
            4 => (&rest[..2], &rest[2..]),
            5 if rest.as_bytes()[2] == b':' => (&rest[..2], &rest[3..]),
            _ => return Err(invalid_timezone(value)),
        };
        let parse_part = |part: &str| part.parse::<i64>().map_err(|_| invalid_timezone(value));
        let hours = parse_part(hours)?;
        let minutes = parse_part(minutes)?;
        if hours > 23 || minutes > 59 {
            return Err(invalid_timezone(value));
        }
        Ok(Self { seconds: sign * (hours * 3600 + minutes * 60) })
    }

    /// `+HHMM` 形式的文本，供 `%z` 使用。
    fn text(self) -> String {
        let sign = if self.seconds < 0 { '-' } else { '+' };
        let total = self.seconds.abs();
        format!("{sign}{:02}{:02}", total / 3600, (total % 3600) / 60)
    }
}

fn invalid_timezone(value: &str) -> CommandError {
    CommandError::new(
        40485,
        format!(
            "unrecognized time zone identifier: \"{value}\"; this facade supports UTC and fixed ±HH:MM offsets only (IANA zone rules pending real mongod verification)"
        ),
    )
}

/// 日期记录：UTC 毫秒与解释用的固定偏移。
#[derive(Clone, Copy)]
struct DateValue {
    millis: i64,
    offset: FixedOffset,
}

/// 从算子参数文档里取可选的 `timezone`。
fn evaluate_timezone(document: &BsonDocument, options: &BsonDocument, variables: &ExpressionVariables) -> Result<FixedOffset, CommandError> {
    match options.get("timezone") {
        None | Some(BsonValue::Null) => Ok(FixedOffset::utc()),
        Some(expression) => match evaluate_expression_vars(document, expression, variables)? {
            BsonValue::String(name) => FixedOffset::parse(&name),
            other => Err(CommandError::new(40517, format!("timezone must evaluate to a string, found: {}", bson_type_name(&other)))),
        },
    }
}

/// 把 BSON 值解释为 UTC 毫秒；`null` 与缺失返回 `None`。
///
/// Date、Timestamp 与 ObjectId 都按手册接受；错误码与措辞待真机核对。
fn date_millis(value: &BsonValue, operator: &str) -> Result<Option<i64>, CommandError> {
    match value {
        BsonValue::Null | BsonValue::Undefined => Ok(None),
        BsonValue::DateTime(millis) => Ok(Some(*millis)),
        BsonValue::Timestamp { t, .. } => Ok(Some(i64::from(*t) * 1000)),
        BsonValue::ObjectId(bytes) => Ok(Some(i64::from(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])) * 1000)),
        other => Err(CommandError::new(16006, format!("{operator} only supports Date, Timestamp, or ObjectId, not {}", bson_type_name(other)))),
    }
}

/// 字段类日期算子的参数：既可以是裸日期表达式，也可以是 `{ date, timezone }`。
fn evaluate_date_argument(
    document: &BsonDocument,
    argument: &BsonValue,
    variables: &ExpressionVariables,
    operator: &str,
) -> Result<Option<DateValue>, CommandError> {
    if let BsonValue::Document(options) = argument
        && let Some(date) = options.get("date")
    {
        let offset = evaluate_timezone(document, options, variables)?;
        let value = evaluate_expression_vars(document, date, variables)?;
        return Ok(date_millis(&value, operator)?.map(|millis| DateValue { millis, offset }));
    }
    let value = evaluate_expression_vars(document, argument, variables)?;
    Ok(date_millis(&value, operator)?.map(|millis| DateValue { millis, offset: FixedOffset::utc() }))
}

/// UTC 民用日期时间，不携带时区偏移。
#[derive(Clone, Copy)]
struct CivilDateTime {
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    millisecond: u32,
}

/// 把本地偏移加到 UTC 毫秒上，供民用历字段计算使用。
fn local_millis(value: DateValue) -> Result<i64, CommandError> {
    let offset =
        value.offset.seconds.checked_mul(1000).ok_or_else(|| CommandError::new(2, "timezone offset overflows a 64-bit millisecond count"))?;
    value.millis.checked_add(offset).ok_or_else(|| CommandError::new(2, "date arithmetic overflows a 64-bit millisecond count"))
}

/// 公历日期转自 Unix 纪元起的天数，供日期算子复用既有的民用历换算。
fn date_days(year: i64, month: u32, day: u32) -> i64 {
    days_from_civil(year, i64::from(month), i64::from(day))
}

impl CivilDateTime {
    fn from_millis(millis: i64) -> Self {
        let days = millis.div_euclid(86_400_000);
        let within = millis.rem_euclid(86_400_000);
        let (year, month, day) = civil_from_days(days);
        Self {
            year,
            month: month as u32,
            day: day as u32,
            hour: (within / 3_600_000) as u32,
            minute: ((within / 60_000) % 60) as u32,
            second: ((within / 1_000) % 60) as u32,
            millisecond: (within % 1_000) as u32,
        }
    }

    fn to_millis(self) -> i64 {
        date_days(self.year, self.month, self.day) * 86_400_000
            + i64::from(self.hour) * 3_600_000
            + i64::from(self.minute) * 60_000
            + i64::from(self.second) * 1_000
            + i64::from(self.millisecond)
    }

    fn days(self) -> i64 {
        date_days(self.year, self.month, self.day)
    }

    fn day_of_year(self) -> i64 {
        self.days() - date_days(self.year, 1, 1) + 1
    }

    /// Sunday 记 1 的星期序号。
    fn day_of_week(self) -> i64 {
        sunday_day_of_week(self.days())
    }

    /// ISO 星期序号，Monday 记 1。
    fn iso_day_of_week(self) -> i64 {
        iso_day_of_week(self.days())
    }

    /// Sunday 记 1 的周序号，年初不足一周的部分记 0。
    fn week_of_year(self) -> i64 {
        (self.day_of_year() - 1 + 7 - self.day_of_week()) / 7
    }

    /// ISO 周年与周序号。
    fn iso_week(self) -> (i64, i64) {
        let thursday = self.days() - (self.iso_day_of_week() - 4);
        let (iso_year, _, _) = civil_from_days(thursday);
        let january_fourth = days_from_civil(iso_year, 1, 4);
        let first_thursday = january_fourth + (4 - iso_day_of_week(january_fourth));
        (iso_year, (thursday - first_thursday) / 7 + 1)
    }
}

/// Sunday 记 1 的星期序号。
fn sunday_day_of_week(days: i64) -> i64 {
    (days + 4).rem_euclid(7) + 1
}

/// ISO 星期序号，Monday 记 1。
fn iso_day_of_week(days: i64) -> i64 {
    (days + 3).rem_euclid(7) + 1
}

/// 日期算子支持的时间单位。
#[derive(Clone, Copy, PartialEq, Eq)]
enum DateUnit {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
}

impl DateUnit {
    fn parse(value: &str, operator: &str) -> Result<Self, CommandError> {
        Ok(match value {
            "year" => Self::Year,
            "quarter" => Self::Quarter,
            "month" => Self::Month,
            "week" => Self::Week,
            "day" => Self::Day,
            "hour" => Self::Hour,
            "minute" => Self::Minute,
            "second" => Self::Second,
            "millisecond" => Self::Millisecond,
            other => return Err(CommandError::new(2, format!("{operator} does not support unit \"{other}\""))),
        })
    }

    /// 年 / 季 / 月折算的月数；其余单位返回 0。
    fn months(self) -> i64 {
        match self {
            Self::Year => 12,
            Self::Quarter => 3,
            Self::Month => 1,
            _ => 0,
        }
    }
}

/// 解析 `startOfWeek`，返回 Sunday 记 1 的星期序号；缺省为 Sunday。
fn parse_start_of_week(options: &BsonDocument, operator: &str) -> Result<i64, CommandError> {
    let Some(expression) = options.get("startOfWeek")
    else {
        return Ok(1);
    };
    let name = match expression {
        BsonValue::String(name) => name.to_ascii_lowercase(),
        other => return Err(CommandError::new(2, format!("{operator} startOfWeek must be a string, found: {}", bson_type_name(other)))),
    };
    Ok(match name.as_str() {
        "sunday" | "sun" => 1,
        "monday" | "mon" => 2,
        "tuesday" | "tue" => 3,
        "wednesday" | "wed" => 4,
        "thursday" | "thu" => 5,
        "friday" | "fri" => 6,
        "saturday" | "sat" => 7,
        other => return Err(CommandError::new(2, format!("{operator} does not recognize startOfWeek \"{other}\""))),
    })
}

/// 求值日期类聚合算子。
///
/// 语义按 `mongod` 手册实现：`null` 输入返回 `null`，字段算子接受 Date、
/// Timestamp 与 ObjectId。时区只支持 UTC 与固定偏移，`$dateFromString` 的格式
/// 串只支持本文档列出的数字型占位符。所有错误码、错误措辞、月末截断、周边界与
/// `$dateTrunc` 的参考日期语义均待真实 `mongod 8.0.9` 录制核对。
fn evaluate_date_operator(
    document: &BsonDocument,
    operator: &str,
    argument: &BsonValue,
    variables: &ExpressionVariables,
) -> Result<BsonValue, CommandError> {
    match operator {
        "$year" | "$month" | "$dayOfMonth" | "$hour" | "$minute" | "$second" | "$millisecond" | "$dayOfWeek" | "$dayOfYear" | "$week"
        | "$isoWeek" | "$isoWeekYear" | "$isoDayOfWeek" => {
            let Some(value) = evaluate_date_argument(document, argument, variables, operator)?
            else {
                return Ok(BsonValue::Null);
            };
            let civil = CivilDateTime::from_millis(local_millis(value)?);
            let number = match operator {
                "$year" => civil.year,
                "$month" => i64::from(civil.month),
                "$dayOfMonth" => i64::from(civil.day),
                "$hour" => i64::from(civil.hour),
                "$minute" => i64::from(civil.minute),
                "$second" => i64::from(civil.second),
                "$millisecond" => i64::from(civil.millisecond),
                "$dayOfWeek" => civil.day_of_week(),
                "$dayOfYear" => civil.day_of_year(),
                "$week" => civil.week_of_year(),
                "$isoWeek" => civil.iso_week().1,
                "$isoWeekYear" => civil.iso_week().0,
                _ => civil.iso_day_of_week(),
            };
            Ok(count_value(number))
        }
        "$dateToString" => {
            let BsonValue::Document(options) = argument
            else {
                return Err(CommandError::new(2, "$dateToString requires a document argument"));
            };
            let date_expression = options.get("date").ok_or_else(|| CommandError::new(2, "$dateToString requires a 'date' field"))?;
            let value = evaluate_expression_vars(document, date_expression, variables)?;
            let Some(millis) = date_millis(&value, operator)?
            else {
                return match options.get("onNull") {
                    Some(expression) => evaluate_expression_vars(document, expression, variables),
                    None => Ok(BsonValue::Null),
                };
            };
            let offset = evaluate_timezone(document, options, variables)?;
            let format = match options.get("format") {
                Some(expression) => match evaluate_expression_vars(document, expression, variables)? {
                    BsonValue::String(format) => format,
                    other => {
                        return Err(CommandError::new(
                            18533,
                            format!("$dateToString format must be a string, found: {}", bson_type_name(&other)),
                        ));
                    }
                },
                None => "%Y-%m-%dT%H:%M:%S.%LZ".to_string(),
            };
            let civil = CivilDateTime::from_millis(local_millis(DateValue { millis, offset })?);
            Ok(BsonValue::String(format_date(&civil, &format, offset)?))
        }
        "$dateToParts" => {
            let BsonValue::Document(options) = argument
            else {
                return Err(CommandError::new(2, "$dateToParts requires a document argument"));
            };
            let date_expression = options.get("date").ok_or_else(|| CommandError::new(2, "$dateToParts requires a 'date' field"))?;
            let value = evaluate_expression_vars(document, date_expression, variables)?;
            let Some(millis) = date_millis(&value, operator)?
            else {
                return Ok(BsonValue::Null);
            };
            let offset = evaluate_timezone(document, options, variables)?;
            let iso = match options.get("iso8601") {
                Some(expression) => match evaluate_expression_vars(document, expression, variables)? {
                    BsonValue::Bool(flag) => flag,
                    other => {
                        return Err(CommandError::new(2, format!("$dateToParts iso8601 must be a boolean, found: {}", bson_type_name(&other))));
                    }
                },
                None => false,
            };
            let civil = CivilDateTime::from_millis(local_millis(DateValue { millis, offset })?);
            let mut parts = BsonDocument::new();
            if iso {
                let (year, week) = civil.iso_week();
                parts.push("isoWeekYear", count_value(year));
                parts.push("isoWeek", count_value(week));
                parts.push("isoDayOfWeek", count_value(civil.iso_day_of_week()));
            }
            else {
                parts.push("year", count_value(civil.year));
                parts.push("month", count_value(i64::from(civil.month)));
                parts.push("day", count_value(i64::from(civil.day)));
            }
            parts.push("hour", count_value(i64::from(civil.hour)));
            parts.push("minute", count_value(i64::from(civil.minute)));
            parts.push("second", count_value(i64::from(civil.second)));
            parts.push("millisecond", count_value(i64::from(civil.millisecond)));
            Ok(BsonValue::Document(parts))
        }
        "$dateFromParts" => {
            let BsonValue::Document(options) = argument
            else {
                return Err(CommandError::new(2, "$dateFromParts requires a document argument"));
            };
            let offset = evaluate_timezone(document, options, variables)?;
            let number = |name: &str| -> Result<Option<i64>, CommandError> {
                match options.get(name) {
                    Some(expression) => match integral_i64(&evaluate_expression_vars(document, expression, variables)?) {
                        Some(number) => Ok(Some(number)),
                        None => Err(CommandError::new(40515, format!("$dateFromParts '{name}' must be an integer"))),
                    },
                    None => Ok(None),
                }
            };
            let hour = number("hour")?.unwrap_or(0);
            let minute = number("minute")?.unwrap_or(0);
            let second = number("second")?.unwrap_or(0);
            let millisecond = number("millisecond")?.unwrap_or(0);
            let time = hour
                .checked_mul(3_600_000)
                .and_then(|value| value.checked_add(minute.checked_mul(60_000)?))
                .and_then(|value| value.checked_add(second.checked_mul(1_000)?))
                .and_then(|value| value.checked_add(millisecond))
                .ok_or_else(|| CommandError::new(2, "$dateFromParts time components overflow"))?;
            let days = if options.get("isoWeekYear").is_some() {
                let year = number("isoWeekYear")?.ok_or_else(|| CommandError::new(2, "$dateFromParts requires 'isoWeekYear'"))?;
                let week = number("isoWeek")?.unwrap_or(1);
                let weekday = number("isoDayOfWeek")?.unwrap_or(1);
                let january_fourth = days_from_civil(year, 1, 4);
                let monday = january_fourth - (iso_day_of_week(january_fourth) - 1);
                week.checked_sub(1)
                    .and_then(|week| week.checked_mul(7))
                    .and_then(|offset| monday.checked_add(offset))
                    .and_then(|offset| offset.checked_add(weekday.checked_sub(1)?))
                    .ok_or_else(|| CommandError::new(2, "$dateFromParts result overflows"))?
            }
            else {
                let year = number("year")?.ok_or_else(|| CommandError::new(2, "$dateFromParts requires a 'year' or 'isoWeekYear' field"))?;
                let month = number("month")?.unwrap_or(1);
                let day = number("day")?.unwrap_or(1);
                let total_months = year
                    .checked_mul(12)
                    .and_then(|value| value.checked_add(month.checked_sub(1)?))
                    .ok_or_else(|| CommandError::new(2, "$dateFromParts month index overflows"))?;
                let normalized_year = total_months.div_euclid(12);
                let normalized_month = (total_months.rem_euclid(12) + 1) as u32;
                date_days(normalized_year, normalized_month, 1)
                    .checked_add(day.checked_sub(1).ok_or_else(|| CommandError::new(2, "$dateFromParts day index overflows"))?)
                    .ok_or_else(|| CommandError::new(2, "$dateFromParts result overflows"))?
            };
            let millis = days
                .checked_mul(86_400_000)
                .and_then(|value| value.checked_add(time))
                .and_then(|value| value.checked_sub(offset.seconds.checked_mul(1000)?))
                .ok_or_else(|| CommandError::new(2, "$dateFromParts result overflows a 64-bit millisecond count"))?;
            Ok(BsonValue::DateTime(millis))
        }
        "$dateAdd" | "$dateSubtract" => {
            let BsonValue::Document(options) = argument
            else {
                return Err(CommandError::new(2, format!("{operator} requires a document argument")));
            };
            let start_expression =
                options.get("startDate").ok_or_else(|| CommandError::new(2, format!("{operator} requires a 'startDate' field")))?;
            let unit_expression = options.get("unit").ok_or_else(|| CommandError::new(2, format!("{operator} requires a 'unit' field")))?;
            let amount_expression =
                options.get("amount").ok_or_else(|| CommandError::new(2, format!("{operator} requires an 'amount' field")))?;
            let offset = evaluate_timezone(document, options, variables)?;
            let value = evaluate_expression_vars(document, start_expression, variables)?;
            let Some(millis) = date_millis(&value, operator)?
            else {
                return Ok(BsonValue::Null);
            };
            let unit = match evaluate_expression_vars(document, unit_expression, variables)? {
                BsonValue::String(name) => DateUnit::parse(&name, operator)?,
                other => return Err(CommandError::new(2, format!("{operator} unit must be a string, found: {}", bson_type_name(&other)))),
            };
            let amount_value = evaluate_expression_vars(document, amount_expression, variables)?;
            let amount = integral_i64(&amount_value).ok_or_else(|| CommandError::new(2, format!("{operator} amount must be an integer")))?;
            let amount = if operator == "$dateSubtract" {
                amount.checked_neg().ok_or_else(|| CommandError::new(2, format!("{operator} amount overflows")))?
            }
            else {
                amount
            };
            Ok(BsonValue::DateTime(add_date(DateValue { millis, offset }, unit, amount)?))
        }
        "$dateDiff" => {
            let BsonValue::Document(options) = argument
            else {
                return Err(CommandError::new(2, "$dateDiff requires a document argument"));
            };
            let start_expression = options.get("startDate").ok_or_else(|| CommandError::new(2, "$dateDiff requires a 'startDate' field"))?;
            let end_expression = options.get("endDate").ok_or_else(|| CommandError::new(2, "$dateDiff requires an 'endDate' field"))?;
            let unit_expression = options.get("unit").ok_or_else(|| CommandError::new(2, "$dateDiff requires a 'unit' field"))?;
            let offset = evaluate_timezone(document, options, variables)?;
            let start_value = evaluate_expression_vars(document, start_expression, variables)?;
            let end_value = evaluate_expression_vars(document, end_expression, variables)?;
            let (Some(start), Some(end)) = (date_millis(&start_value, operator)?, date_millis(&end_value, operator)?)
            else {
                return Ok(BsonValue::Null);
            };
            let unit = match evaluate_expression_vars(document, unit_expression, variables)? {
                BsonValue::String(name) => DateUnit::parse(&name, operator)?,
                other => return Err(CommandError::new(2, format!("$dateDiff unit must be a string, found: {}", bson_type_name(&other)))),
            };
            let start_of_week = parse_start_of_week(options, operator)?;
            let start_local = local_millis(DateValue { millis: start, offset })?;
            let end_local = local_millis(DateValue { millis: end, offset })?;
            let start_civil = CivilDateTime::from_millis(start_local);
            let end_civil = CivilDateTime::from_millis(end_local);
            let difference = match unit {
                DateUnit::Year => end_civil.year - start_civil.year,
                DateUnit::Quarter => {
                    (end_civil.year * 4 + i64::from((end_civil.month - 1) / 3))
                        - (start_civil.year * 4 + i64::from((start_civil.month - 1) / 3))
                }
                DateUnit::Month => {
                    (end_civil.year * 12 + i64::from(end_civil.month) - 1) - (start_civil.year * 12 + i64::from(start_civil.month) - 1)
                }
                DateUnit::Week => week_index(end_civil.days(), start_of_week) - week_index(start_civil.days(), start_of_week),
                DateUnit::Day => end_civil.days() - start_civil.days(),
                DateUnit::Hour => end_local.div_euclid(3_600_000) - start_local.div_euclid(3_600_000),
                DateUnit::Minute => end_local.div_euclid(60_000) - start_local.div_euclid(60_000),
                DateUnit::Second => end_local.div_euclid(1_000) - start_local.div_euclid(1_000),
                DateUnit::Millisecond => end_local - start_local,
            };
            Ok(count_value(difference))
        }
        "$dateTrunc" => {
            let BsonValue::Document(options) = argument
            else {
                return Err(CommandError::new(2, "$dateTrunc requires a document argument"));
            };
            let date_expression = options.get("date").ok_or_else(|| CommandError::new(2, "$dateTrunc requires a 'date' field"))?;
            let unit_expression = options.get("unit").ok_or_else(|| CommandError::new(2, "$dateTrunc requires a 'unit' field"))?;
            let offset = evaluate_timezone(document, options, variables)?;
            let start_of_week = parse_start_of_week(options, operator)?;
            let bin_size = match options.get("binSize") {
                Some(expression) => match integral_i64(&evaluate_expression_vars(document, expression, variables)?) {
                    Some(number) if number > 0 => number,
                    _ => return Err(CommandError::new(2, "$dateTrunc binSize must be a positive integer")),
                },
                None => 1,
            };
            let value = evaluate_expression_vars(document, date_expression, variables)?;
            let Some(millis) = date_millis(&value, operator)?
            else {
                return Ok(BsonValue::Null);
            };
            let unit = match evaluate_expression_vars(document, unit_expression, variables)? {
                BsonValue::String(name) => DateUnit::parse(&name, operator)?,
                other => return Err(CommandError::new(2, format!("$dateTrunc unit must be a string, found: {}", bson_type_name(&other)))),
            };
            let local = local_millis(DateValue { millis, offset })?;
            let truncated = truncate_local(local, unit, bin_size, start_of_week)?;
            Ok(BsonValue::DateTime(
                truncated
                    .checked_sub(offset.seconds.checked_mul(1000).ok_or_else(|| CommandError::new(2, "timezone offset overflows"))?)
                    .ok_or_else(|| CommandError::new(2, "date arithmetic overflows a 64-bit millisecond count"))?,
            ))
        }
        "$dateFromString" => {
            let BsonValue::Document(options) = argument
            else {
                return Err(CommandError::new(2, "$dateFromString requires a document argument"));
            };
            let string_expression =
                options.get("dateString").ok_or_else(|| CommandError::new(2, "$dateFromString requires a 'dateString' field"))?;
            let value = evaluate_expression_vars(document, string_expression, variables)?;
            let text = match value {
                BsonValue::Null | BsonValue::Undefined => {
                    return match options.get("onNull") {
                        Some(expression) => evaluate_expression_vars(document, expression, variables),
                        None => Ok(BsonValue::Null),
                    };
                }
                BsonValue::String(text) => text,
                other => {
                    let message = format!("$dateFromString requires a string, found: {}", bson_type_name(&other));
                    return match options.get("onError") {
                        Some(expression) => evaluate_expression_vars(document, expression, variables),
                        None => Err(CommandError::new(40516, message)),
                    };
                }
            };
            let offset = evaluate_timezone(document, options, variables)?;
            let parsed = match options.get("format") {
                Some(expression) => match evaluate_expression_vars(document, expression, variables)? {
                    BsonValue::String(format) => parse_formatted_datetime(&text, &format),
                    other => Err(format!("format must be a string, found: {}", bson_type_name(&other))),
                },
                None => parse_iso_datetime(&text),
            };
            match parsed {
                Ok((local, embedded)) => {
                    let shift = embedded.unwrap_or(offset.seconds);
                    let millis = local
                        .checked_sub(shift.checked_mul(1000).ok_or_else(|| CommandError::new(2, "timezone offset overflows"))?)
                        .ok_or_else(|| CommandError::new(2, "date arithmetic overflows a 64-bit millisecond count"))?;
                    Ok(BsonValue::DateTime(millis))
                }
                Err(message) => match options.get("onError") {
                    Some(expression) => evaluate_expression_vars(document, expression, variables),
                    None => Err(CommandError::new(40516, format!("$dateFromString could not parse \"{text}\": {message}"))),
                },
            }
        }
        other => unreachable!("evaluate_date_operator called with {other}"),
    }
}

/// 转换类算子的目标 BSON 类型。
///
/// 只覆盖 `$convert` 手册列出的标量目标；`binData` 需要子类型语义，当前
/// 直接拒绝，缺口在待真机清单里跟踪。
#[derive(Clone, Copy, PartialEq, Eq)]
enum ConversionTarget {
    Bool,
    Date,
    Decimal,
    Double,
    Int,
    Long,
    ObjectId,
    String,
}

impl ConversionTarget {
    /// 解析 `$convert` 的 `to` 类型别名。
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "bool" => Self::Bool,
            "date" => Self::Date,
            "decimal" => Self::Decimal,
            "double" => Self::Double,
            "int" => Self::Int,
            "long" => Self::Long,
            "objectId" => Self::ObjectId,
            "string" => Self::String,
            _ => return None,
        })
    }

    /// 解析 `$convert` 的 `to` 数值类型码，沿用 BSON 元素编号。
    fn from_code(code: i64) -> Option<Self> {
        Some(match code {
            1 => Self::Double,
            2 => Self::String,
            7 => Self::ObjectId,
            8 => Self::Bool,
            9 => Self::Date,
            16 => Self::Int,
            18 => Self::Long,
            19 => Self::Decimal,
            _ => return None,
        })
    }

    /// 错误消息与 `$type` 输出共用的类型名。
    fn name(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Date => "date",
            Self::Decimal => "decimal",
            Self::Double => "double",
            Self::Int => "int",
            Self::Long => "long",
            Self::ObjectId => "objectId",
            Self::String => "string",
        }
    }
}

/// 求值转换类聚合算子。
///
/// `$toX` 是 `$convert` 的语法糖：`null` 与缺失返回 `null`，转换失败抛
/// `ConversionFailure`（`241`）。`$convert` 额外支持 `onNull` / `onError`。
/// `$type` 对缺失字段返回 `"missing"`，而当前表达式求值把缺失折叠成 `null`，
/// 因此这里单独回查字段路径。精确错误码、错误措辞、`double` 的科学计数写法与
/// 边界值处理均待真实 `mongod 8.0.9` 录制核对。
fn evaluate_conversion_operator(
    document: &BsonDocument,
    operator: &str,
    argument: &BsonValue,
    variables: &ExpressionVariables,
) -> Result<BsonValue, CommandError> {
    match operator {
        "$isNumber" => {
            let value = evaluate_expression_vars(document, argument, variables)?;
            Ok(BsonValue::Bool(matches!(value, BsonValue::Int32(_) | BsonValue::Int64(_) | BsonValue::Double(_) | BsonValue::Decimal128(_))))
        }
        "$type" => {
            let value = evaluate_expression_vars(document, argument, variables)?;
            // 缺失字段与显式 `undefined` 都按 `"missing"` 上报。
            let missing = matches!(value, BsonValue::Undefined)
                || matches!(argument, BsonValue::String(path)
                    if path.starts_with('$') && !path.starts_with("$$") && lookup_field(document, &path[1..]).is_none());
            Ok(BsonValue::String(if missing { "missing".to_string() } else { bson_type_name(&value).to_string() }))
        }
        "$convert" => evaluate_convert(document, argument, variables),
        _ => {
            let target = match operator {
                "$toBool" => ConversionTarget::Bool,
                "$toDate" => ConversionTarget::Date,
                "$toDecimal" => ConversionTarget::Decimal,
                "$toDouble" => ConversionTarget::Double,
                "$toInt" => ConversionTarget::Int,
                "$toLong" => ConversionTarget::Long,
                "$toObjectId" => ConversionTarget::ObjectId,
                _ => ConversionTarget::String,
            };
            let value = evaluate_expression_vars(document, argument, variables)?;
            if matches!(value, BsonValue::Null | BsonValue::Undefined) {
                return Ok(BsonValue::Null);
            }
            convert_value(&value, target)
        }
    }
}

/// 求值 `$convert: { input, to, onNull, onError }`。
fn evaluate_convert(document: &BsonDocument, argument: &BsonValue, variables: &ExpressionVariables) -> Result<BsonValue, CommandError> {
    let BsonValue::Document(options) = argument
    else {
        return Err(CommandError::new(2, "$convert requires an object argument"));
    };
    let input_expression = options.get("input").ok_or_else(|| CommandError::new(2, "$convert requires an 'input' field"))?;
    let to_expression = options.get("to").ok_or_else(|| CommandError::new(2, "$convert requires a 'to' field"))?;
    let target = match evaluate_expression_vars(document, to_expression, variables)? {
        BsonValue::String(name) => {
            ConversionTarget::parse(&name).ok_or_else(|| CommandError::new(2, format!("Unknown type name alias: {name}")))?
        }
        BsonValue::Document(_) => {
            return Err(CommandError::new(2, "conversion to binData is not implemented"));
        }
        other => match integral_i64(&other) {
            Some(code) => {
                ConversionTarget::from_code(code).ok_or_else(|| CommandError::new(2, format!("Invalid numerical type code: {code}")))?
            }
            None => {
                return Err(CommandError::new(
                    2,
                    format!("$convert 'to' must be a type name or a numeric type code, found: {}", bson_type_name(&other)),
                ));
            }
        },
    };
    let value = evaluate_expression_vars(document, input_expression, variables)?;
    if matches!(value, BsonValue::Null | BsonValue::Undefined) {
        return match options.get("onNull") {
            Some(expression) => evaluate_expression_vars(document, expression, variables),
            None => Ok(BsonValue::Null),
        };
    }
    match convert_value(&value, target) {
        Ok(result) => Ok(result),
        Err(error) => match options.get("onError") {
            Some(expression) => evaluate_expression_vars(document, expression, variables),
            None => Err(error),
        },
    }
}

/// 按目标类型转换一个非空值。
fn convert_value(value: &BsonValue, target: ConversionTarget) -> Result<BsonValue, CommandError> {
    match target {
        ConversionTarget::Bool => convert_to_bool(value),
        ConversionTarget::Date => convert_to_date(value),
        ConversionTarget::Decimal => convert_to_decimal(value),
        ConversionTarget::Double => convert_to_double(value),
        ConversionTarget::Int => convert_to_integer(value, ConversionTarget::Int),
        ConversionTarget::Long => convert_to_integer(value, ConversionTarget::Long),
        ConversionTarget::ObjectId => convert_to_object_id(value),
        ConversionTarget::String => convert_to_string(value),
    }
}

fn conversion_failure(value: &BsonValue, target: ConversionTarget) -> CommandError {
    CommandError::new(
        241,
        format!("Unsupported conversion from {} to {} in $convert with no onError value", bson_type_name(value), target.name()),
    )
}

fn convert_to_bool(value: &BsonValue) -> Result<BsonValue, CommandError> {
    let flag = match value {
        BsonValue::Bool(flag) => *flag,
        BsonValue::Int32(number) => *number != 0,
        BsonValue::Int64(number) => *number != 0,
        BsonValue::Double(number) => *number != 0.0,
        BsonValue::Decimal128(decimal) => decimal.as_f64() != 0.0,
        // 任何字符串（含空串）、日期与 ObjectId 都转成 `true`。
        BsonValue::String(_) | BsonValue::DateTime(_) | BsonValue::ObjectId(_) => true,
        other => return Err(conversion_failure(other, ConversionTarget::Bool)),
    };
    Ok(BsonValue::Bool(flag))
}

fn convert_to_date(value: &BsonValue) -> Result<BsonValue, CommandError> {
    let millis = match value {
        BsonValue::DateTime(millis) => *millis,
        BsonValue::Timestamp { t, .. } => i64::from(*t) * 1000,
        BsonValue::ObjectId(bytes) => i64::from(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])) * 1000,
        BsonValue::Int32(number) => i64::from(*number),
        BsonValue::Int64(number) => *number,
        BsonValue::Double(number) => truncated_double(*number).ok_or_else(|| conversion_failure(value, ConversionTarget::Date))?,
        BsonValue::Decimal128(decimal) => decimal.as_i64().ok_or_else(|| conversion_failure(value, ConversionTarget::Date))?,
        BsonValue::String(text) => {
            let (local, embedded) = parse_iso_datetime(text).map_err(|_| conversion_failure(value, ConversionTarget::Date))?;
            local
                .checked_sub(embedded.unwrap_or(0).checked_mul(1000).ok_or_else(|| conversion_failure(value, ConversionTarget::Date))?)
                .ok_or_else(|| conversion_failure(value, ConversionTarget::Date))?
        }
        other => return Err(conversion_failure(other, ConversionTarget::Date)),
    };
    Ok(BsonValue::DateTime(millis))
}

fn convert_to_decimal(value: &BsonValue) -> Result<BsonValue, CommandError> {
    let text = match value {
        BsonValue::Bool(flag) => if *flag { "1" } else { "0" }.to_string(),
        BsonValue::Int32(number) => number.to_string(),
        BsonValue::Int64(number) => number.to_string(),
        BsonValue::Double(number) => display_double(*number),
        BsonValue::Decimal128(decimal) => return Ok(BsonValue::Decimal128(*decimal)),
        BsonValue::String(text) => text.clone(),
        BsonValue::DateTime(millis) => millis.to_string(),
        other => return Err(conversion_failure(other, ConversionTarget::Decimal)),
    };
    Decimal128::from_str(&text).map(BsonValue::Decimal128).ok_or_else(|| conversion_failure(value, ConversionTarget::Decimal))
}

fn convert_to_double(value: &BsonValue) -> Result<BsonValue, CommandError> {
    let number = match value {
        BsonValue::Bool(flag) => {
            if *flag {
                1.0
            }
            else {
                0.0
            }
        }
        BsonValue::Int32(number) => f64::from(*number),
        BsonValue::Int64(number) => *number as f64,
        BsonValue::Double(number) => *number,
        BsonValue::Decimal128(decimal) => decimal.as_f64(),
        BsonValue::String(text) => text.parse::<f64>().map_err(|_| conversion_failure(value, ConversionTarget::Double))?,
        BsonValue::DateTime(millis) => *millis as f64,
        other => return Err(conversion_failure(other, ConversionTarget::Double)),
    };
    Ok(BsonValue::Double(number))
}

fn convert_to_integer(value: &BsonValue, target: ConversionTarget) -> Result<BsonValue, CommandError> {
    let number = match value {
        BsonValue::Bool(flag) => i64::from(*flag),
        BsonValue::Int32(number) => i64::from(*number),
        BsonValue::Int64(number) => *number,
        BsonValue::Double(number) => truncated_double(*number).ok_or_else(|| conversion_failure(value, target))?,
        BsonValue::Decimal128(decimal) => {
            decimal.as_i64().or_else(|| truncated_double(decimal.as_f64())).ok_or_else(|| conversion_failure(value, target))?
        }
        BsonValue::String(text) => text.parse::<i64>().map_err(|_| conversion_failure(value, target))?,
        BsonValue::DateTime(millis) => *millis,
        other => return Err(conversion_failure(other, target)),
    };
    match target {
        ConversionTarget::Int => i32::try_from(number).map(BsonValue::Int32).map_err(|_| conversion_failure(value, target)),
        _ => Ok(BsonValue::Int64(number)),
    }
}

fn convert_to_object_id(value: &BsonValue) -> Result<BsonValue, CommandError> {
    match value {
        BsonValue::ObjectId(bytes) => Ok(BsonValue::ObjectId(*bytes)),
        BsonValue::String(text) if text.len() == 24 => {
            let mut bytes = [0_u8; 12];
            for (index, slot) in bytes.iter_mut().enumerate() {
                let pair = &text[index * 2..index * 2 + 2];
                *slot = u8::from_str_radix(pair, 16).map_err(|_| conversion_failure(value, ConversionTarget::ObjectId))?;
            }
            Ok(BsonValue::ObjectId(bytes))
        }
        other => Err(conversion_failure(other, ConversionTarget::ObjectId)),
    }
}

fn convert_to_string(value: &BsonValue) -> Result<BsonValue, CommandError> {
    let text = match value {
        BsonValue::String(text) => text.clone(),
        BsonValue::Bool(flag) => if *flag { "true" } else { "false" }.to_string(),
        BsonValue::Int32(number) => number.to_string(),
        BsonValue::Int64(number) => number.to_string(),
        BsonValue::Double(number) => display_double(*number),
        BsonValue::Decimal128(decimal) => decimal.as_str(),
        BsonValue::ObjectId(bytes) => hex_text(bytes),
        BsonValue::DateTime(millis) => format_date(&CivilDateTime::from_millis(*millis), "%Y-%m-%dT%H:%M:%S.%LZ", FixedOffset::utc())?,
        other => return Err(conversion_failure(other, ConversionTarget::String)),
    };
    Ok(BsonValue::String(text))
}

/// 按 `mongod` 的 `Double.toString()` 口径渲染双精度值。
///
/// 非有限值写成 `"NaN"` / `"Infinity"`；有限值交给 Rust 的最短往返表示，
/// 其科学计数写法（`1e300` 与 `1E+300`）待真机核对。
fn display_double(number: f64) -> String {
    if number.is_nan() {
        "NaN".to_string()
    }
    else if number.is_infinite() {
        if number.is_sign_negative() { "-Infinity".to_string() } else { "Infinity".to_string() }
    }
    else {
        number.to_string()
    }
}

/// 双精度值截断到整数部分，超出 `i64` 或非有限值返回 `None`。
fn truncated_double(number: f64) -> Option<i64> {
    if !number.is_finite() {
        return None;
    }
    let truncated = number.trunc();
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    if truncated >= -TWO_POW_63 && truncated < TWO_POW_63 { Some(truncated as i64) } else { None }
}

/// 按 `$dateToString` 的格式串渲染日期。
///
/// 支持的占位符：`%Y %m %d %H %M %S %L %j %w %u %U %V %G %z %Z %%`。其余
/// 占位符按不支持处理，具体错误码与措辞待真机核对。
fn format_date(civil: &CivilDateTime, format: &str, offset: FixedOffset) -> Result<String, CommandError> {
    let mut output = String::new();
    let mut characters = format.chars();
    while let Some(character) = characters.next() {
        if character != '%' {
            output.push(character);
            continue;
        }
        let specifier = characters.next().ok_or_else(|| CommandError::new(18536, "$dateToString format ends with a lone '%'"))?;
        match specifier {
            'Y' => output.push_str(&format!("{:04}", civil.year)),
            'm' => output.push_str(&format!("{:02}", civil.month)),
            'd' => output.push_str(&format!("{:02}", civil.day)),
            'H' => output.push_str(&format!("{:02}", civil.hour)),
            'M' => output.push_str(&format!("{:02}", civil.minute)),
            'S' => output.push_str(&format!("{:02}", civil.second)),
            'L' => output.push_str(&format!("{:03}", civil.millisecond)),
            'j' => output.push_str(&format!("{:03}", civil.day_of_year())),
            'w' => output.push_str(&format!("{:01}", civil.day_of_week())),
            'u' => output.push_str(&format!("{:01}", civil.iso_day_of_week())),
            'U' => output.push_str(&format!("{:02}", civil.week_of_year())),
            'V' => output.push_str(&format!("{:02}", civil.iso_week().1)),
            'G' => output.push_str(&format!("{:04}", civil.iso_week().0)),
            'z' => output.push_str(&offset.text()),
            'Z' => output.push_str(&offset.seconds.div_euclid(60).to_string()),
            '%' => output.push('%'),
            other => return Err(CommandError::new(18536, format!("unrecognized format specifier: %{other}"))),
        }
    }
    Ok(output)
}

/// 解析 ISO-8601 日期时间，返回本地毫秒与串内自带的偏移秒数。
///
/// 支持 `YYYY-MM-DD`、可选的 `THH:MM[:SS[.fff]]` 与可选的 `Z` / `±HH[:MM]`。
/// 闰秒、负年份与扩展年份不支持；具体报错文案待真机核对。
fn parse_iso_datetime(text: &str) -> Result<(i64, Option<i64>), String> {
    let text = text.trim();
    let separator = text.find(['T', 't', ' ']);
    let (date_part, time_part) = match separator {
        Some(index) => (&text[..index], Some(&text[index + 1..])),
        None => (text, None),
    };
    if date_part.len() != 10 || !date_part.is_ascii() {
        return Err(format!("cannot parse \"{text}\" as an ISO-8601 date"));
    }
    let bytes = date_part.as_bytes();
    if bytes[4] != b'-' || bytes[7] != b'-' {
        return Err(format!("cannot parse \"{text}\" as an ISO-8601 date"));
    }
    let year: i64 = date_part[0..4].parse().map_err(|_| format!("cannot parse year in \"{text}\""))?;
    let month: u32 = date_part[5..7].parse().map_err(|_| format!("cannot parse month in \"{text}\""))?;
    let day: u32 = date_part[8..10].parse().map_err(|_| format!("cannot parse day in \"{text}\""))?;
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, i64::from(month)) as u32 {
        return Err(format!("date out of range in \"{text}\""));
    }
    let (hour, minute, second, millisecond, offset) = match time_part {
        Some(rest) => parse_clock_with_zone(rest, text)?,
        None => (0, 0, 0, 0, None),
    };
    let local = CivilDateTime { year, month, day, hour, minute, second, millisecond }.to_millis();
    Ok((local, offset))
}

/// 解析 `HH:MM[:SS[.fff]]` 与其后的时区偏移。
fn parse_clock_with_zone(text: &str, original: &str) -> Result<(u32, u32, u32, u32, Option<i64>), String> {
    let (clock, offset) = if text.ends_with('Z') || text.ends_with('z') {
        (&text[..text.len() - 1], Some(0))
    }
    else {
        match text[1..].find(['+', '-']).map(|index| index + 1) {
            Some(index) => {
                let offset = FixedOffset::parse(&text[index..]).map_err(|error| error.message)?;
                (&text[..index], Some(offset.seconds))
            }
            None => (text, None),
        }
    };
    let mut parts = clock.split(':');
    let hour: u32 = parts
        .next()
        .ok_or_else(|| format!("missing hour in \"{original}\""))?
        .parse()
        .map_err(|_| format!("cannot parse hour in \"{original}\""))?;
    let minute: u32 = parts
        .next()
        .ok_or_else(|| format!("missing minute in \"{original}\""))?
        .parse()
        .map_err(|_| format!("cannot parse minute in \"{original}\""))?;
    let (second, millisecond) = match parts.next() {
        Some(part) => {
            let (second_text, fraction) = match part.split_once('.') {
                Some((second, fraction)) => (second, Some(fraction)),
                None => (part, None),
            };
            let second: u32 = second_text.parse().map_err(|_| format!("cannot parse second in \"{original}\""))?;
            let millisecond = match fraction {
                Some(fraction) => {
                    if fraction.is_empty() || fraction.len() > 9 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
                        return Err(format!("cannot parse fractional seconds in \"{original}\""));
                    }
                    let digits: String = format!("{fraction:0<3}").chars().take(3).collect();
                    digits.parse().map_err(|_| format!("cannot parse fractional seconds in \"{original}\""))?
                }
                None => 0,
            };
            (second, millisecond)
        }
        None => (0, 0),
    };
    if parts.next().is_some() {
        return Err(format!("trailing time components in \"{original}\""));
    }
    if hour > 23 || minute > 59 || second > 59 {
        return Err(format!("time out of range in \"{original}\""));
    }
    Ok((hour, minute, second, millisecond, offset))
}

/// 按格式串解析日期时间。
///
/// 只支持 `%Y %m %d %H %M %S %L %z %Z %%`，且数字宽度按输出格式固定；其余
/// 占位符与宽松匹配行为待真机核对。
fn parse_formatted_datetime(text: &str, format: &str) -> Result<(i64, Option<i64>), String> {
    let mut year = 1970i64;
    let mut month = 1u32;
    let mut day = 1u32;
    let mut hour = 0u32;
    let mut minute = 0u32;
    let mut second = 0u32;
    let mut millisecond = 0u32;
    let mut offset = None;
    let mut remaining = text;
    let mut characters = format.chars();
    while let Some(character) = characters.next() {
        if character != '%' {
            let mut buffer = [0u8; 4];
            let literal: &str = character.encode_utf8(&mut buffer);
            remaining = remaining.strip_prefix(literal).ok_or_else(|| format!("expected '{character}' in \"{text}\""))?;
            continue;
        }
        let specifier = characters.next().ok_or_else(|| "format ends with a lone '%'".to_string())?;
        match specifier {
            '%' => remaining = remaining.strip_prefix('%').ok_or_else(|| format!("expected '%' in \"{text}\""))?,
            'z' | 'Z' => {
                let (zone, rest) = parse_zone_prefix(remaining)?;
                offset = Some(zone);
                remaining = rest;
            }
            'Y' | 'm' | 'd' | 'H' | 'M' | 'S' | 'L' => {
                let width = if specifier == 'Y' {
                    4
                }
                else if specifier == 'L' {
                    3
                }
                else {
                    2
                };
                let (digits, rest) = take_digits(remaining, width)?;
                let number: u32 = digits.parse().map_err(|_| format!("cannot parse %{specifier} in \"{text}\""))?;
                match specifier {
                    'Y' => year = i64::from(number),
                    'm' => month = number,
                    'd' => day = number,
                    'H' => hour = number,
                    'M' => minute = number,
                    'S' => second = number,
                    _ => millisecond = number,
                }
                remaining = rest;
            }
            other => return Err(format!("unrecognized format specifier: %{other}")),
        }
    }
    if !remaining.is_empty() {
        return Err(format!("trailing characters in \"{text}\""));
    }
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, i64::from(month)) as u32 {
        return Err(format!("date out of range in \"{text}\""));
    }
    if hour > 23 || minute > 59 || second > 59 {
        return Err(format!("time out of range in \"{text}\""));
    }
    Ok((CivilDateTime { year, month, day, hour, minute, second, millisecond }.to_millis(), offset))
}

/// 从输入开头取固定宽度的 ASCII 数字。
fn take_digits(text: &str, width: usize) -> Result<(&str, &str), String> {
    if text.len() < width || !text.as_bytes()[..width].iter().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("expected {width} digits"));
    }
    Ok((&text[..width], &text[width..]))
}

/// 从输入开头取时区偏移，返回偏移秒数与剩余文本。
fn parse_zone_prefix(text: &str) -> Result<(i64, &str), String> {
    let bytes = text.as_bytes();
    match bytes.first() {
        Some(b'Z') | Some(b'z') => return Ok((0, &text[1..])),
        Some(b'+') | Some(b'-') => {}
        _ => return Err("missing timezone offset".to_string()),
    }
    let length = if text.len() >= 6 && bytes[3] == b':' {
        6
    }
    else if text.len() >= 5 {
        5
    }
    else if text.len() >= 3 {
        3
    }
    else {
        return Err("truncated timezone offset".to_string());
    };
    let offset = FixedOffset::parse(&text[..length]).map_err(|error| error.message)?;
    Ok((offset.seconds, &text[length..]))
}

/// 按单位平移日期。年 / 季 / 月先落到目标月的同一天，超出月末则截到月末。
fn add_date(value: DateValue, unit: DateUnit, amount: i64) -> Result<i64, CommandError> {
    if unit.months() > 0 {
        let civil = CivilDateTime::from_millis(local_millis(value)?);
        let months =
            amount.checked_mul(unit.months()).ok_or_else(|| CommandError::new(2, "date arithmetic overflows a 64-bit millisecond count"))?;
        let total = civil
            .year
            .checked_mul(12)
            .and_then(|total| total.checked_add(i64::from(civil.month) - 1))
            .and_then(|total| total.checked_add(months))
            .ok_or_else(|| CommandError::new(2, "date arithmetic overflows a 64-bit millisecond count"))?;
        let year = total.div_euclid(12);
        let month = (total.rem_euclid(12) + 1) as u32;
        let day = civil.day.min(days_in_month(year, i64::from(month)) as u32);
        let updated = CivilDateTime { year, month, day, ..civil };
        let offset = value.offset.seconds.checked_mul(1000).ok_or_else(|| CommandError::new(2, "timezone offset overflows"))?;
        return updated
            .to_millis()
            .checked_sub(offset)
            .ok_or_else(|| CommandError::new(2, "date arithmetic overflows a 64-bit millisecond count"));
    }
    let unit_millis = match unit {
        DateUnit::Week => 7 * 86_400_000,
        DateUnit::Day => 86_400_000,
        DateUnit::Hour => 3_600_000,
        DateUnit::Minute => 60_000,
        DateUnit::Second => 1_000,
        _ => 1,
    };
    let delta = amount.checked_mul(unit_millis).ok_or_else(|| CommandError::new(2, "date arithmetic overflows a 64-bit millisecond count"))?;
    value.millis.checked_add(delta).ok_or_else(|| CommandError::new(2, "date arithmetic overflows a 64-bit millisecond count"))
}

/// 以 `startOfWeek` 为界，把本地天数折算成周序号。
fn week_index(days: i64, start_of_week: i64) -> i64 {
    // 1970-01-01 是星期四，Sunday 记 1 时为 5。
    let reference = start_of_week - 5;
    (days - reference).div_euclid(7)
}

/// `$dateTrunc` 的参考点是本地时间 2000-01-01T00:00:00，按 `binSize` 向下取整。
fn truncate_local(local: i64, unit: DateUnit, bin_size: i64, start_of_week: i64) -> Result<i64, CommandError> {
    let reference_days = days_from_civil(2000, 1, 1);
    let reference_millis = reference_days * 86_400_000;
    let zero = |year: i64, month: u32| CivilDateTime { year, month, day: 1, hour: 0, minute: 0, second: 0, millisecond: 0 };
    Ok(match unit {
        DateUnit::Year => {
            let civil = CivilDateTime::from_millis(local);
            let index = (civil.year - 2000).div_euclid(bin_size);
            zero(2000 + index * bin_size, 1).to_millis()
        }
        DateUnit::Quarter => {
            let civil = CivilDateTime::from_millis(local);
            let index = (civil.year - 2000) * 4 + i64::from((civil.month - 1) / 3);
            let start = index.div_euclid(bin_size) * bin_size;
            zero(2000 + start.div_euclid(4), (start.rem_euclid(4) * 3 + 1) as u32).to_millis()
        }
        DateUnit::Month => {
            let civil = CivilDateTime::from_millis(local);
            let index = (civil.year - 2000) * 12 + i64::from(civil.month) - 1;
            let start = index.div_euclid(bin_size) * bin_size;
            zero(2000 + start.div_euclid(12), (start.rem_euclid(12) + 1) as u32).to_millis()
        }
        DateUnit::Week => {
            let days = local.div_euclid(86_400_000);
            let reference_week = reference_days - (sunday_day_of_week(reference_days) - start_of_week).rem_euclid(7);
            let index = (days - reference_week).div_euclid(7 * bin_size);
            (reference_week + index * 7 * bin_size)
                .checked_mul(86_400_000)
                .ok_or_else(|| CommandError::new(2, "$dateTrunc result overflows a 64-bit millisecond count"))?
        }
        DateUnit::Day | DateUnit::Hour | DateUnit::Minute | DateUnit::Second | DateUnit::Millisecond => {
            let unit_millis = match unit {
                DateUnit::Day => 86_400_000,
                DateUnit::Hour => 3_600_000,
                DateUnit::Minute => 60_000,
                DateUnit::Second => 1_000,
                _ => 1,
            };
            let index = (local - reference_millis).div_euclid(unit_millis * bin_size);
            reference_millis
                .checked_add(
                    index
                        .checked_mul(unit_millis * bin_size)
                        .ok_or_else(|| CommandError::new(2, "$dateTrunc result overflows a 64-bit millisecond count"))?,
                )
                .ok_or_else(|| CommandError::new(2, "$dateTrunc result overflows a 64-bit millisecond count"))?
        }
    })
}

/// Aggregation truthiness: `false`, `0`, `null`, and missing are false.
fn expression_truthy(value: &BsonValue) -> bool {
    match value {
        BsonValue::Bool(flag) => *flag,
        BsonValue::Null => false,
        BsonValue::Int32(number) => *number != 0,
        BsonValue::Int64(number) => *number != 0,
        BsonValue::Double(number) => *number != 0.0,
        _ => true,
    }
}

fn rule_is_inclusion(value: &BsonValue) -> bool {
    matches!(value, BsonValue::Int32(1) | BsonValue::Int64(1) | BsonValue::Bool(true))
        || matches!(value, BsonValue::Double(number) if *number == 1.0)
}

fn rule_is_exclusion(value: &BsonValue) -> bool {
    matches!(value, BsonValue::Int32(0) | BsonValue::Int64(0) | BsonValue::Bool(false))
        || matches!(value, BsonValue::Double(number) if *number == 0.0)
}

fn count_value(count: i64) -> BsonValue {
    i32::try_from(count).map(BsonValue::Int32).unwrap_or(BsonValue::Int64(count))
}

fn reply_count(reply: &BsonDocument, field: &str) -> i64 {
    match reply.get(field) {
        Some(BsonValue::Int32(value)) => i64::from(*value),
        Some(BsonValue::Int64(value)) => *value,
        Some(BsonValue::Double(value)) => *value as i64,
        _ => 0,
    }
}

fn parse_sort(sort: Option<&BsonValue>) -> Vec<(String, i32)> {
    match sort {
        Some(BsonValue::Document(document)) => document
            .fields
            .iter()
            .filter_map(|(field, direction)| int_value(direction).filter(|value| *value != 0).map(|value| (field.clone(), value as i32)))
            .collect(),
        _ => Vec::new(),
    }
}

fn sort_documents(documents: &mut [BsonDocument], sort: &[(String, i32)]) {
    if sort.is_empty() {
        return;
    }
    documents.sort_by(|left, right| {
        for (field, direction) in sort {
            let ordering = compare_values(&sort_key(left, field, *direction), &sort_key(right, field, *direction));
            if ordering != Ordering::Equal {
                return if *direction < 0 { ordering.reverse() } else { ordering };
            }
        }
        Ordering::Equal
    });
}

fn document_matches(document: &BsonDocument, filter: &BsonDocument) -> Result<bool, CommandError> {
    document_matches_vars(document, filter, &empty_variables())
}

fn document_matches_vars(document: &BsonDocument, filter: &BsonDocument, variables: &ExpressionVariables) -> Result<bool, CommandError> {
    for (field_index, (field, expected)) in filter.fields.iter().enumerate() {
        match field.as_str() {
            "$and" => {
                let BsonValue::Array(clauses) = expected
                else {
                    return Err(CommandError::new(2, format!("$and requires an array of filter documents (fieldIndex: {field_index})")));
                };
                for clause in clauses {
                    let BsonValue::Document(clause) = clause
                    else {
                        return Err(CommandError::new(2, format!("$and requires filter documents (fieldIndex: {field_index})")));
                    };
                    if !document_matches_vars(document, clause, variables)? {
                        return Ok(false);
                    }
                }
            }
            "$or" => {
                let BsonValue::Array(clauses) = expected
                else {
                    return Err(CommandError::new(2, format!("$or requires an array of filter documents (fieldIndex: {field_index})")));
                };
                let mut matched = false;
                for clause in clauses {
                    let BsonValue::Document(clause) = clause
                    else {
                        return Err(CommandError::new(2, format!("$or requires filter documents (fieldIndex: {field_index})")));
                    };
                    if document_matches_vars(document, clause, variables)? {
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    return Ok(false);
                }
            }
            "$nor" => {
                let BsonValue::Array(clauses) = expected
                else {
                    return Err(CommandError::new(2, format!("$nor requires an array of filter documents (fieldIndex: {field_index})")));
                };
                for clause in clauses {
                    let BsonValue::Document(clause) = clause
                    else {
                        return Err(CommandError::new(2, format!("$nor requires filter documents (fieldIndex: {field_index})")));
                    };
                    if document_matches_vars(document, clause, variables)? {
                        return Ok(false);
                    }
                }
            }
            "$expr" => {
                let value = evaluate_expression_vars(document, expected, variables)?;
                if !expression_truthy(&value) {
                    return Ok(false);
                }
            }
            _ if field.starts_with('$') => {
                return Err(CommandError::new(2, format!("unknown top level operator: {field} (fieldIndex: {field_index})")));
            }
            _ => {
                let candidates = collect_field(document, field);
                let matched = match expected {
                    BsonValue::Document(operators) if has_operator(operators) => operator_matches(&candidates, operators, field, field_index)?,
                    _ => equality_candidates(&candidates, expected),
                };
                if !matched {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

/// Resolves a (possibly dotted) field path into every value the path selects.
///
/// A dotted segment below an array descends into every element, which is what
/// lets `a.b` match an array of documents; an all-digit segment indexes the
/// array instead. A missing path yields no candidates, which is how `$exists:
/// false`, `{a: null}` and `$nin` tell "absent" apart from "explicit null".
fn collect_field<'a>(document: &'a BsonDocument, path: &str) -> Vec<&'a BsonValue> {
    if let Some(value) = document.get(path) {
        return vec![value];
    }
    let segments: Vec<&str> = path.split('.').collect();
    let mut values = Vec::new();
    if let Some(first) = document.get(segments[0]) {
        descend(first, &segments[1..], &mut values);
    }
    values
}

fn descend<'a>(value: &'a BsonValue, segments: &[&str], values: &mut Vec<&'a BsonValue>) {
    if segments.is_empty() {
        values.push(value);
        return;
    }
    match value {
        BsonValue::Document(document) => {
            if let Some(next) = document.get(segments[0]) {
                descend(next, &segments[1..], values);
            }
        }
        BsonValue::Array(items) => {
            if let Ok(index) = segments[0].parse::<usize>() {
                if let Some(next) = items.get(index) {
                    descend(next, &segments[1..], values);
                }
            }
            else {
                for item in items {
                    descend(item, segments, values);
                }
            }
        }
        _ => {}
    }
}

fn operator_matches(candidates: &[&BsonValue], operators: &BsonDocument, field: &str, field_index: usize) -> Result<bool, CommandError> {
    let options = match operators.get("$options") {
        Some(BsonValue::String(options)) => options.as_str(),
        _ => "",
    };
    for (operator_index, (operator, operand)) in operators.fields.iter().enumerate() {
        if operator == "$options" {
            continue;
        }
        let matched = match operator.as_str() {
            "$eq" => equality_candidates(candidates, operand),
            "$ne" => !equality_candidates(candidates, operand),
            "$gt" => comparison_candidates(candidates, operand, |ordering| ordering == Ordering::Greater),
            "$gte" => comparison_candidates(candidates, operand, |ordering| ordering != Ordering::Less),
            "$lt" => comparison_candidates(candidates, operand, |ordering| ordering == Ordering::Less),
            "$lte" => comparison_candidates(candidates, operand, |ordering| ordering != Ordering::Greater),
            "$in" => in_candidates(candidates, operand),
            "$nin" => !in_candidates(candidates, operand),
            "$exists" => exists_candidates(candidates, operand),
            "$regex" => regex_candidates(candidates, operand, options)?,
            "$size" => size_candidates(candidates, operand),
            "$type" => type_candidates(candidates, operand)?,
            "$elemMatch" => elem_match_candidates(candidates, operand)?,
            "$all" => all_candidates(candidates, operand)?,
            "$not" => not_candidates(candidates, operand)?,
            "$mod" => mod_candidates(candidates, operand)?,
            "$bitsAllSet" => bits_candidates(candidates, operand, BitTest::AllSet)?,
            "$bitsAnySet" => bits_candidates(candidates, operand, BitTest::AnySet)?,
            "$bitsAllClear" => bits_candidates(candidates, operand, BitTest::AllClear)?,
            "$bitsAnyClear" => bits_candidates(candidates, operand, BitTest::AnyClear)?,
            other => return Err(unknown_operator(other, "filter", field, field_index, operator_index)),
        };
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

fn equality_candidates(candidates: &[&BsonValue], expected: &BsonValue) -> bool {
    if candidates.is_empty() {
        return matches!(expected, BsonValue::Null);
    }
    candidates.iter().any(|candidate| equality_one(candidate, expected))
}

fn equality_one(candidate: &BsonValue, expected: &BsonValue) -> bool {
    if values_equal(Some(candidate), expected) {
        return true;
    }
    match (candidate, expected) {
        (BsonValue::Array(items), _) => items.iter().any(|item| values_equal(Some(item), expected)),
        _ => false,
    }
}

fn in_candidates(candidates: &[&BsonValue], operand: &BsonValue) -> bool {
    let BsonValue::Array(values) = operand
    else {
        return false;
    };
    // One `$eq` predicate per list element, so `$in` containing `null` also
    // matches a missing field exactly like `{field: null}` does.
    values.iter().any(|value| equality_candidates(candidates, value))
}

fn exists_candidates(candidates: &[&BsonValue], operand: &BsonValue) -> bool {
    let BsonValue::Bool(exists) = operand
    else {
        return false;
    };
    candidates.is_empty() != *exists
}

fn comparison_candidates(candidates: &[&BsonValue], operand: &BsonValue, accept: impl Fn(Ordering) -> bool) -> bool {
    candidates.iter().any(|candidate| {
        if same_type_bracket(candidate, operand) && accept(compare_values(candidate, operand)) {
            return true;
        }
        if let BsonValue::Array(items) = candidate {
            return items.iter().any(|item| same_type_bracket(item, operand) && accept(compare_values(item, operand)));
        }
        false
    })
}

/// Comparison operators are type-bracketed: `{$gt: 5}` never matches a string,
/// even though strings sort after numbers in BSON order.
fn same_type_bracket(left: &BsonValue, right: &BsonValue) -> bool {
    value_rank(left) == value_rank(right)
}

fn regex_candidates(candidates: &[&BsonValue], operand: &BsonValue, options: &str) -> Result<bool, CommandError> {
    let BsonValue::String(pattern) = operand
    else {
        return Ok(false);
    };
    let regex = compile_regex(pattern, options)?;
    Ok(candidates.iter().any(|candidate| match candidate {
        BsonValue::String(text) => regex.is_match(text),
        BsonValue::Array(items) => items.iter().any(|item| matches!(item, BsonValue::String(text) if regex.is_match(text))),
        _ => false,
    }))
}

fn compile_regex(pattern: &str, options: &str) -> Result<regex::Regex, CommandError> {
    let mut builder = regex::RegexBuilder::new(pattern);
    for option in options.chars() {
        match option {
            'i' => {
                builder.case_insensitive(true);
            }
            'm' => {
                builder.multi_line(true);
            }
            's' => {
                builder.dot_matches_new_line(true);
            }
            'x' => {
                builder.ignore_whitespace(true);
            }
            // `u` is documented as accepted but redundant for UTF-8 strings.
            'u' => {}
            other => return Err(CommandError::new(2, format!("unsupported $regex option: {other}"))),
        }
    }
    builder.build().map_err(|error| CommandError::new(2, format!("invalid $regex pattern: {error}")))
}

fn size_candidates(candidates: &[&BsonValue], operand: &BsonValue) -> bool {
    let Some(size) = integer_of(operand)
    else {
        return false;
    };
    candidates.iter().any(|candidate| matches!(candidate, BsonValue::Array(items) if items.len() as i128 == size))
}

fn all_candidates(candidates: &[&BsonValue], operand: &BsonValue) -> Result<bool, CommandError> {
    let BsonValue::Array(values) = operand
    else {
        return Ok(false);
    };
    // The manual states an empty `$all` array matches no documents.
    if values.is_empty() {
        return Ok(false);
    }
    for candidate in candidates {
        let BsonValue::Array(items) = candidate
        else {
            if values.len() == 1 && equality_one(candidate, &values[0]) {
                return Ok(true);
            }
            continue;
        };
        let mut matches_all = true;
        for value in values {
            let satisfied = match value {
                BsonValue::Document(entry) if entry.fields.len() == 1 && entry.get("$elemMatch").is_some() => {
                    elem_match_candidates(&[*candidate], entry.get("$elemMatch").unwrap())?
                }
                _ => items.iter().any(|item| values_equal(Some(item), value)),
            };
            if !satisfied {
                matches_all = false;
                break;
            }
        }
        if matches_all {
            return Ok(true);
        }
    }
    Ok(false)
}

fn elem_match_candidates(candidates: &[&BsonValue], operand: &BsonValue) -> Result<bool, CommandError> {
    let BsonValue::Document(specification) = operand
    else {
        return Ok(false);
    };
    let operator_form = has_operator(specification);
    for candidate in candidates {
        let BsonValue::Array(items) = candidate
        else {
            continue;
        };
        for item in items {
            let matched = if operator_form {
                operator_matches(&[item], specification, "filter", 0)?
            }
            else {
                match item {
                    BsonValue::Document(document) => document_matches(document, specification)?,
                    _ => false,
                }
            };
            if matched {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn not_candidates(candidates: &[&BsonValue], operand: &BsonValue) -> Result<bool, CommandError> {
    let BsonValue::Document(specification) = operand
    else {
        return Ok(false);
    };
    Ok(!operator_matches(candidates, specification, "filter", 0)?)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypeFilter {
    Number,
    Null,
    Bool,
    Int32,
    Int64,
    Double,
    String,
    Document,
    Array,
    Binary,
    ObjectId,
    Never,
}

fn parse_type_operand(operand: &BsonValue) -> Result<Vec<TypeFilter>, CommandError> {
    match operand {
        BsonValue::Array(values) => values.iter().map(parse_type_value).collect(),
        other => Ok(vec![parse_type_value(other)?]),
    }
}

fn parse_type_value(value: &BsonValue) -> Result<TypeFilter, CommandError> {
    if let BsonValue::String(name) = value {
        let filter = match name.as_str() {
            "double" => TypeFilter::Double,
            "string" => TypeFilter::String,
            "object" => TypeFilter::Document,
            "array" => TypeFilter::Array,
            "binData" => TypeFilter::Binary,
            "objectId" => TypeFilter::ObjectId,
            "bool" => TypeFilter::Bool,
            "null" => TypeFilter::Null,
            "int" => TypeFilter::Int32,
            "long" => TypeFilter::Int64,
            "number" => TypeFilter::Number,
            // Documented BSON types this store cannot hold: the query is valid
            // but can never match, exactly like a real server with no such value.
            "undefined"
            | "date"
            | "regex"
            | "dbPointer"
            | "javascript"
            | "symbol"
            | "javascriptWithScope"
            | "timestamp"
            | "decimal"
            | "minKey"
            | "maxKey" => TypeFilter::Never,
            other => return Err(CommandError::new(2, format!("unknown $type alias: {other}"))),
        };
        return Ok(filter);
    }
    let code = integer_of(value).ok_or_else(|| CommandError::new(2, "$type requires a type alias or a BSON type code"))?;
    let filter = match code {
        1 => TypeFilter::Double,
        2 => TypeFilter::String,
        3 => TypeFilter::Document,
        4 => TypeFilter::Array,
        5 => TypeFilter::Binary,
        7 => TypeFilter::ObjectId,
        8 => TypeFilter::Bool,
        10 => TypeFilter::Null,
        16 => TypeFilter::Int32,
        18 => TypeFilter::Int64,
        6 | 9 | 11 | 12 | 13 | 14 | 15 | 17 | 19 | -1 | 127 => TypeFilter::Never,
        other => return Err(CommandError::new(2, format!("unknown $type code: {other}"))),
    };
    Ok(filter)
}

fn type_candidates(candidates: &[&BsonValue], operand: &BsonValue) -> Result<bool, CommandError> {
    let filters = parse_type_operand(operand)?;
    let wanted = |value: &BsonValue| filters.iter().any(|filter| type_filter_matches(value, *filter));
    Ok(candidates.iter().any(|candidate| {
        if wanted(candidate) {
            return true;
        }
        match candidate {
            BsonValue::Array(items) => items.iter().any(&wanted),
            _ => false,
        }
    }))
}

fn type_filter_matches(value: &BsonValue, filter: TypeFilter) -> bool {
    match filter {
        TypeFilter::Number => matches!(value, BsonValue::Int32(_) | BsonValue::Int64(_) | BsonValue::Double(_)),
        TypeFilter::Null => matches!(value, BsonValue::Null),
        TypeFilter::Bool => matches!(value, BsonValue::Bool(_)),
        TypeFilter::Int32 => matches!(value, BsonValue::Int32(_)),
        TypeFilter::Int64 => matches!(value, BsonValue::Int64(_)),
        TypeFilter::Double => matches!(value, BsonValue::Double(_)),
        TypeFilter::String => matches!(value, BsonValue::String(_)),
        TypeFilter::Document => matches!(value, BsonValue::Document(_)),
        TypeFilter::Array => matches!(value, BsonValue::Array(_)),
        TypeFilter::Binary => matches!(value, BsonValue::Binary(_)),
        TypeFilter::ObjectId => matches!(value, BsonValue::ObjectId(_)),
        TypeFilter::Never => false,
    }
}

fn lookup_field<'a>(document: &'a BsonDocument, path: &str) -> Option<&'a BsonValue> {
    if let Some(value) = document.get(path) {
        return Some(value);
    }
    let (head, tail) = path.split_once('.')?;
    match document.get(head)? {
        BsonValue::Document(inner) => inner.get(tail),
        _ => None,
    }
}

fn values_equal(actual: Option<&BsonValue>, expected: &BsonValue) -> bool {
    match (actual, expected) {
        (None, BsonValue::Null) => true,
        (None, _) => false,
        (Some(left), right) => match (left, right) {
            (BsonValue::Document(left), BsonValue::Document(right)) => encode_bson(left) == encode_bson(right),
            (BsonValue::Array(left), BsonValue::Array(right)) => {
                left.len() == right.len() && left.iter().zip(right.iter()).all(|(left, right)| values_equal(Some(left), right))
            }
            (BsonValue::Null, BsonValue::Null) => true,
            _ => numeric_equal(left, right).unwrap_or_else(|| scalar_key(left) == scalar_key(right)),
        },
    }
}

/// MongoDB compares `int32`, `int64` and `double` by numeric value, so `1`,
/// `NumberLong(1)` and `1.0` are the same value. Integral doubles are compared
/// through their exact integer value, which keeps values above `2^53` exact
/// instead of collapsing them through `f64`.
fn numeric_equal(left: &BsonValue, right: &BsonValue) -> Option<bool> {
    if let (Some(left), Some(right)) = (integer_of(left), integer_of(right)) {
        return Some(left == right);
    }
    let (integer, double) = match (integer_of(left), double_of(right)) {
        (Some(integer), Some(double)) => (integer, double),
        _ => match (double_of(left), integer_of(right)) {
            (Some(double), Some(integer)) => (integer, double),
            _ => return None,
        },
    };
    Some(integral_value(double).map(|exact| i128::from(exact) == integer).unwrap_or(false))
}

fn scalar_key(value: &BsonValue) -> String {
    match value {
        BsonValue::String(text) => format!("s:{text}"),
        BsonValue::Bool(flag) => format!("b:{flag}"),
        BsonValue::ObjectId(bytes) => format!("o:{bytes:?}"),
        BsonValue::Binary(binary) => format!("y:{:02x}:{:?}", binary.subtype, binary.bytes),
        other => format!("x:{other:?}"),
    }
}

fn integer_of(value: &BsonValue) -> Option<i128> {
    match value {
        BsonValue::Int32(number) => Some(i128::from(*number)),
        BsonValue::Int64(number) => Some(i128::from(*number)),
        _ => None,
    }
}

fn double_of(value: &BsonValue) -> Option<f64> {
    match value {
        BsonValue::Double(number) => Some(*number),
        _ => None,
    }
}

fn number_of(value: &BsonValue) -> Option<f64> {
    match value {
        BsonValue::Int32(number) => Some(*number as f64),
        BsonValue::Int64(number) => Some(*number as f64),
        BsonValue::Double(number) => Some(*number),
        _ => None,
    }
}

/// 取数值的精确整数视图：仅当值可整除且落在 `i64` 范围内。
/// `$mod` 与位算子只检查整数值。
fn integral_i64(value: &BsonValue) -> Option<i64> {
    match value {
        BsonValue::Int32(number) => Some(i64::from(*number)),
        BsonValue::Int64(number) => Some(*number),
        BsonValue::Double(number) if number.fract() == 0.0 && *number >= i64::MIN as f64 && *number <= i64::MAX as f64 => Some(*number as i64),
        _ => None,
    }
}

/// 解析 `$mod` 操作数：`[divisor, remainder]`。
///
/// 文案对齐 `mongod` 解析器的报错，并由单测钉住；确切措辞仍待真实
/// `mongod 8.0.9` 录制核对。
fn parse_mod_operand(operand: &BsonValue) -> Result<(&BsonValue, &BsonValue), &'static str> {
    let BsonValue::Array(parts) = operand
    else {
        return Err("malformed mod, needs to be an array");
    };
    if parts.len() < 2 {
        return Err("malformed mod, not enough elements");
    }
    if parts.len() > 2 {
        return Err("malformed mod, too many elements");
    }
    let Some(divisor) = number_of(&parts[0])
    else {
        return Err("malformed mod, divisor not a number");
    };
    if number_of(&parts[1]).is_none() {
        return Err("malformed mod, remainder not a number");
    }
    if divisor == 0.0 {
        return Err("divisor cannot be 0");
    }
    Ok((&parts[0], &parts[1]))
}

fn mod_candidates(candidates: &[&BsonValue], operand: &BsonValue) -> Result<bool, CommandError> {
    let (divisor, remainder) = parse_mod_operand(operand).map_err(|detail| CommandError::new(2, detail))?;
    Ok(candidates.iter().any(|candidate| mod_value_matches(candidate, divisor, remainder)))
}

fn mod_value_matches(candidate: &BsonValue, divisor: &BsonValue, remainder: &BsonValue) -> bool {
    if let BsonValue::Array(items) = candidate {
        return items.iter().any(|item| mod_value_matches(item, divisor, remainder));
    }
    if let (Some(value), Some(divisor), Some(remainder)) = (integral_i64(candidate), integral_i64(divisor), integral_i64(remainder)) {
        return value.checked_rem(divisor) == Some(remainder);
    }
    let (Some(value), Some(divisor), Some(remainder)) = (number_of(candidate), number_of(divisor), number_of(remainder))
    else {
        return false;
    };
    // `f64` 上的 `%` 就是 `fmod`，`mongod` 对 double 应用 `$mod` 也是这个语义。
    value % divisor == remainder
}

/// `$bits*` 系列算子各自的判定方式。
#[derive(Debug, Clone, Copy)]
enum BitTest {
    AllSet,
    AnySet,
    AllClear,
    AnyClear,
}

/// 解析位运算操作数：64 位二补码掩码，或显式位号列表。
/// `mongod` 把掩码文档化为有符号 64 位整数，因此负数会置起高位。
fn bit_mask(operand: &BsonValue) -> Result<u64, &'static str> {
    match operand {
        BsonValue::Int32(value) => Ok(i64::from(*value) as u64),
        BsonValue::Int64(value) => Ok(*value as u64),
        BsonValue::Double(value) if value.fract() == 0.0 && *value >= i64::MIN as f64 && *value <= i64::MAX as f64 => Ok(*value as i64 as u64),
        BsonValue::Array(positions) => {
            let mut mask = 0_u64;
            for position in positions {
                let Some(position) = integral_i64(position)
                else {
                    return Err("bit positions must be integers");
                };
                if !(0..64).contains(&position) {
                    return Err("bit positions must be between 0 and 63");
                }
                mask |= 1_u64 << (position as u32);
            }
            Ok(mask)
        }
        _ => Err("bitwise operators need an integer mask or an array of bit positions"),
    }
}

fn bits_candidates(candidates: &[&BsonValue], operand: &BsonValue, test: BitTest) -> Result<bool, CommandError> {
    let mask = bit_mask(operand).map_err(|detail| CommandError::new(2, detail))?;
    Ok(candidates.iter().any(|candidate| bits_value_matches(candidate, mask, test)))
}

fn bits_value_matches(candidate: &BsonValue, mask: u64, test: BitTest) -> bool {
    if let BsonValue::Array(items) = candidate {
        return items.iter().any(|item| bits_value_matches(item, mask, test));
    }
    let Some(bits) = integral_i64(candidate).map(|value| value as u64)
    else {
        return false;
    };
    match test {
        BitTest::AllSet => bits & mask == mask,
        BitTest::AnySet => bits & mask != 0,
        BitTest::AllClear => bits & mask == 0,
        BitTest::AnyClear => bits & mask != mask,
    }
}

/// BSON sort order between the value types this store can hold.
///
/// Mirrors the canonical total order encoded in [`BsonValue::compare`], which
/// itself was read from `mongod 8.0.9` `sort` results. Numeric types share one
/// bracket and `Symbol` shares the `String` bracket.
fn value_rank(value: &BsonValue) -> u8 {
    match value {
        BsonValue::MinKey => 0,
        BsonValue::Undefined => 1,
        BsonValue::Null => 2,
        BsonValue::Int32(_) | BsonValue::Int64(_) | BsonValue::Double(_) | BsonValue::Decimal128(_) => 3,
        BsonValue::String(_) | BsonValue::Symbol(_) => 4,
        BsonValue::Document(_) => 5,
        BsonValue::Array(_) => 6,
        BsonValue::Binary(_) => 7,
        BsonValue::ObjectId(_) => 8,
        BsonValue::Bool(_) => 9,
        BsonValue::DateTime(_) => 10,
        BsonValue::Timestamp { .. } => 11,
        BsonValue::Regex { .. } => 12,
        BsonValue::DbPointer { .. } => 13,
        BsonValue::Code(_) => 14,
        BsonValue::CodeWithScope { .. } => 15,
        BsonValue::MaxKey => 16,
    }
}

fn compare_values(left: &BsonValue, right: &BsonValue) -> Ordering {
    left.compare(right)
}

fn parse_count(value: Option<&BsonValue>) -> usize {
    value.and_then(int_value).filter(|count| *count > 0).map(|count| count as usize).unwrap_or(0)
}

fn int_value(value: &BsonValue) -> Option<i64> {
    match value {
        BsonValue::Int32(number) => Some(*number as i64),
        BsonValue::Int64(number) => Some(*number),
        BsonValue::Double(number) => Some(*number as i64),
        _ => None,
    }
}
