//! Encode structured operations into VOS source (generated client ABI).

use serde::Deserialize;
use serde_json::Value as JsonValue;

use super::bind;

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum Operation {
    FindMany {
        entity: String,
        #[serde(rename = "where")]
        filter: Option<WhereEq>,
        #[allow(dead_code)]
        take: Option<u32>,
    },
    FindUnique {
        entity: String,
        #[serde(rename = "where")]
        filter: WhereEq,
    },
    DeclaredVos {
        source: String,
    },
}

#[derive(Debug, Deserialize)]
struct WhereEq {
    field: String,
    value: JsonValue,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct OperationIdentity {
    operation_id: String,
    contract_fingerprint: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OperationRequest {
    #[allow(dead_code)]
    identity: OperationIdentity,
    operation: Operation,
    parameters: Option<JsonValue>,
    #[allow(dead_code)]
    deadline_ms: Option<u64>,
}

/// Encode a request payload and bind its optional parameters before planning.
pub fn encode_request_json(json: &str) -> Result<bind::BoundParameters, String> {
    let request: OperationRequest = serde_json::from_str(json).map_err(|err| err.to_string())?;
    let source = encode_operation(&request.operation)?;
    match request.parameters.as_ref() {
        Some(value) => {
            let parameters_json = serde_json::to_string(value).map_err(|err| err.to_string())?;
            bind::bind_parameters_typed(&source, &parameters_json)
        }
        None => Ok(bind::BoundParameters::unbound(&source)),
    }
}

fn encode_operation(op: &Operation) -> Result<String, String> {
    match op {
        Operation::FindMany { entity, filter, take: _ } => {
            let pipeline = match filter {
                None => format!("{entity}.collect()"),
                Some(pred) => format!("{entity}.filter(x => x.{field}{cmp}).collect()", field = pred.field, cmp = cmp_suffix(&pred.value)?),
            };
            Ok(pipeline)
        }
        Operation::FindUnique { entity, filter } => {
            let pipeline = format!("{entity}.filter(x => x.{field}{cmp}).collect()", field = filter.field, cmp = cmp_suffix(&filter.value)?);
            Ok(pipeline)
        }
        Operation::DeclaredVos { source } => Ok(source.clone()),
    }
}

fn cmp_suffix(value: &JsonValue) -> Result<String, String> {
    match value {
        JsonValue::Bool(true) => Ok(String::new()),
        JsonValue::Bool(false) => Ok(".eq(false)".into()),
        JsonValue::Number(n) => Ok(format!(".eq({n})")),
        JsonValue::String(s) => Ok(format!(".eq(\"{s}\")")),
        JsonValue::Null => Ok(".eq(null)".into()),
        _ => Err("unsupported predicate value in operation ABI".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_vos_binds_parameters() {
        let request = r#"{
            "identity": { "operationId": "User.findMany", "contractFingerprint": "fp" },
            "operation": { "kind": "declared-vos", "source": "User.filter(x => x.active == $where_active).collect()" },
            "parameters": { "where_active": true }
        }"#;
        let bound = encode_request_json(request).expect("request");
        assert_eq!(bound.source, "User.filter(x => x.active == true).collect()");
        assert!(bound.value_parameters.is_empty());
    }

    #[test]
    fn find_unique_encodes_an_equality_filter() {
        let request = r#"{
            "identity": { "operationId": "User.findUnique", "contractFingerprint": "fp" },
            "operation": { "kind": "find-unique", "entity": "User", "where": { "field": "user_id", "value": "u1" } }
        }"#;
        let bound = encode_request_json(request).expect("request");
        assert_eq!(bound.source, r#"User.filter(x => x.user_id.eq("u1")).collect()"#);
    }
}
