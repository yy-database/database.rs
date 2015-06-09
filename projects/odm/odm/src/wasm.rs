//! Read-only schema introspection JSON (the `iris::wasm::introspect_schema_json` path).
//!
//! Hosts that cannot open a project from disk use this to preview the
//! generator's view of a schema source without writing any files.

use odm_generate::GenerationModel;
use serde_json::Value;

/// Read-only schema introspection JSON (`GenerationModel` shape).
pub fn introspect_schema_json(source: &str) -> String {
    match GenerationModel::from_vos_schema(source) {
        Ok(model) => model_to_introspect_json(&model),
        Err(err) => introspect_error_json(&err.to_string()),
    }
}

fn model_to_introspect_json(model: &GenerationModel) -> String {
    let value = serde_json::json!({
        "ok": true,
        "generatorVersion": model.generator_version,
        "schemaFingerprint": model.schema_fingerprint,
        "tables": model.tables.iter().map(|table| {
            serde_json::json!({
                "name": table.name,
                "rustType": table.rust_type,
                "fields": table.fields.iter().map(|field| {
                    let mut obj = serde_json::json!({
                        "name": field.name,
                        "rustTy": field.rust_ty,
                        "vosType": field.vos_type,
                        "primary": field.primary,
                        "optional": field.optional,
                    });
                    if let Some(target) = &field.reference_target {
                        obj.as_object_mut().expect("field json object").insert(
                            "referenceTarget".into(),
                            Value::String(target.clone()),
                        );
                    }
                    obj
                }).collect::<Vec<Value>>(),
            })
        }).collect::<Vec<Value>>(),
        "macros": model.macros.iter().map(|macro_def| {
            serde_json::json!({
                "name": macro_def.name,
                "returnType": macro_def.return_type,
                "params": macro_def.params.iter().map(|param| {
                    serde_json::json!({
                        "name": param.name,
                        "vosType": param.vos_type,
                        "tsType": param.ts_type,
                    })
                }).collect::<Vec<Value>>(),
            })
        }).collect::<Vec<Value>>(),
        "operations": model.operations.iter().map(|op| {
            let params_json: Vec<Value> = op.params.iter().map(|p| {
                serde_json::json!({ "name": p.name, "vosType": p.vos_type, "tsType": p.ts_type })
            }).collect();
            let ret = op.return_type.as_deref();
            serde_json::json!({
                "operationId": op.operation_id,
                "kind": op.kind,
                "name": op.name,
                "identityKey": op.identity_key,
                "params": params_json,
                "returnType": ret,
            })
        }).collect::<Vec<Value>>(),
        "error": Value::Null,
    });
    value.to_string()
}

fn introspect_error_json(err: &str) -> String {
    serde_json::json!({
        "ok": false,
        "generatorVersion": "",
        "schemaFingerprint": "",
        "tables": [],
        "macros": [],
        "operations": [],
        "error": err,
    })
    .to_string()
}
