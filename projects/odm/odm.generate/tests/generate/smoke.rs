//! Generator smoke: one artifact parse and one emit per target language.
//!
//! Generated TypeScript and Python clients are compiled and executed by the
//! `@voml/vos` and `@yydb/yydb` conformance suites, so this file only proves
//! the generator wires each target up and writes its documented layout.

use std::path::PathBuf;

use odm_generate::{GenerationModel, IrisOperationArtifact, write_python_client, write_typescript_client};

const USER_SCHEMA: &str = r#"
table User {
    @@user_id: uuid,
    user_name: utf8,
    active: bool,
}
"#;

const MINIMAL_OP_ARTIFACT: &str = r#"{
        "envelope": {
            "stage": "operation-syntax-projection",
            "contractFormatVersion": "vos-operation-contract-v0",
            "languageVersion": "vos-language-v0",
            "canonicalizationVersion": "vos-canonical-v0",
            "schemaFingerprint": null,
            "sourceUnits": [
                { "sourceUnitId": 0, "source": "query get_user(id: uuid) -> User?" }
            ]
        },
        "operations": [
            {
                "kind": "query",
                "name": "get_user",
                "identityKey": "query:get_user(id:uuid)->User?",
                "operationId": "op_0123456789abcdef",
                "parameters": [
                    { "name": "id", "typeExpr": { "named": ["uuid"] }, "span": { "start": 15, "end": 23 } }
                ],
                "returnType": { "optional": { "named": ["User"] } },
                "signature": { "text": "get_user(id: uuid) -> User?", "span": { "start": 6, "end": 32 } },
                "body": null,
                "span": { "start": 0, "end": 50 }
            }
        ]
    }"#;

fn scratch(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("iris-gen-{label}-{nanos}"))
}

#[test]
fn operation_artifact_parses_minimal_envelope() {
    let artifact = IrisOperationArtifact::from_json(MINIMAL_OP_ARTIFACT).expect("minimal artifact");
    assert_eq!(artifact.len(), 1);
    assert_eq!(artifact.operations[0].name, "get_user");
    assert_eq!(artifact.operations[0].kind, vos::OperationKind::Query);
}

#[test]
fn operation_artifact_rejects_wrong_stage() {
    let bad = MINIMAL_OP_ARTIFACT.replace("operation-syntax-projection", "wrong-stage");
    let error = IrisOperationArtifact::from_json(&bad).expect_err("wrong stage");
    assert!(error.to_string().contains("OPR011"), "{error}");
}

#[test]
fn typescript_emit_writes_client_layout() {
    let model = GenerationModel::from_vos_schema(USER_SCHEMA).expect("schema");
    let dir = scratch("ts");
    let root = dir.join("generated/iris");
    let paths = write_typescript_client(&model, &root).expect("typescript emit");
    assert_eq!(paths.len(), 12);
    assert!(root.join("index.ts").is_file());
    assert!(root.join("models.ts").is_file());
    assert!(root.join("_internal/contract-types.ts").is_file());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn python_emit_writes_client() {
    let model = GenerationModel::from_vos_schema(USER_SCHEMA).expect("schema");
    let dir = scratch("py");
    let paths = write_python_client(&model, &dir).expect("python emit");
    assert!(!paths.is_empty());
    assert!(dir.join("models.py").is_file());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn rust_emit_writes_domain() {
    let model = GenerationModel::from_vos_schema(USER_SCHEMA).expect("schema");
    let dir = scratch("rs");
    let paths = odm_generate::generate(&model, "rust", &dir, Some("yydb")).expect("rust emit");
    assert!(!paths.is_empty());
    let _ = std::fs::remove_dir_all(dir);
}
