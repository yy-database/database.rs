//! `DiagnosticEnvelope` stability: code, operation, retryability, and JSON shape.

use std::io;

use database_types::{DiagnosticEnvelope, Error};

/// Every Error variant roundtrips through DiagnosticEnvelope preserving stable fields.
#[test]
fn diagnostic_envelope_roundtrip_preserves_code_and_retryable() {
    let err_non_retryable = Error::UdfArity { name: "my_fn".into(), expected: 3, got: 5 };
    let envelope = DiagnosticEnvelope::from_error(&err_non_retryable);
    assert_eq!(envelope.code, "database.udf.arity");
    assert_eq!(envelope.operation, "udf");
    assert!(!envelope.retryable);
    let params = envelope.params.as_ref().expect("UdfArity must carry params");
    assert_eq!(params.get("name"), Some(&"my_fn".to_owned()));
    assert_eq!(params.get("expected"), Some(&"3".to_owned()));
    assert_eq!(params.get("got"), Some(&"5".to_owned()));

    let json = envelope.to_string();
    let parsed = DiagnosticEnvelope::from_string(&json).expect("roundtrip parse");
    assert_eq!(parsed, envelope);

    let err_retryable = Error::LeaseUnavailable { key: "lock".into() };
    let env2 = DiagnosticEnvelope::from_error(&err_retryable);
    assert_eq!(env2.code, "database.lease.unavailable");
    assert!(env2.retryable);
    let json2 = env2.to_string();
    let parsed2 = DiagnosticEnvelope::from_string(&json2).expect("roundtrip parse");
    assert_eq!(parsed2, env2);

    let err_io = Error::Io(io::Error::new(io::ErrorKind::PermissionDenied, "no permission"));
    let env3 = DiagnosticEnvelope::from_error(&err_io);
    assert_eq!(env3.code, "database.io");
    assert_eq!(env3.operation, "io");
    assert!(env3.retryable);
    assert!(env3.params.is_none(), "Io must not carry params");
}

/// `Error::to_json()` must produce exactly the DiagnosticEnvelope Value.
#[test]
fn to_json_matches_envelope() {
    let err = Error::Schema { message: "bad schema".into() };
    let via_method = err.to_json();
    let via_env = serde_json::to_value(DiagnosticEnvelope::from_error(&err)).unwrap();
    assert_eq!(via_method, via_env);
}
