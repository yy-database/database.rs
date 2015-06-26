//! Host session facade — profile dispatch, real SQLite execution, envelopes.

use odm_adapter::host::{
    AUTHOR_CODES, CODE_CONNECTION_FAILED, CODE_INVALID_ARGUMENT, CODE_UNIQUE_CONFLICT, DIAG_BACKEND_UNSUPPORTED, DIAG_SESSION_CLOSED,
    DIAG_TRANSACTION_ALREADY_OPEN, DIAG_TRANSACTION_NOT_OPEN, ErrorEnvelope, ExecuteOutcome, HostSession, version,
};

const USER_SCHEMA: &str = r#"
table User {
    @@user_id: utf8,
    @user_name: utf8,
    active: bool,
}
"#;

fn assert_author_code(error: &ErrorEnvelope) {
    assert!(AUTHOR_CODES.contains(&error.code.as_str()), "code `{}` is outside the author table", error.code);
}

#[test]
fn version_is_non_empty() {
    assert!(!version().is_empty());
}

#[test]
fn memory_profile_is_a_reference_session() {
    let mut session = HostSession::open_memory();
    let outcome = session.query("User.collect()", None);
    // The reference profile is unseeded, so a read either succeeds empty or
    // fails with a structured envelope — never an untyped host exception.
    if !outcome.ok {
        assert_author_code(outcome.error.as_ref().expect("error envelope"));
    }
    session.close();
    let closed = session.query("User.collect()", None);
    assert!(!closed.ok);
    assert_eq!(closed.error_diagnostic_code().as_deref(), Some(DIAG_SESSION_CLOSED));
}

#[test]
fn sqlite_session_executes_real_ddl_and_dml_through_the_facade() {
    let mut session = HostSession::open_sqlite(":memory:").expect("sqlite session");
    assert!(!session.sqlite_engine_version().expect("engine version").is_empty());
    session.managed_push(USER_SCHEMA).expect("managed push");

    let inserted = session.execute(r#"User::insert({ user_id: "u1", user_name: "Ada", active: true })"#, None);
    assert!(inserted.ok, "{:?}", inserted.error.as_ref().map(ErrorEnvelope::to_json));

    let rows = session.query("User.filter(x => x.active).collect()", None);
    assert!(rows.ok, "{:?}", rows.error.as_ref().map(ErrorEnvelope::to_json));
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0]["user_name"], "Ada");

    let duplicate = session.execute(r#"User::insert({ user_id: "u1", user_name: "Dup", active: false })"#, None);
    assert!(!duplicate.ok, "a duplicate primary key must fail");
    assert_eq!(duplicate.error_code().as_deref(), Some(CODE_UNIQUE_CONFLICT));

    session.close();
}

#[test]
fn sqlite_transactions_commit_and_roll_back() {
    let mut session = HostSession::open_sqlite(":memory:").expect("sqlite session");
    session.managed_push(USER_SCHEMA).expect("managed push");

    session.begin();
    session.execute(r#"User::insert({ user_id: "u2", user_name: "Grace", active: true })"#, None);
    let rollback = session.rollback();
    assert!(rollback.ok);
    assert_eq!(session.query("User.collect()", None).rows.len(), 0);

    session.begin();
    session.execute(r#"User::insert({ user_id: "u3", user_name: "Lin", active: true })"#, None);
    let commit = session.commit();
    assert!(commit.ok);
    assert_eq!(session.query("User.collect()", None).rows.len(), 1);
    session.close();
}

#[test]
fn foreign_sql_profiles_are_refused_at_open() {
    for (profile, opened) in [
        ("postgres", HostSession::open_postgres("postgres://127.0.0.1:5432/iris")),
        ("mysql", HostSession::open_mysql("mysql://127.0.0.1:3306/iris")),
    ] {
        let error = match opened {
            Ok(_) => panic!("{profile} must be refused instead of opening a half-dead session"),
            Err(error) => error,
        };
        assert_eq!(error.code, CODE_CONNECTION_FAILED, "{profile}");
        assert_eq!(error.diagnostic_code.as_deref(), Some(DIAG_BACKEND_UNSUPPORTED), "{profile}");
    }
}

#[test]
fn sqlite_transaction_state_is_reported_with_diagnostics() {
    let mut session = HostSession::open_sqlite(":memory:").expect("sqlite session");

    let commit_without_transaction = session.commit();
    assert!(!commit_without_transaction.ok);
    assert_eq!(commit_without_transaction.error_diagnostic_code().as_deref(), Some(DIAG_TRANSACTION_NOT_OPEN));

    assert!(session.begin().ok, "the first begin must open a transaction");
    let second_begin = session.begin();
    assert!(!second_begin.ok);
    assert_eq!(second_begin.error_diagnostic_code().as_deref(), Some(DIAG_TRANSACTION_ALREADY_OPEN));
    assert!(session.rollback().ok);
    session.close();
}

#[test]
fn open_dsn_applies_the_managed_schema() {
    let mut session = HostSession::open_dsn("sqlite::memory:", Some(USER_SCHEMA)).expect("dsn session");
    let inserted = session.execute(r#"User::insert({ user_id: "u4", user_name: "Mina", active: false })"#, None);
    assert!(inserted.ok, "{:?}", inserted.error.as_ref().map(ErrorEnvelope::to_json));
    assert_eq!(session.query("User.collect()", None).rows.len(), 1);
    session.close();
}

#[test]
fn bind_failures_use_the_argument_envelope() {
    let session = HostSession::open_sqlite(":memory:").expect("sqlite session");
    let outcome = session.query("User.filter(x => x.active == $missing).collect()", Some("{}"));
    assert!(!outcome.ok);
    assert_eq!(outcome.error_code().as_deref(), Some(CODE_INVALID_ARGUMENT));
    assert!(outcome.error_message().unwrap().contains("unbound"));
}

#[test]
fn unsupported_dsn_is_rejected_with_the_author_code() {
    let error = match HostSession::open_dsn("nope://x", None) {
        Ok(_) => panic!("unknown dsn must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.code, CODE_INVALID_ARGUMENT);
    assert_author_code(&error);
}

#[test]
fn outcome_json_keeps_the_envelope_shape() {
    let unit: ExecuteOutcome = ExecuteOutcome::unit();
    assert_eq!(unit.to_json(), r#"{"error":null,"ok":true,"rows":[]}"#);
}
