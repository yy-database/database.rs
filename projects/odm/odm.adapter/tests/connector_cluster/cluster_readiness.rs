//! Phase 5: YYDS connector readiness + SQL-gateway refusal.

use odm_adapter::connector_cluster::{BACKEND_ID, ClusterSessionContext, ClusterSource, FORBIDDEN_LEGACY_SURFACES, READINESS_CODE};

#[test]
fn readiness_probe_is_not_ready_and_stable() {
    let report = ClusterSource::readiness();
    assert_eq!(report.backend_id, BACKEND_ID);
    assert_eq!(report.code, READINESS_CODE);
    assert!(!report.is_ready());
    assert!(!report.vos_executor_ready);
    assert!(!report.catalog_lifecycle_ready);
    assert!(!report.control_plane_context_ready);
    assert!(!report.forbidden_legacy_surfaces.is_empty());
}

#[test]
fn connect_refuses_until_ready() {
    let err = ClusterSource::connect("yyds://127.0.0.1:9000", ClusterSessionContext::default()).expect_err("must not connect");
    let msg = err.to_string();
    assert!(msg.contains(READINESS_CODE), "{msg}");
}

#[test]
fn rejects_legacy_sql_gateway_surfaces() {
    for surface in ["yyds-gateway/src/sql", "we-trust-mysql", "yyds-odbc", "query_with_sql"] {
        let err = ClusterSource::reject_legacy_surface(surface).expect_err(surface);
        assert!(matches!(err, odm_adapter::connector_cluster::Error::ForbiddenLegacy(_)), "{surface} => {err}");
    }
    ClusterSource::reject_legacy_surface("formal-vos-protocol").expect("neutral name must pass");
}

#[test]
fn connector_does_not_depend_on_sql_or_yydb_crates() {
    let manifest = include_str!("../Cargo.toml");
    assert_eq!(BACKEND_ID, "yyds");
    for banned in ["yydb", "rusqlite", "postgres", "mysql", "sqlx", "oak-sql", "yyds-odbc", "we-trust-sqlite"] {
        assert!(!manifest.contains(banned), "iris-connector-yyds must not depend on `{banned}` while gated");
    }
    assert!(manifest.contains("serde"));
    assert!(!FORBIDDEN_LEGACY_SURFACES.is_empty());
}

#[test]
fn target_capabilities_advertise_yyds_not_yydb() {
    let caps = ClusterSource::target_capabilities();
    assert_eq!(caps.backend_id, "yyds");
    assert_ne!(caps.backend_id, "yydb");
    assert_ne!(caps.backend_id, "reference");
}
