//! Dynamic validation of the YYDB-only measurement harness at a small scale.
//!
//! These tests assert that every timing is positive. They deliberately assert
//! **no absolute timing threshold**: numbers drift across machines and builds,
//! and a benchmark must never turn an environment-dependent value into a
//! pass/fail gate.

use std::time::{SystemTime, UNIX_EPOCH};

use yydb_bench::{BenchConfig, run};

fn small_config() -> BenchConfig {
    BenchConfig {
        rows: 400,
        point_ops: 200,
        range_scans: 8,
        range_len: 25,
        batch_size: 50,
        single_inserts: 20,
        value_bytes: 32,
        measure_rounds: 7,
    }
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("yydb-bench-{label}-{nonce}"))
}

#[test]
fn harness_runs_yydb_with_positive_timings() {
    let dir = temp_dir("positive");
    let baseline = run(&dir, &small_config()).expect("harness runs yydb");

    let report = &baseline.yydb;
    assert!(report.batch_load_ns > 0, "yydb batch load must measure");
    assert!(report.single_insert_ns_per_op > 0, "yydb single insert must measure");
    assert!(report.point_get_ns_per_op > 0, "yydb cold point get must measure");
    assert!(report.point_get_warm_ns_per_op > 0, "yydb warm point get must measure");
    assert!(report.range_scan_ns_per_row > 0, "yydb range scan must measure");
    assert!(report.reopen_point_get_ns > 0, "yydb reopen must measure");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn baseline_serializes_with_scale_and_engine_report() {
    let dir = temp_dir("serialize");
    let baseline = run(&dir, &small_config()).expect("harness runs");
    let json = serde_json::to_value(&baseline).expect("baseline serializes");
    assert_eq!(json["config"]["rows"], 400);
    assert_eq!(json["yydb"]["engine"], "yydb");
    assert!(json.get("sqlite").is_none(), "sqlite comparison must stay retired");
    assert!(json["os"].is_string());
    let _ = std::fs::remove_dir_all(&dir);
}
