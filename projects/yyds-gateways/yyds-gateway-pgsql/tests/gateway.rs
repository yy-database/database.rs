use yyds_gateway_pgsql::{handle_input, DEFAULT_PORT, GATEWAY_ID};

#[test]
fn pgsql_select_one() {
    let lines = handle_input("SELECT 1").expect("select");
    assert_eq!(lines.last().map(String::as_str), Some("(1 row)"));
}

#[test]
fn pgsql_conninfo() {
    let lines = handle_input("\\conninfo").expect("conninfo");
    assert!(lines[0].contains("database \"yyds\""));
    assert_eq!(GATEWAY_ID, "pgsql");
    assert_eq!(DEFAULT_PORT, 5432);
}
