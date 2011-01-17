use yyds_gateway_mysql::{DEFAULT_PORT, GATEWAY_ID, GATEWAY_KIND, startup_banner};

#[test]
fn mysql_gateway_metadata_is_stable() {
    assert_eq!(GATEWAY_ID, "mysql");
    assert_eq!(DEFAULT_PORT, 3306);
    assert_eq!(GATEWAY_KIND.id(), GATEWAY_ID);
    assert!(startup_banner(DEFAULT_PORT).contains("3306"));
}
