use yyds_gateway_redis::{handle_command, DEFAULT_PORT, GATEWAY_ID};

#[test]
fn redis_ping_returns_pong() {
    assert_eq!(handle_command("PING").expect("ping"), "PONG");
}

#[test]
fn redis_info_mentions_yyds() {
    let info = handle_command("INFO").expect("info");
    assert!(info.contains("redis_version:yyds-"));
    assert_eq!(GATEWAY_ID, "redis");
    assert_eq!(DEFAULT_PORT, 6379);
}
