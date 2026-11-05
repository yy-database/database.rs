//! 从 `src/lib.rs` 迁出的测试。

use odm_adapter::{Error, adapter_redis::parse_redis_url};
use std::time::Duration;

#[test]
fn parse_redis_url_refuses_tls_scheme() {
    let err = parse_redis_url("rediss://127.0.0.1:6379").unwrap_err();
    assert!(matches!(err, Error::Policy(_)), "expected a policy error, got {err:?}");
    assert!(err.to_string().contains("TLS"), "error should name the missing TLS capability: {err}");
}

#[test]
fn parse_redis_url_refuses_url_credentials() {
    for url in ["redis://:secret@127.0.0.1:6379", "redis://user:secret@127.0.0.1:6379"] {
        let err = parse_redis_url(url).unwrap_err();
        assert!(matches!(err, Error::Policy(_)), "expected a policy error for {url}, got {err:?}");
        assert!(err.to_string().contains("credentials"), "error should name the dropped credentials: {err}");
    }
}

#[test]
fn parse_redis_url_accepts_plain_host_and_port() {
    let (address, timeout) = parse_redis_url("redis://127.0.0.1:6379").unwrap();
    assert_eq!(address.port(), 6379);
    assert_eq!(timeout, Duration::from_secs(5));

    let (address, _) = parse_redis_url("127.0.0.1").unwrap();
    assert_eq!(address.port(), 6379);
}
