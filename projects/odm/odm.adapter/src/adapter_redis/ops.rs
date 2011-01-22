//! Private Redis command helpers (KEY/SET/GET/DEL/TTL only — no SCAN).

use database_client::redis::Client;

use crate::{
    Error, Result,
    mapping::{KeyEncoding, KeyspaceMapping},
};

pub(crate) fn get_primary(conn: &mut Client, map: &KeyspaceMapping, primary_key: &str) -> Result<Option<String>> {
    let key = map.redis_key(primary_key);
    let value = conn.get(key.as_bytes()).map_err(Error::Wire)?;
    match value {
        None => Ok(None),
        Some(raw) => {
            let text = String::from_utf8(raw).map_err(|e| Error::Policy(format!("Redis value is not UTF-8: {e}")))?;
            validate_encoding(map.encoding, &text)?;
            Ok(Some(text))
        }
    }
}

pub(crate) fn put_primary(conn: &mut Client, map: &KeyspaceMapping, primary_key: &str, value: &str) -> Result<()> {
    validate_encoding(map.encoding, value)?;
    let key = map.redis_key(primary_key);
    match map.ttl_secs {
        Some(ttl) => conn.set_ex(key.as_bytes(), value.as_bytes(), ttl).map_err(Error::Wire)?,
        None => conn.set(key.as_bytes(), value.as_bytes()).map_err(Error::Wire)?,
    }
    Ok(())
}

pub(crate) fn put_primary_nx(conn: &mut Client, map: &KeyspaceMapping, primary_key: &str, value: &str) -> Result<bool> {
    validate_encoding(map.encoding, value)?;
    let key = map.redis_key(primary_key);
    match map.ttl_secs {
        Some(ttl) => conn.set_nx_ex(key.as_bytes(), value.as_bytes(), ttl).map_err(Error::Wire),
        None => conn.set_nx(key.as_bytes(), value.as_bytes()).map_err(Error::Wire),
    }
}

pub(crate) fn delete_primary(conn: &mut Client, map: &KeyspaceMapping, primary_key: &str) -> Result<bool> {
    let key = map.redis_key(primary_key);
    let n = conn.del(key.as_bytes()).map_err(Error::Wire)?;
    Ok(n > 0)
}

pub(crate) fn ttl_primary(conn: &mut Client, map: &KeyspaceMapping, primary_key: &str) -> Result<i64> {
    let key = map.redis_key(primary_key);
    conn.ttl(key.as_bytes()).map_err(Error::Wire)
}

fn validate_encoding(encoding: KeyEncoding, value: &str) -> Result<()> {
    match encoding {
        KeyEncoding::Utf8String => Ok(()),
        KeyEncoding::JsonDocument => {
            serde_json::from_str::<serde_json::Value>(value)
                .map_err(|e| Error::Policy(format!("JsonDocument encoding requires valid JSON: {e}")))?;
            Ok(())
        }
    }
}
