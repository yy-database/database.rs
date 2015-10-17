//! MySQL classic-protocol wire client.
//!
//! Server disguise lives in [`crate::connection`] / [`crate::service`]. This
//! module is the matching client surface so adapters never depend on the
//! third-party `mysql` crate.
//!
//! [`client::Connection`] owns the TCP session, authentication, `COM_QUERY`
//! text results and the `COM_STMT_PREPARE` / `COM_STMT_EXECUTE` parameterised
//! path with binary row decoding.

use std::{
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use database_types::{Error, Result};

/// Password authentication: native SHA-1 and caching SHA-256.
mod auth;

/// Connection primitives: handshake, `COM_QUERY` and prepared statements.
pub mod client;

pub use client::{ClientConfig, Connection};

/// One textual cell from a result set (bytes preserved when non-UTF-8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cell {
    /// SQL NULL.
    Null,
    /// Textual / binary payload as octets.
    Bytes(Vec<u8>),
}

/// Shape of one result-set column, as reported by the server.
///
/// Prepared-statement result rows are binary encoded, so the adapter needs the
/// declared type, the unsigned flag and the charset to restore a [`Cell`] into
/// a typed value instead of guessing from the payload text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    /// Column label (`SELECT … AS name` becomes `name`).
    pub name: String,
    /// `MYSQL_TYPE_*` code from the column definition.
    pub type_code: u8,
    /// Column flags (`UNSIGNED_FLAG` is `0x0020`, `BINARY_FLAG` is `0x0080`).
    pub flags: u16,
    /// Character set number, `63` meaning the binary collation.
    pub charset: u16,
    /// Declared display length (`TINYINT(1)` reports `1`).
    pub column_length: u32,
}

impl ColumnInfo {
    /// Whether the column is declared unsigned.
    pub fn unsigned(&self) -> bool {
        self.flags & 0x0020 != 0
    }

    /// Whether the column carries raw octets rather than text.
    pub fn binary(&self) -> bool {
        self.charset == 63
    }
}

/// Result of one statement (text or prepared protocol).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueryResult {
    /// Column names when the server provided a result set.
    pub columns: Vec<String>,
    /// Full column shape, parallel to [`Self::columns`].
    pub column_info: Vec<ColumnInfo>,
    /// Row-major cells.
    pub rows: Vec<Vec<Cell>>,
    /// Rows affected for DML; `0` for pure queries.
    pub affected_rows: u64,
    /// `AUTO_INCREMENT` value assigned by the last `INSERT`, when any.
    pub insert_id: u64,
}

/// One prepared-statement parameter.
///
/// Text parameters are declared as `MYSQL_TYPE_VAR_STRING` and octet payloads
/// as `MYSQL_TYPE_BLOB`, so the server performs the coercion against the target
/// column instead of the adapter concatenating literals into the statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Param<'a> {
    /// UTF-8 text; `None` binds SQL `NULL`.
    Text(Option<&'a str>),
    /// Raw octets (`BINARY(16)` uuids, `BLOB`/`VARBINARY` payloads).
    Binary(&'a [u8]),
}

impl<'a> Param<'a> {
    /// Bind an optional text parameter.
    pub fn text(value: Option<&'a str>) -> Self {
        Self::Text(value)
    }

    /// Bind a raw octet parameter.
    pub fn binary(value: &'a [u8]) -> Self {
        Self::Binary(value)
    }

    fn is_null(&self) -> bool {
        matches!(self, Self::Text(None))
    }

    fn type_code(&self) -> u8 {
        match self {
            Self::Text(None) => mysql_type::NULL,
            Self::Text(Some(_)) => mysql_type::VAR_STRING,
            Self::Binary(_) => mysql_type::BLOB,
        }
    }
}

/// `MYSQL_TYPE_*` wire codes shared with the prepared-statement encoder.
pub mod mysql_type {
    #![allow(missing_docs)]
    pub const DECIMAL: u8 = 0x00;
    pub const TINY: u8 = 0x01;
    pub const SHORT: u8 = 0x02;
    pub const LONG: u8 = 0x03;
    pub const FLOAT: u8 = 0x04;
    pub const DOUBLE: u8 = 0x05;
    pub const NULL: u8 = 0x06;
    pub const TIMESTAMP: u8 = 0x07;
    pub const LONGLONG: u8 = 0x08;
    pub const INT24: u8 = 0x09;
    pub const DATE: u8 = 0x0a;
    pub const TIME: u8 = 0x0b;
    pub const DATETIME: u8 = 0x0c;
    pub const YEAR: u8 = 0x0d;
    pub const NEWDATE: u8 = 0x0e;
    pub const VARCHAR: u8 = 0x0f;
    pub const BIT: u8 = 0x10;
    pub const TIMESTAMP2: u8 = 0x11;
    pub const DATETIME2: u8 = 0x12;
    pub const TIME2: u8 = 0x13;
    pub const JSON: u8 = 0xf5;
    pub const NEWDECIMAL: u8 = 0xf6;
    pub const ENUM: u8 = 0xf7;
    pub const SET: u8 = 0xf8;
    pub const TINY_BLOB: u8 = 0xf9;
    pub const MEDIUM_BLOB: u8 = 0xfa;
    pub const LONG_BLOB: u8 = 0xfb;
    pub const BLOB: u8 = 0xfc;
    pub const VAR_STRING: u8 = 0xfd;
    pub const STRING: u8 = 0xfe;
    pub const GEOMETRY: u8 = 0xff;
}

/// Single MySQL wire session (one TCP connection).
#[derive(Debug)]
pub struct Session {
    connection: Connection,
    address: SocketAddr,
}

impl Session {
    /// Open a TCP session from a `mysql://user:pass@host:port/db` URL.
    ///
    /// The startup handshake and authentication run immediately, so a wrong
    /// password or an unreachable server fails here instead of at the first
    /// query.
    pub fn connect(url: &str, timeout: Duration) -> Result<Self> {
        let (mut config, address) = parse_mysql_url(url)?;
        config.timeout = timeout;
        let connection = Connection::open(&config)?;
        Ok(Self { connection, address })
    }

    /// Peer address used for this session.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Connectivity probe: run `SELECT 1` through the live session.
    pub fn ping(&mut self) -> Result<()> {
        self.query("SELECT 1")?;
        Ok(())
    }

    /// Run a text-protocol statement (`COM_QUERY`).
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        self.connection.query(sql)
    }

    /// Run a statement that should not return rows.
    pub fn execute(&mut self, sql: &str) -> Result<u64> {
        Ok(self.query(sql)?.affected_rows)
    }

    /// Run a parameterised statement through `COM_STMT_PREPARE` +
    /// `COM_STMT_EXECUTE`.
    ///
    /// Values travel as parameters, never concatenated into the SQL. Binary
    /// result rows are decoded against the column definitions the server
    /// returns, and [`QueryResult::column_info`] keeps the declared type for the
    /// adapter's value restoration.
    pub fn query_with_params(&mut self, sql: &str, params: &[Param<'_>]) -> Result<QueryResult> {
        self.connection.query_with_params(sql, params)
    }

    /// Run a parameterised statement and report only the affected row count.
    pub fn execute_with_params(&mut self, sql: &str, params: &[Param<'_>]) -> Result<u64> {
        Ok(self.query_with_params(sql, params)?.affected_rows)
    }
}

fn parse_mysql_url(url: &str) -> Result<(ClientConfig, SocketAddr)> {
    let rest = url
        .strip_prefix("mysql://")
        .or_else(|| url.strip_prefix("mysql+tcp://"))
        .ok_or(Error::Corrupt("mysql URL must start with mysql://"))?;
    let rest = rest.split('?').next().unwrap_or(rest);
    let (authority, database) = match rest.split_once('/') {
        Some((authority, database)) => (authority, Some(database)),
        None => (rest, None),
    };
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((userinfo, hostport)) => (Some(userinfo), hostport),
        None => (None, authority),
    };
    let (user, password) = match userinfo {
        Some(userinfo) => match userinfo.split_once(':') {
            Some((user, password)) => (decode(user), Some(decode(password))),
            None => (decode(userinfo), None),
        },
        None => ("root".to_owned(), None),
    };
    let (host, port) = split_host_port(hostport)?;
    let database = database.map(decode).filter(|database| !database.is_empty());
    let address = (host.as_str(), port).to_socket_addrs().map_err(Error::Io)?.next().ok_or(Error::Corrupt("mysql URL host did not resolve"))?;
    let user = if user.is_empty() { "root".to_owned() } else { user };
    let mut config = ClientConfig::new(host, port, user);
    if let Some(password) = password {
        config = config.with_password(password);
    }
    if let Some(database) = database {
        config = config.with_database(database);
    }
    Ok((config, address))
}

fn split_host_port(hostport: &str) -> Result<(String, u16)> {
    match hostport.rsplit_once(':') {
        Some((host, port)) => {
            let port = port.parse().map_err(|_| Error::Corrupt("mysql URL port is not a u16"))?;
            Ok((host.to_owned(), port))
        }
        None if hostport.is_empty() => Ok(("127.0.0.1".into(), 3306)),
        None => Ok((hostport.to_owned(), 3306)),
    }
}

fn decode(value: &str) -> String {
    let mut out = Vec::with_capacity(value.len());
    let mut bytes = value.as_bytes().iter().copied();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            match (bytes.next().and_then(hex), bytes.next().and_then(hex)) {
                (Some(hi), Some(lo)) => out.push(hi << 4 | lo),
                _ => out.push(b'%'),
            }
        }
        else {
            out.push(byte);
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_url_splits_user_password_host_and_database() {
        let (config, address) = parse_mysql_url("mysql://alice:s%40cret@127.0.0.1:3307/app").expect("url");
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 3307);
        assert_eq!(config.user, "alice");
        assert_eq!(config.password.as_deref(), Some("s@cret"));
        assert_eq!(config.database.as_deref(), Some("app"));
        assert_eq!(address.ip().to_string(), "127.0.0.1");
    }

    #[test]
    fn a_url_without_credentials_uses_root_and_no_database() {
        let (config, _) = parse_mysql_url("mysql://127.0.0.1").expect("url");
        assert_eq!(config.user, "root");
        assert!(config.password.is_none());
        assert!(config.database.is_none());
    }

    #[test]
    fn a_non_mysql_scheme_is_rejected() {
        assert!(parse_mysql_url("postgres://127.0.0.1/db").is_err());
    }

    #[test]
    fn parameter_types_follow_the_payload_shape() {
        assert_eq!(Param::text(Some("x")).type_code(), mysql_type::VAR_STRING);
        assert_eq!(Param::binary(&[0]).type_code(), mysql_type::BLOB);
        assert_eq!(Param::text(None).type_code(), mysql_type::NULL);
        assert!(Param::text(None).is_null());
        assert!(!Param::text(Some("")).is_null());
    }

    #[test]
    fn column_info_reads_the_unsigned_and_binary_flags() {
        let column = ColumnInfo { name: "c".into(), type_code: mysql_type::LONGLONG, flags: 0x0020, charset: 63, column_length: 20 };
        assert!(column.unsigned());
        assert!(column.binary());
    }
}
