//! MySQL classic-protocol wire client (Iris / tooling entry).
//!
//! Server disguise lives in [`crate::connection`] / [`crate::service`]. This
//! module is the matching client surface so adapters never depend on the
//! third-party `mysql` crate.
//!
//! The protocol semantics live in `database-protocol-mysql`; this module owns
//! the TCP session, authentication and `COM_QUERY` result reading, and maps
//! wire errors into [`database_types::Error`].

use std::{
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use database_types::{Error, Result};

/// Password authentication: native SHA-1 and caching SHA-256.
mod auth;

/// Connection primitives: handshake and `COM_QUERY`.
#[allow(missing_docs)]
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

/// Result of a text-protocol statement.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueryResult {
    /// Column names when the server provided a result set.
    pub columns: Vec<String>,
    /// Row-major cells.
    pub rows: Vec<Vec<Cell>>,
    /// Rows affected for DML; `0` for pure queries.
    pub affected_rows: u64,
    /// Optional MySQL server error code when the statement failed at the server.
    pub server_code: Option<u16>,
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

    /// Run a text-protocol statement.
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        self.connection.query(sql)
    }

    /// Run a statement that should not return rows.
    pub fn execute(&mut self, sql: &str) -> Result<u64> {
        Ok(self.query(sql)?.affected_rows)
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
    let address = (host.as_str(), port)
        .to_socket_addrs()
        .map_err(Error::Io)?
        .next()
        .ok_or(Error::Corrupt("mysql URL host did not resolve"))?;
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
    let mut out = String::with_capacity(value.len());
    let mut bytes = value.as_bytes().iter().copied();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let hi = bytes.next();
            let lo = bytes.next();
            match (hi.and_then(|byte| hex(byte)), lo.and_then(|byte| hex(byte))) {
                (Some(hi), Some(lo)) => out.push(char::from(hi << 4 | lo)),
                _ => out.push('%'),
            }
        }
        else {
            out.push(char::from(byte));
        }
    }
    out
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
}
