//! PostgreSQL protocol-v3 wire client (Iris / tooling entry).
//!
//! Server disguise lives in [`crate::connection`] / [`crate::service`]. This
//! module is the matching client surface so adapters never depend on the
//! third-party `postgres` / `r2d2` crates.
//!
//! The protocol semantics live in `database-protocol-pgsql`; this module owns
//! the TCP session and maps wire errors into [`database_types::Error`].

use std::{
    io,
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use database_protocol_pgsql::PostgresError;
use database_types::{Error, Result};

/// PostgreSQL wire errors, shared with `database-protocol-pgsql`.
pub mod error;

/// Connection primitives: startup handshake, simple and extended query.
#[allow(missing_docs)]
pub mod client;

pub use client::{
    BackendKeyData, ClientConfig, Column, Connection, CopyInReport, CopyOutReport, Param, Portal, PortalReport, Prepared, Report,
};

/// One textual cell from a result set (bytes preserved when non-UTF-8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cell {
    /// SQL NULL.
    Null,
    /// Textual / binary payload as octets.
    Bytes(Vec<u8>),
}

/// Result of a simple-query statement.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueryResult {
    /// Column names when the server provided a result set.
    pub columns: Vec<String>,
    /// Row-major cells.
    pub rows: Vec<Vec<Cell>>,
    /// Rows affected for DML; `0` for pure queries.
    pub affected_rows: u64,
    /// Optional PostgreSQL SQLSTATE when the statement failed at the server.
    pub sqlstate: Option<String>,
}

/// Single PostgreSQL wire session (one TCP connection).
#[derive(Debug)]
pub struct Session {
    connection: Connection,
    address: SocketAddr,
}

impl Session {
    /// Open a TCP session from a `postgres://` URL or `host=…` libpq-style string.
    ///
    /// The startup handshake runs immediately, so an unauthenticated or
    /// unreachable server fails here instead of at the first query.
    pub fn connect(endpoint: &str, _timeout: Duration) -> Result<Self> {
        let (config, address) = parse_postgres_endpoint(endpoint)?;
        let connection = Connection::open(&config).map_err(map_wire_error)?;
        Ok(Self { connection, address })
    }

    /// Peer address used for this session.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Connectivity probe: run `SELECT 1` through the live session.
    pub fn ping(&mut self) -> Result<()> {
        self.connection.execute("SELECT 1").map_err(map_wire_error)?;
        Ok(())
    }

    /// Run a simple-query statement.
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let results = self.connection.execute(sql).map_err(map_wire_error)?;
        Ok(results.into_iter().last().map(convert_result).unwrap_or_default())
    }

    /// Run a statement that should not return rows.
    pub fn execute(&mut self, sql: &str) -> Result<u64> {
        Ok(self.query(sql)?.affected_rows)
    }
}

fn convert_result(result: client::QueryResult) -> QueryResult {
    let columns = result.columns.iter().map(|column| column.name.clone()).collect();
    let rows = result
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|value| value.map(|text| Cell::Bytes(text.into_bytes())).unwrap_or(Cell::Null)).collect())
        .collect();
    QueryResult { columns, rows, affected_rows: affected_rows(&result.tag), sqlstate: None }
}

fn affected_rows(tag: &str) -> u64 {
    if !tag.starts_with("INSERT") && !tag.starts_with("UPDATE") && !tag.starts_with("DELETE") && !tag.starts_with("COPY") {
        return 0;
    }
    tag.split_whitespace().last().and_then(|value| value.parse().ok()).unwrap_or(0)
}

fn map_wire_error(error: PostgresError) -> Error {
    match error {
        PostgresError::Io(message) => Error::Io(io::Error::other(message)),
        PostgresError::Server { code, message, .. } => Error::Protocol { message: format!("{code}: {message}") },
        PostgresError::Protocol(message) => Error::Protocol { message },
        PostgresError::Unsupported(feature) => Error::Protocol { message: format!("unsupported: {feature}") },
    }
}

fn parse_postgres_endpoint(endpoint: &str) -> Result<(ClientConfig, SocketAddr)> {
    if let Some(rest) = endpoint.strip_prefix("postgres://").or_else(|| endpoint.strip_prefix("postgresql://")) {
        let rest = rest.split('?').next().unwrap_or(rest);
        let (authority, database) = match rest.split_once('/') {
            Some((authority, database)) => (authority, database),
            None => (rest, ""),
        };
        let (userinfo, hostport) = match authority.rsplit_once('@') {
            Some((userinfo, hostport)) => (userinfo, hostport),
            None => ("", authority),
        };
        let user = userinfo.split(':').next().filter(|user| !user.is_empty()).unwrap_or("postgres");
        let (host, port) = split_host_port(hostport)?;
        let database = if database.is_empty() { user } else { database };
        let config = ClientConfig::new(host.clone(), port, user, database);
        let address = resolve(&host, port)?;
        return Ok((config, address));
    }

    let mut host = "127.0.0.1".to_owned();
    let mut port = 5432u16;
    let mut user = "postgres".to_owned();
    let mut database = String::new();
    for part in endpoint.split_whitespace() {
        if let Some(value) = part.strip_prefix("host=") {
            host = value.to_owned();
        }
        else if let Some(value) = part.strip_prefix("port=") {
            port = value.parse().map_err(|_| Error::Protocol { message: "postgres port is not a u16".into() })?;
        }
        else if let Some(value) = part.strip_prefix("user=") {
            user = value.to_owned();
        }
        else if let Some(value) = part.strip_prefix("dbname=") {
            database = value.to_owned();
        }
    }
    if database.is_empty() {
        database = user.clone();
    }
    let address = resolve(&host, port)?;
    Ok((ClientConfig::new(host, port, user, database), address))
}

fn split_host_port(hostport: &str) -> Result<(String, u16)> {
    match hostport.rsplit_once(':') {
        Some((host, port)) => {
            let port = port.parse().map_err(|_| Error::Protocol { message: "postgres port is not a u16".into() })?;
            Ok((host.to_owned(), port))
        }
        None if hostport.is_empty() => Ok(("127.0.0.1".into(), 5432)),
        None => Ok((hostport.to_owned(), 5432)),
    }
}

fn resolve(host: &str, port: u16) -> Result<SocketAddr> {
    format!("{host}:{port}")
        .to_socket_addrs()
        .map_err(Error::Io)?
        .next()
        .ok_or(Error::Protocol { message: "postgres host did not resolve".into() })
}
