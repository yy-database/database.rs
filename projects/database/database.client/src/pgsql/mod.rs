//! PostgreSQL protocol-v3 wire client (Iris / tooling entry).
//!
//! Server disguise lives in [`crate::connection`] / [`crate::service`]. This
//! module is the matching client surface so adapters never depend on the
//! third-party `postgres` / `r2d2` crates.
//!
//! Status: session + endpoint parse are live; startup / simple-query execution
//! are the next extraction from [`crate::wire`].

use std::{
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    time::Duration,
};

use database_types::{Error, Result};

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
    stream: TcpStream,
    address: SocketAddr,
}

impl Session {
    /// Open a TCP session from a `postgres://` URL or `host=…` libpq-style string.
    ///
    /// Startup / auth / simple query are not filled yet; [`Self::ping`] and
    /// query helpers return [`Error::Unsupported`] until that path lands.
    pub fn connect(endpoint: &str, timeout: Duration) -> Result<Self> {
        let address = parse_postgres_endpoint(endpoint)?;
        let stream = TcpStream::connect_timeout(&address, timeout).map_err(Error::Io)?;
        stream.set_read_timeout(Some(timeout)).map_err(Error::Io)?;
        stream.set_write_timeout(Some(timeout)).map_err(Error::Io)?;
        Ok(Self { stream, address })
    }

    /// Peer address used for this session.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Connectivity probe once startup exists.
    pub fn ping(&mut self) -> Result<()> {
        let _ = &self.stream;
        Err(Error::Unsupported("PostgreSQL wire client startup / simple query not extracted yet (server side is yyds-gateway-pgsql)"))
    }

    /// Run a simple-query statement.
    pub fn query(&mut self, _sql: &str) -> Result<QueryResult> {
        self.ping()?;
        unreachable!()
    }

    /// Run a statement that should not return rows.
    pub fn execute(&mut self, sql: &str) -> Result<u64> {
        Ok(self.query(sql)?.affected_rows)
    }
}

fn parse_postgres_endpoint(endpoint: &str) -> Result<SocketAddr> {
    if let Some(rest) = endpoint.strip_prefix("postgres://").or_else(|| endpoint.strip_prefix("postgresql://")) {
        let hostport = rest.rsplit('@').next().unwrap_or(rest);
        let hostport = hostport.split('/').next().unwrap_or(hostport).split('?').next().unwrap_or(hostport);
        let defaulted = if hostport.contains(':') { hostport.to_owned() } else { format!("{hostport}:5432") };
        return defaulted.to_socket_addrs().map_err(Error::Io)?.next().ok_or(Error::Corrupt("postgres URL host did not resolve"));
    }

    let mut host = "127.0.0.1";
    let mut port = 5432u16;
    for part in endpoint.split_whitespace() {
        if let Some(value) = part.strip_prefix("host=") {
            host = value;
        }
        else if let Some(value) = part.strip_prefix("port=") {
            port = value.parse().map_err(|_| Error::Corrupt("postgres port is not a u16"))?;
        }
    }
    format!("{host}:{port}").to_socket_addrs().map_err(Error::Io)?.next().ok_or(Error::Corrupt("postgres host did not resolve"))
}
