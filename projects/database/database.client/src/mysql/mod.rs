//! MySQL classic-protocol wire client (Iris / tooling entry).
//!
//! Server disguise lives in [`crate::connection`] / [`crate::service`]. This
//! module is the matching client surface so adapters never depend on the
//! third-party `mysql` crate.
//!
//! Status: session + URL parse are live; handshake and `COM_QUERY` execution
//! are the next extraction from the server framing helpers in [`crate::wire`].

use std::{
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    time::Duration,
};

use database_protocol_mysql::MAX_PACKET_PAYLOAD;
use database_types::{Error, Result};

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
    stream: TcpStream,
    address: SocketAddr,
}

impl Session {
    /// Open a TCP session from a `mysql://user:pass@host:port/db` URL.
    ///
    /// Handshake / auth / `COM_QUERY` are not filled yet; [`Self::ping`] and
    /// query helpers return [`Error::Unsupported`] until that path lands.
    pub fn connect(url: &str, timeout: Duration) -> Result<Self> {
        let address = parse_mysql_url(url)?;
        let stream = TcpStream::connect_timeout(&address, timeout).map_err(Error::Io)?;
        stream.set_read_timeout(Some(timeout)).map_err(Error::Io)?;
        stream.set_write_timeout(Some(timeout)).map_err(Error::Io)?;
        let _ = MAX_PACKET_PAYLOAD;
        Ok(Self { stream, address })
    }

    /// Peer address used for this session.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Connectivity probe once handshake exists.
    pub fn ping(&mut self) -> Result<()> {
        let _ = &self.stream;
        Err(Error::Unsupported("MySQL wire client handshake / COM_QUERY not extracted yet (server side is yyds-gateway-mysql)"))
    }

    /// Run a text-protocol statement.
    pub fn query(&mut self, _sql: &str) -> Result<QueryResult> {
        self.ping()?;
        unreachable!()
    }

    /// Run a text-protocol statement that should not return rows.
    pub fn execute(&mut self, sql: &str) -> Result<u64> {
        Ok(self.query(sql)?.affected_rows)
    }
}

fn parse_mysql_url(url: &str) -> Result<SocketAddr> {
    let rest = url
        .strip_prefix("mysql://")
        .or_else(|| url.strip_prefix("mysql+tcp://"))
        .ok_or(Error::Corrupt("mysql URL must start with mysql://"))?;
    let hostport = rest.rsplit('@').next().unwrap_or(rest);
    let hostport = hostport.split('/').next().unwrap_or(hostport);
    let defaulted = if hostport.contains(':') { hostport.to_owned() } else { format!("{hostport}:3306") };
    defaulted.to_socket_addrs().map_err(Error::Io)?.next().ok_or(Error::Corrupt("mysql URL host did not resolve"))
}
