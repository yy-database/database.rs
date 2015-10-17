//! MySQL classic-protocol connection: handshake, authentication and
//! `COM_QUERY` result reading.
//!
//! Framing comes from `database-protocol-mysql`; this module owns the session
//! state machine so adapters never depend on a third-party `mysql` crate.

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    time::Duration,
};

use database_protocol_mysql::MAX_PACKET_PAYLOAD;
use database_types::{Error, Result};

use super::{Cell, ColumnInfo, Param, QueryResult, auth, mysql_type};

/// Client-side connection settings resolved from the endpoint string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// Server host name or address.
    pub host: String,
    /// Server TCP port.
    pub port: u16,
    /// Authenticating user.
    pub user: String,
    /// Password, when the endpoint carried one.
    pub password: Option<String>,
    /// Default schema selected by `COM_INIT_DB`-style connect-with-db.
    pub database: Option<String>,
    /// Connect and per-read timeout.
    pub timeout: Duration,
}

impl ClientConfig {
    /// Build a configuration with the default five-second timeout.
    pub fn new(host: impl Into<String>, port: u16, user: impl Into<String>) -> Self {
        Self { host: host.into(), port, user: user.into(), password: None, database: None, timeout: Duration::from_secs(5) }
    }

    /// Attach a password.
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Attach a default schema.
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Attach a connect/read timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

// Capability flags used in `HandshakeResponse41`.
const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;

/// `COM_STMT_PREPARE`.
const COM_STMT_PREPARE: u8 = 0x16;
/// `COM_STMT_EXECUTE`.
const COM_STMT_EXECUTE: u8 = 0x17;
/// `COM_STMT_CLOSE` (no server response).
const COM_STMT_CLOSE: u8 = 0x19;

/// `utf8mb4_general_ci`.
const UTF8MB4_GENERAL_CI: u8 = 45;
/// Advertised maximum packet size in the handshake response.
const MAX_PACKET_SIZE: u32 = 0x0100_0000;

/// One classic-protocol connection with a strictly incrementing packet
/// sequence.
#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    next_sequence: u8,
}

impl Connection {
    /// Resolve the address and complete the startup handshake.
    pub fn open(config: &ClientConfig) -> Result<Self> {
        let address = resolve(&config.host, config.port)?;
        let stream = TcpStream::connect_timeout(&address, config.timeout).map_err(Error::Io)?;
        stream.set_read_timeout(Some(config.timeout)).map_err(Error::Io)?;
        stream.set_write_timeout(Some(config.timeout)).map_err(Error::Io)?;
        let mut connection = Self { stream, next_sequence: 0 };
        connection.handshake(config)?;
        Ok(connection)
    }

    /// Run one `COM_QUERY` statement.
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        self.next_sequence = 0;
        let mut payload = Vec::with_capacity(sql.len() + 1);
        payload.push(0x03);
        payload.extend_from_slice(sql.as_bytes());
        self.write_packet(&payload)?;
        self.read_result(false)
    }

    /// Run one parameterised statement: prepare, execute, close.
    ///
    /// The statement is prepared per call so the adapter never has to hold
    /// server-side handles, and closing runs even when execution failed.
    pub fn query_with_params(&mut self, sql: &str, params: &[Param<'_>]) -> Result<QueryResult> {
        let prepared = self.prepare(sql)?;
        if params.len() != usize::from(prepared.parameter_count) {
            let _ = self.close_prepared(prepared.id);
            return Err(Error::Protocol {
                message: format!("MySQL prepared statement expects {} parameters but {} were bound", prepared.parameter_count, params.len()),
            });
        }
        let result = self.execute_prepared(prepared.id, params);
        let closed = self.close_prepared(prepared.id);
        result.and_then(|result| closed.map(|_| result))
    }

    /// `COM_STMT_PREPARE`: consume the parameter and column definitions so the
    /// connection is positioned for `COM_STMT_EXECUTE`.
    fn prepare(&mut self, sql: &str) -> Result<PreparedStatement> {
        self.next_sequence = 0;
        let mut payload = Vec::with_capacity(sql.len() + 1);
        payload.push(COM_STMT_PREPARE);
        payload.extend_from_slice(sql.as_bytes());
        self.write_packet(&payload)?;

        let response = self.read_packet()?;
        match response.first().copied() {
            Some(0xff) => return Err(server_error(&response)),
            Some(0x00) => {}
            _ => return Err(Error::Protocol { message: "MySQL prepare response is not an OK packet".into() }),
        }
        let mut offset = 1;
        let id = read_u32(&response, &mut offset)?;
        let column_count = read_u16(&response, &mut offset)?;
        let parameter_count = read_u16(&response, &mut offset)?;

        for _ in 0..parameter_count {
            self.read_packet()?;
        }
        if parameter_count > 0 && !is_eof(&self.read_packet()?) {
            return Err(Error::Protocol { message: "MySQL parameter definitions did not end with EOF".into() });
        }
        for _ in 0..column_count {
            self.read_packet()?;
        }
        if column_count > 0 && !is_eof(&self.read_packet()?) {
            return Err(Error::Protocol { message: "MySQL prepared column definitions did not end with EOF".into() });
        }
        Ok(PreparedStatement { id, parameter_count })
    }

    /// `COM_STMT_EXECUTE` with the parameter block bound once.
    fn execute_prepared(&mut self, id: u32, params: &[Param<'_>]) -> Result<QueryResult> {
        self.next_sequence = 0;
        let mut payload = Vec::with_capacity(16 + params.len() * 8);
        payload.push(COM_STMT_EXECUTE);
        payload.extend_from_slice(&id.to_le_bytes());
        payload.push(0); // no cursor
        payload.extend_from_slice(&1u32.to_le_bytes()); // iteration count

        if !params.is_empty() {
            let bitmap_len = params.len().div_ceil(8);
            let mut null_bitmap = vec![0u8; bitmap_len];
            for (index, param) in params.iter().enumerate() {
                if param.is_null() {
                    null_bitmap[index / 8] |= 1 << (index % 8);
                }
            }
            payload.extend_from_slice(&null_bitmap);
            payload.push(1); // parameter types are bound in this packet
            for param in params {
                payload.push(param.type_code());
                payload.push(0); // not unsigned
            }
            for param in params {
                match param {
                    Param::Text(None) => {}
                    Param::Text(Some(value)) => {
                        push_lenenc(&mut payload, value.len());
                        payload.extend_from_slice(value.as_bytes());
                    }
                    Param::Binary(value) => {
                        push_lenenc(&mut payload, value.len());
                        payload.extend_from_slice(value);
                    }
                }
            }
        }

        self.write_packet(&payload)?;
        self.read_result(true)
    }

    /// `COM_STMT_CLOSE`; the server sends no reply.
    fn close_prepared(&mut self, id: u32) -> Result<()> {
        self.next_sequence = 0;
        let mut payload = Vec::with_capacity(5);
        payload.push(COM_STMT_CLOSE);
        payload.extend_from_slice(&id.to_le_bytes());
        let result = self.write_packet(&payload);
        // A closed connection cannot be recovered into a reusable session.
        if result.is_err() {
            return result;
        }
        Ok(())
    }

    /// Send the startup response and drive authentication to completion.
    fn handshake(&mut self, config: &ClientConfig) -> Result<()> {
        let greeting = self.read_packet()?;
        let handshake = Handshake::parse(&greeting)?;

        let mut capabilities =
            CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG | CLIENT_PROTOCOL_41 | CLIENT_TRANSACTIONS | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        if config.database.is_some() {
            capabilities |= CLIENT_CONNECT_WITH_DB;
        }
        let capabilities = capabilities & handshake.capabilities;
        if capabilities & CLIENT_PROTOCOL_41 == 0 || capabilities & CLIENT_SECURE_CONNECTION == 0 {
            return Err(Error::Protocol { message: "MySQL server does not offer protocol 4.1 secure framing".into() });
        }

        let plugin = auth::Plugin::parse(&handshake.plugin)?;
        let password = config.password.as_deref().unwrap_or_default();
        let response = if config.password.is_some() { plugin.scramble(password, &handshake.nonce) } else { Vec::new() };
        let mut payload = Vec::with_capacity(64 + response.len());
        payload.extend_from_slice(&capabilities.to_le_bytes());
        payload.extend_from_slice(&MAX_PACKET_SIZE.to_le_bytes());
        payload.push(UTF8MB4_GENERAL_CI);
        payload.resize(32, 0);
        payload.extend_from_slice(config.user.as_bytes());
        payload.push(0);
        payload.push(response.len() as u8);
        payload.extend_from_slice(&response);
        if capabilities & CLIENT_CONNECT_WITH_DB != 0 {
            if let Some(database) = &config.database {
                payload.extend_from_slice(database.as_bytes());
                payload.push(0);
            }
        }
        if capabilities & CLIENT_PLUGIN_AUTH != 0 {
            payload.extend_from_slice(handshake.plugin.as_bytes());
            payload.push(0);
        }
        self.write_packet(&payload)?;

        let mut nonce = handshake.nonce;
        let mut plugin = plugin;
        loop {
            let packet = self.read_packet()?;
            match packet.first().copied() {
                Some(0x00) => return Ok(()),
                Some(0xff) => return Err(server_error(&packet)),
                Some(0xfe) => {
                    // AuthSwitchRequest: plugin name NUL then a fresh nonce.
                    let (name, switched_nonce) = split_switch_request(&packet[1..])?;
                    plugin = auth::Plugin::parse(&name)?;
                    nonce = switched_nonce.to_vec();
                    let response = plugin.scramble(password, &nonce);
                    self.write_packet(&response)?;
                }
                Some(0x01) => {
                    // AuthMoreData. caching_sha2_password uses 0x03 for a
                    // cached fast-auth success and 0x04 for full auth.
                    match packet.get(1).copied() {
                        Some(0x03) => {}
                        Some(0x04) => {
                            if matches!(plugin, auth::Plugin::Native) {
                                return Err(Error::Protocol { message: "mysql_native_password sent full-auth data".into() });
                            }
                            self.write_packet(&[0x02])?;
                            let key_packet = self.read_packet()?;
                            if key_packet.first() != Some(&0x01) {
                                return Err(Error::Protocol { message: "MySQL did not return the caching_sha2 RSA public key".into() });
                            }
                            let encrypted = auth::rsa_encrypt(password, &nonce, &key_packet[1..])?;
                            self.write_packet(&encrypted)?;
                        }
                        other => {
                            return Err(Error::Protocol { message: format!("unsupported MySQL auth continuation `{other:?}`") });
                        }
                    }
                }
                other => {
                    return Err(Error::Protocol { message: format!("unexpected MySQL auth packet `{other:?}`") });
                }
            }
        }
    }

    /// Read one logical packet, joining continuation chunks and validating the
    /// sequence number.
    fn read_packet(&mut self) -> Result<Vec<u8>> {
        let mut payload = Vec::new();
        loop {
            let mut header = [0u8; 4];
            self.stream.read_exact(&mut header).map_err(Error::Io)?;
            let length = usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
            if header[3] != self.next_sequence {
                return Err(Error::Protocol {
                    message: format!("unexpected MySQL packet sequence {} (expected {})", header[3], self.next_sequence),
                });
            }
            let mut chunk = vec![0u8; length];
            self.stream.read_exact(&mut chunk).map_err(Error::Io)?;
            payload.extend_from_slice(&chunk);
            self.next_sequence = self.next_sequence.wrapping_add(1);
            if length < MAX_PACKET_PAYLOAD {
                return Ok(payload);
            }
        }
    }

    fn write_packet(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() > MAX_PACKET_PAYLOAD {
            return Err(Error::Protocol { message: "MySQL client payload exceeds one packet".into() });
        }
        let bytes = database_protocol_mysql::encode_packet(self.next_sequence, payload)
            .map_err(|_| Error::Protocol { message: "MySQL client payload exceeds one packet".into() })?;
        self.stream.write_all(&bytes).map_err(Error::Io)?;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        Ok(())
    }

    /// Read one command response: OK, ERR or a result set.
    ///
    /// `binary` marks the `COM_STMT_EXECUTE` row encoding; the column
    /// definitions are parsed into [`ColumnInfo`] in both protocols so callers
    /// can restore declared types.
    fn read_result(&mut self, binary: bool) -> Result<QueryResult> {
        let first = self.read_packet()?;
        let Some(header) = first.first().copied()
        else {
            return Err(Error::Corrupt("empty MySQL command response"));
        };
        match header {
            0xff => Err(server_error(&first)),
            0x00 => {
                let (affected_rows, insert_id, _status, _warnings) = parse_ok(&first)?;
                Ok(QueryResult { affected_rows, insert_id, ..QueryResult::default() })
            }
            0xfb => Err(Error::Unsupported("MySQL LOCAL INFILE is not supported")),
            _ => {
                let mut offset = 0;
                let count = lenenc_usize(&first, &mut offset)?.ok_or(Error::Corrupt("MySQL result is missing a column count"))?;
                if count == 0 {
                    return Err(Error::Corrupt("MySQL result set has zero columns"));
                }
                let mut column_info = Vec::with_capacity(count);
                for _ in 0..count {
                    let definition = self.read_packet()?;
                    column_info.push(parse_column(&definition)?);
                }
                let terminator = self.read_packet()?;
                if !is_eof(&terminator) {
                    return Err(Error::Protocol { message: "MySQL column definitions did not end with EOF".into() });
                }
                let columns = column_info.iter().map(|column| column.name.clone()).collect();
                let mut rows = Vec::new();
                loop {
                    let packet = self.read_packet()?;
                    if packet.first() == Some(&0xff) {
                        return Err(server_error(&packet));
                    }
                    if is_eof(&packet) {
                        break;
                    }
                    rows.push(if binary { parse_binary_row(&packet, &column_info)? } else { parse_row(&packet, count)? });
                }
                Ok(QueryResult { columns, column_info, rows, ..QueryResult::default() })
            }
        }
    }
}

/// Handle returned by `COM_STMT_PREPARE`.
struct PreparedStatement {
    id: u32,
    parameter_count: u16,
}

/// Parsed `Initial Handshake Packet`.
struct Handshake {
    capabilities: u32,
    plugin: String,
    nonce: Vec<u8>,
}

impl Handshake {
    fn parse(payload: &[u8]) -> Result<Self> {
        if payload.first() != Some(&10) {
            return Err(Error::Protocol { message: "only MySQL protocol version 10 is supported".into() });
        }
        let mut offset = 1;
        take_nul(payload, &mut offset)?;
        offset = offset.checked_add(4).ok_or(Error::Corrupt("truncated MySQL handshake"))?;
        let mut nonce = take(payload, &mut offset, 8)?.to_vec();
        take(payload, &mut offset, 1)?;
        let lower = u32::from(read_u16(payload, &mut offset)?);
        offset = offset.checked_add(1).ok_or(Error::Corrupt("truncated MySQL handshake"))?;
        offset = offset.checked_add(2).ok_or(Error::Corrupt("truncated MySQL handshake"))?;
        let upper = u32::from(read_u16(payload, &mut offset)?) << 16;
        let capabilities = lower | upper;
        let auth_len = if capabilities & CLIENT_PLUGIN_AUTH != 0 { read_u8(payload, &mut offset)? } else { 0 };
        take(payload, &mut offset, 10)?;
        let part2_len = std::cmp::max(13, usize::from(auth_len).saturating_sub(8));
        if offset + part2_len <= payload.len() {
            nonce.extend_from_slice(take(payload, &mut offset, part2_len)?);
        }
        let plugin = if capabilities & CLIENT_PLUGIN_AUTH != 0 && offset < payload.len() {
            take_nul(payload, &mut offset)?
        }
        else {
            "mysql_native_password".to_owned()
        };
        while nonce.last() == Some(&0) {
            nonce.pop();
        }
        if nonce.is_empty() {
            return Err(Error::Corrupt("MySQL handshake has no authentication nonce"));
        }
        Ok(Self { capabilities, plugin, nonce })
    }
}

fn resolve(host: &str, port: u16) -> Result<SocketAddr> {
    format!("{host}:{port}").to_socket_addrs().map_err(Error::Io)?.next().ok_or(Error::Corrupt("MySQL host did not resolve"))
}

fn take<'a>(payload: &'a [u8], offset: &mut usize, width: usize) -> Result<&'a [u8]> {
    let end = offset.checked_add(width).ok_or(Error::Corrupt("MySQL packet offset overflow"))?;
    let slice = payload.get(*offset..end).ok_or(Error::Corrupt("truncated MySQL packet"))?;
    *offset = end;
    Ok(slice)
}

fn take_nul(payload: &[u8], offset: &mut usize) -> Result<String> {
    let start = *offset;
    let end = payload[start..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|position| start + position)
        .ok_or(Error::Corrupt("unterminated MySQL handshake string"))?;
    *offset = end + 1;
    Ok(String::from_utf8_lossy(&payload[start..end]).into_owned())
}

fn read_u8(payload: &[u8], offset: &mut usize) -> Result<u8> {
    Ok(take(payload, offset, 1)?[0])
}

fn read_u16(payload: &[u8], offset: &mut usize) -> Result<u16> {
    let bytes = take(payload, offset, 2)?;
    Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn read_u32(payload: &[u8], offset: &mut usize) -> Result<u32> {
    let bytes = take(payload, offset, 4)?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn split_switch_request(payload: &[u8]) -> Result<(String, &[u8])> {
    let end = payload.iter().position(|byte| *byte == 0).ok_or(Error::Corrupt("MySQL auth switch has no plugin name"))?;
    let name = String::from_utf8_lossy(&payload[..end]).into_owned();
    let mut nonce = &payload[end + 1..];
    if nonce.last() == Some(&0) {
        nonce = &nonce[..nonce.len() - 1];
    }
    if nonce.is_empty() {
        return Err(Error::Corrupt("MySQL auth switch has no nonce"));
    }
    Ok((name, nonce))
}

fn server_error(payload: &[u8]) -> Error {
    if payload.len() < 3 {
        return Error::Corrupt("truncated MySQL error packet");
    }
    let code = u16::from_le_bytes([payload[1], payload[2]]);
    let mut offset = 3;
    let mut state = String::new();
    if payload.get(offset) == Some(&b'#') && payload.len() >= offset + 6 {
        state = String::from_utf8_lossy(&payload[offset + 1..offset + 6]).into_owned();
        offset += 6;
    }
    let message = String::from_utf8_lossy(&payload[offset..]).into_owned();
    if state.is_empty() {
        Error::Protocol { message: format!("{code}: {message}") }
    }
    else {
        Error::Protocol { message: format!("{code} ({state}): {message}") }
    }
}

/// Decode a length-encoded integer; `None` means an SQL `NULL` marker.
fn lenenc(payload: &[u8], offset: &mut usize) -> Result<Option<u64>> {
    let marker = read_u8(payload, offset)?;
    match marker {
        0x00..=0xfa => Ok(Some(u64::from(marker))),
        0xfb => Ok(None),
        0xfc => {
            let bytes = take(payload, offset, 2)?;
            Ok(Some(u64::from(u16::from_le_bytes([bytes[0], bytes[1]]))))
        }
        0xfd => {
            let bytes = take(payload, offset, 3)?;
            Ok(Some(u64::from(bytes[0]) | (u64::from(bytes[1]) << 8) | (u64::from(bytes[2]) << 16)))
        }
        0xfe => {
            let bytes = take(payload, offset, 8)?;
            let mut value = [0u8; 8];
            value.copy_from_slice(bytes);
            Ok(Some(u64::from_le_bytes(value)))
        }
        0xff => Err(Error::Corrupt("invalid MySQL length-encoded value")),
    }
}

/// Append a length-encoded integer (values below `0xfb` fit one byte).
fn push_lenenc(payload: &mut Vec<u8>, value: usize) {
    if value < 0xfb {
        payload.push(value as u8);
    }
    else if value <= 0xffff {
        payload.push(0xfc);
        payload.extend_from_slice(&(value as u16).to_le_bytes());
    }
    else if value <= 0x00ff_ffff {
        payload.push(0xfd);
        payload.extend_from_slice(&(value as u32).to_le_bytes()[..3]);
    }
    else {
        payload.push(0xfe);
        payload.extend_from_slice(&(value as u64).to_le_bytes());
    }
}

fn lenenc_usize(payload: &[u8], offset: &mut usize) -> Result<Option<usize>> {
    let value = lenenc(payload, offset)?;
    Ok(match value {
        Some(value) => Some(usize::try_from(value).map_err(|_| Error::Corrupt("MySQL length exceeds usize"))?),
        None => None,
    })
}

fn parse_ok(payload: &[u8]) -> Result<(u64, u64, u16, u16)> {
    if payload.first() != Some(&0x00) {
        return Err(Error::Corrupt("expected a MySQL OK packet"));
    }
    let mut offset = 1;
    let affected = lenenc(payload, &mut offset)?.unwrap_or(0);
    let insert_id = lenenc(payload, &mut offset)?.unwrap_or(0);
    let status = read_u16(payload, &mut offset).unwrap_or(0);
    let warnings = read_u16(payload, &mut offset).unwrap_or(0);
    Ok((affected, insert_id, status, warnings))
}

fn is_eof(payload: &[u8]) -> bool {
    payload.first() == Some(&0xfe) && payload.len() < 9
}

/// Parse one column definition packet
/// (`ColumnDefinition41`) into its label and declared shape.
fn parse_column(payload: &[u8]) -> Result<ColumnInfo> {
    let mut offset = 0;
    let mut name = String::new();
    for index in 0..6 {
        let value = lenenc_usize(payload, &mut offset)?.ok_or(Error::Corrupt("MySQL column definition has a NULL string"))?;
        let bytes = take(payload, &mut offset, value)?;
        if index == 4 {
            name = String::from_utf8_lossy(bytes).into_owned();
        }
    }
    let fixed_len = lenenc_usize(payload, &mut offset)?.ok_or(Error::Corrupt("MySQL column definition lacks fixed fields"))?;
    if fixed_len != 12 {
        return Err(Error::Corrupt("MySQL column definition fixed fields are not 12 bytes"));
    }
    let charset = read_u16(payload, &mut offset)?;
    let column_length = read_u32(payload, &mut offset)?;
    let type_code = read_u8(payload, &mut offset)?;
    let flags = read_u16(payload, &mut offset)?;
    let _decimals = read_u8(payload, &mut offset)?;
    take(payload, &mut offset, 2)?;
    Ok(ColumnInfo { name, type_code, flags, charset, column_length })
}

/// Decode one `COM_STMT_EXECUTE` binary row.
///
/// Layout: `0x00` marker, a null bitmap offset by two bits, then one payload
/// per non-null column. Values are widened into [`Cell::Bytes`] text the way
/// the text protocol would show them; raw octets stay raw for binary columns.
fn parse_binary_row(payload: &[u8], columns: &[ColumnInfo]) -> Result<Vec<Cell>> {
    if payload.first() != Some(&0x00) {
        return Err(Error::Corrupt("MySQL binary row does not start with 0x00"));
    }
    let mut offset = 1;
    let bitmap_len = (columns.len() + 2).div_ceil(8);
    let bitmap = take(payload, &mut offset, bitmap_len)?.to_vec();
    let mut row = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let bit = index + 2;
        if bitmap[bit / 8] & (1 << (bit % 8)) != 0 {
            row.push(Cell::Null);
            continue;
        }
        row.push(decode_binary_value(payload, &mut offset, column)?);
    }
    if offset != payload.len() {
        return Err(Error::Corrupt("MySQL binary row has trailing bytes"));
    }
    Ok(row)
}

fn decode_binary_value(payload: &[u8], offset: &mut usize, column: &ColumnInfo) -> Result<Cell> {
    let unsigned = column.unsigned();
    match column.type_code {
        mysql_type::TINY => {
            let value = if unsigned { i64::from(read_u8(payload, offset)?) } else { i64::from(read_u8(payload, offset)? as i8) };
            Ok(Cell::Bytes(value.to_string().into_bytes()))
        }
        mysql_type::SHORT | mysql_type::YEAR => {
            let value = if unsigned { i64::from(read_u16(payload, offset)?) } else { i64::from(read_u16(payload, offset)? as i16) };
            Ok(Cell::Bytes(value.to_string().into_bytes()))
        }
        mysql_type::LONG | mysql_type::INT24 => {
            let value = if unsigned { i64::from(read_u32(payload, offset)?) } else { i64::from(read_u32(payload, offset)? as i32) };
            Ok(Cell::Bytes(value.to_string().into_bytes()))
        }
        mysql_type::LONGLONG => {
            let bytes = take(payload, offset, 8)?;
            let mut buffer = [0u8; 8];
            buffer.copy_from_slice(bytes);
            let value = if unsigned { (u64::from_le_bytes(buffer)).to_string() } else { i64::from_le_bytes(buffer).to_string() };
            Ok(Cell::Bytes(value.into_bytes()))
        }
        mysql_type::FLOAT => {
            let bytes = take(payload, offset, 4)?;
            let value = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            if !value.is_finite() {
                return Err(Error::Corrupt("MySQL binary row carries a non-finite float"));
            }
            Ok(Cell::Bytes(value.to_string().into_bytes()))
        }
        mysql_type::DOUBLE => {
            let bytes = take(payload, offset, 8)?;
            let mut buffer = [0u8; 8];
            buffer.copy_from_slice(bytes);
            let value = f64::from_le_bytes(buffer);
            if !value.is_finite() {
                return Err(Error::Corrupt("MySQL binary row carries a non-finite double"));
            }
            Ok(Cell::Bytes(value.to_string().into_bytes()))
        }
        mysql_type::NULL => Ok(Cell::Null),
        mysql_type::DATE | mysql_type::NEWDATE => Ok(Cell::Bytes(read_binary_date(payload, offset)?.into_bytes())),
        mysql_type::DATETIME | mysql_type::DATETIME2 | mysql_type::TIMESTAMP | mysql_type::TIMESTAMP2 => {
            Ok(Cell::Bytes(read_binary_datetime(payload, offset)?.into_bytes()))
        }
        mysql_type::TIME | mysql_type::TIME2 => Ok(Cell::Bytes(read_binary_time(payload, offset)?.into_bytes())),
        _ => {
            let length = lenenc_usize(payload, offset)?.ok_or(Error::Corrupt("MySQL binary value is NULL without a bitmap bit"))?;
            Ok(Cell::Bytes(take(payload, offset, length)?.to_vec()))
        }
    }
}

fn read_binary_date(payload: &[u8], offset: &mut usize) -> Result<String> {
    let length = lenenc_usize(payload, offset)?.ok_or(Error::Corrupt("MySQL binary DATE is NULL without a bitmap bit"))?;
    if length == 0 {
        return Ok("0000-00-00".into());
    }
    let year = read_u16(payload, offset)?;
    let month = read_u8(payload, offset)?;
    let day = read_u8(payload, offset)?;
    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

fn read_binary_datetime(payload: &[u8], offset: &mut usize) -> Result<String> {
    let length = lenenc_usize(payload, offset)?.ok_or(Error::Corrupt("MySQL binary DATETIME is NULL without a bitmap bit"))?;
    if length == 0 {
        return Ok("0000-00-00 00:00:00".into());
    }
    let year = read_u16(payload, offset)?;
    let month = read_u8(payload, offset)?;
    let day = read_u8(payload, offset)?;
    if length == 4 {
        return Ok(format!("{year:04}-{month:02}-{day:02}"));
    }
    let hour = read_u8(payload, offset)?;
    let minute = read_u8(payload, offset)?;
    let second = read_u8(payload, offset)?;
    if length >= 11 {
        Ok(format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{:06}", read_u32(payload, offset)?))
    }
    else {
        Ok(format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}"))
    }
}

fn read_binary_time(payload: &[u8], offset: &mut usize) -> Result<String> {
    let length = lenenc_usize(payload, offset)?.ok_or(Error::Corrupt("MySQL binary TIME is NULL without a bitmap bit"))?;
    if length == 0 {
        return Ok("00:00:00".into());
    }
    let negative = read_u8(payload, offset)? != 0;
    let days = read_u32(payload, offset)?;
    let hours = read_u8(payload, offset)?;
    let minutes = read_u8(payload, offset)?;
    let seconds = read_u8(payload, offset)?;
    let sign = if negative { "-" } else { "" };
    let total_hours = days * 24 + u32::from(hours);
    if length >= 12 {
        Ok(format!("{sign}{total_hours:03}:{minutes:02}:{seconds:02}.{:06}", read_u32(payload, offset)?))
    }
    else {
        Ok(format!("{sign}{total_hours:03}:{minutes:02}:{seconds:02}"))
    }
}

fn parse_row(payload: &[u8], columns: usize) -> Result<Vec<Cell>> {
    let mut offset = 0;
    let mut row = Vec::with_capacity(columns);
    for _ in 0..columns {
        match lenenc(payload, &mut offset)? {
            None => row.push(Cell::Null),
            Some(length) => {
                let length = usize::try_from(length).map_err(|_| Error::Corrupt("MySQL cell length exceeds usize"))?;
                row.push(Cell::Bytes(take(payload, &mut offset, length)?.to_vec()));
            }
        }
    }
    if offset != payload.len() {
        return Err(Error::Corrupt("MySQL row has trailing bytes"));
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minimal_handshake_parses_native_auth() {
        let mut payload = vec![10];
        payload.extend_from_slice(b"8.4.6\0");
        payload.extend_from_slice(&7u32.to_le_bytes());
        payload.extend_from_slice(b"12345678");
        payload.push(0);
        payload.extend_from_slice(&0x8200u16.to_le_bytes());
        payload.push(UTF8MB4_GENERAL_CI);
        payload.extend_from_slice(&2u16.to_le_bytes());
        payload.extend_from_slice(&0x0008u16.to_le_bytes());
        payload.push(20);
        payload.extend_from_slice(&[0u8; 10]);
        payload.extend_from_slice(b"901234567890\0");
        payload.extend_from_slice(b"mysql_native_password\0");
        let parsed = Handshake::parse(&payload).expect("handshake");
        assert_eq!(parsed.capabilities, 0x0008_8200);
        assert_eq!(parsed.plugin, "mysql_native_password");
        assert_eq!(parsed.nonce, b"12345678901234567890");
    }

    #[test]
    fn eof_requires_a_short_packet() {
        assert!(is_eof(&[0xfe, 0, 0, 0, 0]));
        assert!(!is_eof(&[0xfe, 0, 0, 0, 0, 0, 0, 0, 0]));
    }

    #[test]
    fn a_row_keeps_null_and_empty_cells_distinct() {
        let row = parse_row(&[0xfb, 0x00], 2).expect("row");
        assert_eq!(row, vec![Cell::Null, Cell::Bytes(Vec::new())]);
    }

    fn column(name: &str, type_code: u8, flags: u16, charset: u16) -> ColumnInfo {
        ColumnInfo { name: name.into(), type_code, flags, charset, column_length: 255 }
    }

    #[test]
    fn a_column_definition_keeps_type_flags_and_charset() {
        let mut payload = Vec::new();
        for value in ["def", "app", "t", "t", "id", "id"] {
            payload.push(value.len() as u8);
            payload.extend_from_slice(value.as_bytes());
        }
        payload.push(0x0c);
        payload.extend_from_slice(&45u16.to_le_bytes());
        payload.extend_from_slice(&11u32.to_le_bytes());
        payload.push(mysql_type::LONG);
        payload.extend_from_slice(&0x0021u16.to_le_bytes());
        payload.push(0);
        payload.extend_from_slice(&[0u8; 2]);
        let parsed = parse_column(&payload).expect("column");
        assert_eq!(parsed.name, "id");
        assert_eq!(parsed.type_code, mysql_type::LONG);
        assert_eq!(parsed.charset, 45);
        assert_eq!(parsed.column_length, 11);
        assert!(parsed.unsigned());
        assert!(!parsed.binary());
    }

    #[test]
    fn binary_rows_decode_ints_text_and_null_bits() {
        let columns = [
            column("id", mysql_type::LONG, 0x0020, 63),
            column("name", mysql_type::VAR_STRING, 0, 45),
            column("gone", mysql_type::LONG, 0, 63),
        ];
        // Three columns need one bitmap byte; index 2 sets bit 2 + 2 = 4.
        let payload = [0x00, 0x10, 0x2a, 0x00, 0x00, 0x00, 0x03, b'b', b'o', b'b'];
        let row = parse_binary_row(&payload, &columns).expect("row");
        assert_eq!(row, vec![Cell::Bytes(b"42".to_vec()), Cell::Bytes(b"bob".to_vec()), Cell::Null]);
    }

    #[test]
    fn binary_rows_decode_unsigned_bigints() {
        let columns = [column("n", mysql_type::LONGLONG, 0x0020, 63)];
        let mut payload = vec![0x00, 0x00];
        payload.extend_from_slice(&u64::MAX.to_le_bytes());
        let row = parse_binary_row(&payload, &columns).expect("row");
        assert_eq!(row, vec![Cell::Bytes(b"18446744073709551615".to_vec())]);
    }

    #[test]
    fn binary_rows_decode_dates_and_times() {
        let date = column("d", mysql_type::DATE, 0, 63);
        let payload = [0x00, 0x00, 0x04, 0xe8, 0x07, 0x01, 0x02];
        let row = parse_binary_row(&payload, std::slice::from_ref(&date)).expect("row");
        assert_eq!(row, vec![Cell::Bytes(b"2024-01-02".to_vec())]);

        let time = column("t", mysql_type::TIME, 0, 63);
        let payload = [0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03];
        let row = parse_binary_row(&payload, std::slice::from_ref(&time)).expect("row");
        assert_eq!(row, vec![Cell::Bytes(b"001:02:03".to_vec())]);
    }

    #[test]
    fn binary_rows_reject_a_missing_marker() {
        let columns = [column("id", mysql_type::LONG, 0, 63)];
        assert!(parse_binary_row(&[0x01, 0x00, 0, 0, 0, 0], &columns).is_err());
    }

    #[test]
    fn server_errors_keep_code_and_sqlstate() {
        let mut packet = vec![0xff, 0x48, 0x04, b'#'];
        packet.extend_from_slice(b"HY000");
        packet.extend_from_slice(b"boom");
        let Error::Protocol { message } = server_error(&packet)
        else {
            panic!("expected Protocol")
        };
        assert_eq!(message, "1096 (HY000): boom");
    }
}
