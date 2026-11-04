//! RESP2 client for Redis-compatible servers (YYDS disguise or foreign Redis).

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use yyds_types::{Error, Result};

use crate::resp::{encode_request, FrameError, RequestLimits};

/// One decoded RESP2 reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// Simple string (`+`).
    Status(String),
    /// Error string (`-`).
    Error(String),
    /// Integer (`:`).
    Integer(i64),
    /// Bulk string (`$`), `None` for null bulk.
    Bulk(Option<Vec<u8>>),
}

/// A bounded Redis wire client backed by a single TCP connection.
#[derive(Debug)]
pub struct Client {
    stream: TcpStream,
    limits: RequestLimits,
    pending: Vec<u8>,
}

impl Client {
    /// Opens a TCP connection and verifies the server with `PING`.
    pub fn connect(address: SocketAddr, timeout: Duration) -> Result<Self> {
        let stream = TcpStream::connect_timeout(&address, timeout).map_err(Error::Io)?;
        stream.set_read_timeout(Some(timeout)).map_err(Error::Io)?;
        stream.set_write_timeout(Some(timeout)).map_err(Error::Io)?;
        let mut client = Self { stream, limits: RequestLimits::default(), pending: Vec::new() };
        client.ping()?;
        Ok(client)
    }

    /// Sends `PING` and expects `PONG`.
    pub fn ping(&mut self) -> Result<()> {
        match self.command(&[b"PING"])? {
            Response::Status(status) if status.eq_ignore_ascii_case("PONG") => Ok(()),
            _other => Err(Error::Corrupt("unexpected PING reply")),
        }
    }

    /// Sends one command and returns the first reply.
    pub fn command(&mut self, arguments: &[&[u8]]) -> Result<Response> {
        let request = encode_request(arguments, self.limits).map_err(map_frame_error)?;
        self.stream.write_all(&request).map_err(Error::Io)?;
        self.stream.flush().map_err(Error::Io)?;
        self.read_response()
    }

    /// Returns a bulk value or `None` when the key is absent.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.command(&[b"GET", key])? {
            Response::Bulk(value) => Ok(value),
            other => Err(unexpected_reply("GET", other)),
        }
    }

    /// Stores a bulk value and expects `OK`.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        match self.command(&[b"SET", key, value])? {
            Response::Status(status) if status == "OK" => Ok(()),
            other => Err(unexpected_reply("SET", other)),
        }
    }

    /// Stores a bulk value with expiry in seconds.
    pub fn set_ex(&mut self, key: &[u8], value: &[u8], seconds: u64) -> Result<()> {
        let ttl = seconds.to_string();
        match self.command(&[b"SET", key, value, b"EX", ttl.as_bytes()])? {
            Response::Status(status) if status == "OK" => Ok(()),
            other => Err(unexpected_reply("SET EX", other)),
        }
    }

    /// Stores only when the key is absent. Returns whether the key was created.
    pub fn set_nx(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        match self.command(&[b"SET", key, value, b"NX"])? {
            Response::Status(status) if status == "OK" => Ok(true),
            Response::Bulk(None) => Ok(false),
            other => Err(unexpected_reply("SET NX", other)),
        }
    }

    /// Deletes a key and returns how many entries were removed.
    pub fn del(&mut self, key: &[u8]) -> Result<u64> {
        match self.command(&[b"DEL", key])? {
            Response::Integer(count) if count >= 0 => Ok(count as u64),
            other => Err(unexpected_reply("DEL", other)),
        }
    }

    /// Returns the TTL in seconds (`-1` no expiry, `-2` missing key).
    pub fn ttl(&mut self, key: &[u8]) -> Result<i64> {
        match self.command(&[b"TTL", key])? {
            Response::Integer(ttl) => Ok(ttl),
            other => Err(unexpected_reply("TTL", other)),
        }
    }

    fn read_response(&mut self) -> Result<Response> {
        loop {
            if let Some(response) = decode_response(&self.pending)? {
                let consumed = response.1;
                self.pending.drain(..consumed);
                return Ok(response.0);
            }
            let mut chunk = [0u8; 4096];
            let received = self.stream.read(&mut chunk).map_err(Error::Io)?;
            if received == 0 {
                return Err(Error::Corrupt("redis connection closed before reply"));
            }
            self.pending.extend_from_slice(&chunk[..received]);
        }
    }
}

fn decode_response(buffer: &[u8]) -> Result<Option<(Response, usize)>> {
    let Some(marker) = buffer.first()
    else {
        return Ok(None);
    };
    match marker {
        b'+' | b'-' | b':' => decode_line_response(buffer, *marker),
        b'$' => decode_bulk_response(buffer),
        _ => Err(Error::Corrupt("unsupported redis reply type")),
    }
}

fn decode_line_response(buffer: &[u8], marker: u8) -> Result<Option<(Response, usize)>> {
    let Some(end) = find_crlf(buffer, 1)
    else {
        return Ok(None);
    };
    let line = &buffer[1..end];
    let text = std::str::from_utf8(line).map_err(|_| Error::Corrupt("redis reply is not UTF-8"))?;
    let response = match marker {
        b'+' => Response::Status(text.to_owned()),
        b'-' => Response::Error(text.to_owned()),
        b':' => Response::Integer(text.parse().map_err(|_| Error::Corrupt("redis integer reply"))?),
        _ => return Err(Error::Corrupt("unsupported redis line reply")),
    };
    Ok(Some((response, end + 2)))
}

fn decode_bulk_response(buffer: &[u8]) -> Result<Option<(Response, usize)>> {
    let Some(end) = find_crlf(buffer, 1)
    else {
        return Ok(None);
    };
    let length_text = std::str::from_utf8(&buffer[1..end]).map_err(|_| Error::Corrupt("redis bulk length"))?;
    let length = length_text.parse::<i64>().map_err(|_| Error::Corrupt("redis bulk length"))?;
    if length < 0 {
        return Ok(Some((Response::Bulk(None), end + 2)));
    }
    let length = usize::try_from(length).map_err(|_| Error::Corrupt("redis bulk length"))?;
    let payload_start = end + 2;
    let payload_end = payload_start.checked_add(length).ok_or(Error::Corrupt("redis bulk length"))?;
    let terminator_end = payload_end.checked_add(2).ok_or(Error::Corrupt("redis bulk length"))?;
    if buffer.len() < terminator_end {
        return Ok(None);
    }
    if buffer[payload_end..terminator_end] != *b"\r\n" {
        return Err(Error::Corrupt("redis bulk terminator"));
    }
    Ok(Some((Response::Bulk(Some(buffer[payload_start..payload_end].to_vec())), terminator_end)))
}

fn find_crlf(buffer: &[u8], start: usize) -> Option<usize> {
    buffer[start..].windows(2).position(|window| window == b"\r\n").map(|relative| start + relative)
}

fn map_frame_error(error: FrameError) -> Error {
    match error {
        FrameError::FrameTooLarge | FrameError::TooManyArguments | FrameError::BulkTooLarge => {
            Error::Unsupported("redis request exceeds configured limits")
        }
        _ => Error::Corrupt("redis request framing error"),
    }
}

fn unexpected_reply(_command: &str, _response: Response) -> Error {
    Error::Corrupt("unexpected redis reply for command")
}
