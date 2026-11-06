//! Listener-scoped pub/sub subscriptions.
//!
//! Redis keeps one channel table per server, not per connection, because a message
//! published on one connection has to reach subscribers parked on other sockets.
//! This module holds that table and the shared write side each connection registers,
//! so a publisher can append a push frame to another connection's socket without
//! touching that connection's read loop.

use std::{
    collections::{BTreeSet, HashMap},
    io::{self, ErrorKind, Write},
    net::{Shutdown, TcpStream},
    sync::{
        Mutex, MutexGuard, Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::{Duration, Instant},
};

use super::connection::glob_match;

const MESSAGE: &[u8] = b"message";
const PMESSAGE: &[u8] = b"pmessage";

/// One connection's write side.
///
/// The owning thread writes its own replies through [`SinkRef`] and publishers
/// write push frames through [`ClientSink::write_frame`]. Both take the same mutex,
/// so two frames can never interleave on the socket.
pub struct ClientSink {
    stream: Mutex<TcpStream>,
    protocol: AtomicU8,
    stopping: Option<Arc<AtomicBool>>,
}

impl ClientSink {
    /// Wraps the write half of a connection, starting on RESP2.
    pub fn new(stream: TcpStream, stopping: Option<Arc<AtomicBool>>) -> Self {
        Self { stream: Mutex::new(stream), protocol: AtomicU8::new(2), stopping }
    }

    /// Protocol version the connection negotiated through `HELLO`.
    pub fn protocol(&self) -> u8 {
        self.protocol.load(Ordering::Acquire)
    }

    /// Records a protocol switch so later pushes use the negotiated frame prefix.
    pub fn set_protocol(&self, protocol: u8) {
        self.protocol.store(protocol, Ordering::Release);
    }

    /// Writes a complete frame, ignoring a closed or cancelled socket.
    pub fn write_frame(&self, frame: &[u8]) {
        let _ = self.write_locked(frame);
    }

    /// Closes the socket from another connection, waking any blocked read or write.
    ///
    /// `CLIENT KILL` runs on the killing connection, so it cannot borrow the victim's
    /// reader. Shutting the socket down on all clones makes the victim's read loop
    /// observe end-of-file and exit, which is the clean EOF the real server produces.
    pub fn shutdown(&self) {
        let stream = self.lock();
        let _ = stream.shutdown(Shutdown::Both);
    }

    fn lock(&self) -> MutexGuard<'_, TcpStream> {
        self.stream.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    fn write_locked(&self, bytes: &[u8]) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut stream = self.lock();
        let mut written = 0;
        while written < bytes.len() {
            if let Some(stopping) = &self.stopping {
                if stopping.load(Ordering::Acquire) {
                    return Err(io::Error::new(ErrorKind::ConnectionAborted, "node service stopping"));
                }
            }
            match stream.write(&bytes[written..]) {
                Ok(0) => return Err(io::Error::new(ErrorKind::WriteZero, "socket write returned zero")),
                Ok(count) => written += count,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err(io::Error::new(ErrorKind::TimedOut, "socket I/O timeout"));
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

/// Adapts a shared sink to the `Write` trait used by the command responder.
pub struct SinkRef(pub Arc<ClientSink>);

impl Write for SinkRef {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.write_locked(bytes)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Subscription bookkeeping of one connected client.
struct Entry {
    sink: Arc<ClientSink>,
    channels: BTreeSet<Vec<u8>>,
    patterns: BTreeSet<Vec<u8>>,
}

impl Entry {
    fn total(&self) -> usize {
        self.channels.len() + self.patterns.len()
    }
}

struct State {
    next_client: u64,
    clients: HashMap<u64, Entry>,
}

/// Listener-scoped channel and pattern table shared by every connection.
pub struct PubSubRegistry {
    state: Mutex<State>,
}

impl PubSubRegistry {
    /// Creates an empty table.
    pub fn new() -> Self {
        Self { state: Mutex::new(State { next_client: 0, clients: HashMap::new() }) }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    /// Registers one connection and returns its client id.
    pub fn attach(&self, sink: Arc<ClientSink>) -> u64 {
        let mut state = self.lock();
        state.next_client += 1;
        let client = state.next_client;
        state.clients.insert(client, Entry { sink, channels: BTreeSet::new(), patterns: BTreeSet::new() });
        client
    }

    /// Drops every subscription of a disconnected client.
    pub fn detach(&self, client: u64) {
        self.lock().clients.remove(&client);
    }

    /// Whether the client holds at least one channel or pattern subscription.
    pub fn is_subscribed(&self, client: u64) -> bool {
        self.lock().clients.get(&client).is_some_and(|entry| entry.total() > 0)
    }

    /// Channel and pattern subscription counts of one client.
    ///
    /// `CLIENT LIST` reports them as `sub=` / `psub=`, so they are read from the same
    /// table that owns the subscriptions instead of being mirrored per connection.
    pub fn counts(&self, client: u64) -> (usize, usize) {
        self.lock().clients.get(&client).map_or((0, 0), |entry| (entry.channels.len(), entry.patterns.len()))
    }

    /// Adds one subscription and answers the total count Redis reports.
    pub fn subscribe(&self, client: u64, name: &[u8], pattern: bool) -> usize {
        let mut state = self.lock();
        let Some(entry) = state.clients.get_mut(&client) else {
            return 0;
        };
        if pattern {
            entry.patterns.insert(name.to_vec());
        }
        else {
            entry.channels.insert(name.to_vec());
        }
        entry.total()
    }

    /// Removes subscriptions and answers one `(name, remaining)` pair per reply frame.
    ///
    /// An empty `names` list removes everything of that kind. With nothing to remove
    /// Redis still answers one frame carrying a null name, so it is returned as `None`.
    pub fn unsubscribe(&self, client: u64, names: &[&[u8]], pattern: bool) -> Vec<(Option<Vec<u8>>, usize)> {
        let mut state = self.lock();
        let Some(entry) = state.clients.get_mut(&client) else {
            return vec![(None, 0)];
        };
        let mut replies = Vec::new();
        if names.is_empty() {
            let existing: Vec<Vec<u8>> =
                if pattern { entry.patterns.iter().cloned().collect() } else { entry.channels.iter().cloned().collect() };
            if existing.is_empty() {
                replies.push((None, entry.total()));
            }
            for name in existing {
                if pattern {
                    entry.patterns.remove(&name);
                }
                else {
                    entry.channels.remove(&name);
                }
                replies.push((Some(name), entry.total()));
            }
        }
        else {
            for name in names {
                if pattern {
                    entry.patterns.remove(&name.to_vec());
                }
                else {
                    entry.channels.remove(&name.to_vec());
                }
                replies.push((Some(name.to_vec()), entry.total()));
            }
        }
        replies
    }

    /// Delivers one message and answers how many subscriptions received it.
    ///
    /// A client subscribed both directly and through a matching pattern receives two
    /// frames and counts twice, matching the upstream receiver count.
    pub fn publish(&self, channel: &[u8], payload: &[u8]) -> usize {
        let state = self.lock();
        let mut delivered = 0;
        for entry in state.clients.values() {
            let protocol = entry.sink.protocol();
            if entry.channels.contains(channel) {
                entry.sink.write_frame(&encode_message(protocol, None, channel, payload));
                delivered += 1;
            }
            for pattern in &entry.patterns {
                if glob_match(pattern, channel) {
                    entry.sink.write_frame(&encode_message(protocol, Some(pattern), channel, payload));
                    delivered += 1;
                }
            }
        }
        delivered
    }

    /// Active channels, optionally filtered by a glob, in sorted order.
    pub fn channels(&self, pattern: Option<&[u8]>) -> Vec<Vec<u8>> {
        let state = self.lock();
        let mut names = BTreeSet::new();
        for entry in state.clients.values() {
            for channel in &entry.channels {
                if pattern.is_none_or(|pattern| glob_match(pattern, channel)) {
                    names.insert(channel.clone());
                }
            }
        }
        names.into_iter().collect()
    }

    /// Subscriber count of each requested channel, unknown channels counting zero.
    pub fn numsub(&self, channels: &[&[u8]]) -> Vec<(Vec<u8>, usize)> {
        let state = self.lock();
        channels
            .iter()
            .map(|channel| {
                let count = state.clients.values().filter(|entry| entry.channels.contains(*channel)).count();
                (channel.to_vec(), count)
            })
            .collect()
    }

    /// Number of distinct pattern subscriptions across all clients.
    pub fn numpat(&self) -> usize {
        self.lock().clients.values().map(|entry| entry.patterns.len()).sum()
    }
}

impl Default for PubSubRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Encodes a subscription or unsubscription acknowledgement.
///
/// RESP2 spells every pub/sub frame as an array, RESP3 as a push. A null name is the
/// "nothing left to unsubscribe" form.
pub fn encode_ack(protocol: u8, kind: &[u8], name: Option<&[u8]>, count: usize) -> Vec<u8> {
    let mut out = Vec::new();
    push_header(&mut out, protocol, 3);
    write_bulk(&mut out, kind);
    match name {
        Some(name) => write_bulk(&mut out, name),
        None => out.extend_from_slice(if protocol >= 3 { b"_\r\n" as &[u8] } else { b"$-1\r\n" }),
    }
    out.extend_from_slice(format!(":{count}\r\n").as_bytes());
    out
}

/// Encodes one `message` or `pmessage` frame.
pub fn encode_message(protocol: u8, pattern: Option<&[u8]>, channel: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    match pattern {
        Some(pattern) => {
            push_header(&mut out, protocol, 4);
            write_bulk(&mut out, PMESSAGE);
            write_bulk(&mut out, pattern);
            write_bulk(&mut out, channel);
            write_bulk(&mut out, payload);
        }
        None => {
            push_header(&mut out, protocol, 3);
            write_bulk(&mut out, MESSAGE);
            write_bulk(&mut out, channel);
            write_bulk(&mut out, payload);
        }
    }
    out
}

fn push_header(out: &mut Vec<u8>, protocol: u8, count: usize) {
    out.extend_from_slice(if protocol >= 3 { b">" } else { b"*" });
    out.extend_from_slice(format!("{count}\r\n").as_bytes());
}

fn write_bulk(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
    out.extend_from_slice(bytes);
    out.extend_from_slice(b"\r\n");
}
