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

/// One connection's subscription sets. It carries no socket and no protocol.
#[derive(Default)]
struct Subscriptions {
    channels: BTreeSet<Vec<u8>>,
    patterns: BTreeSet<Vec<u8>>,
}

impl Subscriptions {
    fn total(&self) -> usize {
        self.channels.len() + self.patterns.len()
    }
}

/// One delivery target of a published message; `pattern` is `Some` for `pmessage`.
pub struct Delivery {
    /// Connection id that holds the subscription.
    pub client: u64,
    /// Matching pattern when the delivery comes from `PSUBSCRIBE`.
    pub pattern: Option<Vec<u8>>,
    /// Channel the message was published to.
    pub channel: Vec<u8>,
    /// Message body.
    pub payload: Vec<u8>,
}

/// Listener-scoped channel table holding only pub/sub semantics.
///
/// It answers who subscribed to what and which clients a message reaches, without
/// holding sockets or encoding protocol frames. The language binding holds this table
/// directly, while [`PubSubRegistry`] wraps it for the Rust listener's write side.
pub struct PubSubState {
    clients: Mutex<HashMap<u64, Subscriptions>>,
}

impl Default for PubSubState {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubState {
    /// Creates an empty table.
    pub fn new() -> Self {
        Self { clients: Mutex::new(HashMap::new()) }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<u64, Subscriptions>> {
        self.clients.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    /// Drops every subscription of a disconnected client.
    pub fn detach(&self, client: u64) {
        self.lock().remove(&client);
    }

    /// Whether the client holds at least one channel or pattern subscription.
    pub fn is_subscribed(&self, client: u64) -> bool {
        self.lock().get(&client).is_some_and(|entry| entry.total() > 0)
    }

    /// Channel and pattern subscription counts of one client.
    ///
    /// `CLIENT LIST` reports them as `sub=` / `psub=`, so they are read from the same
    /// table that owns the subscriptions instead of being mirrored per connection.
    pub fn counts(&self, client: u64) -> (usize, usize) {
        self.lock().get(&client).map_or((0, 0), |entry| (entry.channels.len(), entry.patterns.len()))
    }

    /// Adds one subscription and answers the total count Redis reports.
    ///
    /// The binding passes the host connection id directly, so a first subscription
    /// creates the record on demand.
    pub fn subscribe(&self, client: u64, name: &[u8], pattern: bool) -> usize {
        let mut clients = self.lock();
        let entry = clients.entry(client).or_default();
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
        let mut clients = self.lock();
        let Some(entry) = clients.get_mut(&client) else {
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

    /// Computes the delivery targets of one message and answers `(receivers, deliveries)`.
    ///
    /// A client subscribed both directly and through a matching pattern gets two
    /// deliveries and counts twice, matching the real server's receiver count.
    pub fn publish(&self, channel: &[u8], payload: &[u8]) -> (usize, Vec<Delivery>) {
        let clients = self.lock();
        let mut delivered = 0;
        let mut deliveries = Vec::new();
        for (client, entry) in clients.iter() {
            if entry.channels.contains(channel) {
                deliveries.push(Delivery {
                    client: *client,
                    pattern: None,
                    channel: channel.to_vec(),
                    payload: payload.to_vec(),
                });
                delivered += 1;
            }
            for pattern in &entry.patterns {
                if glob_match(pattern, channel) {
                    deliveries.push(Delivery {
                        client: *client,
                        pattern: Some(pattern.to_vec()),
                        channel: channel.to_vec(),
                        payload: payload.to_vec(),
                    });
                    delivered += 1;
                }
            }
        }
        (delivered, deliveries)
    }

    /// Active channels, optionally filtered by a glob, in sorted order.
    pub fn channels(&self, pattern: Option<&[u8]>) -> Vec<Vec<u8>> {
        let clients = self.lock();
        let mut names = BTreeSet::new();
        for entry in clients.values() {
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
        let clients = self.lock();
        channels
            .iter()
            .map(|channel| {
                let count = clients.values().filter(|entry| entry.channels.contains(*channel)).count();
                (channel.to_vec(), count)
            })
            .collect()
    }

    /// Number of distinct patterns subscribed across all clients.
    ///
    /// The real server counts unique patterns rather than the sum of per-client
    /// subscriptions, so `PUBSUB NUMPAT` and `INFO stats` share this answer.
    pub fn numpat(&self) -> usize {
        let clients = self.lock();
        clients
            .values()
            .flat_map(|entry| entry.patterns.iter().map(|pattern| pattern.as_slice()))
            .collect::<BTreeSet<&[u8]>>()
            .len()
    }
}

struct Writes {
    next_client: u64,
    sinks: HashMap<u64, Arc<ClientSink>>,
}

/// Listener runtime wrapper: the pure table plus each connection's write side.
pub struct PubSubRegistry {
    state: Arc<PubSubState>,
    writes: Mutex<Writes>,
}

impl PubSubRegistry {
    /// Creates a registry with a fresh table.
    pub fn new() -> Self {
        Self::with_state(Arc::new(PubSubState::new()))
    }

    /// Wraps the table the language binding also holds.
    pub fn with_state(state: Arc<PubSubState>) -> Self {
        Self { state, writes: Mutex::new(Writes { next_client: 0, sinks: HashMap::new() }) }
    }

    /// Shared handle to the pure table.
    pub fn state(&self) -> Arc<PubSubState> {
        Arc::clone(&self.state)
    }

    fn lock_writes(&self) -> MutexGuard<'_, Writes> {
        self.writes.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    /// Registers one connection and returns its client id.
    pub fn attach(&self, sink: Arc<ClientSink>) -> u64 {
        let mut writes = self.lock_writes();
        writes.next_client += 1;
        let client = writes.next_client;
        writes.sinks.insert(client, sink);
        client
    }

    /// Drops every subscription and the write side of a disconnected client.
    pub fn detach(&self, client: u64) {
        self.lock_writes().sinks.remove(&client);
        self.state.detach(client);
    }

    /// Whether the client holds at least one channel or pattern subscription.
    pub fn is_subscribed(&self, client: u64) -> bool {
        self.state.is_subscribed(client)
    }

    /// Channel and pattern subscription counts of one client.
    pub fn counts(&self, client: u64) -> (usize, usize) {
        self.state.counts(client)
    }

    /// Adds one subscription and answers the total count Redis reports.
    pub fn subscribe(&self, client: u64, name: &[u8], pattern: bool) -> usize {
        self.state.subscribe(client, name, pattern)
    }

    /// Removes subscriptions and answers one `(name, remaining)` pair per reply frame.
    pub fn unsubscribe(&self, client: u64, names: &[&[u8]], pattern: bool) -> Vec<(Option<Vec<u8>>, usize)> {
        self.state.unsubscribe(client, names, pattern)
    }

    /// Delivers one message and answers how many subscriptions received it.
    pub fn publish(&self, channel: &[u8], payload: &[u8]) -> usize {
        let (delivered, deliveries) = self.state.publish(channel, payload);
        let targets = {
            let writes = self.lock_writes();
            deliveries
                .into_iter()
                .filter_map(|delivery| {
                    writes.sinks.get(&delivery.client).map(|sink| (sink.protocol(), Arc::clone(sink), delivery))
                })
                .collect::<Vec<_>>()
        };
        for (protocol, sink, delivery) in targets {
            sink.write_frame(&encode_message(protocol, delivery.pattern.as_deref(), &delivery.channel, &delivery.payload));
        }
        delivered
    }

    /// Active channels, optionally filtered by a glob, in sorted order.
    pub fn channels(&self, pattern: Option<&[u8]>) -> Vec<Vec<u8>> {
        self.state.channels(pattern)
    }

    /// Subscriber count of each requested channel, unknown channels counting zero.
    pub fn numsub(&self, channels: &[&[u8]]) -> Vec<(Vec<u8>, usize)> {
        self.state.numsub(channels)
    }

    /// Number of distinct patterns subscribed across all clients.
    pub fn numpat(&self) -> usize {
        self.state.numpat()
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
