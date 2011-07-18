//! Per-connection frame loop.
//!
//! Reads a frame, dispatches its command document through `super::handle_op_msg`
//! or `super::handle_op_query`, and wraps the returned BSON document into the
//! matching reply opcode.

use std::{
    io,
    net::TcpStream,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use database_execution::cluster::KeyValueExecutor;

use super::wire::{FrameLimits, OP_QUERY, read_frame, write_frame, write_reply_frame};

/// Runs the request/reply loop on a single TCP stream.
///
/// `cancellation` is polled after every frame so a listener shutdown can
/// interrupt idle connections.
pub fn serve_cancellable(stream: TcpStream, limits: FrameLimits, cancellation: Arc<AtomicBool>) -> io::Result<()> {
    serve_inner(stream, limits, cancellation, None)
}

/// Same as [`serve_cancellable`] but additionally runs command bodies through
/// a YYDS KV executor so `insert` / `find` / `delete` CRUD lands on the
/// backing store.
pub fn serve_cancellable_with_executor(
    stream: TcpStream,
    limits: FrameLimits,
    cancellation: Arc<AtomicBool>,
    executor: Arc<dyn KeyValueExecutor>,
) -> io::Result<()> {
    serve_inner(stream, limits, cancellation, Some(executor))
}

fn serve_inner(
    mut stream: TcpStream,
    limits: FrameLimits,
    cancellation: Arc<AtomicBool>,
    executor: Option<Arc<dyn KeyValueExecutor>>,
) -> io::Result<()> {
    while !cancellation.load(Ordering::Acquire) {
        let frame = match read_frame(&mut stream, limits) {
            Ok(Some(f)) => f,
            Ok(None) => return Ok(()),
            Err(error) => {
                tracing_command_error(error);
                return Ok(());
            }
        };

        if frame.header.opcode == OP_QUERY {
            // Legacy handshake frames get the legacy reply shape, otherwise the
            // driver rejects the answer as malformed.
            let document = super::handle_op_query(&frame.body, executor.as_deref());
            if let Err(error) = write_reply_frame(&mut stream, 0, frame.header.request_id, &document) {
                tracing_command_error(error);
                return Ok(());
            }
            continue;
        }

        // Dispatch through the BSON-aware handler. All errors are already
        // turned into a generic BSON document by the handler itself.
        let body = super::handle_op_msg(&frame.body, executor.as_deref());

        // OP_MSG replies carry flag bits and a section kind before the BSON
        // document. Drivers reject a bare document as a malformed message.
        let mut message = Vec::with_capacity(5 + body.len());
        message.extend_from_slice(&0_u32.to_le_bytes());
        message.push(0);
        message.extend_from_slice(&body);

        if let Err(error) = write_frame(&mut stream, 0, frame.header.request_id, &message) {
            tracing_command_error(error);
            return Ok(());
        }
    }
    Ok(())
}

fn tracing_command_error(_error: impl std::fmt::Debug) {
    // Placeholder — real logging lands when a tracing subscriber is wired in.
}
