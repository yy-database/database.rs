//! `database-server` owns service sessions, auth ingress, prepared-statement lifecycle and request dispatch.
#![deny(missing_docs)]

pub mod mongo;
pub mod mysql;
pub mod pgsql;
pub mod redis;

/// Shared YY wire server (embedded/distributed differ only by product magic).
pub mod yy;
