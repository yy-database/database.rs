//! `database-server` owns service sessions, auth ingress, prepared-statement lifecycle and request dispatch.
#![deny(missing_docs)]

/// Embedded engine host service — loopback-first YY wire serve for out-of-process clients.
pub mod yydb;

/// Distributed runtime gateways — MySQL / PostgreSQL / Redis / Mongo disguise protocols.
pub mod yyds;
