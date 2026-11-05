//! YYDS distributed runtime gateways.
//!
//! Each dialect module is a disguise gateway over a YYDS node executor. They own
//! protocol sessions, not storage formats; persisted bytes stay in `.yyds` /
//! `.yykv` codecs owned by `database.formats`.

pub mod mongo;
pub mod mysql;
pub mod pgsql;
pub mod redis;
