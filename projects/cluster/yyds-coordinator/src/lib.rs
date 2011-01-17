//! YYDS distributed coordinator.
//!
//! Consumes `DistributedPlan` / `Fragment` values from `yyds-types` and
//! schedules work across shards. Today's `LocalExecutor` is single-process;
//! this crate is the multi-fragment scheduling boundary.
//!
//! Status: scaffold.
