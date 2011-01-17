# yyds-control

YYDS-owned node lifecycle primitives, shared by all network gateways.

`NodeLease::acquire(directory, identity)` exclusively locks a node data directory and verifies its persisted cluster/node identity. The versioned `node.json` identity is immutable across restarts. Invalid, mismatched, truncated, or oversized identities fail closed rather than being replaced.

The operating system releases ownership when the lease is dropped or its process exits. `node.lock` remains on disk and must not be deleted or replaced, particularly while a process owns it. Data directories must be trusted, operator-managed directories, not concurrently renamed or modified by external processes.

`NodeLease::inspect` reports only local directory ownership at the instant of observation. An available or held lease does not prove membership, replication, leader authority, quorum, query readiness, or cluster health. Process supervision, remote administration, gateway attachment, distributed membership, and maintenance are not yet implemented by this crate.

The first identity publication is synchronized before acquisition returns. An interrupted publication can leave a corrupt identity, which requires explicit operator investigation and is never silently reinitialized. This is not a complete power-loss recovery protocol.
