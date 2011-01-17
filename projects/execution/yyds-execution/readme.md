# YYDS Execution

This crate owns the execution boundary between protocol adapters and YYDS shard storage. `LocalExecutor` routes namespace-scoped key/value commands using a published `ShardMap` and executes them against mounted `.yykv` shards.

The current executor is process-local. It does not implement replication, leader election, remote shard calls, quorum durability, rebalancing, or cluster health. Callers must not treat successful local execution as a distributed commit.
