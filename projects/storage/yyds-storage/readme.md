# yyds-kv

The smallest storage unit in YYDS: a shard-local key/value engine backed by `.yykv` files.

This crate is **not** the embedded `.yydb` engine. It owns shard placement, key encoding, and record envelopes for the distributed data plane. **VOS typing lives in the `.yyds` catalog**, not inside individual KV records. Higher layers (catalog replication, VOS executor) build on top of `yyds-kv`.

## `.yykv` snapshot format

One file holds one shard. All integers are little-endian.

| Field | Size | Meaning |
|-------|------|---------|
| magic | 5 bytes | `YYKV` plus format version `0x03` |
| checksum | 8 bytes | FNV-1a 64 of every byte after this field |
| body | variable | revision, record count, records |

Body layout:

1. `next_revision` (`u64`) — revisions are shard-local and strictly increase.
2. `record_count` (`u32`).
3. Records sorted by namespace then key bytes. Each record is:
   `namespace` (length-prefixed UTF-8), `key` (length-prefixed bytes),
   `revision` (`u64`), `expires_at` (`u64`, Unix milliseconds, `0` = no expiry),
   value kind (`0` = inline value, `1` = object reference), then either a
   length-prefixed value or a length-prefixed object hash string.

Length prefixes are `u32` byte counts; a value larger than `u32::MAX` is refused
with `Unsupported` rather than truncated.

### Durability and failure behaviour

- A commit writes a sibling temp file, `sync_all`s it, atomically replaces the
  shard, then syncs the parent directory where the platform requires it.
- A failed write before the rename leaves the previous snapshot intact and
  removes the temp file. Memory is published only after the durable commit
  returns `Ok`.
- Opening verifies the magic and checksum before parsing, so truncation or a
  flipped byte fails with `Corrupt` instead of returning damaged data.
- Version `0x02` snapshots predate the checksum and are rejected explicitly;
  delete the shard and let the node recreate it.

The checksum is an integrity check, not a security primitive. It detects
accidental corruption and truncation only.
