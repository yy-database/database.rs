# yyds-formats

On-disk codecs for YYDS:

| Module | Magic | Role |
|--------|-------|------|
| `catalog` | `YYDS\x01` | `.yyds` catalog bytes |
| `kv` | `YYKV\x03` | `.yykv` shard snapshot body |
| `replication` | `YYRL\x01` | durable replica-log frames |

Storage (`yyds-kv`) and control (`yyds-catalog`) own lifecycle and IO. This crate only owns encode/decode. MySQL / PostgreSQL / Redis disguise gateways do **not** introduce foreign on-disk formats — service compatibility rides YYDS storage.