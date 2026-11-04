# sqlite

SQLite product facade for **format-3 import/export only**.

SQL dialect over `.yydb` is [`yydb::sql::SqliteConnection`](https://docs.rs/yydb) —
it must not be pulled into this crate (keeps the SQLite product free of the YYDB engine).

## Read-only upstream file inspection

`SqliteDatabase` reads an existing SQLite format-3 file so its schema and rows
can be adopted into YYDB / Iris. `sqlite-storage` owns the main-file runtime
(file/memory handle, schema table reads, rowid B-tree scans, resource limits)
and `sqlite-formats` owns the byte-level format facts (header validation,
record decoding, encoding primitives). There is no SQL engine and no WAL
recovery on this path.

```rust
use sqlite::{SchemaObjectKind, SqliteDatabase};

let db = SqliteDatabase::open("app.sqlite")?;
let tables = db
    .schema(Default::default())?
    .into_iter()
    .filter(|o| o.kind == SchemaObjectKind::Table);
let rows = db.table("samples", Default::default())?;
```

## Writable snapshot builder

`SqliteWriter` builds new format-3 files for fixtures and migration hand-off.

No SQLite C library. No YYDB engine dependency.
