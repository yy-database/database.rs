//! Client-side `.yyds` catalog plus `.yykv` shard store.
//!
//! Both byte formats are owned by `database-formats`, and the shard write path
//! is owned by `database-storage`: [`Store`] is only an adapter around
//! [`database_storage::cluster_kv::FileShard`]. The snapshot bytes are the
//! shared `.yykv` codec (magic `YYKV\x03`, FNV-1a body checksum, then
//! `next_revision u64`, record count `u32`, and records made of namespace, key,
//! revision `u64`, deadline `u64`, value tag `u8` and value bytes), which
//! `FileShard` writes through a temp file and an atomic replace on every
//! mutation.
//!
//! This module only keeps what PostgreSQL owns:
//!
//! - the catalog file (`local-0.yyds`) itself, written as the shared
//!   contract-layout document (magic `YYDS\x01`, little-endian version, schema
//!   slot, routing slot, shard registrations, resolved contract and owners);
//! - the catalog shard id (`<id>.yykv` next to it) and the explicit rejection
//!   of legacy big-endian catalogs (`postgres-0.yyds`), which are never sniffed
//!   or guessed;
//! - namespace validation, because `database-formats`' `Key::new` accepts any
//!   namespace string and the empty/NUL checks are this crate's contract;
//! - the exact error wording PostgreSQL users already see for shard failures,
//!   mapped from the shared codec's reasons.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

use database_formats::yy::yykv::{KV_LEGACY_MAGIC, KV_MAGIC, Snapshot, decode_snapshot_strict};
use database_storage::cluster_kv::{FileShard, InlineValue, Key, KvStore, StoredValue};
use database_types::{
    ReasonCode,
    cluster::{Error as ClusterError, Namespace},
};

use super::{
    error::PostgresError,
    postmaster::{CATALOG_FILE, LEGACY_CATALOG_FILE, catalog_shard_id, encode_catalog_bytes, write_atomic},
};

fn io_error(error: impl std::fmt::Display) -> PostgresError {
    PostgresError::Io(error.to_string())
}

fn protocol(message: &str) -> PostgresError {
    PostgresError::Protocol(message.to_owned())
}

fn storage_error(error: ClusterError) -> PostgresError {
    match error {
        ClusterError::Io(error) => io_error(error),
        other => protocol(&other.to_string()),
    }
}

/// Re-words a shared `.yykv` codec failure as the PostgreSQL-facing message.
///
/// The shared codec names the format without a product prefix; PostgreSQL
/// messages have always called it `.yykv`, so the mapping keeps the external
/// wording byte-for-byte while the decoder itself stays in `database-formats`.
fn shard_error(error: ClusterError) -> PostgresError {
    match error {
        ClusterError::Io(error) => io_error(error.to_string()),
        ClusterError::Corrupt(reason) => protocol(shard_message(reason)),
        ClusterError::CorruptReason { message, .. } => protocol(shard_message(message)),
        other => protocol(&other.to_string()),
    }
}

fn shard_message(message: &'static str) -> &'static str {
    match message {
        "unknown yykv header" => "unknown .yykv header",
        "yykv checksum mismatch" => ".yykv checksum mismatch",
        "legacy yykv v2 snapshot has no checksum: delete the shard file and let the node recreate it" => {
            "legacy .yykv v2 snapshot carries no checksum: delete the shard and let the node recreate it"
        }
        // The shared decoder reports both pre-contract revision layouts as legacy;
        // PostgreSQL has always folded them into one message.
        "legacy yykv revision layout: next_revision is zero" | "legacy yykv revision layout: record sits on next_revision" => {
            "invalid shard revision"
        }
        "shard namespace utf8" => "shard namespace is not utf8",
        "invalid shard value kind" => "unknown shard record tag",
        "unexpected shard eof" => "truncated shard record",
        other => other,
    }
}

/// Validates a `.yykv` snapshot and reports its record count and next revision.
///
/// The bytes are decoded by the shared codec; only the PostgreSQL-facing
/// classification and the inline-only record model stay here.
fn decode_shard(bytes: &[u8]) -> Result<(usize, u64), PostgresError> {
    // Header checks stay here so the shared decoder's generic EOF/legacy
    // classification keeps mapping onto the messages PostgreSQL has always
    // returned for these shard shapes.
    if bytes.len() < KV_MAGIC.len() {
        return Err(protocol("truncated .yykv header"));
    }
    if bytes.starts_with(KV_LEGACY_MAGIC) {
        return Err(protocol("legacy .yykv v2 snapshot carries no checksum: delete the shard and let the node recreate it"));
    }
    if !bytes.starts_with(KV_MAGIC) {
        return Err(protocol("unknown .yykv header"));
    }
    if bytes.len() < KV_MAGIC.len() + 8 {
        return Err(protocol("truncated shard checksum"));
    }
    let body = &bytes[KV_MAGIC.len() + 8..];
    let Snapshot { next_revision, records } = decode_snapshot_strict(bytes).map_err(|error| {
        // The shared decoder reports one generic EOF class; PostgreSQL has always
        // split it by whether the fixed shard header itself fit.
        if matches!(error, ClusterError::CorruptReason { reason: ReasonCode::Truncated, .. }) {
            return protocol(if body.len() < 12 { "truncated shard header" } else { "truncated shard record" });
        }
        shard_error(error)
    })?;
    for record in records.values() {
        if record.expires_at.is_some() {
            return Err(protocol("expired shard records are unsupported"));
        }
        if !matches!(record.value, StoredValue::Inline(_)) {
            return Err(protocol("unknown shard record tag"));
        }
    }
    Ok((records.len(), next_revision))
}

/// A durable namespace/key store backed by one `.yyds` catalog and one `.yykv` shard.
///
/// Every mutation is delegated to the shard's own durable write path, so the
/// bytes on disk are exactly what `FileShard` writes.
pub struct Store {
    dir: PathBuf,
    catalog: PathBuf,
    shard_path: PathBuf,
    shard: Mutex<FileShard>,
}

impl Store {
    /// Opens or creates the store directory.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, PostgresError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(io_error)?;
        let catalog = dir.join(CATALOG_FILE);
        let legacy = dir.join(LEGACY_CATALOG_FILE);
        if legacy.exists() {
            return Err(protocol("legacy PostgreSQL catalog layout: delete the store directory and reinitialize"));
        }
        let shard_id = catalog_shard_id(&catalog)?;
        let shard_path = dir.join(format!("{shard_id}.yykv"));
        if !catalog.exists() {
            write_atomic(&catalog, &encode_catalog_bytes(&shard_id)?)?;
        }
        // Validate the on-disk bytes with the PostgreSQL-facing wording before
        // handing the file to the shared shard implementation, which reports
        // its own generic reasons.
        if shard_path.exists() {
            let bytes = fs::read(&shard_path).map_err(io_error)?;
            decode_shard(&bytes)?;
        }
        let shard = FileShard::open(&shard_path).map_err(storage_error)?;
        Ok(Self { dir, catalog, shard_path, shard: Mutex::new(shard) })
    }

    /// Absolute directory the store lives in.
    pub fn data_dir(&self) -> String {
        self.dir.display().to_string()
    }

    /// Absolute `.yykv` shard path.
    pub fn shard_path(&self) -> String {
        self.shard_path.display().to_string()
    }

    /// Absolute `.yyds` catalog path.
    pub fn catalog_path(&self) -> String {
        self.catalog.display().to_string()
    }

    /// Writes one row, assigning the next revision.
    pub fn put(&self, namespace: &str, key: &[u8], value: &[u8]) -> Result<(), PostgresError> {
        validate_namespace(namespace)?;
        let mut shard = self.shard.lock().expect("store lock");
        shard.put(Key::new(namespace, key.to_vec()), StoredValue::Inline(InlineValue(value.to_vec()))).map(|_| ()).map_err(storage_error)
    }

    /// Reads one row, or `None` when the key is absent.
    pub fn get(&self, namespace: &str, key: &[u8]) -> Option<Vec<u8>> {
        let shard = self.shard.lock().expect("store lock");
        let record = shard.get(&Key::new(namespace, key.to_vec())).ok().flatten()?;
        match record.value {
            StoredValue::Inline(value) => Some(value.0),
            _ => None,
        }
    }

    /// Removes one row and reports whether it existed.
    pub fn delete(&self, namespace: &str, key: &[u8]) -> Result<bool, PostgresError> {
        let mut shard = self.shard.lock().expect("store lock");
        shard.delete(&Key::new(namespace, key.to_vec())).map_err(storage_error)
    }

    /// Keys of one namespace with the given byte prefix, ordered by key.
    pub fn keys(&self, namespace: &str, prefix: &[u8]) -> Vec<Vec<u8>> {
        self.rows(namespace, prefix).into_iter().map(|(key, _)| key).collect()
    }

    /// Key/value pairs of one namespace with the given byte prefix.
    pub fn scan(&self, namespace: &str, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.rows(namespace, prefix)
    }

    fn rows(&self, namespace: &str, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let shard = self.shard.lock().expect("store lock");
        let records = shard.scan(&Namespace(namespace.to_owned()), prefix, None, None, 0).unwrap_or_default();
        records
            .into_iter()
            .filter_map(|record| match record.value {
                StoredValue::Inline(value) => Some((record.key.bytes, value.0)),
                _ => None,
            })
            .collect()
    }

    /// Distinct namespaces that currently hold at least one row.
    pub fn namespaces(&self) -> Vec<String> {
        let shard = self.shard.lock().expect("store lock");
        shard.namespaces().unwrap_or_default().into_iter().map(|namespace| namespace.0).collect()
    }

    /// Number of rows in one namespace.
    pub fn len(&self, namespace: &str) -> usize {
        let shard = self.shard.lock().expect("store lock");
        shard.len(&Namespace(namespace.to_owned())).unwrap_or_default()
    }

    /// Whether the namespace holds no rows.
    pub fn is_empty(&self, namespace: &str) -> bool {
        self.len(namespace) == 0
    }

    /// Removes every row of one namespace and returns how many were removed.
    pub fn flush(&self, namespace: &str) -> Result<usize, PostgresError> {
        let mut shard = self.shard.lock().expect("store lock");
        // `FileShard::flush()` is the inherent "persist now" helper, so the
        // namespace flush must be called through the trait explicitly.
        KvStore::flush(&mut *shard, &Namespace(namespace.to_owned())).map_err(storage_error)
    }
}

fn validate_namespace(namespace: &str) -> Result<(), PostgresError> {
    if namespace.is_empty() {
        return Err(protocol("namespace must not be empty"));
    }
    if namespace.as_bytes().contains(&0) {
        return Err(protocol("namespace must not contain a NUL byte"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use database_formats::yy::{
        yyds::{CATALOG_MAGIC, encode_catalog},
        yykv::{InlineValue as TestInlineValue, Key as TestKey, Record as ShardRecord, encode_snapshot},
    };
    use database_types::cluster::ShardId;

    use crate::pgsql::postmaster::{DEFAULT_SHARD_ID, decode_catalog_shard_id};

    /// Every test gets its own directory under the test binary directory.
    fn temporary(label: &str) -> PathBuf {
        let mut path = std::env::current_exe().expect("test binary path");
        path.pop();
        if path.ends_with("deps") {
            path.pop();
        }
        path.push(format!("postgres-store-{label}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        path
    }

    #[test]
    fn namespace_validation_stays_on_the_binding_side() {
        let dir = temporary("namespace");
        let store = Store::open(&dir).unwrap();
        assert_eq!(store.put("", b"k", b"v").unwrap_err().message(), "namespace must not be empty");
        assert_eq!(store.put("public.\0bad", b"k", b"v").unwrap_err().message(), "namespace must not contain a NUL byte");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn flush_removes_one_namespace_and_keeps_the_revision_counter() {
        let dir = temporary("flush");
        let store = Store::open(&dir).unwrap();
        store.put("public.users", b"u1", b"alice").unwrap();
        store.put("public.users", b"u2", b"bob").unwrap();
        store.put("public.audit", b"a1", b"one").unwrap();
        assert_eq!(store.flush("public.users").unwrap(), 2);
        store.put("public.users", b"u3", b"carol").unwrap();
        drop(store);

        let reopened = Store::open(&dir).unwrap();
        assert_eq!(reopened.get("public.users", b"u1"), None, "flushed rows stay gone after reopen");
        assert_eq!(reopened.get("public.audit", b"a1"), Some(b"one".to_vec()));
        assert_eq!(reopened.get("public.users", b"u3"), Some(b"carol".to_vec()));
        let snapshot = decode_snapshot_strict(&fs::read(reopened.shard_path()).unwrap()).unwrap();
        assert_eq!(snapshot.next_revision, 5, "flush must not reuse a flushed revision");
        drop(reopened);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn round_trip_keeps_revision_order() {
        let dir = temporary("roundtrip");
        let store = Store::open(&dir).unwrap();
        store.put("public.users", b"u1", b"alice").unwrap();
        store.put("public.users", b"u2", b"bob").unwrap();
        drop(store);
        let reopened = Store::open(&dir).unwrap();
        assert_eq!(reopened.get("public.users", b"u1"), Some(b"alice".to_vec()));
        assert_eq!(reopened.namespaces(), vec!["public.users".to_owned()]);
        let bytes = fs::read(reopened.shard_path()).unwrap();
        assert_eq!(&bytes[..5], KV_MAGIC);
        let (records, next) = decode_shard(&bytes).unwrap();
        assert_eq!(next, 3);
        assert_eq!(records, 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn checksum_rejects_tampering() {
        let bytes = b"YYKV\x03\0\0\0\0\0\0\0\0";
        assert_eq!(decode_shard(bytes).unwrap_err().message(), ".yykv checksum mismatch");
    }

    #[test]
    fn catalog_matches_shared_contract_layout() {
        let bytes = encode_catalog_bytes(DEFAULT_SHARD_ID).unwrap();
        assert_eq!(&bytes[..5], CATALOG_MAGIC);
        assert_eq!(&bytes[5..9], &[0, 0, 0, 0], "version is little endian");
        assert_eq!(&bytes[9..13], &[0, 0, 0, 0], "schema slot absent");
        assert_eq!(&bytes[13..17], &[0, 0, 0, 0], "routing slot absent");
        assert_eq!(&bytes[17..21], &[1, 0, 0, 0], "one shard registration");
        assert_eq!(&bytes[21..25], &[7, 0, 0, 0], "shard id length");
        assert_eq!(&bytes[25..32], b"local-0");
        assert_eq!(&bytes[32..36], &[0, 0, 0, 0], "resolved contract absent");
        assert_eq!(&bytes[36..40], &[0, 0, 0, 0], "no owners");
        assert_eq!(bytes.len(), 40);
        assert_eq!(decode_catalog_shard_id(&bytes).unwrap(), DEFAULT_SHARD_ID);
    }

    #[test]
    fn legacy_big_endian_catalog_is_rejected() {
        let legacy = b"YYDS\x01\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\x0apostgres-0\0\0\0\0\0\0\0\0".to_vec();
        assert_eq!(
            decode_catalog_shard_id(&legacy).unwrap_err().message(),
            "legacy PostgreSQL catalog layout: delete the store directory and reinitialize"
        );
    }

    #[test]
    fn shared_codec_decodes_and_reencodes_store_catalog_bytes() {
        let dir = temporary("catalog-interop-out");
        let store = Store::open(&dir).unwrap();
        let bytes = fs::read(store.catalog_path()).unwrap();
        let shard_id = decode_catalog_shard_id(&bytes).unwrap();
        assert_eq!(shard_id, DEFAULT_SHARD_ID);
        assert_eq!(encode_catalog(None, None, &[ShardId(shard_id)], None, &[]).unwrap(), bytes);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn shared_codec_catalog_opens_as_a_store() {
        let dir = temporary("catalog-interop-in");
        let bytes = encode_catalog(None, None, &[ShardId("custom-7".to_owned())], None, &[]).unwrap();
        let hex = bytes.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        // Same catalog bytes the TS and PY corpora seed for `catalog-authoritative-shard-id`.
        assert_eq!(hex, "59594453010000000000000000000000000100000008000000637573746f6d2d370000000000000000");

        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(CATALOG_FILE), &bytes).unwrap();
        let shard = encode_snapshot(&BTreeMap::from([shared_record("public.seed", b"k", b"v", 1)]), 2).unwrap();
        fs::write(dir.join("custom-7.yykv"), &shard).unwrap();

        let store = Store::open(&dir).unwrap();
        assert_eq!(Path::new(&store.shard_path()).file_name().unwrap().to_string_lossy(), "custom-7.yykv");
        assert_eq!(store.get("public.seed", b"k"), Some(b"v".to_vec()));
        assert_eq!(fs::read(store.catalog_path()).unwrap(), bytes, "opening must not rewrite shared catalog bytes");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn nul_shard_id_stays_unusable() {
        let dir = temporary("catalog-interop-nul");
        let bytes = encode_catalog(None, None, &[ShardId("a\0b".to_owned())], None, &[]).unwrap();
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(CATALOG_FILE), &bytes).unwrap();
        assert_eq!(Store::open(&dir).err().unwrap().message(), "catalog shard id is unusable");
        let _ = fs::remove_dir_all(&dir);
    }

    /// Assembles one shared-codec record, i.e. exactly what a node outside this
    /// repo writes when `database-formats` owns the shard.
    fn shared_record(namespace: &str, key: &[u8], value: &[u8], revision: u64) -> (TestKey, ShardRecord) {
        let key = TestKey::new(namespace, key.to_vec());
        let stored = ShardRecord {
            key: key.clone(),
            value: StoredValue::Inline(TestInlineValue(value.to_vec())),
            revision,
            expires_at: None,
        };
        (key, stored)
    }

    #[test]
    fn shared_codec_decodes_and_reencodes_store_bytes() {
        let dir = temporary("interop-out");
        let store = Store::open(&dir).unwrap();
        store.put("public.users", b"u1", b"alice").unwrap();
        store.put("public.users", b"u2", b"bob").unwrap();
        let bytes = fs::read(store.shard_path()).unwrap();
        let snapshot = decode_snapshot_strict(&bytes).unwrap();
        assert_eq!(snapshot.next_revision, 3);
        assert_eq!(
            snapshot.records[&TestKey::new("public.users", b"u1".to_vec())].value,
            StoredValue::Inline(TestInlineValue(b"alice".to_vec()))
        );
        assert_eq!(
            snapshot.records[&TestKey::new("public.users", b"u2".to_vec())].value,
            StoredValue::Inline(TestInlineValue(b"bob".to_vec()))
        );
        assert_eq!(encode_snapshot(&snapshot.records, snapshot.next_revision).unwrap(), bytes);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn shared_codec_snapshot_opens_as_a_store() {
        let dir = temporary("interop-in");
        Store::open(&dir).unwrap();
        let written_by_shared_codec =
            BTreeMap::from([shared_record("public.seed", b"a", b"1", 1), shared_record("public.seed", b"b", b"2", 2)]);
        let bytes = encode_snapshot(&written_by_shared_codec, 3).unwrap();
        fs::write(dir.join(format!("{DEFAULT_SHARD_ID}.yykv")), &bytes).unwrap();

        let store = Store::open(&dir).unwrap();
        assert_eq!(store.get("public.seed", b"a"), Some(b"1".to_vec()));
        assert_eq!(store.get("public.seed", b"b"), Some(b"2".to_vec()));
        assert_eq!(fs::read(store.shard_path()).unwrap(), bytes, "opening must not rewrite shared bytes");

        store.put("public.seed", b"c", b"3").unwrap();
        let expected = BTreeMap::from([
            shared_record("public.seed", b"a", b"1", 1),
            shared_record("public.seed", b"b", b"2", 2),
            shared_record("public.seed", b"c", b"3", 3),
        ]);
        assert_eq!(fs::read(store.shard_path()).unwrap(), encode_snapshot(&expected, 4).unwrap());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_shard_matches_the_shared_corpus_bytes() {
        let dir = temporary("interop-empty");
        let store = Store::open(&dir).unwrap();
        let bytes = fs::read(store.shard_path()).unwrap();
        let hex = bytes.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        // Same empty-shard bytes the TS and PY corpora seed in `store.json`.
        assert_eq!(hex, "59594b5603e42b42c2392d245f010000000000000000000000");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn every_write_matches_a_full_encode() {
        let dir = temporary("incremental-bytes");
        let store = Store::open(&dir).unwrap();
        let big = [9_u8; 257];
        let long_key = [3_u8; 40];
        type Step<'a> = (&'a str, &'a [u8], Option<&'a [u8]>);
        let steps: Vec<Step<'_>> = vec![
            ("public.users", b"u2", Some(b"bob")),
            ("public.users", b"u1", Some(b"alice")),
            ("public.users", b"u3", Some(big.as_slice())),
            ("public.users", b"u2", Some(b"bobby")),
            ("public.users", b"u2", None),
            ("public.audit", b"a1", Some(b"")),
            ("public.audit", b"a0", Some(b"zero")),
            ("sys.utf8", "雪".as_bytes(), Some(long_key.as_slice())),
            ("public.users", b"u1", Some(b"alice-again")),
        ];
        let mut expected: BTreeMap<(String, Vec<u8>), (u64, Vec<u8>)> = BTreeMap::new();
        let mut next_revision = 1_u64;
        for (namespace, key, value) in steps {
            match value {
                Some(value) => {
                    store.put(namespace, key, value).unwrap();
                    expected.insert((namespace.to_owned(), key.to_vec()), (next_revision, value.to_vec()));
                    next_revision += 1;
                }
                None => {
                    store.delete(namespace, key).unwrap();
                    expected.remove(&(namespace.to_owned(), key.to_vec()));
                }
            }
            let written = fs::read(store.shard_path()).unwrap();
            let snapshot = expected
                .iter()
                .map(|((namespace, key), (revision, value))| shared_record(namespace, key, value, *revision))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(written, encode_snapshot(&snapshot, next_revision).unwrap(), "step {namespace}/{key:?}");
        }
        drop(store);
        let reopened = Store::open(&dir).unwrap();
        assert_eq!(reopened.get("public.users", b"u1"), Some(b"alice-again".to_vec()));
        assert_eq!(reopened.get("public.users", b"u2"), None);
        assert_eq!(reopened.get("sys.utf8", "雪".as_bytes()), Some(long_key.to_vec()));
        drop(reopened);
        let _ = fs::remove_dir_all(&dir);
    }
}
