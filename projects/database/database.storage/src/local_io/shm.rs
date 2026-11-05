//! `YYSH` v0 shared-memory coordination block.
//!
//! Byte layout lives in `database_formats::yy::yysm`; this module owns sidecar
//! path resolution and on-disk mirroring.

use std::path::{Path, PathBuf};

use database_formats::yy::yywl::{committed_tail_lsn, parse_wal_recover};
use database_types::Result;

pub use database_formats::yy::yysm::{SHM_BYTES, SHM_MAGIC, ShmBlock, encode_empty_shm, encode_shm, parse_shm};

/// Resolve `<main>-shm` for a database main file path.
pub fn shm_sidecar_path(main: &Path) -> PathBuf {
    let mut sidecar = main.as_os_str().to_owned();
    sidecar.push("-shm");
    PathBuf::from(sidecar)
}

/// Mirror committed WAL tail metadata into the `-shm` sidecar.
///
/// This path re-parses the whole WAL, so callers that already appended a
/// committed transaction and know its tail should use
/// [`sync_shm_from_commit`] instead.
pub fn sync_shm_from_wal(main: &Path, wal: &Path) -> Result<()> {
    let wal_bytes = std::fs::read(wal)?;
    let parsed = parse_wal_recover(&wal_bytes)?;
    write_shm(main, committed_tail_lsn(&parsed), wal_bytes.len() as u64)
}

/// Mirror a WAL tail that the writer already reported into the `-shm` sidecar.
///
/// `tail_lsn` must come from a successful append and `wal_file_bytes` must be
/// the sidecar length after that append, so the block is byte-identical to
/// what [`sync_shm_from_wal`] would derive from a fresh parse.
pub fn sync_shm_from_commit(main: &Path, tail_lsn: u64, wal_file_bytes: u64) -> Result<()> {
    write_shm(main, tail_lsn, wal_file_bytes)
}

fn write_shm(main: &Path, tail_lsn: u64, wal_file_bytes: u64) -> Result<()> {
    let block = ShmBlock { format_version: 0, writer_epoch: 0, active_readers: 0, wal_tail_lsn: tail_lsn, wal_file_bytes };
    std::fs::write(shm_sidecar_path(main), encode_shm(&block))?;
    Ok(())
}
