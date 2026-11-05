//! Physical-plan execution over the MySQL wire client.
//!
//! Status: COM_QUERY / binary row decode still live on the former `mysql` crate
//! path. They move onto [`database_client::mysql::Session`] next; until then
//! relational execute returns [`Error::Unsupported`].

use std::collections::HashSet;

use database_query::ir::PhysicalPlan;
use odm_types::{Row, RowWrite};

use crate::{Error, Result};

use super::Session;

pub(crate) fn execute_plan(_conn: &mut Session, _plan: &PhysicalPlan, _uuid_fields: &HashSet<(String, String)>) -> Result<Vec<Row>> {
    Err(Error::Unsupported("MySQL adapter execute_plan awaits yyds-gateway-mysql::client COM_QUERY (third-party mysql crate removed)".into()))
}

pub(crate) fn insert_row(_conn: &mut Session, _write: &RowWrite, _uuid_fields: &HashSet<(String, String)>) -> Result<()> {
    Err(Error::Unsupported("MySQL adapter insert awaits gateway wire client".into()))
}

pub(crate) fn update_row(_conn: &mut Session, _write: &RowWrite, _uuid_fields: &HashSet<(String, String)>) -> Result<u64> {
    Err(Error::Unsupported("MySQL adapter update awaits gateway wire client".into()))
}

pub(crate) fn delete_row(
    _conn: &mut Session,
    _table: &str,
    _primary_key: &str,
    _key: &odm_types::Value,
    _uuid_fields: &HashSet<(String, String)>,
) -> Result<u64> {
    Err(Error::Unsupported("MySQL adapter delete awaits gateway wire client".into()))
}
