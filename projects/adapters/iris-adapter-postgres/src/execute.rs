//! Physical-plan execution over the PostgreSQL wire client.
//!
//! Status: simple-query / extended-query decode still lived on the former
//! `postgres` crate path. They move onto [`yyds_gateway_pgsql::client::Session`]
//! next; until then relational execute returns [`Error::Unsupported`].

use iris_ir::PhysicalPlan;
use iris_types::{Row, RowWrite, Value};

use crate::{Error, Result, Session};

pub(crate) fn execute_plan(_client: &mut Session, _plan: &PhysicalPlan) -> Result<Vec<Row>> {
    Err(Error::Unsupported(
        "PostgreSQL adapter execute_plan awaits yyds-gateway-pgsql::client simple query (third-party postgres crate removed)".into(),
    ))
}

pub(crate) fn insert_row(_client: &mut Session, _write: &RowWrite) -> Result<()> {
    Err(Error::Unsupported("PostgreSQL adapter insert awaits gateway wire client".into()))
}

pub(crate) fn update_row(_client: &mut Session, _write: &RowWrite) -> Result<u64> {
    Err(Error::Unsupported("PostgreSQL adapter update awaits gateway wire client".into()))
}

pub(crate) fn delete_row(_client: &mut Session, _table: &str, _primary_key: &str, _key: &Value) -> Result<u64> {
    Err(Error::Unsupported("PostgreSQL adapter delete awaits gateway wire client".into()))
}
