//! Sequence allocation for `BIGSERIAL` / `AUTO_INCREMENT` inserts.
//!
//! The binder works from the published catalog, which describes columns but not
//! value ownership. After binding, the gateway fills every omitted sequence
//! column from the local schema registry so clients can write the same INSERT
//! they would write against PostgreSQL or MySQL.
//!
//! Allocation is deliberately not transactional: the counter advances before
//! the write executes and survives a rollback, matching PostgreSQL `nextval`
//! and MySQL `AUTO_INCREMENT`. A supplied value never advances the sequence,
//! exactly as an explicit `INSERT ... (id) VALUES (...)` does against a real
//! server.

use crate::{
    catalog_bind::{SqlCatalogBoundColumn, SqlCatalogBoundExpression, SqlCatalogBoundStatement},
    sql::{SqlBoundLiteral, SqlFrontendError},
    sql_schema::SqlSchemaRegistry,
};

/// Fills omitted sequence columns of one bound INSERT.
///
/// Only `INSERT ... VALUES` is rewritten. `INSERT ... SELECT` and statements
/// that already name every sequence column pass through unchanged.
pub fn apply_auto_increment(
    registry: &SqlSchemaRegistry,
    statement: SqlCatalogBoundStatement,
) -> Result<SqlCatalogBoundStatement, SqlFrontendError> {
    let SqlCatalogBoundStatement::Insert(mut insert) = statement
    else {
        return Ok(statement);
    };
    if insert.select.is_some() {
        return Ok(SqlCatalogBoundStatement::Insert(insert));
    }
    let supplied: Vec<u64> = insert.fields.iter().map(|field| field.field_id).collect();
    let missing: Vec<_> = registry
        .auto_increment_fields(insert.table.type_id)
        .into_iter()
        .filter(|field| !supplied.contains(&field.field_id))
        .collect();
    if missing.is_empty() {
        return Ok(SqlCatalogBoundStatement::Insert(insert));
    }
    for field in &missing {
        insert.fields.push(SqlCatalogBoundColumn {
            type_id: insert.table.type_id,
            field_id: field.field_id,
            row_id: field.field_id,
            table_name: insert.table.name.clone(),
            field_name: field.name.clone(),
            visible_name: field.name.clone(),
            capacity: field.capacity,
        });
    }
    for row in &mut insert.rows {
        for field in &missing {
            let value = registry.allocate_auto_increment(insert.table.type_id, field.field_id)?;
            row.push(SqlCatalogBoundExpression::Literal(SqlBoundLiteral::Integer(value)));
        }
    }
    Ok(SqlCatalogBoundStatement::Insert(insert))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        bind_sql_catalog,
        sql::SqlDialect,
        sql_ddl::{parse_ddl, SqlDdlStatement},
    };

    fn registry_with(table: &str) -> SqlSchemaRegistry {
        let registry = SqlSchemaRegistry::new_in_memory();
        let statement = parse_ddl(table, SqlDialect::Postgres).expect("DDL parses").expect("DDL statement");
        registry.apply_ddl(&statement).expect("DDL applies");
        registry
    }

    fn bind(registry: &SqlSchemaRegistry, sql: &str, dialect: SqlDialect) -> SqlCatalogBoundStatement {
        let catalog = registry.catalog();
        bind_sql_catalog(sql, &catalog, catalog.schema_version(), dialect).expect("bind succeeds")
    }

    /// Allocates one sequence value through the same path a gateway write uses.
    fn generated_id(registry: &SqlSchemaRegistry, sql: &str) -> i64 {
        let bound = bind(registry, sql, SqlDialect::Postgres);
        let SqlCatalogBoundStatement::Insert(insert) = apply_auto_increment(registry, bound).expect("allocation succeeds")
        else {
            panic!("expected an INSERT");
        };
        match insert.rows[0].last() {
            Some(SqlCatalogBoundExpression::Literal(SqlBoundLiteral::Integer(value))) => *value,
            other => panic!("expected an allocated integer, got {other:?}"),
        }
    }

    fn unique_path() -> std::path::PathBuf {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|elapsed| elapsed.as_nanos()).unwrap_or(0);
        std::env::temp_dir().join(format!("yyds-sql-sequence-{}-{nanos}.json", std::process::id()))
    }

    #[test]
    fn omitted_bigserial_column_gets_the_next_value() {
        let registry = registry_with("CREATE TABLE \"events\" (\"id\" BIGSERIAL PRIMARY KEY, \"name\" TEXT NOT NULL)");
        let bound = bind(&registry, "INSERT INTO \"events\" (\"name\") VALUES ('first')", SqlDialect::Postgres);
        let SqlCatalogBoundStatement::Insert(insert) = apply_auto_increment(&registry, bound).expect("allocation succeeds")
        else {
            panic!("expected an INSERT");
        };
        assert_eq!(insert.fields.len(), 2);
        assert_eq!(insert.fields[1].field_name, "id");
        assert_eq!(insert.rows[0][1], SqlCatalogBoundExpression::Literal(SqlBoundLiteral::Integer(1)));
    }

    #[test]
    fn supplied_serial_value_does_not_advance_the_sequence() {
        let registry = registry_with("CREATE TABLE \"events\" (\"id\" BIGSERIAL PRIMARY KEY, \"name\" TEXT NOT NULL)");
        let bound = bind(&registry, "INSERT INTO \"events\" (\"id\", \"name\") VALUES (7, 'explicit')", SqlDialect::Postgres);
        let SqlCatalogBoundStatement::Insert(insert) = apply_auto_increment(&registry, bound).expect("allocation succeeds")
        else {
            panic!("expected an INSERT");
        };
        assert_eq!(insert.fields.len(), 2);
        let next = bind(&registry, "INSERT INTO \"events\" (\"name\") VALUES ('generated')", SqlDialect::Postgres);
        let SqlCatalogBoundStatement::Insert(insert) = apply_auto_increment(&registry, next).expect("allocation succeeds")
        else {
            panic!("expected an INSERT");
        };
        assert_eq!(insert.rows[0][1], SqlCatalogBoundExpression::Literal(SqlBoundLiteral::Integer(1)));
    }

    #[test]
    fn mysql_auto_increment_column_is_filled_the_same_way() {
        let registry = SqlSchemaRegistry::new_in_memory();
        let ddl = parse_ddl(
            "CREATE TABLE `jobs` (`id` BIGINT NOT NULL AUTO_INCREMENT, `name` TEXT NOT NULL, PRIMARY KEY (`id`))",
            SqlDialect::Mysql,
        )
        .expect("DDL parses")
        .expect("DDL statement");
        assert!(matches!(ddl, SqlDdlStatement::CreateTable { .. }));
        registry.apply_ddl(&ddl).expect("DDL applies");
        let bound = bind(&registry, "INSERT INTO `jobs` (`name`) VALUES ('queued')", SqlDialect::Mysql);
        let SqlCatalogBoundStatement::Insert(insert) = apply_auto_increment(&registry, bound).expect("allocation succeeds")
        else {
            panic!("expected an INSERT");
        };
        assert_eq!(insert.rows[0][1], SqlCatalogBoundExpression::Literal(SqlBoundLiteral::Integer(1)));
    }

    #[test]
    fn consecutive_inserts_receive_consecutive_values() {
        let registry = registry_with("CREATE TABLE \"events\" (\"id\" BIGSERIAL PRIMARY KEY, \"name\" TEXT NOT NULL)");
        for expected in 1..=3 {
            let value = generated_id(&registry, "INSERT INTO \"events\" (\"name\") VALUES ('row')");
            assert_eq!(value, expected);
        }
    }

    #[test]
    fn two_connections_never_allocate_the_same_value() {
        let registry =
            Arc::new(registry_with("CREATE TABLE \"events\" (\"id\" BIGSERIAL PRIMARY KEY, \"name\" TEXT NOT NULL)"));
        let workers: Vec<_> = (0..2)
            .map(|_| {
                let registry = Arc::clone(&registry);
                std::thread::spawn(move || {
                    (0..25)
                        .map(|_| generated_id(&registry, "INSERT INTO \"events\" (\"name\") VALUES ('row')"))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut values: Vec<i64> = workers.into_iter().flat_map(|worker| worker.join().expect("worker")).collect();
        values.sort_unstable();
        assert_eq!(values, (1..=50).collect::<Vec<_>>());
    }

    #[test]
    fn reopen_continues_the_persisted_sequence() {
        let path = unique_path();
        let ddl = "CREATE TABLE \"events\" (\"id\" BIGSERIAL PRIMARY KEY, \"name\" TEXT NOT NULL)";
        {
            let registry = SqlSchemaRegistry::open(&path).expect("open registry");
            let statement = parse_ddl(ddl, SqlDialect::Postgres).expect("DDL parses").expect("DDL statement");
            registry.apply_ddl(&statement).expect("DDL applies");
            assert_eq!(generated_id(&registry, "INSERT INTO \"events\" (\"name\") VALUES ('row')"), 1);
            assert_eq!(generated_id(&registry, "INSERT INTO \"events\" (\"name\") VALUES ('row')"), 2);
        }
        {
            let registry = SqlSchemaRegistry::open(&path).expect("reopen registry");
            assert_eq!(generated_id(&registry, "INSERT INTO \"events\" (\"name\") VALUES ('row')"), 3);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rolled_back_allocation_is_not_handed_out_again() {
        let registry = registry_with("CREATE TABLE \"events\" (\"id\" BIGSERIAL PRIMARY KEY, \"name\" TEXT NOT NULL)");
        let checkpoint = registry.checkpoint();
        assert_eq!(generated_id(&registry, "INSERT INTO \"events\" (\"name\") VALUES ('rolled back')"), 1);
        registry.restore(checkpoint).expect("rollback restores schema");
        assert_eq!(generated_id(&registry, "INSERT INTO \"events\" (\"name\") VALUES ('committed')"), 2);
    }
}
