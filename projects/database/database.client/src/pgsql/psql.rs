//! `psql` command-line primitives: argument normalization and output lines.
//!
//! These are pure transformations, so the TypeScript and Python command lines
//! can compose the same behavior from one implementation.

use super::client::QueryResult;

/// Effective `psql` command line options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PsqlOptions {
    /// Server host.
    pub host: String,
    /// Server port.
    pub port: u16,
    /// Role name.
    pub user: String,
    /// Database name.
    pub database: String,
    /// SQL text passed with `-c`.
    pub sql: String,
    /// Whether column headers and command tags are printed.
    pub headers: bool,
    /// Field separator between values.
    pub separator: String,
    /// Text printed for SQL `NULL`.
    pub null_value: String,
}

impl Default for PsqlOptions {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "postgres".into(),
            database: "postgres".into(),
            sql: String::new(),
            headers: true,
            separator: "|".into(),
            null_value: String::new(),
        }
    }
}

fn invalid(option: &str) -> String {
    format!("invalid value for {option}")
}

/// Parses the supported `psql` flags, rejecting anything else.
pub fn parse_arguments(args: &[String]) -> Result<PsqlOptions, String> {
    let mut options = PsqlOptions::default();
    let mut index = 0;
    while index < args.len() {
        let option = args[index].as_str();
        match option {
            "-t" | "--tuples-only" => {
                options.headers = false;
                index += 1;
            }
            "-F" | "--field-separator" => {
                index += 1;
                let value = args.get(index).ok_or_else(|| invalid(option))?;
                if value.contains('\0') {
                    return Err(invalid(option));
                }
                options.separator = value.clone();
                index += 1;
            }
            "-P" | "--pset" => {
                index += 1;
                let value = args.get(index).ok_or("only psql null output is configurable")?;
                let Some(null_value) = value.strip_prefix("null=")
                else {
                    return Err("only psql null output is configurable".into());
                };
                options.null_value = null_value.to_owned();
                index += 1;
            }
            _ => {
                let target = match option {
                    "-h" | "--host" => "host",
                    "-p" | "--port" => "port",
                    "-U" | "--username" => "user",
                    "-d" | "--dbname" => "database",
                    "-c" | "--command" => "sql",
                    _ => return Err(format!("unsupported option '{option}'")),
                };
                index += 1;
                let value = args.get(index).ok_or_else(|| invalid(option))?;
                if value.contains('\0') || value.len() > 64 * 1024 {
                    return Err(invalid(option));
                }
                match target {
                    "port" => {
                        let port = value.parse::<i64>().map_err(|_| "invalid port".to_owned())?;
                        if !(1..=65535).contains(&port) {
                            return Err("invalid port".into());
                        }
                        options.port = port as u16;
                    }
                    "sql" => options.sql = value.clone(),
                    "host" => options.host = value.clone(),
                    "user" => options.user = value.clone(),
                    _ => options.database = value.clone(),
                }
                index += 1;
            }
        }
    }
    if options.sql.is_empty() || options.user.is_empty() || options.database.is_empty() {
        return Err("a nonempty command, user and database are required".into());
    }
    Ok(options)
}

/// Turns result sets into the lines `psql` prints.
pub fn format_results(results: &[QueryResult], options: &PsqlOptions) -> Vec<String> {
    let mut lines = Vec::new();
    for result in results {
        if options.headers && !result.columns.is_empty() {
            lines.push(result.column_names().collect::<Vec<_>>().join(&options.separator));
        }
        for row in &result.rows {
            lines.push(
                row.iter()
                    .map(|value| value.as_deref().unwrap_or(options.null_value.as_str()))
                    .collect::<Vec<_>>()
                    .join(&options.separator),
            );
        }
        if result.columns.is_empty() && options.headers {
            lines.push(result.tag.clone());
        }
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgsql::client::Column;

    #[test]
    fn defaults_and_flags() {
        let options = parse_arguments(&["-t".into(), "-F".into(), ",".into(), "-c".into(), "SELECT 1".into()]).unwrap();
        assert!(!options.headers);
        assert_eq!(options.separator, ",");
        assert_eq!(options.port, 5432);
    }

    #[test]
    fn rejects_unknown_options() {
        assert_eq!(parse_arguments(&["--nope".into()]).unwrap_err(), "unsupported option '--nope'");
    }

    #[test]
    fn formats_nulls_and_tags() {
        let options = PsqlOptions { null_value: "NULL".into(), ..PsqlOptions::default() };
        let results = vec![
            QueryResult {
                columns: vec![Column { name: "a".into() }, Column { name: "b".into() }],
                rows: vec![vec![Some("1".into()), None]],
                tag: "SELECT 1".into(),
            },
            QueryResult { columns: vec![], rows: vec![], tag: "INSERT 0 1".into() },
        ];
        assert_eq!(format_results(&results, &options), vec!["a|b", "1|NULL", "INSERT 0 1"]);
    }
}
