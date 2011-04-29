//! DDL subset parser for the SQL compatibility gateways.
//!
//! The gateways only accept the DDL the Iris ORM adapter emits so a local schema
//! registry can stay in sync with the client's expectations. Every construct
//! outside that subset fails with a structured diagnostic naming it — this
//! parser never decides that an unknown clause is safe to ignore.

use crate::sql::{SqlDialect, SqlFrontendError};

/// One DDL statement the gateway can apply to a local schema registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlDdlStatement {
    /// `CREATE TABLE ...`
    CreateTable {
        /// Table name after the accepted `public.` schema prefix.
        name: String,
        /// Whether `IF NOT EXISTS` was written.
        if_not_exists: bool,
        /// Column definitions in declaration order.
        columns: Vec<DdlColumn>,
        /// Primary key columns collected from inline and table-level declarations.
        primary_key: Vec<String>,
        /// Unique constraints collected from table-level declarations.
        unique_constraints: Vec<Vec<String>>,
        /// `CHARSET` table option, as written.
        charset: Option<String>,
        /// `COLLATE` table option, as written.
        collation: Option<String>,
    },
    /// `ALTER TABLE ... ADD COLUMN ...`
    AlterTableAddColumn {
        /// Target table name.
        table: String,
        /// Column definition to append.
        column: DdlColumn,
        /// Whether `IF NOT EXISTS` was written.
        if_not_exists: bool,
    },
    /// `DROP TABLE ...`
    DropTable {
        /// Tables to drop.
        names: Vec<String>,
        /// Whether `IF EXISTS` was written.
        if_exists: bool,
    },
    /// `CREATE [UNIQUE] INDEX ...`
    CreateIndex {
        /// Index name.
        name: String,
        /// Indexed table.
        table: String,
        /// Whether `IF NOT EXISTS` was written.
        if_not_exists: bool,
        /// Whether `UNIQUE` was written.
        unique: bool,
        /// Indexed columns in declaration order.
        columns: Vec<String>,
    },
    /// `DROP INDEX ...`
    DropIndex {
        /// Index name.
        name: String,
        /// Table name (MySQL requires it, PostgreSQL does not).
        table: Option<String>,
        /// Whether `IF EXISTS` was written.
        if_exists: bool,
    },
}

/// One column definition in a `CREATE TABLE` / `ALTER TABLE ADD COLUMN`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlColumn {
    /// Column name.
    pub name: String,
    /// Raw SQL type text as written, normalised to uppercase words with no spaces
    /// inside parameter lists, e.g. `TINYINT(1)`, `DOUBLE PRECISION`, `VARCHAR(191)`.
    pub sql_type: String,
    /// Whether `NOT NULL` was written.
    pub not_null: bool,
    /// Whether `PRIMARY KEY` was written inline.
    pub inline_primary_key: bool,
    /// Whether the column allocates its own values: MySQL `AUTO_INCREMENT` or
    /// PostgreSQL `SERIAL` / `BIGSERIAL` / `SMALLSERIAL`.
    pub auto_increment: bool,
    /// Raw `DEFAULT` literal text, when written.
    pub default: Option<String>,
}

/// Collations the gateway accepts as metadata.
///
/// Text is always compared by exact code points, so the declared collation
/// never changes query semantics. Names that drivers emit by default (for
/// example `utf8mb4_0900_ai_ci` from mysql-connector-python) are accepted so a
/// stock client can connect, and the registry stores them as metadata only.
pub const ACCEPTED_COLLATIONS: &[&str] = &[
    "utf8mb4_bin",
    "utf8mb4_0900_bin",
    "utf8mb4_0900_ai_ci",
    "utf8mb4_0900_as_ci",
    "utf8mb4_0900_as_cs",
    "utf8mb4_general_ci",
    "utf8mb4_unicode_ci",
    "utf8mb4_unicode_520_ci",
    "utf8_bin",
    "utf8_general_ci",
    "utf8_unicode_ci",
];

/// Whether `name` is an accepted UTF-8 collation name.
///
/// Used by both DDL (`COLLATE = ...`) and the MySQL session `SET NAMES`
/// command so a connection and a table cannot disagree about accepted names.
pub fn is_accepted_collation(name: &str) -> bool {
    ACCEPTED_COLLATIONS.iter().any(|candidate| candidate.eq_ignore_ascii_case(name))
}

/// Constraint keywords that terminate a column type.
const CONSTRAINT_KEYWORDS: &[&str] =
    &["NOT", "NULL", "PRIMARY", "UNIQUE", "DEFAULT", "CHECK", "REFERENCES", "COLLATE", "AUTO_INCREMENT", "COMMENT", "GENERATED"];

/// Returns true when the first SQL keyword is `CREATE`, `ALTER`, or `DROP`.
///
/// Whitespace, `--` line comments, and `/* */` block comments are skipped. Any
/// other leading keyword means the statement is not DDL and belongs to the
/// regular SQL path.
pub fn is_ddl_statement(source: &str) -> bool {
    matches!(leading_keyword(source).as_deref(), Some("CREATE") | Some("ALTER") | Some("DROP"))
}

/// Parses one DDL statement, or returns `Ok(None)` when the source is not DDL.
pub fn parse_ddl(source: &str, dialect: SqlDialect) -> Result<Option<SqlDdlStatement>, SqlFrontendError> {
    if !is_ddl_statement(source) {
        return Ok(None);
    }
    let tokens = tokenize(source, dialect)?;
    if tokens.is_empty() {
        return Ok(None);
    }
    let mut parser = Parser { tokens, cursor: 0, dialect };
    let statement = parser.statement()?;
    parser.finish()?;
    Ok(Some(statement))
}

/// Reads the first keyword without tokenizing the rest of the statement.
fn leading_keyword(source: &str) -> Option<String> {
    let bytes = source.as_bytes();
    let mut index = 0;
    loop {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index >= bytes.len() {
            return None;
        }
        if bytes[index] == b'-' && bytes.get(index + 1) == Some(&b'-') {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            while index < bytes.len() && !(bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/')) {
                index += 1;
            }
            // An unterminated block comment cannot expose a keyword.
            if index >= bytes.len() {
                return None;
            }
            index += 2;
            continue;
        }
        let start = index;
        while index < bytes.len() && is_word_byte(bytes[index]) {
            index += 1;
        }
        if start == index {
            return None;
        }
        return Some(source[start..index].to_ascii_uppercase());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    /// Bare word: letters, digits, `_`, `$`.
    Word(String),
    /// Backtick-quoted identifier (MySQL only).
    Backtick(String),
    /// Double-quoted identifier (PostgreSQL only).
    Quoted(String),
    /// Single-quoted string literal, unescaped.
    String(String),
    /// Numeric literal, including a leading `-`.
    Number(String),
    /// `(`, `)`, `,`, `;`, `=`, or `.`.
    Punct(char),
}

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$'
}

fn token_text(token: &Token) -> String {
    match token {
        Token::Word(text) | Token::Number(text) => text.clone(),
        Token::Backtick(text) => format!("`{text}`"),
        Token::Quoted(text) => format!("\"{text}\""),
        Token::String(text) => format!("'{text}'"),
        Token::Punct(ch) => ch.to_string(),
    }
}

fn tokenize(source: &str, dialect: SqlDialect) -> Result<Vec<Token>, SqlFrontendError> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
            continue;
        }
        if byte == b'-' && bytes.get(index + 1) == Some(&b'-') {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            let mut closed = false;
            while index + 1 < bytes.len() {
                if bytes[index] == b'*' && bytes[index + 1] == b'/' {
                    index += 2;
                    closed = true;
                    break;
                }
                index += 1;
            }
            if !closed {
                return Err(SqlFrontendError::syntax("unclosed `/* */` comment in DDL statement"));
            }
            continue;
        }
        match byte {
            b'`' => {
                if dialect != SqlDialect::Mysql {
                    return Err(SqlFrontendError::syntax(
                        "backtick-quoted identifiers are MySQL syntax and are not accepted in PostgreSQL DDL",
                    ));
                }
                index += 1;
                let start = index;
                while index < bytes.len() && bytes[index] != b'`' {
                    index += 1;
                }
                if index >= bytes.len() {
                    return Err(SqlFrontendError::syntax("unclosed backtick-quoted identifier in DDL statement"));
                }
                tokens.push(Token::Backtick(source[start..index].to_string()));
                index += 1;
            }
            b'"' => {
                if dialect != SqlDialect::Postgres {
                    return Err(SqlFrontendError::syntax("double-quoted identifiers are PostgreSQL syntax and are not accepted in MySQL DDL"));
                }
                index += 1;
                let start = index;
                while index < bytes.len() && bytes[index] != b'"' {
                    index += 1;
                }
                if index >= bytes.len() {
                    return Err(SqlFrontendError::syntax("unclosed double-quoted identifier in DDL statement"));
                }
                tokens.push(Token::Quoted(source[start..index].to_string()));
                index += 1;
            }
            b'\'' => {
                index += 1;
                let mut value = String::new();
                loop {
                    if index >= bytes.len() {
                        return Err(SqlFrontendError::syntax("unclosed string literal in DDL statement"));
                    }
                    match bytes[index] {
                        b'\'' if bytes.get(index + 1) == Some(&b'\'') => {
                            value.push('\'');
                            index += 2;
                        }
                        b'\'' => {
                            index += 1;
                            break;
                        }
                        b'\\' if dialect == SqlDialect::Mysql && index + 1 < bytes.len() => {
                            value.push(bytes[index + 1] as char);
                            index += 2;
                        }
                        byte => {
                            value.push(byte as char);
                            index += 1;
                        }
                    }
                }
                tokens.push(Token::String(value));
            }
            b'(' | b')' | b',' | b';' | b'=' | b'.' => {
                tokens.push(Token::Punct(char::from(byte)));
                index += 1;
            }
            b'-' if bytes.get(index + 1).is_some_and(u8::is_ascii_digit) => {
                let start = index;
                index += 1;
                index = take_number(bytes, index);
                tokens.push(Token::Number(source[start..index].to_string()));
            }
            byte if byte.is_ascii_digit() => {
                let start = index;
                index = take_number(bytes, index);
                tokens.push(Token::Number(source[start..index].to_string()));
            }
            byte if is_word_byte(byte) => {
                let start = index;
                while index < bytes.len() && is_word_byte(bytes[index]) {
                    index += 1;
                }
                tokens.push(Token::Word(source[start..index].to_string()));
            }
            byte => {
                return Err(SqlFrontendError::syntax(format!("unexpected character `{}` in DDL statement", char::from(byte))));
            }
        }
    }
    Ok(tokens)
}

/// Consumes digits, an optional fractional part, and an optional exponent.
fn take_number(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        index += 1;
    }
    if index < bytes.len() && bytes[index] == b'.' && bytes.get(index + 1).is_some_and(u8::is_ascii_digit) {
        index += 1;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
    }
    if index < bytes.len() && matches!(bytes[index], b'e' | b'E') {
        let mut exponent = index + 1;
        if exponent < bytes.len() && matches!(bytes[exponent], b'+' | b'-') {
            exponent += 1;
        }
        if exponent < bytes.len() && bytes[exponent].is_ascii_digit() {
            index = exponent;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
        }
    }
    index
}

struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
    dialect: SqlDialect,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.cursor)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.cursor).cloned();
        if token.is_some() {
            self.cursor += 1;
        }
        token
    }

    fn peek_word(&self) -> Option<String> {
        match self.peek() {
            Some(Token::Word(word)) => Some(word.clone()),
            _ => None,
        }
    }

    fn peek_keyword(&self) -> Option<String> {
        self.peek_word().map(|word| word.to_ascii_uppercase())
    }

    fn eat_keyword(&mut self, keyword: &str) -> bool {
        if self.peek_keyword().as_deref() == Some(keyword) {
            self.cursor += 1;
            true
        }
        else {
            false
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<(), SqlFrontendError> {
        if self.eat_keyword(keyword) { Ok(()) } else { Err(self.expected(keyword)) }
    }

    fn expected(&self, keyword: &str) -> SqlFrontendError {
        match self.peek() {
            Some(token) => SqlFrontendError::syntax(format!("expected `{keyword}`, found `{}`", token_text(token))),
            None => SqlFrontendError::syntax(format!("expected `{keyword}`, found end of statement")),
        }
    }

    fn peek_punct(&self, expected: char) -> bool {
        matches!(self.peek(), Some(Token::Punct(punct)) if *punct == expected)
    }

    fn eat_punct(&mut self, expected: char) -> bool {
        if self.peek_punct(expected) {
            self.cursor += 1;
            true
        }
        else {
            false
        }
    }

    fn expect_punct(&mut self, expected: char) -> Result<(), SqlFrontendError> {
        if self.eat_punct(expected) { Ok(()) } else { Err(self.expected(&expected.to_string())) }
    }

    fn statement(&mut self) -> Result<SqlDdlStatement, SqlFrontendError> {
        if self.eat_keyword("CREATE") {
            self.create()
        }
        else if self.eat_keyword("ALTER") {
            self.alter()
        }
        else if self.eat_keyword("DROP") {
            self.drop_statement()
        }
        else {
            Err(SqlFrontendError::syntax("expected a `CREATE`, `ALTER`, or `DROP` statement"))
        }
    }

    /// Consumes a trailing `;` and rejects anything after it.
    fn finish(&mut self) -> Result<(), SqlFrontendError> {
        let terminated = self.eat_punct(';');
        match self.peek() {
            None => Ok(()),
            Some(_) if terminated => Err(SqlFrontendError::unsupported("more than one SQL statement per gateway message is not supported")),
            Some(_) => Err(SqlFrontendError::syntax("unexpected token after the DDL statement")),
        }
    }

    fn create(&mut self) -> Result<SqlDdlStatement, SqlFrontendError> {
        if self.eat_keyword("TABLE") {
            return self.create_table();
        }
        if self.peek_keyword().as_deref() == Some("UNIQUE") || self.peek_keyword().as_deref() == Some("INDEX") {
            return self.create_index();
        }
        Err(self.unsupported_prefix("CREATE"))
    }

    fn create_table(&mut self) -> Result<SqlDdlStatement, SqlFrontendError> {
        let if_not_exists = self.if_not_exists()?;
        let name = self.table_name()?;
        self.expect_punct('(')?;
        let mut columns = Vec::new();
        let mut primary_key = Vec::new();
        let mut unique_constraints = Vec::new();
        while !self.peek_punct(')') {
            self.table_item(&mut columns, &mut primary_key, &mut unique_constraints)?;
            if !self.eat_punct(',') {
                break;
            }
        }
        self.expect_punct(')')?;
        let (charset, collation) = self.table_options()?;
        Ok(SqlDdlStatement::CreateTable { name, if_not_exists, columns, primary_key, unique_constraints, charset, collation })
    }

    fn table_item(
        &mut self,
        columns: &mut Vec<DdlColumn>,
        primary_key: &mut Vec<String>,
        unique_constraints: &mut Vec<Vec<String>>,
    ) -> Result<(), SqlFrontendError> {
        match self.peek_keyword().as_deref() {
            Some("CONSTRAINT") => {
                self.cursor += 1;
                let _constraint_name = self.identifier()?;
                if self.eat_keyword("PRIMARY") {
                    self.expect_keyword("KEY")?;
                    let names = self.column_list()?;
                    if !primary_key.is_empty() {
                        return Err(SqlFrontendError::syntax("table has more than one PRIMARY KEY definition"));
                    }
                    *primary_key = names;
                    Ok(())
                }
                else if self.eat_keyword("UNIQUE") {
                    self.eat_keyword("KEY");
                    if !self.peek_punct('(') {
                        let _index_name = self.identifier()?;
                    }
                    unique_constraints.push(self.column_list()?);
                    Ok(())
                }
                else if self.peek_keyword().as_deref() == Some("FOREIGN") {
                    Err(SqlFrontendError::unsupported("`FOREIGN KEY` table constraints are not supported by the cluster SQL gateway"))
                }
                else if self.peek_keyword().as_deref() == Some("CHECK") {
                    Err(SqlFrontendError::unsupported("`CHECK` constraints are not supported by the cluster SQL gateway"))
                }
                else {
                    Err(self.unsupported_prefix("CONSTRAINT"))
                }
            }
            Some("PRIMARY") => {
                self.cursor += 1;
                self.expect_keyword("KEY")?;
                let names = self.column_list()?;
                if !primary_key.is_empty() {
                    return Err(SqlFrontendError::syntax("table has more than one PRIMARY KEY definition"));
                }
                *primary_key = names;
                Ok(())
            }
            Some("UNIQUE") => {
                self.cursor += 1;
                self.eat_keyword("KEY");
                if !self.peek_punct('(') {
                    let _index_name = self.identifier()?;
                }
                unique_constraints.push(self.column_list()?);
                Ok(())
            }
            Some("FOREIGN") => {
                Err(SqlFrontendError::unsupported("`FOREIGN KEY` table constraints are not supported by the cluster SQL gateway"))
            }
            Some("CHECK") => Err(SqlFrontendError::unsupported("`CHECK` constraints are not supported by the cluster SQL gateway")),
            Some("KEY") | Some("INDEX") => Err(SqlFrontendError::unsupported(
                "`KEY` / `INDEX` table constraints are not supported by the cluster SQL gateway, use `CREATE INDEX`",
            )),
            Some("FULLTEXT") => Err(SqlFrontendError::unsupported("`FULLTEXT` indexes are not supported by the cluster SQL gateway")),
            Some("SPATIAL") => Err(SqlFrontendError::unsupported("`SPATIAL` indexes are not supported by the cluster SQL gateway")),
            _ => {
                columns.push(self.column_definition()?);
                Ok(())
            }
        }
    }

    fn column_definition(&mut self) -> Result<DdlColumn, SqlFrontendError> {
        let name = self.identifier()?;
        let mut sql_type = String::new();
        loop {
            match self.peek() {
                Some(Token::Word(word)) if !is_constraint_keyword(word) => {
                    let word = word.to_ascii_uppercase();
                    self.cursor += 1;
                    if !sql_type.is_empty() {
                        sql_type.push(' ');
                    }
                    sql_type.push_str(&word);
                }
                Some(Token::Punct('(')) => {
                    let parameters = self.type_parameters()?;
                    sql_type.push('(');
                    sql_type.push_str(&parameters);
                    sql_type.push(')');
                }
                _ => break,
            }
        }
        if sql_type.is_empty() {
            return Err(SqlFrontendError::syntax(format!("column `{name}` declares no type")));
        }
        let mut not_null = false;
        let mut inline_primary_key = false;
        // PostgreSQL serial pseudo-types imply an owned sequence.
        let mut auto_increment = matches!(sql_type.as_str(), "SERIAL" | "BIGSERIAL" | "SMALLSERIAL");
        let mut default = None;
        loop {
            match self.peek_keyword().as_deref() {
                Some("NOT") => {
                    self.cursor += 1;
                    self.expect_keyword("NULL")?;
                    not_null = true;
                }
                Some("NULL") => {
                    self.cursor += 1;
                }
                Some("PRIMARY") => {
                    self.cursor += 1;
                    self.expect_keyword("KEY")?;
                    inline_primary_key = true;
                    not_null = true;
                }
                Some("UNIQUE") => {
                    self.cursor += 1;
                }
                Some("DEFAULT") => {
                    self.cursor += 1;
                    default = Some(self.default_literal()?);
                }
                Some("COLLATE") => {
                    self.cursor += 1;
                    let _collation = self.identifier()?;
                }
                Some("AUTO_INCREMENT") => {
                    self.cursor += 1;
                    auto_increment = true;
                }
                Some("COMMENT") => {
                    self.cursor += 1;
                    self.string_literal("COMMENT")?;
                }
                Some("CHECK") => {
                    return Err(SqlFrontendError::unsupported("inline `CHECK` constraints are not supported by the cluster SQL gateway"));
                }
                Some("REFERENCES") => {
                    return Err(SqlFrontendError::unsupported(
                        "`REFERENCES` / `FOREIGN KEY` constraints are not supported by the cluster SQL gateway",
                    ));
                }
                Some("GENERATED") => {
                    return Err(SqlFrontendError::unsupported("`GENERATED` columns are not supported by the cluster SQL gateway"));
                }
                _ => break,
            }
        }
        Ok(DdlColumn { name, sql_type, not_null, inline_primary_key, auto_increment, default })
    }

    fn type_parameters(&mut self) -> Result<String, SqlFrontendError> {
        self.expect_punct('(')?;
        let mut text = String::new();
        loop {
            match self.next() {
                Some(Token::Word(word)) | Some(Token::Number(word)) => text.push_str(&word),
                Some(Token::String(value)) => {
                    text.push('\'');
                    text.push_str(&value);
                    text.push('\'');
                }
                Some(Token::Punct(',')) => text.push(','),
                Some(Token::Punct(')')) => return Ok(text),
                Some(token) => {
                    return Err(SqlFrontendError::syntax(format!("invalid column type parameter `{}`", token_text(&token))));
                }
                None => return Err(SqlFrontendError::syntax("unclosed column type parameter list")),
            }
        }
    }

    fn default_literal(&mut self) -> Result<String, SqlFrontendError> {
        match self.next() {
            Some(Token::Number(text)) => Ok(text),
            Some(Token::String(value)) => Ok(format!("'{value}'")),
            Some(Token::Word(word)) => {
                let keyword = word.to_ascii_uppercase();
                match keyword.as_str() {
                    "NULL" | "TRUE" | "FALSE" => Ok(word),
                    "CURRENT_TIMESTAMP" => {
                        if self.eat_punct('(') {
                            self.expect_punct(')')?;
                            Ok(format!("{word}()"))
                        }
                        else {
                            Ok(word)
                        }
                    }
                    _ => Err(SqlFrontendError::unsupported(format!("`DEFAULT {word}` is not supported by the cluster SQL gateway"))),
                }
            }
            Some(token) => Err(SqlFrontendError::syntax(format!("invalid `DEFAULT` literal `{}`", token_text(&token)))),
            None => Err(SqlFrontendError::syntax("`DEFAULT` requires a literal")),
        }
    }

    fn column_list(&mut self) -> Result<Vec<String>, SqlFrontendError> {
        self.expect_punct('(')?;
        let mut names = Vec::new();
        loop {
            if self.peek_punct(')') {
                break;
            }
            names.push(self.identifier()?);
            if self.eat_punct(',') {
                continue;
            }
            if self.peek_punct(')') {
                break;
            }
            return Err(SqlFrontendError::unsupported("expressions inside a column list are not supported by the cluster SQL gateway"));
        }
        self.expect_punct(')')?;
        Ok(names)
    }

    fn table_options(&mut self) -> Result<(Option<String>, Option<String>), SqlFrontendError> {
        if self.dialect == SqlDialect::Postgres {
            if self.peek().is_some() && !self.peek_punct(';') {
                return Err(SqlFrontendError::syntax("PostgreSQL `CREATE TABLE` does not accept trailing table options"));
            }
            return Ok((None, None));
        }
        let mut charset = None;
        let mut collation = None;
        loop {
            while self.eat_punct(',') {}
            if self.peek().is_none() || self.peek_punct(';') {
                break;
            }
            if self.eat_keyword("ENGINE") {
                self.eat_punct('=');
                self.option_word("ENGINE")?;
                continue;
            }
            if self.eat_keyword("DEFAULT") {
                if !self.eat_keyword("CHARSET") {
                    return Err(self.unsupported_prefix("DEFAULT"));
                }
                self.eat_punct('=');
                charset = Some(self.option_word("CHARSET")?);
                continue;
            }
            if self.eat_keyword("CHARSET") {
                self.eat_punct('=');
                charset = Some(self.option_word("CHARSET")?);
                continue;
            }
            if self.eat_keyword("COLLATE") {
                self.eat_punct('=');
                collation = Some(self.option_word("COLLATE")?);
                continue;
            }
            if self.eat_keyword("AUTO_INCREMENT") {
                self.eat_punct('=');
                self.option_number("AUTO_INCREMENT")?;
                continue;
            }
            if self.eat_keyword("COMMENT") {
                self.eat_punct('=');
                self.string_literal("COMMENT")?;
                continue;
            }
            return Err(self.unsupported_prefix(""));
        }
        if let Some(character_set) = &charset {
            let accepted = character_set.eq_ignore_ascii_case("utf8mb4") || character_set.eq_ignore_ascii_case("utf8");
            if !accepted {
                return Err(SqlFrontendError::unsupported(format!(
                    "table character set `{character_set}` is not supported: the gateway compares text by exact code points, use \
                     `utf8mb4` or `utf8`"
                )));
            }
        }
        if let Some(name) = &collation {
            if !is_accepted_collation(name) {
                return Err(SqlFrontendError::unsupported(format!(
                    "table collation `{name}` is not supported: accepted collations are {}",
                    ACCEPTED_COLLATIONS.join(", ")
                )));
            }
        }
        Ok((charset, collation))
    }

    fn option_word(&mut self, option: &str) -> Result<String, SqlFrontendError> {
        match self.next() {
            Some(Token::Word(word)) => Ok(word),
            Some(token) => {
                Err(SqlFrontendError::unsupported(format!("table option `{option}` requires a bare word, found `{}`", token_text(&token))))
            }
            None => Err(SqlFrontendError::syntax(format!("table option `{option}` requires a value"))),
        }
    }

    fn option_number(&mut self, option: &str) -> Result<(), SqlFrontendError> {
        match self.next() {
            Some(Token::Number(_)) => Ok(()),
            Some(token) => {
                Err(SqlFrontendError::unsupported(format!("table option `{option}` requires a number, found `{}`", token_text(&token))))
            }
            None => Err(SqlFrontendError::syntax(format!("table option `{option}` requires a value"))),
        }
    }

    fn string_literal(&mut self, option: &str) -> Result<(), SqlFrontendError> {
        match self.next() {
            Some(Token::String(_)) => Ok(()),
            Some(token) => {
                Err(SqlFrontendError::unsupported(format!("table option `{option}` requires a string literal, found `{}`", token_text(&token))))
            }
            None => Err(SqlFrontendError::syntax(format!("table option `{option}` requires a value"))),
        }
    }

    fn alter(&mut self) -> Result<SqlDdlStatement, SqlFrontendError> {
        if !self.eat_keyword("TABLE") {
            return Err(self.unsupported_prefix("ALTER"));
        }
        let table = self.table_name()?;
        if !self.eat_keyword("ADD") {
            return Err(self.unsupported_action("ALTER TABLE"));
        }
        if !self.eat_keyword("COLUMN") {
            return Err(SqlFrontendError::unsupported(
                "`ALTER TABLE ... ADD` without `COLUMN` is not supported by the cluster SQL gateway, write `ADD COLUMN`",
            ));
        }
        let if_not_exists = self.if_not_exists()?;
        let column = self.column_definition()?;
        if self.peek_punct(',') {
            return Err(SqlFrontendError::unsupported("`ALTER TABLE` supports exactly one `ADD COLUMN` action per gateway message"));
        }
        Ok(SqlDdlStatement::AlterTableAddColumn { table, column, if_not_exists })
    }

    fn drop_statement(&mut self) -> Result<SqlDdlStatement, SqlFrontendError> {
        if self.eat_keyword("TABLE") {
            return self.drop_table();
        }
        if self.eat_keyword("INDEX") {
            return self.drop_index();
        }
        Err(self.unsupported_prefix("DROP"))
    }

    fn drop_table(&mut self) -> Result<SqlDdlStatement, SqlFrontendError> {
        let if_exists = self.if_exists()?;
        let mut names = Vec::new();
        loop {
            names.push(self.table_name()?);
            if !self.eat_punct(',') {
                break;
            }
        }
        while self.eat_keyword("CASCADE") || self.eat_keyword("RESTRICT") {}
        if let Some(keyword) = self.peek_keyword() {
            return Err(SqlFrontendError::unsupported(format!("`DROP TABLE ... {keyword}` is not supported by the cluster SQL gateway")));
        }
        Ok(SqlDdlStatement::DropTable { names, if_exists })
    }

    fn drop_index(&mut self) -> Result<SqlDdlStatement, SqlFrontendError> {
        let if_exists = if self.dialect == SqlDialect::Mysql {
            if self.peek_keyword().as_deref() == Some("IF") {
                return Err(SqlFrontendError::unsupported("`DROP INDEX IF EXISTS` is PostgreSQL syntax"));
            }
            false
        }
        else {
            self.if_exists()?
        };
        let missing_name = self.peek().is_none()
            || self.peek_punct(';')
            || matches!(self.peek_keyword().as_deref(), Some("ON") | Some("CASCADE") | Some("RESTRICT"));
        if missing_name {
            return Err(SqlFrontendError::syntax("`DROP INDEX` requires an index name"));
        }
        let name = self.table_name()?;
        let table = if self.dialect == SqlDialect::Mysql {
            self.expect_keyword("ON")?;
            Some(self.table_name()?)
        }
        else {
            while self.eat_keyword("CASCADE") || self.eat_keyword("RESTRICT") {}
            None
        };
        Ok(SqlDdlStatement::DropIndex { name, table, if_exists })
    }

    fn create_index(&mut self) -> Result<SqlDdlStatement, SqlFrontendError> {
        let unique = self.eat_keyword("UNIQUE");
        self.expect_keyword("INDEX")?;
        let if_not_exists = self.if_not_exists()?;
        let name = self.table_name()?;
        if self.eat_keyword("USING") {
            self.index_method()?;
        }
        self.expect_keyword("ON")?;
        let table = self.table_name()?;
        if self.eat_keyword("USING") {
            self.index_method()?;
        }
        self.expect_punct('(')?;
        let mut columns = Vec::new();
        loop {
            if self.peek_punct(')') {
                break;
            }
            columns.push(self.identifier()?);
            if self.eat_keyword("ASC") || self.eat_keyword("DESC") {}
            if self.eat_punct(',') {
                continue;
            }
            if self.peek_punct(')') {
                break;
            }
            return Err(SqlFrontendError::unsupported(
                "expressions inside a `CREATE INDEX` column list are not supported by the cluster SQL gateway",
            ));
        }
        self.expect_punct(')')?;
        if self.eat_keyword("USING") {
            self.index_method()?;
        }
        if self.peek_keyword().as_deref() == Some("WHERE") {
            return Err(SqlFrontendError::unsupported("partial indexes with `WHERE ...` are not supported by the cluster SQL gateway"));
        }
        if let Some(keyword) = self.peek_keyword() {
            return Err(SqlFrontendError::unsupported(format!("`CREATE INDEX ... {keyword}` is not supported by the cluster SQL gateway")));
        }
        Ok(SqlDdlStatement::CreateIndex { name, table, if_not_exists, unique, columns })
    }

    fn index_method(&mut self) -> Result<(), SqlFrontendError> {
        match self.peek_keyword().as_deref() {
            Some("BTREE") | Some("HASH") => {
                self.cursor += 1;
                Ok(())
            }
            Some(method) => Err(SqlFrontendError::unsupported(format!(
                "index method `{method}` is not supported by the cluster SQL gateway, use `BTREE` or `HASH`"
            ))),
            None => Err(self.expected("BTREE")),
        }
    }

    fn if_not_exists(&mut self) -> Result<bool, SqlFrontendError> {
        if !self.eat_keyword("IF") {
            return Ok(false);
        }
        self.expect_keyword("NOT")?;
        self.expect_keyword("EXISTS")?;
        Ok(true)
    }

    fn if_exists(&mut self) -> Result<bool, SqlFrontendError> {
        if !self.eat_keyword("IF") {
            return Ok(false);
        }
        self.expect_keyword("EXISTS")?;
        Ok(true)
    }

    fn identifier(&mut self) -> Result<String, SqlFrontendError> {
        match self.next() {
            Some(Token::Word(word)) | Some(Token::Backtick(word)) | Some(Token::Quoted(word)) => Ok(word),
            Some(token) => Err(SqlFrontendError::syntax(format!("expected an identifier, found `{}`", token_text(&token)))),
            None => Err(SqlFrontendError::syntax("expected an identifier, found end of statement")),
        }
    }

    /// Parses one table name, accepting PostgreSQL's `public.` prefix.
    fn table_name(&mut self) -> Result<String, SqlFrontendError> {
        let first = self.identifier()?;
        if !self.eat_punct('.') {
            return Ok(first);
        }
        let second = self.identifier()?;
        if self.dialect == SqlDialect::Postgres && first.eq_ignore_ascii_case("public") {
            return Ok(second);
        }
        Err(SqlFrontendError::unsupported(format!("qualified table name `{first}.{second}` is not supported by the cluster SQL gateway")))
    }

    /// Builds a message for an unsupported statement prefix, naming the next keyword.
    fn unsupported_prefix(&self, prefix: &str) -> SqlFrontendError {
        match self.peek_keyword() {
            Some(keyword) if prefix.is_empty() => {
                SqlFrontendError::unsupported(format!("`{keyword}` table option is not supported by the cluster SQL gateway"))
            }
            Some(keyword) => SqlFrontendError::unsupported(format!("`{prefix} {keyword}` is not supported by the cluster SQL gateway")),
            None if prefix.is_empty() => SqlFrontendError::syntax("expected a table option, found end of statement"),
            None => SqlFrontendError::syntax(format!("`{prefix}` requires a statement object")),
        }
    }

    /// Builds a message for an unsupported `ALTER TABLE` action.
    fn unsupported_action(&self, prefix: &str) -> SqlFrontendError {
        match self.peek_keyword() {
            Some(keyword) => SqlFrontendError::unsupported(format!(
                "`{prefix} ... {keyword}` is not supported by the cluster SQL gateway, only `ADD COLUMN` is"
            )),
            None => SqlFrontendError::syntax(format!("`{prefix}` requires an action")),
        }
    }
}

fn is_constraint_keyword(word: &str) -> bool {
    let keyword = word.to_ascii_uppercase();
    CONSTRAINT_KEYWORDS.contains(&keyword.as_str())
}
