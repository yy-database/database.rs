use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};

use yyds_gateway_pgsql::{connection::serve_connection, wire::encode_message};

const PROTOCOL_V3: u32 = 196_608;

fn read_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 5];
    stream.read_exact(&mut header).unwrap();
    let length = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
    let mut payload = vec![0; length - 4];
    stream.read_exact(&mut payload).unwrap();
    (header[0], payload)
}

fn receive_until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut messages = Vec::new();
    loop {
        let message = read_message(stream);
        let ready = message.0 == b'Z';
        messages.push(message);
        if ready {
            return messages;
        }
    }
}

fn send_query(stream: &mut TcpStream, sql: &str) {
    let mut payload = sql.as_bytes().to_vec();
    payload.push(0);
    stream.write_all(&encode_message(b'Q', &payload, 1024).unwrap()).unwrap();
}

fn start_server() -> (TcpStream, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_connection(stream, None).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut startup = Vec::from(0_u32.to_be_bytes());
    startup.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
    startup.extend_from_slice(b"user\0test\0\0");
    let length = startup.len() as u32;
    startup[..4].copy_from_slice(&length.to_be_bytes());
    client.write_all(&startup).unwrap();
    let startup_responses = receive_until_ready(&mut client);
    assert_eq!(startup_responses.last().unwrap(), &(b'Z', b"I".to_vec()));
    (client, server)
}

#[test]
fn session_variable_statements_follow_postgresql_semantics() {
    let (mut client, server) = start_server();

    send_query(&mut client, "SET client_encoding = 'UTF8'");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"SET\0".to_vec()), (b'Z', b"I".to_vec())]);

    // `SHOW server_version` must surface the crate version from the gateway,
    // not a hard-coded PostgreSQL release tag.
    send_query(&mut client, "SHOW server_version");
    let shown = receive_until_ready(&mut client);
    assert_eq!(shown.len(), 4);
    assert_eq!(shown[0].0, b'T');
    assert_eq!(shown[1].0, b'D');
    let text = String::from_utf8_lossy(&shown[1].1);
    assert!(text.contains("YYDS ") && text.contains(env!("CARGO_PKG_VERSION")), "server_version must carry the YYDS crate version, got {text}");
    assert_eq!(shown[2], (b'C', b"SHOW\0".to_vec()));
    assert_eq!(shown[3], (b'Z', b"I".to_vec()));

    // `tokio-postgres` 在连接期固定发出 `SHOW transaction_read_only`，缺这一行连接直接失败。
    send_query(&mut client, "SHOW transaction_read_only");
    let shown = receive_until_ready(&mut client);
    assert_eq!(shown.len(), 4);
    assert_eq!(shown[0].0, b'T');
    assert_eq!(shown[1].0, b'D');
    assert_eq!(shown[1].1.len(), 2 + 4 + 3);
    assert!(String::from_utf8_lossy(&shown[1].1).ends_with("off"));
    assert_eq!(shown[2], (b'C', b"SHOW\0".to_vec()));
    assert_eq!(shown[3], (b'Z', b"I".to_vec()));

    // `SHOW search_path` defaults to PostgreSQL convention `"$user, public"`.
    send_query(&mut client, "SHOW search_path");
    let shown = receive_until_ready(&mut client);
    assert_eq!(shown[1].0, b'D');
    let text = String::from_utf8_lossy(&shown[1].1);
    assert!(text.contains("$user") && text.contains("public"), "default search_path must include $user and public, got {text}");

    // `SET search_path` mutates the session; a subsequent `SHOW` must reflect it.
    send_query(&mut client, "SET search_path = 'public'");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SET\0".to_vec()));
    send_query(&mut client, "SHOW search_path");
    let shown = receive_until_ready(&mut client);
    assert_eq!(shown[1].0, b'D');
    let text = String::from_utf8_lossy(&shown[1].1);
    assert!(text.contains("public"), "after SET search_path = 'public', SHOW must echo it, got {text}");

    // MySQL 专有语法在 PostgreSQL 下必须报语法错误
    send_query(&mut client, "SHOW VARIABLES");
    let mysql_only = receive_until_ready(&mut client);
    assert_eq!(mysql_only[0].0, b'E');
    assert!(mysql_only[0].1.windows(5).any(|window| window == b"42601"));
    assert_eq!(mysql_only.last().unwrap(), &(b'Z', b"I".to_vec()));

    send_query(&mut client, "PRAGMA user_version");
    let pragma = receive_until_ready(&mut client);
    assert_eq!(pragma[0].0, b'E');
    assert!(pragma[0].1.windows(5).any(|window| window == b"42601"));

    // 未知参数必须明确报未识别，而不是返回空值
    send_query(&mut client, "SHOW nope_param");
    let unknown = receive_until_ready(&mut client);
    assert_eq!(unknown[0].0, b'E');
    assert!(unknown[0].1.windows(5).any(|window| window == b"42704"));
    assert_eq!(unknown.last().unwrap(), &(b'Z', b"I".to_vec()));

    drop(client);
    server.join().unwrap();
}

#[test]
fn transaction_commands_track_failed_state_and_rollback_recovery() {
    let (mut client, server) = start_server();

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"BEGIN\0".to_vec()), (b'Z', b"T".to_vec())]);

    send_query(&mut client, "SELECT 1");
    let failed = receive_until_ready(&mut client);
    assert_eq!(failed[0].0, b'E');
    assert!(failed[0].1.windows(5).any(|window| window == b"0A000"));
    assert_eq!(failed.last().unwrap(), &(b'Z', b"E".to_vec()));

    send_query(&mut client, "SELECT 2");
    let aborted = receive_until_ready(&mut client);
    assert_eq!(aborted[0].0, b'E');
    assert!(aborted[0].1.windows(5).any(|window| window == b"25P02"));
    assert_eq!(aborted.last().unwrap(), &(b'Z', b"E".to_vec()));

    send_query(&mut client, "BEGIN");
    let still_failed = receive_until_ready(&mut client);
    assert_eq!(still_failed[0].0, b'E');
    assert!(still_failed[0].1.windows(5).any(|window| window == b"25P02"));
    assert_eq!(still_failed.last().unwrap(), &(b'Z', b"E".to_vec()));

    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"ROLLBACK\0".to_vec()), (b'Z', b"I".to_vec())]);

    send_query(&mut client, "SELECT (");
    let syntax_error = receive_until_ready(&mut client);
    assert_eq!(syntax_error[0].0, b'E');
    assert!(syntax_error[0].1.windows(5).any(|window| window == b"42601"));
    assert_eq!(syntax_error.last().unwrap(), &(b'Z', b"I".to_vec()));

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "SELECT 1");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"E".to_vec()));
    send_query(&mut client, "COMMIT");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"ROLLBACK\0".to_vec()), (b'Z', b"I".to_vec())]);

    drop(client);
    server.join().unwrap();
}

fn assert_sqlstate(client: &mut TcpStream, sql: &str, state: &[u8]) {
    send_query(client, sql);
    let responses = receive_until_ready(client);
    assert_eq!(responses[0].0, b'E', "{sql} must fail");
    assert!(
        responses[0].1.windows(state.len()).any(|window| window == state),
        "{sql} must report SQLSTATE {}, got {}",
        String::from_utf8_lossy(state),
        String::from_utf8_lossy(&responses[0].1)
    );
    assert_eq!(responses.last().unwrap(), &(b'Z', b"I".to_vec()), "{sql} leaves the session ready");
}

/// Like `assert_sqlstate` but expects the session to be in Failed state
/// after the error, i.e. the SQL was issued inside a transaction block.
fn assert_sqlstate_in_tx(client: &mut TcpStream, sql: &str, state: &[u8]) {
    send_query(client, sql);
    let responses = receive_until_ready(client);
    assert_eq!(responses[0].0, b'E', "{sql} must fail");
    assert!(
        responses[0].1.windows(state.len()).any(|window| window == state),
        "{sql} must report SQLSTATE {}, got {}",
        String::from_utf8_lossy(state),
        String::from_utf8_lossy(&responses[0].1)
    );
    assert_eq!(responses.last().unwrap(), &(b'Z', b"E".to_vec()), "{sql} leaves the session in transaction-failed state");
}

#[test]
fn sqlstate_reflects_structured_gateway_errors() {
    let (mut client, server) = start_server_with_runtime();

    assert_sqlstate(&mut client, "SELECT id FROM missing", b"42P01");
    assert_sqlstate(&mut client, "SELECT nope FROM users", b"42703");
    assert_sqlstate(&mut client, "SELECT nope.id FROM users", b"42P01");
    assert_sqlstate(&mut client, "SELECT id FROM users INNER JOIN orders ON users.id = orders.user_id", b"42702");
    assert_sqlstate(&mut client, "SELECT ( FROM users", b"42601");
    assert_sqlstate(&mut client, "SELECT nope_fn(name) FROM users", b"42883");
    // 负长度在 PostgreSQL 是 `22011`；MySQL 对同一表达式返回空串。
    assert_sqlstate(&mut client, "SELECT SUBSTRING('abcdef', 2, -1)", b"22011");
    // `CURDATE()` / `CURTIME()` 与 `EXTRACT` 是 MySQL 拼写或未实现的语义，按未知函数报 `42883`。
    assert_sqlstate(&mut client, "SELECT CURDATE()", b"42883");
    assert_sqlstate(&mut client, "SELECT CURTIME()", b"42883");
    assert_sqlstate(&mut client, "SELECT EXTRACT(id) FROM users", b"42883");
    // 精确整数越界报 `22003`，与除零 `22012` 分开。
    assert_sqlstate(&mut client, "SELECT 9223372036854775807 + 1", b"22003");
    assert_sqlstate(&mut client, "SELECT 9223372036854775807 * 2", b"22003");
    // 外连接已受支持：未限定列名报歧义，而不是「不支持」。
    assert_sqlstate(&mut client, "SELECT id FROM users LEFT JOIN orders ON users.id = orders.user_id", b"42702");
    assert_sqlstate(&mut client, "SELECT id FROM users RIGHT JOIN orders ON users.id = orders.user_id", b"42702");
    assert_sqlstate(&mut client, "SELECT id FROM users FULL JOIN orders ON users.id = orders.user_id", b"42702");
    assert_sqlstate(&mut client, "INSERT INTO users VALUES (1, 42)", b"42804");
    assert_sqlstate(&mut client, "INSERT INTO users VALUES (NULL, 'x')", b"23502");

    send_query(&mut client, "INSERT INTO users VALUES (1, 'alice')");
    let inserted = receive_until_ready(&mut client);
    assert_eq!(inserted[0].0, b'C');
    assert_sqlstate(&mut client, "INSERT INTO users VALUES (1, 'bob')", b"23505");

    // 同名列限定后可正常查询：匹配行带回右表值，未匹配左行保留并把右侧列填 NULL。
    send_query(&mut client, "INSERT INTO users VALUES (2, 'bob')");
    assert_eq!(receive_until_ready(&mut client)[0].0, b'C');
    send_query(&mut client, "INSERT INTO orders VALUES (1, 1)");
    assert_eq!(receive_until_ready(&mut client)[0].0, b'C');
    send_query(&mut client, "SELECT users.id, orders.user_id FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id");
    let joined = receive_until_ready(&mut client);
    assert_eq!(joined[0].0, b'T', "a qualified outer join returns a result set");
    let rows: Vec<Vec<Option<Vec<u8>>>> = joined.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect();
    assert_eq!(
        rows,
        vec![vec![Some(b"1".to_vec()), Some(b"1".to_vec())], vec![Some(b"2".to_vec()), None]],
        "the matched left row carries the right value and the unmatched one stays NULL"
    );

    drop(client);
    server.join().unwrap();
}

/// `INSERT ... SELECT` 必须按写语句返回 `INSERT 0 N` 命令标签，而不是结果集。
#[test]
fn insert_select_reports_affected_rows_as_a_write() {
    let (mut client, server) = start_server_with_runtime();

    send_query(&mut client, "INSERT INTO users (id, name) SELECT 1, 'alice'");
    let inserted = receive_until_ready(&mut client);
    assert_eq!(inserted[0], (b'C', b"INSERT 0 1\0".to_vec()));
    assert_eq!(inserted.last().unwrap(), &(b'Z', b"I".to_vec()));

    // 源查询读同一张表，写入另一张表。
    send_query(&mut client, "INSERT INTO orders (id, user_id) SELECT id, id FROM users WHERE id = 1");
    let copied = receive_until_ready(&mut client);
    assert_eq!(copied[0], (b'C', b"INSERT 0 1\0".to_vec()));
    assert_eq!(copied.last().unwrap(), &(b'Z', b"I".to_vec()));

    send_query(&mut client, "SELECT user_id FROM orders WHERE id = 1");
    let rows = receive_until_ready(&mut client);
    assert_eq!(rows[0].0, b'T');

    assert_sqlstate(&mut client, "INSERT INTO users (id, name) SELECT 1, 'bob'", b"23505");
    assert_sqlstate(&mut client, "INSERT INTO users (id, name) SELECT id FROM users", b"42601");
    assert_sqlstate(&mut client, "INSERT INTO users (id, name) SELECT name, name FROM users", b"42804");

    // `ON CONFLICT DO NOTHING` 跳过冲突行，命令标签的计数只含真正写入的行。
    send_query(&mut client, "INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO NOTHING");
    let skipped = receive_until_ready(&mut client);
    assert_eq!(skipped[0], (b'C', b"INSERT 0 0\0".to_vec()));
    send_query(&mut client, "INSERT INTO orders (id, user_id) SELECT id, id FROM users ON CONFLICT (id) DO NOTHING");
    let skipped_many = receive_until_ready(&mut client);
    assert_eq!(skipped_many[0], (b'C', b"INSERT 0 0\0".to_vec()));
    // `DO UPDATE` 命中既有行时改写，并按写语句返回命令标签。
    send_query(&mut client, "INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO UPDATE SET name = 'bob'");
    let updated = receive_until_ready(&mut client);
    assert_eq!(updated[0], (b'C', b"INSERT 0 1\0".to_vec()));
    send_query(&mut client, "INSERT INTO orders (id, user_id) SELECT id, id FROM users ON CONFLICT (id) DO UPDATE SET user_id = 7");
    let updated_many = receive_until_ready(&mut client);
    assert_eq!(updated_many[0], (b'C', b"INSERT 0 1\0".to_vec()));

    send_query(&mut client, "SELECT name FROM users WHERE id = 1");
    let rewritten = receive_until_ready(&mut client);
    assert_eq!(rewritten[1].0, b'D');
    assert!(rewritten[1].1.windows(3).any(|window| window == b"bob"), "the conflicting row is rewritten");

    // 不符唯一约束的目标继续显式拒绝。
    assert_sqlstate(&mut client, "INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT (name) DO NOTHING", b"0A000");

    // `INSERT ... RETURNING` 在简单查询路径上返回列定义与数据行，命令标签仍是写语句。
    send_query(&mut client, "INSERT INTO users (id, name) VALUES (2, 'carol') RETURNING id, name");
    let returning = receive_until_ready(&mut client);
    assert_eq!(returning.len(), 4, "RowDescription, DataRow, CommandComplete, ReadyForQuery");
    assert_eq!(returning[0].0, b'T');
    assert!(returning[0].1.windows(3).any(|window| window == b"id\0"), "the description names `id`");
    assert!(returning[0].1.windows(5).any(|window| window == b"name\0"), "the description names `name`");
    assert_eq!(returning[1].0, b'D');
    assert!(returning[1].1.windows(5).any(|window| window == b"carol"), "the returned row carries the inserted value");
    assert_eq!(returning[2], (b'C', b"INSERT 0 1\0".to_vec()));

    // 多行 `RETURNING` 每行一条 DataRow。
    send_query(&mut client, "INSERT INTO users (id, name) VALUES (3, 'dave'), (4, 'erin') RETURNING id");
    let returned_many = receive_until_ready(&mut client);
    assert_eq!(returned_many.iter().filter(|message| message.0 == b'D').count(), 2);
    assert_eq!(returned_many.last().unwrap().0, b'Z');

    drop(client);
    server.join().unwrap();
}

/// 链式集合运算在 wire 上按 SQL 优先级合并：`EXCEPT` / `UNION` 同级左结合，
/// 整条链的 `ORDER BY` 只作用于合并后的结果。
#[test]
fn chained_set_operations_merge_in_sql_precedence_order() {
    let (mut client, server) = start_server_with_runtime();

    for sql in [
        "INSERT INTO users VALUES (1, 'alice')",
        "INSERT INTO users VALUES (2, 'bob')",
        "INSERT INTO users VALUES (3, 'carol')",
        "INSERT INTO orders VALUES (1, 1)",
    ] {
        send_query(&mut client, sql);
        assert_eq!(receive_until_ready(&mut client)[0].0, b'C');
    }

    send_query(&mut client, "SELECT id FROM users EXCEPT SELECT user_id FROM orders UNION SELECT 4 ORDER BY 1");
    let chained = receive_until_ready(&mut client);
    assert_eq!(chained[0].0, b'T');
    let rows: Vec<Vec<Option<Vec<u8>>>> = chained.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect();
    // {1,2,3} 减去 {1} 得 {2,3}，再并上 {4}。
    assert_eq!(rows, vec![vec![Some(b"2".to_vec())], vec![Some(b"3".to_vec())], vec![Some(b"4".to_vec())],]);
    assert_eq!(chained.last().unwrap(), &(b'Z', b"I".to_vec()));

    // 列数不兼容的链必须在协议层报语句错误，而不是返回拼接结果。
    assert_sqlstate(&mut client, "SELECT id, name FROM users UNION SELECT id FROM users", b"42601");

    drop(client);
    server.join().unwrap();
}

/// 文本协议上的 standalone `OFFSET`（无 `LIMIT`）：跳过 n 行返回其余全部，
/// 不得被误读成行数上限；`LIMIT m OFFSET n` 行为回归不变。
#[test]
fn standalone_offset_skips_rows_over_the_text_protocol() {
    let (mut client, server) = start_server_with_runtime();

    for sql in ["INSERT INTO users VALUES (1, 'alice')", "INSERT INTO users VALUES (2, 'bob')", "INSERT INTO users VALUES (3, 'carol')"] {
        send_query(&mut client, sql);
        assert_eq!(receive_until_ready(&mut client)[0].0, b'C');
    }

    send_query(&mut client, "SELECT id FROM users ORDER BY id OFFSET 1");
    let skipped = receive_until_ready(&mut client);
    assert_eq!(skipped[0].0, b'T', "a standalone OFFSET returns a result set");
    let rows: Vec<Vec<Option<Vec<u8>>>> = skipped.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect();
    assert_eq!(
        rows,
        vec![vec![Some(b"2".to_vec())], vec![Some(b"3".to_vec())]],
        "OFFSET 1 must skip the first row and keep the rest of the result set"
    );

    send_query(&mut client, "SELECT id FROM users ORDER BY id LIMIT 1 OFFSET 1");
    let paged = receive_until_ready(&mut client);
    let paged_rows: Vec<Vec<Option<Vec<u8>>>> =
        paged.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect();
    assert_eq!(paged_rows, vec![vec![Some(b"2".to_vec())]], "LIMIT 1 OFFSET 1 must take one row after skipping one");

    drop(client);
    server.join().unwrap();
}

/// 带 SQL runtime 的服务端，用于验证 PostgreSQL 语法下 `@@` 必须被拒绝。
fn start_server_with_runtime() -> (TcpStream, thread::JoinHandle<()>) {
    use std::sync::Arc;
    use yyds_execution::LocalExecutor;
    use yyds_gateway::{SqlCatalog, SqlSchemaRegistry};
    use yyds_gateway_pgsql::connection::{PgsqlSqlRuntime, serve_connection};
    use yyds_kv::FileShard;
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let source = "table users { @@id: i64, name: utf8 }\ntable orders { @@id: i64, user_id: i64 }\ntable metrics { @@id: i64, score: f64, active: bool, blob: bytes, small: i32 }\n";
    let projection = vos::parse_oak(source).expect("Oak parses").project_schema().expect("projection");
    let manifest = vos::types::IdentityManifest {
        format_version: vos::types::IDENTITY_MANIFEST_VERSION.into(),
        types: vec![
            vos::types::TypeIdentity {
                canonical_path: vec!["users".into()],
                type_id: 1,
                kind: vos::types::TypeContractKind::Table,
                fields: vec![
                    vos::types::FieldIdentity { canonical_name: "id".into(), field_id: 1, virtual_field_index: Some(0) },
                    vos::types::FieldIdentity { canonical_name: "name".into(), field_id: 2, virtual_field_index: Some(1) },
                ],
            },
            vos::types::TypeIdentity {
                canonical_path: vec!["orders".into()],
                type_id: 2,
                kind: vos::types::TypeContractKind::Table,
                fields: vec![
                    vos::types::FieldIdentity { canonical_name: "id".into(), field_id: 3, virtual_field_index: Some(0) },
                    vos::types::FieldIdentity { canonical_name: "user_id".into(), field_id: 4, virtual_field_index: Some(1) },
                ],
            },
            vos::types::TypeIdentity {
                canonical_path: vec!["metrics".into()],
                type_id: 3,
                kind: vos::types::TypeContractKind::Table,
                fields: vec![
                    vos::types::FieldIdentity { canonical_name: "id".into(), field_id: 5, virtual_field_index: Some(0) },
                    vos::types::FieldIdentity { canonical_name: "score".into(), field_id: 6, virtual_field_index: Some(1) },
                    vos::types::FieldIdentity { canonical_name: "active".into(), field_id: 7, virtual_field_index: Some(2) },
                    vos::types::FieldIdentity { canonical_name: "blob".into(), field_id: 8, virtual_field_index: Some(3) },
                    vos::types::FieldIdentity { canonical_name: "small".into(), field_id: 9, virtual_field_index: Some(4) },
                ],
            },
        ],
    };
    let contract = vos::resolve_contract(&projection, &manifest).expect("resolved contract");
    let catalog = SqlCatalog::from_resolved_contract(7, &contract).expect("gateway catalog");

    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let shard = FileShard::open(directory.path().join("s1.yykv")).expect("open file shard");
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(ShardEpoch(1), vec![shard_id.clone()]).expect("routing");
    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    let executor = LocalExecutor::new(routing, shards, Vec::new()).expect("executor");

    let runtime =
        Arc::new(PgsqlSqlRuntime { schema: Arc::new(SqlSchemaRegistry::from_catalog(&catalog).unwrap()), executor: Arc::new(executor) });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        // 保活 TempFolder：shard 文件必须在服务线程存活期间保持可用。
        let _directory = directory;
        let (stream, _) = listener.accept().unwrap();
        serve_connection(stream, Some(runtime)).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut startup = Vec::from(0_u32.to_be_bytes());
    startup.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
    startup.extend_from_slice(b"user\0test\0\0");
    let length = startup.len() as u32;
    startup[..4].copy_from_slice(&length.to_be_bytes());
    client.write_all(&startup).unwrap();
    let startup_responses = receive_until_ready(&mut client);
    assert_eq!(startup_responses.last().unwrap(), &(b'Z', b"I".to_vec()));
    (client, server)
}

#[test]
fn mysql_style_system_variables_are_rejected_with_a_runtime() {
    let (mut client, server) = start_server_with_runtime();

    // `@@` 在 PostgreSQL 里不是变量语法，必须按未知参数报错而不是当作列名。
    send_query(&mut client, "SELECT @@version");
    let rejected = receive_until_ready(&mut client);
    assert_eq!(rejected[0].0, b'E');
    assert!(rejected[0].1.windows(5).any(|window| window == b"42704"));
    assert!(String::from_utf8_lossy(&rejected[0].1).contains("system variable"));
    assert_eq!(rejected.last().unwrap(), &(b'Z', b"I".to_vec()));

    // 普通 SELECT 仍然走通 runtime（证明拒绝发生在绑定层而不是通用「未实现」分支）。
    send_query(&mut client, "SELECT id FROM users");
    let rows = receive_until_ready(&mut client);
    assert_eq!(rows[0].0, b'T');
    assert_eq!(rows.last().unwrap(), &(b'Z', b"I".to_vec()));

    drop(client);
    server.join().unwrap();
}

fn send_message(client: &mut TcpStream, tag: u8, payload: &[u8]) {
    client.write_all(&encode_message(tag, payload, 64 * 1024).unwrap()).unwrap();
}

fn cstring(value: &str) -> Vec<u8> {
    let mut bytes = value.as_bytes().to_vec();
    bytes.push(0);
    bytes
}

/// `name`, `type OID`, `type size` and `format code` for each described column.
fn parse_row_description(payload: &[u8]) -> Vec<(String, u32, i16, i16)> {
    let mut offset = 0;
    let count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    offset += 2;
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        let end = payload[offset..].iter().position(|byte| *byte == 0).unwrap() + offset;
        let name = String::from_utf8(payload[offset..end].to_vec()).unwrap();
        offset = end + 1 + 4 + 2;
        let oid = u32::from_be_bytes(payload[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let size = i16::from_be_bytes([payload[offset], payload[offset + 1]]);
        offset += 2 + 4;
        let format = i16::from_be_bytes([payload[offset], payload[offset + 1]]);
        offset += 2;
        columns.push((name, oid, size, format));
    }
    columns
}

fn parse_data_row(payload: &[u8]) -> Vec<Option<Vec<u8>>> {
    let count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let length = i32::from_be_bytes(payload[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if length < 0 {
            values.push(None);
            continue;
        }
        let length = length as usize;
        values.push(Some(payload[offset..offset + length].to_vec()));
        offset += length;
    }
    values
}

#[test]
fn describe_statement_reports_catalog_types_and_binary_rows_round_trip() {
    let (mut client, server) = start_server_with_runtime();

    // 空结果集也必须按 catalog 声明类型描述，而不是退化成 text。
    let mut parse = cstring("s2");
    parse.extend_from_slice(&cstring("SELECT id, name FROM users WHERE id = $1"));
    parse.extend_from_slice(&1_i16.to_be_bytes());
    parse.extend_from_slice(&20_u32.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    let mut describe = vec![b'S'];
    describe.extend_from_slice(&cstring("s2"));
    send_message(&mut client, b'D', &describe);
    send_message(&mut client, b'S', &[]);
    let described = receive_until_ready(&mut client);
    assert_eq!(described[0].0, b'1');
    assert_eq!(described[1].0, b't');
    assert_eq!(described[2].0, b'T');
    let columns = parse_row_description(&described[2].1);
    assert_eq!(columns, vec![("id".to_string(), 20, 8, 0), ("name".to_string(), 25, -1, 0)]);

    send_query(&mut client, "INSERT INTO metrics VALUES (1, 1.5, 1, 'bin', 7)");
    let inserted = receive_until_ready(&mut client);
    assert_eq!(inserted[0].0, b'C');

    let mut parse = cstring("s1");
    parse.extend_from_slice(&cstring("SELECT id, score, active, blob, small FROM metrics WHERE id = $1"));
    parse.extend_from_slice(&1_i16.to_be_bytes());
    parse.extend_from_slice(&20_u32.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    send_message(&mut client, b'D', &describe_of(&cstring("s1")));
    send_message(&mut client, b'S', &[]);
    let described = receive_until_ready(&mut client);
    let columns = parse_row_description(&described[2].1);
    assert_eq!(
        columns,
        vec![
            ("id".to_string(), 20, 8, 0),
            ("score".to_string(), 701, 8, 0),
            ("active".to_string(), 16, 1, 0),
            ("blob".to_string(), 17, -1, 0),
            ("small".to_string(), 23, 4, 0),
        ]
    );

    let mut bind = cstring("p1");
    bind.extend_from_slice(&cstring("s1"));
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&8_i32.to_be_bytes());
    bind.extend_from_slice(&1_i64.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);
    let mut describe = vec![b'P'];
    describe.extend_from_slice(&cstring("p1"));
    send_message(&mut client, b'D', &describe);
    send_message(&mut client, b'S', &[]);
    let described = receive_until_ready(&mut client);
    assert_eq!(described[0].0, b'2');
    assert_eq!(described[1].0, b'T');
    let columns = parse_row_description(&described[1].1);
    assert!(columns.iter().all(|(_, _, _, format)| *format == 1), "binary formats requested at Bind must be reported");

    let mut bind = cstring("p2");
    bind.extend_from_slice(&cstring("s1"));
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&8_i32.to_be_bytes());
    bind.extend_from_slice(&1_i64.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);
    let mut execute = cstring("p2");
    execute.extend_from_slice(&0_i32.to_be_bytes());
    send_message(&mut client, b'E', &execute);
    send_message(&mut client, b'S', &[]);
    let executed = receive_until_ready(&mut client);
    assert_eq!(executed[0].0, b'2');
    assert_eq!(executed[1].0, b'D');
    let row = parse_data_row(&executed[1].1);
    assert_eq!(row.len(), 5);
    assert_eq!(row[0], Some(1_i64.to_be_bytes().to_vec()));
    assert_eq!(row[1], Some(1.5_f64.to_be_bytes().to_vec()));
    assert_eq!(row[2], Some(vec![1]));
    assert_eq!(row[3], Some(b"bin".to_vec()));
    assert_eq!(row[4], Some(7_i32.to_be_bytes().to_vec()));

    // 不支持的二进制参数类型必须报 0A000 并指明 OID。
    let mut parse = cstring("s3");
    parse.extend_from_slice(&cstring("SELECT name FROM users WHERE id = $1"));
    parse.extend_from_slice(&1_i16.to_be_bytes());
    parse.extend_from_slice(&1184_u32.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    let mut bind = cstring("p3");
    bind.extend_from_slice(&cstring("s3"));
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&8_i32.to_be_bytes());
    bind.extend_from_slice(&0_i64.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);
    send_message(&mut client, b'S', &[]);
    let rejected = receive_until_ready(&mut client);
    let error = rejected.iter().find(|(tag, _)| *tag == b'E').expect("binary parameter type must be rejected");
    let text = String::from_utf8_lossy(&error.1);
    assert!(text.contains("0A000") && text.contains("1184"), "{text}");

    drop(client);
    server.join().unwrap();
}

/// `bytea` 的二进制格式直接携带原始 octets：NUL、非 UTF-8 字节与零长载荷
/// 都必须原样往返；文本格式无法承载时按 `0A000` 稳定拒绝。
#[test]
fn bytea_binary_parameters_round_trip_and_text_format_is_rejected() {
    let (mut client, server) = start_server_with_runtime();
    let payload: Vec<u8> = vec![0x00, 0xFF, 0xFE, 0x00, 0x41, 0xC3, 0x28];

    // 两个参数都按二进制格式绑定：`id` 是 int8，`blob` 是 bytea。
    let mut parse = cstring("b1");
    parse.extend_from_slice(&cstring("INSERT INTO metrics VALUES ($1, 1.5, 1, $2, 7)"));
    parse.extend_from_slice(&2_i16.to_be_bytes());
    parse.extend_from_slice(&20_u32.to_be_bytes());
    parse.extend_from_slice(&17_u32.to_be_bytes());
    send_message(&mut client, b'P', &parse);

    let mut bind = cstring("bp1");
    bind.extend_from_slice(&cstring("b1"));
    bind.extend_from_slice(&1_i16.to_be_bytes()); // 一个 format 码作用于全部参数
    bind.extend_from_slice(&1_i16.to_be_bytes()); // binary
    bind.extend_from_slice(&2_i16.to_be_bytes());
    bind.extend_from_slice(&8_i32.to_be_bytes());
    bind.extend_from_slice(&1_i64.to_be_bytes());
    bind.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    bind.extend_from_slice(&payload);
    bind.extend_from_slice(&0_i16.to_be_bytes()); // 结果格式沿用文本
    send_message(&mut client, b'B', &bind);
    let mut execute = cstring("bp1");
    execute.extend_from_slice(&0_i32.to_be_bytes());
    send_message(&mut client, b'E', &execute);
    send_message(&mut client, b'S', &[]);
    let executed = receive_until_ready(&mut client);
    let command = executed.iter().find(|(tag, _)| *tag == b'C').expect("CommandComplete");
    assert_eq!(command.1, b"INSERT 0 1\0", "{executed:?}");

    // 零长载荷同样是合法的二进制值，不是 NULL。
    let mut parse = cstring("b2");
    parse.extend_from_slice(&cstring("INSERT INTO metrics VALUES (2, 1.5, 1, $1, 7)"));
    parse.extend_from_slice(&1_i16.to_be_bytes());
    parse.extend_from_slice(&17_u32.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    let mut bind = cstring("bp2");
    bind.extend_from_slice(&cstring("b2"));
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&0_i32.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);
    let mut execute = cstring("bp2");
    execute.extend_from_slice(&0_i32.to_be_bytes());
    send_message(&mut client, b'E', &execute);
    send_message(&mut client, b'S', &[]);
    let inserted = receive_until_ready(&mut client);
    assert!(inserted.iter().any(|(tag, payload)| *tag == b'C' && payload == b"INSERT 0 1\0"), "{inserted:?}");

    // NULL 保持 NULL，不会退化成零长载荷。
    send_query(&mut client, "INSERT INTO metrics VALUES (3, 1.5, 1, NULL, 7)");
    assert_eq!(receive_until_ready(&mut client)[0].0, b'C');

    // 二进制结果格式原样回读；`type_size` 仍按变长（-1）声明。
    let mut parse = cstring("b3");
    parse.extend_from_slice(&cstring("SELECT id, blob FROM metrics ORDER BY id"));
    parse.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    let mut bind = cstring("bp3");
    bind.extend_from_slice(&cstring("b3"));
    bind.extend_from_slice(&0_i16.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);
    let mut describe = vec![b'P'];
    describe.extend_from_slice(&cstring("bp3"));
    send_message(&mut client, b'D', &describe);
    let mut execute = cstring("bp3");
    execute.extend_from_slice(&0_i32.to_be_bytes());
    send_message(&mut client, b'E', &execute);
    send_message(&mut client, b'S', &[]);
    let rows = receive_until_ready(&mut client);
    let description = rows.iter().find(|(tag, _)| *tag == b'T').map(|(_, payload)| parse_row_description(payload)).expect("RowDescription");
    assert_eq!(description, vec![("id".to_string(), 20, 8, 1), ("blob".to_string(), 17, -1, 1)]);
    let data: Vec<Vec<Option<Vec<u8>>>> = rows.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect();
    assert_eq!(data.len(), 3);
    assert_eq!(data[0][1], Some(payload.clone()));
    assert_eq!(data[1][1], Some(Vec::new()));
    assert_eq!(data[2][1], None);

    // 文本结果格式用 PostgreSQL 的 `\\x` 十六进制拼写，仍是无损表示。
    let mut bind = cstring("bp4");
    bind.extend_from_slice(&cstring("b3"));
    bind.extend_from_slice(&0_i16.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);
    let mut execute = cstring("bp4");
    execute.extend_from_slice(&0_i32.to_be_bytes());
    send_message(&mut client, b'E', &execute);
    send_message(&mut client, b'S', &[]);
    let rows = receive_until_ready(&mut client);
    let data: Vec<Vec<Option<Vec<u8>>>> = rows.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect();
    assert_eq!(data[0][1], Some(b"\\x00FFFE0041C328".to_vec()));

    // 文本格式的 bytea 参数是转义拼写而不是原始 octets，必须稳定拒绝。
    let mut parse = cstring("b4");
    parse.extend_from_slice(&cstring("INSERT INTO metrics VALUES (9, 1.5, 1, $1, 7)"));
    parse.extend_from_slice(&1_i16.to_be_bytes());
    parse.extend_from_slice(&17_u32.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    let mut bind = cstring("bp5");
    bind.extend_from_slice(&cstring("b4"));
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes()); // text
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&6_i32.to_be_bytes());
    bind.extend_from_slice(b"\\x00FF");
    bind.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);
    send_message(&mut client, b'S', &[]);
    let rejected = receive_until_ready(&mut client);
    let error = rejected.iter().find(|(tag, _)| *tag == b'E').expect("text-format bytea must be rejected");
    let text = String::from_utf8_lossy(&error.1).to_string();
    assert!(text.contains("0A000"), "{text}");
    assert!(text.contains("bytea"), "{text}");

    drop(client);
    server.join().unwrap();
}

/// 窗口函数的排名列经 `Describe` 下发的是声明的 `int8`（OID 20，8 字节），
/// 不按结果行推断，也不退化成 `text`。
#[test]
fn window_rank_columns_are_described_as_int8() {
    let (mut client, server) = start_server_with_runtime();

    send_query(&mut client, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')");
    let inserted = receive_until_ready(&mut client);
    assert_eq!(inserted[0], (b'C', b"INSERT 0 3\0".to_vec()));

    let mut parse = cstring("w1");
    parse.extend_from_slice(&cstring(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id), RANK() OVER (ORDER BY id), \
         DENSE_RANK() OVER (ORDER BY id) FROM users",
    ));
    parse.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    send_message(&mut client, b'D', &describe_of(&cstring("w1")));
    send_message(&mut client, b'S', &[]);
    let described = receive_until_ready(&mut client);
    assert_eq!(described[0].0, b'1');
    assert_eq!(described[1].0, b't');
    assert_eq!(described[2].0, b'T');
    assert_eq!(
        parse_row_description(&described[2].1),
        vec![
            ("id".to_string(), 20, 8, 0),
            ("ROW_NUMBER".to_string(), 20, 8, 0),
            ("RANK".to_string(), 20, 8, 0),
            ("DENSE_RANK".to_string(), 20, 8, 0),
        ]
    );

    // 实际结果也是 64 位整数：文本格式下每列就是排名文本，行序由 `ORDER BY id` 决定。
    send_query(&mut client, "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM users ORDER BY id");
    let rows = receive_until_ready(&mut client);
    assert_eq!(rows[0].0, b'T');
    assert_eq!(parse_row_description(&rows[0].1), vec![("id".to_string(), 20, 8, 0), ("ROW_NUMBER".to_string(), 20, 8, 0)]);
    let data: Vec<Vec<Option<Vec<u8>>>> = rows.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect();
    assert_eq!(data.len(), 3);
    assert_eq!(data[0], vec![Some(b"1".to_vec()), Some(b"1".to_vec())]);
    assert_eq!(data[2], vec![Some(b"3".to_vec()), Some(b"3".to_vec())]);

    drop(client);
    server.join().unwrap();
}

/// 嵌套调用不改变 `Describe` 的类型：聚合实参里的标量调用让外层聚合照旧下发
/// `int8`，`COALESCE(LOWER(name), UPPER(name))` 照旧下发 `text`。
#[test]
fn nested_call_columns_keep_declared_types() {
    let (mut client, server) = start_server_with_runtime();

    send_query(&mut client, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')");
    let inserted = receive_until_ready(&mut client);
    assert_eq!(inserted[0], (b'C', b"INSERT 0 2\0".to_vec()));

    let mut parse = cstring("n1");
    parse.extend_from_slice(&cstring("SELECT SUM(LENGTH(name)) AS total_len, SUM(CHAR_LENGTH(name)) AS total_chars FROM users"));
    parse.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    send_message(&mut client, b'D', &describe_of(&cstring("n1")));
    send_message(&mut client, b'S', &[]);
    let described = receive_until_ready(&mut client);
    assert_eq!(parse_row_description(&described[2].1), vec![("total_len".to_string(), 20, 8, 0), ("total_chars".to_string(), 20, 8, 0)]);

    send_query(&mut client, "SELECT SUM(LENGTH(name)), SUM(CHAR_LENGTH(name)) FROM users");
    let rows = receive_until_ready(&mut client);
    let data: Vec<Vec<Option<Vec<u8>>>> = rows.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect();
    assert_eq!(data, vec![vec![Some(b"8".to_vec()), Some(b"8".to_vec())]]);

    send_query(&mut client, "SELECT COALESCE(LOWER(name), UPPER(name)) AS name_label FROM users ORDER BY id");
    let rows = receive_until_ready(&mut client);
    assert_eq!(parse_row_description(&rows[0].1), vec![("name_label".to_string(), 25, -1, 0)]);
    let data: Vec<Vec<Option<Vec<u8>>>> = rows.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect();
    assert_eq!(data, vec![vec![Some(b"alice".to_vec())], vec![Some(b"bob".to_vec())]]);

    drop(client);
    server.join().unwrap();
}

/// `Parse` with no declared parameter OIDs must still describe parameter types
/// from the catalog. An `unspecified` (zero) OID forces strict drivers such as
/// `tokio-postgres` into a `pg_catalog` type lookup this gateway does not answer.
#[test]
fn describe_statement_infers_parameter_oids_from_the_catalog() {
    let (mut client, server) = start_server_with_runtime();

    let mut parse = cstring("ins");
    parse.extend_from_slice(&cstring("INSERT INTO users (id, name) VALUES ($1, $2) RETURNING id, name"));
    parse.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    send_message(&mut client, b'D', &describe_of(&cstring("ins")));
    send_message(&mut client, b'S', &[]);

    let described = receive_until_ready(&mut client);
    assert_eq!(described[0].0, b'1');
    assert_eq!(described[1].0, b't');
    let parameter = &described[1].1;
    let count = i16::from_be_bytes([parameter[0], parameter[1]]) as usize;
    let oids: Vec<u32> = (0..count).map(|index| u32::from_be_bytes(parameter[2 + index * 4..6 + index * 4].try_into().unwrap())).collect();
    // `users.id: i64` -> INT8 and `users.name: utf8` -> TEXT, positionally.
    assert_eq!(oids, vec![20, 25]);

    assert_eq!(described[2].0, b'T');
    let columns = parse_row_description(&described[2].1);
    let names: Vec<&str> = columns.iter().map(|(name, _, _, _)| name.as_str()).collect();
    assert_eq!(names, vec!["id", "name"]);

    drop(client);
    server.join().unwrap();
}

/// `Bind` must decode binary parameter bytes with the same catalog-inferred
/// OIDs `Describe` reported. A statement that declared no OIDs at `Parse` has
/// to bind `int8` / `text` payloads instead of failing with `42P18`.
#[test]
fn bind_decodes_binary_parameters_from_catalog_inferred_oids() {
    let (mut client, server) = start_server_with_runtime();

    let mut parse = cstring("bin");
    parse.extend_from_slice(&cstring("INSERT INTO users (id, name) VALUES ($1, $2) RETURNING id, name"));
    parse.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'P', &parse);

    let mut bind = cstring("bp");
    bind.extend_from_slice(&cstring("bin"));
    bind.extend_from_slice(&1_i16.to_be_bytes()); // one format code for every parameter
    bind.extend_from_slice(&1_i16.to_be_bytes()); // binary
    bind.extend_from_slice(&2_i16.to_be_bytes());
    bind.extend_from_slice(&8_i32.to_be_bytes());
    bind.extend_from_slice(&7_i64.to_be_bytes());
    bind.extend_from_slice(&3_i32.to_be_bytes());
    bind.extend_from_slice(b"ann");
    bind.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);

    let mut execute = cstring("bp");
    execute.extend_from_slice(&0_i32.to_be_bytes());
    send_message(&mut client, b'E', &execute);
    send_message(&mut client, b'S', &[]);

    let executed = receive_until_ready(&mut client);
    assert!(executed.iter().all(|(tag, _)| *tag != b'E'), "{executed:?}");
    assert!(executed.iter().any(|(tag, _)| *tag == b'C'), "{executed:?}");
    assert_eq!(executed.iter().filter(|(tag, _)| *tag == b'D').count(), 1, "{executed:?}");

    drop(client);
    server.join().unwrap();
}

/// A parameter the text scan cannot attribute to a column — a function argument
/// such as `strpos(name, $1)` — must still resolve to a concrete OID. PostgreSQL
/// never returns `unspecified`, and drivers query `pg_catalog` when it does.
#[test]
fn describe_statement_resolves_an_unattributed_parameter_to_text() {
    let (mut client, server) = start_server_with_runtime();

    let mut parse = cstring("sel");
    parse.extend_from_slice(&cstring("SELECT * FROM users WHERE strpos(name, $1) > 0"));
    parse.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    send_message(&mut client, b'D', &describe_of(&cstring("sel")));
    send_message(&mut client, b'S', &[]);

    let described = receive_until_ready(&mut client);
    assert_eq!(described[0].0, b'1');
    assert_eq!(described[1].0, b't');
    let parameter = &described[1].1;
    let count = i16::from_be_bytes([parameter[0], parameter[1]]) as usize;
    let oids: Vec<u32> = (0..count).map(|index| u32::from_be_bytes(parameter[2 + index * 4..6 + index * 4].try_into().unwrap())).collect();
    assert_eq!(oids, vec![25]);
    assert_eq!(described[2].0, b'T');

    drop(client);
    server.join().unwrap();
}

fn describe_of(statement: &[u8]) -> Vec<u8> {
    let mut describe = vec![b'S'];
    describe.extend_from_slice(statement);
    describe
}

/// `SAVEPOINT sp`、`ROLLBACK TO sp`、`RELEASE SAVEPOINT sp` 在 PostgreSQL 协议上
/// 各自发出对应的 CommandComplete 标签，并保持 ReadyForQuery 在事务内是 `T`。
/// 未知名字或重复名字的 SQLSTATE 都是 `3B000`。
#[test]
fn pgsql_savepoint_commands_emit_command_complete_tags() {
    let (mut client, server) = start_server_with_runtime();

    // 事务外发 SAVEPOINT 必须报 25001 (active_sql_transaction)。
    send_query(&mut client, "SAVEPOINT sp");
    let outside_tx = receive_until_ready(&mut client);
    assert_eq!(outside_tx[0].0, b'E');
    assert!(outside_tx[0].1.windows(5).any(|window| window == b"25001"));
    assert_eq!(outside_tx.last().unwrap(), &(b'Z', b"I".to_vec()));

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));

    // 成功创建 savepoint → CommandComplete "SAVEPOINT" + ReadyForQuery(T)
    send_query(&mut client, "SAVEPOINT sp");
    let created = receive_until_ready(&mut client);
    assert_eq!(created[0], (b'C', b"SAVEPOINT\0".to_vec()));
    assert_eq!(created.last().unwrap(), &(b'Z', b"T".to_vec()));

    // 重复名字报 3B000，事务内错误后期望 ReadyForQuery(E)。
    assert_sqlstate_in_tx(&mut client, "SAVEPOINT sp", b"3B000");
    // 事务失败后必须 ROLLBACK 才能继续。
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));

    // 重新开始测试 ROLLBACK TO / RELEASE 的错误码。
    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "SAVEPOINT sp");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SAVEPOINT\0".to_vec()));

    assert_sqlstate_in_tx(&mut client, "ROLLBACK TO nope", b"3B000");

    // 事务失败后必须 ROLLBACK + 重新 BEGIN 才能继续测试 RELEASE。
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));
    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "SAVEPOINT sp");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SAVEPOINT\0".to_vec()));

    assert_sqlstate_in_tx(&mut client, "RELEASE SAVEPOINT nope", b"3B000");

    // ROLLBACK TO 成功路径 — 独立事务。
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));
    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "SAVEPOINT sp");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SAVEPOINT\0".to_vec()));
    send_query(&mut client, "ROLLBACK TO sp");
    let rolled = receive_until_ready(&mut client);
    assert_eq!(rolled[0], (b'C', b"ROLLBACK\0".to_vec()));
    assert_eq!(rolled.last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "COMMIT");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));

    // RELEASE SAVEPOINT 成功路径 — 独立事务（RELEASE 不移除写状态只移除名字）。
    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "SAVEPOINT sp");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SAVEPOINT\0".to_vec()));
    send_query(&mut client, "RELEASE SAVEPOINT sp");
    let released = receive_until_ready(&mut client);
    assert_eq!(released[0], (b'C', b"RELEASE\0".to_vec()));
    assert_eq!(released.last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "COMMIT");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));

    drop(client);
    server.join().unwrap();
}

/// 事务内写→savepoint→写→rollback-to 必须把 PostgreSQL 状态恢复到 savepoint 时刻。
#[test]
fn pgsql_savepoint_rollback_restores_snapshot() {
    let (mut client, server) = start_server_with_runtime();

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));

    send_query(&mut client, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 2\0".to_vec()));

    send_query(&mut client, "SAVEPOINT sp");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SAVEPOINT\0".to_vec()));

    send_query(&mut client, "INSERT INTO users VALUES (3, 'carol')");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 1\0".to_vec()));

    // ROLLBACK TO 之后只剩 savepoint 之前的行。
    send_query(&mut client, "ROLLBACK TO sp");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"ROLLBACK\0".to_vec()));

    send_query(&mut client, "SELECT COUNT(*) FROM users");
    let count = receive_until_ready(&mut client);
    assert_eq!(parse_data_row(&count[1].1), vec![Some(b"2".to_vec())]);

    send_query(&mut client, "COMMIT");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));

    drop(client);
    server.join().unwrap();
}

/// `RELEASE SAVEPOINT sp` 移除命名但不回滚状态。
/// 之后 ROLLBACK TO 该名字必须报 3B000。
#[test]
fn pgsql_savepoint_release_removes_name_without_dropping_state() {
    let (mut client, server) = start_server_with_runtime();

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));

    send_query(&mut client, "SAVEPOINT a");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SAVEPOINT\0".to_vec()));
    send_query(&mut client, "SAVEPOINT b");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SAVEPOINT\0".to_vec()));

    send_query(&mut client, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 2\0".to_vec()));

    send_query(&mut client, "RELEASE SAVEPOINT a");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"RELEASE\0".to_vec()));

    // 释放之后状态仍然可见。
    send_query(&mut client, "SELECT COUNT(*) FROM users");
    let count = receive_until_ready(&mut client);
    assert_eq!(parse_data_row(&count[1].1), vec![Some(b"2".to_vec())]);

    // 已释放的名字不可再 rollback。
    assert_sqlstate_in_tx(&mut client, "ROLLBACK TO a", b"3B000");

    // 失败事务需要 COMMIT/ROLLBACK 恢复。
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));

    drop(client);
    server.join().unwrap();
}

/// PostgreSQL `SET known_var` must be accepted (no-op for non-`search_path` params),
/// `SET unknown_var` must report 42704, and in a failed transaction both must be rejected with 25P02.
#[test]
fn pgsql_set_known_variables_are_accepted_unknown_rejected() {
    let (mut client, server) = start_server();

    // 事务外：已知的非 search_path 参数接受为 no-op。
    send_query(&mut client, "SET client_encoding = 'UTF8'");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"SET\0".to_vec()), (b'Z', b"I".to_vec())]);

    send_query(&mut client, "SET timezone = 'UTC'");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"SET\0".to_vec()), (b'Z', b"I".to_vec())]);

    // 未知参数必须明确报 42704。
    send_query(&mut client, "SET nope_param = 'value'");
    let unknown = receive_until_ready(&mut client);
    assert_eq!(unknown[0].0, b'E');
    assert!(unknown[0].1.windows(5).any(|window| window == b"42704"));
    assert_eq!(unknown.last().unwrap(), &(b'Z', b"I".to_vec()));

    // 事务内执行一条导致 session.failed 的语句。
    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "SELECT (");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"E".to_vec()));

    // 失败事务内任何 SET / SHOW 都必须被 25P02 拒绝。
    assert_sqlstate_in_tx(&mut client, "SET client_encoding = 'UTF8'", b"25P02");
    assert_sqlstate_in_tx(&mut client, "SHOW server_version", b"25P02");

    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));

    // 恢复后 SET 又能工作。
    send_query(&mut client, "SET search_path = 'public'");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SET\0".to_vec()));

    drop(client);
    server.join().unwrap();
}

/// 事务内执行除零会把 session 切到 Failed 状态，之后任何非 ROLLBACK/COMMIT
/// 语句都被 25P02 拒绝。ROLLBACK 之后回到 Idle，正常查询恢复。
#[test]
fn pgsql_transaction_error_recovery_after_division_by_zero() {
    let (mut client, server) = start_server_with_runtime();

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));

    // PG 除零报 22012，并标记 session 失败 → ReadyForQuery(E)。
    send_query(&mut client, "SELECT 1 / 0");
    let divide = receive_until_ready(&mut client);
    assert_eq!(divide[0].0, b'E');
    assert!(divide[0].1.windows(5).any(|window| window == b"22012"));
    assert_eq!(divide.last().unwrap(), &(b'Z', b"E".to_vec()));

    // Failed 事务内后续 SELECT 被 25P02 拒绝。
    send_query(&mut client, "SELECT id FROM users");
    let aborted = receive_until_ready(&mut client);
    assert_eq!(aborted[0].0, b'E');
    assert!(aborted[0].1.windows(5).any(|window| window == b"25P02"));
    assert_eq!(aborted.last().unwrap(), &(b'Z', b"E".to_vec()));

    // ROLLBACK 回到 Idle。
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"ROLLBACK\0".to_vec()), (b'Z', b"I".to_vec())]);

    // 恢复后 SELECT 正常执行。
    send_query(&mut client, "SELECT 1");
    let ok = receive_until_ready(&mut client);
    assert_eq!(ok[0].0, b'T');
    assert_eq!(ok.last().unwrap(), &(b'Z', b"I".to_vec()));

    drop(client);
    server.join().unwrap();
}

/// `RESET <param>` / `RESET ALL` 恢复会话 GUC 默认值，未知参数报 `42704`，
/// 未实现的其他 `DISCARD` 目标显式拒绝。
#[test]
fn reset_and_discard_restore_session_defaults() {
    let (mut client, server) = start_server();

    send_query(&mut client, "SET search_path = 'public'");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"SET\0".to_vec()), (b'Z', b"I".to_vec())]);

    send_query(&mut client, "RESET search_path");
    let reset = receive_until_ready(&mut client);
    assert_eq!(reset[0], (b'C', b"RESET\0".to_vec()));
    assert_eq!(reset.last().unwrap(), &(b'Z', b"I".to_vec()));

    send_query(&mut client, "SHOW search_path");
    let shown = receive_until_ready(&mut client);
    assert_eq!(shown[1].0, b'D');
    let text = String::from_utf8_lossy(&shown[1].1);
    assert!(text.contains("$user") && text.contains("public"), "`RESET search_path` must restore the default, got {text}");

    // 未知参数必须明确报未识别。
    send_query(&mut client, "RESET nope_param");
    let unknown = receive_until_ready(&mut client);
    assert_eq!(unknown[0].0, b'E');
    assert!(unknown[0].1.windows(5).any(|window| window == b"42704"));
    assert_eq!(unknown.last().unwrap(), &(b'Z', b"I".to_vec()));

    // `RESET ALL` 同样回到默认值。
    send_query(&mut client, "SET search_path = 'public'");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SET\0".to_vec()));
    send_query(&mut client, "RESET ALL");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"RESET ALL\0".to_vec()), (b'Z', b"I".to_vec())]);
    send_query(&mut client, "SHOW search_path");
    let shown = receive_until_ready(&mut client);
    let text = String::from_utf8_lossy(&shown[1].1);
    assert!(text.contains("$user") && text.contains("public"), "`RESET ALL` must restore the default, got {text}");

    // 未实现的 DISCARD 目标必须显式拒绝，不能假装完成清理。
    send_query(&mut client, "DISCARD PLANS");
    let plans = receive_until_ready(&mut client);
    assert_eq!(plans[0].0, b'E');
    assert!(plans[0].1.windows(5).any(|window| window == b"0A000"));
    assert_eq!(plans.last().unwrap(), &(b'Z', b"I".to_vec()));

    drop(client);
    server.join().unwrap();
}

/// 事务块外 `DISCARD ALL` 恢复默认 GUC 并丢弃 prepared statement 缓存。
#[test]
fn discard_all_outside_transaction_resets_session_and_clears_caches() {
    let (mut client, server) = start_server_with_runtime();

    // 先登记一条命名 prepared statement。
    let mut parse = cstring("d1");
    parse.extend_from_slice(&cstring("SELECT id FROM users WHERE id = 1"));
    parse.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    send_message(&mut client, b'S', &[]);
    let parsed = receive_until_ready(&mut client);
    assert!(parsed.iter().any(|(tag, _)| *tag == b'1'), "Parse must complete: {parsed:?}");

    send_query(&mut client, "SET search_path = 'public'");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SET\0".to_vec()));

    // PostgreSQL 只允许事务块外执行 `DISCARD ALL`，先正常结束事务块。
    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "INSERT INTO users VALUES (1, 'alice')");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 1\0".to_vec()));
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));

    send_query(&mut client, "DISCARD ALL");
    let discarded = receive_until_ready(&mut client);
    assert_eq!(discarded[0], (b'C', b"DISCARD ALL\0".to_vec()));
    assert_eq!(discarded.last().unwrap(), &(b'Z', b"I".to_vec()));

    // 未提交写入由 `ROLLBACK` 丢弃，`SELECT` 会话仍然可用。
    send_query(&mut client, "SELECT COUNT(*) FROM users");
    let counted = receive_until_ready(&mut client);
    assert_eq!(parse_data_row(&counted[1].1), vec![Some(b"0".to_vec())]);

    // 默认 `search_path` 已恢复。
    send_query(&mut client, "SHOW search_path");
    let shown = receive_until_ready(&mut client);
    let text = String::from_utf8_lossy(&shown[1].1);
    assert!(text.contains("$user") && text.contains("public"), "`DISCARD ALL` must restore the default, got {text}");

    // prepared statement 缓存已丢弃，绑定名字必须报 26000。
    let mut bind = cstring("p1");
    bind.extend_from_slice(&cstring("d1"));
    bind.extend_from_slice(&0_i16.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);
    send_message(&mut client, b'S', &[]);
    let rejected = receive_until_ready(&mut client);
    let error = rejected.iter().find(|(tag, _)| *tag == b'E').expect("`DISCARD ALL` must drop the statement");
    assert!(error.1.windows(5).any(|window| window == b"26000"), "{}", String::from_utf8_lossy(&error.1));

    drop(client);
    server.join().unwrap();
}

/// 事务块内 `DISCARD ALL` 按 PostgreSQL 报 `25001`：不复位任何会话状态
/// （GUC 与 prepared statement 都不动），但错误同普通语句一样中止事务块
/// （ReadyForQuery 为 `E`），后续普通命令报 `25P02`，`ROLLBACK` 后恢复正常。
#[test]
fn discard_all_inside_transaction_block_is_rejected_without_touching_session() {
    let (mut client, server) = start_server_with_runtime();

    // 登记一条命名 prepared statement，被拒绝的 `DISCARD ALL` 不得清除它。
    let mut parse = cstring("d1");
    parse.extend_from_slice(&cstring("SELECT id FROM users WHERE id = 1"));
    parse.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    send_message(&mut client, b'S', &[]);
    let parsed = receive_until_ready(&mut client);
    assert!(parsed.iter().any(|(tag, _)| *tag == b'1'), "Parse must complete: {parsed:?}");

    send_query(&mut client, "SET search_path = 'custom'");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SET\0".to_vec()));

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "INSERT INTO users VALUES (1, 'alice')");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 1\0".to_vec()));

    send_query(&mut client, "DISCARD ALL");
    let rejected = receive_until_ready(&mut client);
    assert_eq!(rejected[0].0, b'E', "DISCARD ALL inside a transaction block must fail: {rejected:?}");
    assert!(rejected[0].1.windows(5).any(|window| window == b"25001"), "{}", String::from_utf8_lossy(&rejected[0].1));
    assert!(
        String::from_utf8_lossy(&rejected[0].1).contains("DISCARD ALL cannot run inside a transaction block"),
        "message must match PostgreSQL, got {}",
        String::from_utf8_lossy(&rejected[0].1)
    );
    // 错误中止事务块：ReadyForQuery 为 `E`。
    assert_eq!(rejected.last().unwrap(), &(b'Z', b"E".to_vec()));

    // 中止事务内的普通命令一律被 `25P02` 拒绝。
    assert_sqlstate_in_tx(&mut client, "SELECT 1", b"25P02");

    // `ROLLBACK` 结束中止的事务块，会话恢复正常。
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"ROLLBACK\0".to_vec()), (b'Z', b"I".to_vec())]);

    // 被拒绝的 `DISCARD ALL` 未做任何复位：GUC 仍是 SET 后的值。
    send_query(&mut client, "SHOW search_path");
    let shown = receive_until_ready(&mut client);
    let text = String::from_utf8_lossy(&shown[1].1);
    assert!(text.contains("custom"), "rejected `DISCARD ALL` must not reset the GUC, got {text}");

    // prepared statement 缓存未被清除。
    let mut bind = cstring("p1");
    bind.extend_from_slice(&cstring("d1"));
    bind.extend_from_slice(&0_i16.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);
    send_message(&mut client, b'S', &[]);
    let rebound = receive_until_ready(&mut client);
    assert!(rebound.iter().any(|(tag, _)| *tag == b'2'), "statement must survive the rejected DISCARD ALL: {rebound:?}");

    // 未提交写入由 `ROLLBACK` 丢弃。
    send_query(&mut client, "SELECT COUNT(*) FROM users");
    let counted = receive_until_ready(&mut client);
    assert_eq!(parse_data_row(&counted[1].1), vec![Some(b"0".to_vec())]);

    drop(client);
    server.join().unwrap();
}

/// `RESET` / `RESET ALL` 是 PostgreSQL 允许在事务块内执行的 utility 语句，
/// 不结束事务块。
#[test]
fn reset_commands_are_allowed_inside_transaction_block() {
    let (mut client, server) = start_server();

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));

    send_query(&mut client, "SET search_path = 'custom'");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"SET\0".to_vec()), (b'Z', b"T".to_vec())]);
    send_query(&mut client, "RESET search_path");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"RESET\0".to_vec()), (b'Z', b"T".to_vec())]);

    send_query(&mut client, "SET search_path = 'custom'");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SET\0".to_vec()));
    send_query(&mut client, "RESET ALL");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"RESET ALL\0".to_vec()), (b'Z', b"T".to_vec())]);

    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"ROLLBACK\0".to_vec()), (b'Z', b"I".to_vec())]);

    drop(client);
    server.join().unwrap();
}

/// 失败事务内 `RESET` / `DISCARD` 与普通语句一样被 `25P02` 拒绝。
#[test]
fn reset_and_discard_are_rejected_in_an_aborted_transaction() {
    let (mut client, server) = start_server();

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));

    // 无 runtime 的 SELECT 让事务进入 aborted 状态。
    send_query(&mut client, "SELECT 1");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"E".to_vec()));

    assert_sqlstate_in_tx(&mut client, "RESET search_path", b"25P02");
    assert_sqlstate_in_tx(&mut client, "RESET ALL", b"25P02");
    assert_sqlstate_in_tx(&mut client, "DISCARD ALL", b"25P02");

    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"ROLLBACK\0".to_vec()), (b'Z', b"I".to_vec())]);

    drop(client);
    server.join().unwrap();
}

/// 跑一轮完整的扩展协议 Parse/Bind/Describe('S')/Execute/Sync，返回服务端消息。
fn extended_utility_round(client: &mut TcpStream, statement: &str, sql: &str, portal: &str) -> Vec<(u8, Vec<u8>)> {
    let mut parse = cstring(statement);
    parse.extend_from_slice(&cstring(sql));
    parse.extend_from_slice(&0_i16.to_be_bytes()); // parameter_count = 0
    send_message(client, b'P', &parse);

    let mut bind = cstring(portal);
    bind.extend_from_slice(&cstring(statement));
    bind.extend_from_slice(&0_i16.to_be_bytes()); // 0 parameter format codes
    bind.extend_from_slice(&0_i16.to_be_bytes()); // 0 parameters
    bind.extend_from_slice(&0_i16.to_be_bytes()); // 0 result format codes
    send_message(client, b'B', &bind);

    let mut describe = vec![b'S'];
    describe.extend_from_slice(&cstring(statement));
    send_message(client, b'D', &describe);

    let mut execute = cstring(portal);
    execute.extend_from_slice(&0_i32.to_be_bytes()); // no row limit
    send_message(client, b'E', &execute);

    send_message(client, b'S', &[]);
    receive_until_ready(client)
}

/// 扩展协议识别 `RESET` / `DISCARD ALL` 为会话命令：Parse/Bind 成功，
/// Describe 回 `NoData`，Execute 只回 `CommandComplete`，执行副作用与
/// SimpleQuery 一致；匿名与命名 statement 都要能走通，`Close` 不受影响。
#[test]
fn extended_protocol_runs_session_reset_and_discard_commands() {
    let (mut client, server) = start_server_with_runtime();

    send_query(&mut client, "SET search_path = 'custom'");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SET\0".to_vec()));

    // 命名 prepared statement 上的 `RESET ALL`。
    let reset_all = extended_utility_round(&mut client, "ra", "RESET ALL", "pa");
    assert_eq!(
        reset_all.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        vec![b'1', b'2', b't', b'n', b'C', b'Z'],
        "RESET ALL round must be ParseComplete/BindComplete/NoData/CommandComplete: {reset_all:?}"
    );
    assert_eq!(reset_all[4], (b'C', b"RESET ALL\0".to_vec()));
    assert_eq!(reset_all.last().unwrap(), &(b'Z', b"I".to_vec()));
    send_query(&mut client, "SHOW search_path");
    let shown = receive_until_ready(&mut client);
    let text = String::from_utf8_lossy(&shown[1].1);
    assert!(text.contains("$user") && text.contains("public"), "extended `RESET ALL` must restore the default, got {text}");

    // `Close` 语义不受影响：会话命令的命名 statement 也能显式关闭。
    let mut close = vec![b'S'];
    close.extend_from_slice(&cstring("ra"));
    send_message(&mut client, b'C', &close);
    send_message(&mut client, b'S', &[]);
    let closed = receive_until_ready(&mut client);
    assert_eq!(closed[0].0, b'3', "CloseComplete expected: {closed:?}");

    // 匿名 prepared statement 上的 `RESET search_path`。
    send_query(&mut client, "SET search_path = 'custom'");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SET\0".to_vec()));
    let reset_one = extended_utility_round(&mut client, "", "RESET search_path", "");
    assert_eq!(
        reset_one.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        vec![b'1', b'2', b't', b'n', b'C', b'Z'],
        "RESET search_path round: {reset_one:?}"
    );
    assert_eq!(reset_one[4], (b'C', b"RESET\0".to_vec()));
    send_query(&mut client, "SHOW search_path");
    let shown = receive_until_ready(&mut client);
    let text = String::from_utf8_lossy(&shown[1].1);
    assert!(text.contains("$user") && text.contains("public"), "extended `RESET search_path` must restore the default, got {text}");

    // `DISCARD ALL` 还要清掉扩展协议 prepared statement 缓存。
    let mut parse = cstring("keep");
    parse.extend_from_slice(&cstring("SELECT id FROM users WHERE id = 1"));
    parse.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    send_message(&mut client, b'S', &[]);
    assert!(receive_until_ready(&mut client).iter().any(|(tag, _)| *tag == b'1'));

    send_query(&mut client, "SET search_path = 'custom'");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SET\0".to_vec()));
    let discard = extended_utility_round(&mut client, "da", "DISCARD ALL", "pd");
    assert_eq!(
        discard.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        vec![b'1', b'2', b't', b'n', b'C', b'Z'],
        "DISCARD ALL round: {discard:?}"
    );
    assert_eq!(discard[4], (b'C', b"DISCARD ALL\0".to_vec()));
    assert_eq!(discard.last().unwrap(), &(b'Z', b"I".to_vec()));

    send_query(&mut client, "SHOW search_path");
    let shown = receive_until_ready(&mut client);
    let text = String::from_utf8_lossy(&shown[1].1);
    assert!(text.contains("$user") && text.contains("public"), "extended `DISCARD ALL` must restore the default, got {text}");

    let mut bind = cstring("pp");
    bind.extend_from_slice(&cstring("keep"));
    bind.extend_from_slice(&0_i16.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);
    send_message(&mut client, b'S', &[]);
    let rejected = receive_until_ready(&mut client);
    let error = rejected.iter().find(|(tag, _)| *tag == b'E').expect("extended `DISCARD ALL` must drop the statement");
    assert!(error.1.windows(5).any(|window| window == b"26000"), "{}", String::from_utf8_lossy(&error.1));

    drop(client);
    server.join().unwrap();
}

/// 扩展协议事务块内 `DISCARD ALL` 报 `25001`：出错后按 discard-until-Sync
/// 丢弃同批次后续消息，错误中止事务块（Sync 后 ReadyForQuery 为 `E`），
/// 会话状态不被复位。
#[test]
fn extended_protocol_discard_all_inside_transaction_block_is_rejected() {
    let (mut client, server) = start_server_with_runtime();

    send_query(&mut client, "SET search_path = 'custom'");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SET\0".to_vec()));
    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));

    // 一批 Parse/Bind/Describe/Execute 之后、Sync 之前再发一个 Bind：必须被丢弃。
    let mut parse = cstring("da");
    parse.extend_from_slice(&cstring("DISCARD ALL"));
    parse.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    let mut bind = cstring("pd");
    bind.extend_from_slice(&cstring("da"));
    bind.extend_from_slice(&0_i16.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);
    let mut describe = vec![b'S'];
    describe.extend_from_slice(&cstring("da"));
    send_message(&mut client, b'D', &describe);
    let mut execute = cstring("pd");
    execute.extend_from_slice(&0_i32.to_be_bytes());
    send_message(&mut client, b'E', &execute);
    send_message(&mut client, b'B', &bind);
    send_message(&mut client, b'S', &[]);

    let responses = receive_until_ready(&mut client);
    assert_eq!(
        responses.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        vec![b'1', b'2', b't', b'n', b'E', b'Z'],
        "the second Bind must be discarded until Sync: {responses:?}"
    );
    assert!(responses[4].1.windows(5).any(|window| window == b"25001"), "{}", String::from_utf8_lossy(&responses[4].1));
    assert!(
        String::from_utf8_lossy(&responses[4].1).contains("DISCARD ALL cannot run inside a transaction block"),
        "message must match PostgreSQL, got {}",
        String::from_utf8_lossy(&responses[4].1)
    );
    // 错误中止事务块：Sync 后 ReadyForQuery 为 `E`。
    assert_eq!(responses.last().unwrap(), &(b'Z', b"E".to_vec()));

    // `ROLLBACK` 结束中止的事务块。
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"ROLLBACK\0".to_vec()), (b'Z', b"I".to_vec())]);

    // 被拒绝的 `DISCARD ALL` 未做任何复位。
    send_query(&mut client, "SHOW search_path");
    let shown = receive_until_ready(&mut client);
    let text = String::from_utf8_lossy(&shown[1].1);
    assert!(text.contains("custom"), "rejected `DISCARD ALL` must not reset the GUC, got {text}");

    drop(client);
    server.join().unwrap();
}

/// PostgreSQL Bind 参数的 boolean OID 必须接受 `on`/`off`/`yes`/`no`/`y`/`n`
/// 等 PG 扩展字面量，大小写不敏感。
#[test]
fn pgsql_boolean_parameter_accepts_postgres_extended_literals() {
    let (mut client, server) = start_server_with_runtime();

    // 准备一条带 bool 参数的语句，不指定 OID 让 gateway 自动推断。
    let mut parse = cstring("b1");
    parse.extend_from_slice(&cstring("SELECT id FROM metrics WHERE active = $1"));
    parse.extend_from_slice(&0_i16.to_be_bytes()); // parameter_count = 0 → 自动推断
    send_message(&mut client, b'P', &parse);
    send_message(&mut client, b'S', &[]);
    let parsed = receive_until_ready(&mut client);
    assert!(parsed.iter().all(|(tag, _)| *tag != b'E'), "parse must succeed");

    // Bind + Execute 用 'On'（大小写混合）作为参数值，gateway 会走 infer_parameter_value。
    let mut bind = cstring("");
    bind.extend_from_slice(&cstring("b1"));
    bind.extend_from_slice(&0_i16.to_be_bytes()); // 0 param format codes
    bind.extend_from_slice(&1_i16.to_be_bytes()); // 1 parameter
    let val: Vec<u8> = b"On".to_vec();
    bind.extend_from_slice(&(val.len() as i32).to_be_bytes());
    bind.extend_from_slice(&val);
    bind.extend_from_slice(&0_i16.to_be_bytes()); // 0 result format codes
    send_message(&mut client, b'B', &bind);
    let mut execute = cstring("");
    execute.extend_from_slice(&0_i32.to_be_bytes());
    send_message(&mut client, b'E', &execute);
    send_message(&mut client, b'S', &[]);
    let executed = receive_until_ready(&mut client);
    assert!(!executed.iter().any(|(tag, _)| *tag == b'E'), "Bind must accept 'On' as boolean-compatible value, got {:?}", executed);

    drop(client);
    server.join().unwrap();
}

/// 走 PostgreSQL extended protocol：`Parse`（显式声明 `text` 参数）+ `Bind`（文本参数）
/// + `Execute` + `Sync`，返回结果行。
fn extended_rows(client: &mut TcpStream, statement: &str, sql: &str, params: &[&str]) -> Vec<Vec<Option<Vec<u8>>>> {
    let mut parse = cstring(statement);
    parse.extend_from_slice(&cstring(sql));
    parse.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for _ in params {
        parse.extend_from_slice(&25_u32.to_be_bytes()); // TEXT oid
    }
    send_message(client, b'P', &parse);

    let mut bind = cstring("portal");
    bind.extend_from_slice(&cstring(statement));
    bind.extend_from_slice(&0_i16.to_be_bytes()); // 全部参数沿用默认 text 格式
    bind.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for param in params {
        bind.extend_from_slice(&(param.len() as i32).to_be_bytes());
        bind.extend_from_slice(param.as_bytes());
    }
    bind.extend_from_slice(&0_i16.to_be_bytes()); // 结果沿用 text 格式
    send_message(client, b'B', &bind);

    let mut execute = cstring("portal");
    execute.extend_from_slice(&0_i32.to_be_bytes());
    send_message(client, b'E', &execute);
    send_message(client, b'S', &[]);
    let responses = receive_until_ready(client);
    assert!(!responses.iter().any(|(tag, _)| *tag == b'E'), "extended query `{sql}` failed: {responses:?}");
    responses.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect()
}

/// Iris PostgreSQL 适配器生成的 `strpos` 与 `left` / `right` 谓词必须按 code point
/// 精确、大小写敏感地执行。
#[test]
fn extended_protocol_executes_iris_text_predicates_case_sensitively() {
    let (mut client, server) = start_server_with_runtime();

    for (id, name) in [(1, "alice"), (2, "Alicia"), (3, "bob")] {
        send_query(&mut client, &format!("INSERT INTO users VALUES ({id}, '{name}')"));
        assert_eq!(receive_until_ready(&mut client)[0].0, b'C');
    }

    // `strpos("f", $n) > 0`
    let matches = extended_rows(&mut client, "i1", "SELECT id FROM users WHERE strpos(name, $1) > 0", &["ice"]);
    assert_eq!(matches, vec![vec![Some(b"1".to_vec())]], "`ice` must match only `alice`");
    let misses = extended_rows(&mut client, "i2", "SELECT id FROM users WHERE strpos(name, $1) > 0", &["ICE"]);
    assert!(misses.is_empty(), "text predicates are case-sensitive, got {misses:?}");

    // `left("f", char_length($n)) = $m`
    let prefix = extended_rows(&mut client, "l1", "SELECT id FROM users WHERE left(name, char_length($1)) = $2", &["ali", "ali"]);
    assert_eq!(prefix, vec![vec![Some(b"1".to_vec())]]);
    let prefix_miss = extended_rows(&mut client, "l2", "SELECT id FROM users WHERE left(name, char_length($1)) = $2", &["ali", "ALI"]);
    assert!(prefix_miss.is_empty(), "the prefix comparison must stay case-sensitive, got {prefix_miss:?}");

    // `right("f", char_length($n)) = $m`
    let suffix = extended_rows(&mut client, "r1", "SELECT id FROM users WHERE right(name, char_length($1)) = $2", &["ice", "ice"]);
    assert_eq!(suffix, vec![vec![Some(b"1".to_vec())]]);
    let suffix_miss = extended_rows(&mut client, "r2", "SELECT id FROM users WHERE right(name, char_length($1)) = $2", &["ice", "ICE"]);
    assert!(suffix_miss.is_empty(), "the suffix comparison must stay case-sensitive, got {suffix_miss:?}");

    // 简单查询路径同样按 code point 计数：1-based 位置与字符前缀。
    send_query(&mut client, "SELECT left(name, 3), strpos(name, 'ice') FROM users WHERE id = 1");
    let rows = receive_until_ready(&mut client);
    let row: Vec<Option<Vec<u8>>> = rows.iter().find(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).unwrap();
    assert_eq!(row, vec![Some(b"ali".to_vec()), Some(b"3".to_vec())]);

    // `INSTR` 是 MySQL 拼写，PostgreSQL 必须按未知函数拒绝而不是近似执行。
    assert_sqlstate(&mut client, "SELECT INSTR(name, 'x') FROM users", b"42883");

    drop(client);
    server.join().unwrap();
}

/// 空 schema registry 的 runtime，用于验证 DDL 自身建立 name-resolution 视图。
fn start_server_with_empty_schema() -> (TcpStream, thread::JoinHandle<()>) {
    use std::sync::Arc;
    use yyds_execution::LocalExecutor;
    use yyds_gateway::SqlSchemaRegistry;
    use yyds_gateway_pgsql::connection::{PgsqlSqlRuntime, serve_connection};
    use yyds_kv::FileShard;
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let shard = FileShard::open(directory.path().join("s1.yykv")).expect("open file shard");
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(ShardEpoch(1), vec![shard_id.clone()]).expect("routing");
    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    let executor = LocalExecutor::new(routing, shards, Vec::new()).expect("executor");

    let runtime = Arc::new(PgsqlSqlRuntime { schema: Arc::new(SqlSchemaRegistry::new_in_memory()), executor: Arc::new(executor) });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        // 保活 TempFolder：shard 文件必须在服务线程存活期间保持可用。
        let _directory = directory;
        let (stream, _) = listener.accept().unwrap();
        serve_connection(stream, Some(runtime)).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut startup = Vec::from(0_u32.to_be_bytes());
    startup.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
    startup.extend_from_slice(b"user\0test\0\0");
    let length = startup.len() as u32;
    startup[..4].copy_from_slice(&length.to_be_bytes());
    client.write_all(&startup).unwrap();
    let startup_responses = receive_until_ready(&mut client);
    assert_eq!(startup_responses.last().unwrap(), &(b'Z', b"I".to_vec()));
    (client, server)
}

/// 读取 SimpleQuery 结果集的唯一一行。
fn single_row(client: &mut TcpStream, sql: &str) -> Vec<Option<Vec<u8>>> {
    send_query(client, sql);
    let responses = receive_until_ready(client);
    assert!(!responses.iter().any(|(tag, _)| *tag == b'E'), "`{sql}` failed: {responses:?}");
    let rows: Vec<Vec<Option<Vec<u8>>>> =
        responses.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect();
    assert_eq!(rows.len(), 1, "`{sql}` must return exactly one row, got {rows:?}");
    rows.into_iter().next().unwrap()
}

/// 读取 SimpleQuery 结果集的所有行。
fn rows(client: &mut TcpStream, sql: &str) -> Vec<Vec<Option<Vec<u8>>>> {
    send_query(client, sql);
    let responses = receive_until_ready(client);
    assert!(!responses.iter().any(|(tag, _)| *tag == b'E'), "`{sql}` failed: {responses:?}");
    responses.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect()
}

fn value(value: &str) -> Option<Vec<u8>> {
    Some(value.as_bytes().to_vec())
}

#[test]
fn pgsql_declared_capacity_rejects_oversized_strings() {
    let (mut client, server) = start_server_with_empty_schema();

    send_query(&mut client, "CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR(191) NOT NULL)");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"CREATE TABLE\0".to_vec()));

    // 191 个字符正好压在声明容量上：必须原样写入并回读。
    let fits = "a".repeat(191);
    send_query(&mut client, &format!("INSERT INTO users VALUES (1, '{fits}')"));
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 1\0".to_vec()));
    assert_eq!(single_row(&mut client, "SELECT name FROM users WHERE id = 1"), vec![Some(fits.into_bytes())]);

    // 192 个字符越界：SQLSTATE 22001，且失败语句不得留下半行。
    let too_long = "a".repeat(192);
    assert_sqlstate(&mut client, &format!("INSERT INTO users VALUES (2, '{too_long}')"), b"22001");
    let stored = rows(&mut client, "SELECT name FROM users WHERE id = 2");
    assert!(stored.is_empty(), "oversized insert must not be stored: {stored:?}");

    // 多字节字符按字符数计数，不按 octets：191 个汉字合法，192 个越界。
    let fits_multibyte = "字".repeat(191);
    send_query(&mut client, &format!("INSERT INTO users VALUES (3, '{fits_multibyte}')"));
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 1\0".to_vec()));
    assert_eq!(single_row(&mut client, "SELECT name FROM users WHERE id = 3"), vec![Some(fits_multibyte.into_bytes())]);
    assert_sqlstate(&mut client, &format!("INSERT INTO users VALUES (4, '{}')", "字".repeat(192)), b"22001");

    drop(client);
    server.join().unwrap();
}

/// 启动一个共享同一 schema registry 的服务器，接受 `connections` 个客户端。
///
/// 值序列必须跨连接共享同一个 registry，否则两个连接会各自从 1 开始发号。
fn start_shared_schema_server(connections: usize) -> (Vec<TcpStream>, thread::JoinHandle<()>) {
    use std::sync::Arc;
    use yyds_execution::LocalExecutor;
    use yyds_gateway::SqlSchemaRegistry;
    use yyds_gateway_pgsql::connection::{PgsqlSqlRuntime, serve_connection};
    use yyds_kv::FileShard;
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let directory = boost_fs::temp::create_folder().expect("temp folder");
    let shard = FileShard::open(directory.path().join("s1.yykv")).expect("open file shard");
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(ShardEpoch(1), vec![shard_id.clone()]).expect("routing");
    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    let executor = LocalExecutor::new(routing, shards, Vec::new()).expect("executor");
    let runtime = Arc::new(PgsqlSqlRuntime { schema: Arc::new(SqlSchemaRegistry::new_in_memory()), executor: Arc::new(executor) });

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        // 保活 TempFolder：shard 文件必须在 worker 存活期间保持可用。
        let _directory = directory;
        let mut workers = Vec::new();
        for _ in 0..connections {
            let (stream, _) = listener.accept().unwrap();
            let runtime = Arc::clone(&runtime);
            workers.push(thread::spawn(move || serve_connection(stream, Some(runtime)).unwrap()));
        }
        for worker in workers {
            worker.join().unwrap();
        }
    });
    let clients = (0..connections).map(|_| connect_client(address)).collect();
    (clients, server)
}

/// 完成 protocol-v3 startup 并等待 `ReadyForQuery`。
fn connect_client(address: std::net::SocketAddr) -> TcpStream {
    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut startup = Vec::from(0_u32.to_be_bytes());
    startup.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
    startup.extend_from_slice(b"user\0test\0\0");
    let length = startup.len() as u32;
    startup[..4].copy_from_slice(&length.to_be_bytes());
    client.write_all(&startup).unwrap();
    let startup_responses = receive_until_ready(&mut client);
    assert_eq!(startup_responses.last().unwrap(), &(b'Z', b"I".to_vec()));
    client
}

/// 执行一条 `INSERT ... RETURNING "id"` 并读出返回的整数。
fn insert_returning_id(client: &mut TcpStream, sql: &str) -> i64 {
    send_query(client, sql);
    let responses = receive_until_ready(client);
    assert!(!responses.iter().any(|(tag, _)| *tag == b'E'), "`{sql}` failed: {responses:?}");
    let row = responses
        .iter()
        .find(|(tag, _)| *tag == b'D')
        .map(|(_, payload)| parse_data_row(payload))
        .expect("RETURNING statement must return one row");
    let text = String::from_utf8_lossy(row[0].as_deref().expect("id must not be null")).into_owned();
    text.parse().expect("id is an integer")
}

#[test]
fn pgsql_bigserial_allocates_ids_and_does_not_rewind_on_rollback() {
    let (mut client, server) = start_server_with_empty_schema();
    send_query(&mut client, "CREATE TABLE \"_iris_outbox\" (\"id\" BIGSERIAL PRIMARY KEY, \"operation_id\" TEXT NOT NULL)");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"CREATE TABLE\0".to_vec()));

    // 连续插入：省略 `id` 时由 registry 分配 1、2、3。
    for expected in 1..=3 {
        let id = insert_returning_id(&mut client, "INSERT INTO \"_iris_outbox\" (\"operation_id\") VALUES ('op') RETURNING \"id\"");
        assert_eq!(id, expected);
    }

    // `nextval` 不回滚：事务内分配后回滚，下一个值继续前进，不复用 4。
    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    let rolled_back = insert_returning_id(&mut client, "INSERT INTO \"_iris_outbox\" (\"operation_id\") VALUES ('rolled') RETURNING \"id\"");
    assert_eq!(rolled_back, 4);
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));
    assert_eq!(insert_returning_id(&mut client, "INSERT INTO \"_iris_outbox\" (\"operation_id\") VALUES ('committed') RETURNING \"id\"",), 5);

    drop(client);
    server.join().unwrap();
}

#[test]
fn pgsql_two_connections_never_allocate_the_same_serial_value() {
    let (mut clients, server) = start_shared_schema_server(2);
    send_query(&mut clients[0], "CREATE TABLE \"_iris_outbox\" (\"id\" BIGSERIAL PRIMARY KEY, \"operation_id\" TEXT NOT NULL)");
    assert_eq!(receive_until_ready(&mut clients[0])[0], (b'C', b"CREATE TABLE\0".to_vec()));

    let mut allocated = Vec::new();
    for _ in 0..5 {
        for client in clients.iter_mut() {
            allocated.push(insert_returning_id(client, "INSERT INTO \"_iris_outbox\" (\"operation_id\") VALUES ('op') RETURNING \"id\""));
        }
    }
    allocated.sort_unstable();
    assert_eq!(allocated, (1..=10).collect::<Vec<_>>());

    drop(clients);
    server.join().unwrap();
}

#[test]
fn pgsql_unique_constraint_reports_23505_and_keeps_the_row_set() {
    let (mut client, server) = start_server_with_empty_schema();
    send_query(
        &mut client,
        "CREATE TABLE \"_iris_outbox\" (\"id\" BIGSERIAL PRIMARY KEY, \"operation_id\" TEXT NOT NULL, \
         \"entity_version\" BIGINT NOT NULL, \"payload\" TEXT, UNIQUE (\"operation_id\", \"entity_version\"))",
    );
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"CREATE TABLE\0".to_vec()));

    send_query(&mut client, "INSERT INTO \"_iris_outbox\" (\"operation_id\", \"entity_version\", \"payload\") VALUES ('op-1', 1, 'first')");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 1\0".to_vec()));

    // 组合键重复必须以 `23505` 拒绝，不能静默写入第二行。
    assert_sqlstate(
        &mut client,
        "INSERT INTO \"_iris_outbox\" (\"operation_id\", \"entity_version\", \"payload\") VALUES ('op-1', 1, 'duplicate')",
        b"23505",
    );
    assert_eq!(rows(&mut client, "SELECT \"payload\" FROM \"_iris_outbox\""), vec![vec![Some(b"first".to_vec())]]);

    send_query(&mut client, "INSERT INTO \"_iris_outbox\" (\"operation_id\", \"entity_version\", \"payload\") VALUES ('op-2', 1, 'second')");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 1\0".to_vec()));
    // UPDATE 撞上同一组合键同样返回 `23505`，且旧行保持原值。
    assert_sqlstate(&mut client, "UPDATE \"_iris_outbox\" SET \"operation_id\" = 'op-1' WHERE \"operation_id\" = 'op-2'", b"23505");
    assert_eq!(
        rows(&mut client, "SELECT \"operation_id\", \"entity_version\" FROM \"_iris_outbox\" ORDER BY \"id\""),
        vec![vec![Some(b"op-1".to_vec()), Some(b"1".to_vec())], vec![Some(b"op-2".to_vec()), Some(b"1".to_vec())],]
    );

    drop(client);
    server.join().unwrap();
}

#[test]
fn pgsql_metadata_tables_columns_and_scalars() {
    let (mut client, server) = start_server_with_empty_schema();

    send_query(&mut client, "CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR(191) NOT NULL, active BOOLEAN, score DOUBLE PRECISION)");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"CREATE TABLE\0".to_vec()));

    assert_eq!(
        rows(
            &mut client,
            "SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
        ),
        vec![vec![value("public"), value("users"), value("BASE TABLE")]]
    );

    let columns = extended_rows(
        &mut client,
        "m1",
        "SELECT column_name, udt_name, is_nullable, character_maximum_length FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position",
        &["users"],
    );
    assert_eq!(
        columns,
        vec![
            vec![value("id"), value("int8"), value("NO"), None],
            vec![value("email"), value("varchar"), value("NO"), value("191")],
            vec![value("active"), value("bool"), value("YES"), None],
            vec![value("score"), value("float8"), value("YES"), None],
        ]
    );

    assert_eq!(single_row(&mut client, "SELECT current_schema()"), vec![value("public")]);
    assert_eq!(single_row(&mut client, "SELECT version()"), vec![value(yyds_gateway::PG_SERVER_VERSION)]);

    drop(client);
    server.join().unwrap();
}

/// Iris PostgreSQL 适配器的两跳 inspect：先列出 `information_schema.tables`，
/// 再用逐字 `EXISTS` 主键探测读取每张表的列。
#[test]
fn pgsql_iris_inspect_two_hop_reports_tables_then_primary_keys() {
    let (mut client, server) = start_server_with_empty_schema();

    for sql in [
        "CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR(191) NOT NULL, active BOOLEAN)",
        "CREATE TABLE posts (id BIGINT PRIMARY KEY, title TEXT)",
        "CREATE TABLE \"_iris_outbox\" (id BIGSERIAL PRIMARY KEY, operation_id TEXT NOT NULL, entity_version BIGINT NOT NULL, payload TEXT NOT NULL, UNIQUE (operation_id, entity_version))",
        "CREATE TABLE \"_iris_commit\" (id BIGINT PRIMARY KEY, shard TEXT NOT NULL, seq BIGINT NOT NULL)",
    ] {
        send_query(&mut client, sql);
        assert_eq!(receive_until_ready(&mut client)[0].0, b'C', "{sql}");
    }

    // 第一跳：Iris `client.query` 的逐字 SQL（无参数）。
    let tables = extended_rows(
        &mut client,
        "t1",
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
         ORDER BY table_name",
        &[],
    );
    assert_eq!(tables, vec![vec![value("_iris_commit")], vec![value("_iris_outbox")], vec![value("posts")], vec![value("users")]]);

    // 第二跳：Iris 逐字的 `EXISTS` 主键探测，`$1` 为表名。
    let sql = "SELECT
            c.column_name,
            c.udt_name,
            c.is_nullable,
            EXISTS (
                SELECT 1
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.table_schema = 'public'
                  AND tc.table_name = c.table_name
                  AND tc.constraint_type = 'PRIMARY KEY'
                  AND kcu.column_name = c.column_name
            ) AS is_pk
         FROM information_schema.columns c
         WHERE c.table_schema = 'public' AND c.table_name = $1
         ORDER BY c.ordinal_position";
    assert_eq!(
        extended_rows(&mut client, "u1", sql, &["users"]),
        vec![
            vec![value("id"), value("int8"), value("NO"), value("t")],
            vec![value("email"), value("varchar"), value("NO"), value("f")],
            vec![value("active"), value("bool"), value("YES"), value("f")],
        ]
    );
    assert_eq!(
        extended_rows(&mut client, "p1", sql, &["posts"]),
        vec![vec![value("id"), value("int8"), value("NO"), value("t")], vec![value("title"), value("text"), value("YES"), value("f")],]
    );

    drop(client);
    server.join().unwrap();
}

#[test]
fn pgsql_metadata_catalog_views_and_rejections() {
    let (mut client, server) = start_server_with_empty_schema();

    send_query(&mut client, "CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR(191) NOT NULL, active BOOLEAN, score DOUBLE PRECISION)");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"CREATE TABLE\0".to_vec()));

    assert_eq!(
        rows(&mut client, "SELECT attname, atttypid, attnotnull, attnum FROM pg_catalog.pg_attribute WHERE attrelid = 1 ORDER BY attnum",),
        vec![
            vec![value("id"), value("20"), value("t"), value("1")],
            vec![value("email"), value("1043"), value("t"), value("2")],
            vec![value("active"), value("16"), value("f"), value("3")],
            vec![value("score"), value("701"), value("f"), value("4")],
        ]
    );

    assert_eq!(
        rows(&mut client, "SELECT oid, typname FROM pg_catalog.pg_type ORDER BY oid"),
        vec![
            vec![value("16"), value("bool")],
            vec![value("20"), value("int8")],
            vec![value("701"), value("float8")],
            vec![value("1043"), value("varchar")],
        ]
    );

    // pg_catalog join 超出支持形状，必须显式 0A000 而不是近似执行。
    assert_sqlstate(
        &mut client,
        "SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace",
        b"0A000",
    );

    drop(client);
    server.join().unwrap();
}

#[test]
fn pgsql_ddl_builds_the_runtime_catalog_and_reports_catalog_errors() {
    let (mut client, server) = start_server_with_empty_schema();

    send_query(&mut client, "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(191) NOT NULL)");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"CREATE TABLE\0".to_vec()), (b'Z', b"I".to_vec())]);

    assert_sqlstate(&mut client, "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY)", b"42P07");
    send_query(&mut client, "CREATE TABLE IF NOT EXISTS users (id BIGINT NOT NULL PRIMARY KEY)");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"CREATE TABLE\0".to_vec()));

    send_query(&mut client, "INSERT INTO users (id, name) VALUES (1, 'alice')");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 1\0".to_vec()));
    assert_eq!(single_row(&mut client, "SELECT id, name FROM users WHERE id = 1"), vec![Some(b"1".to_vec()), Some(b"alice".to_vec())]);

    send_query(&mut client, "ALTER TABLE users ADD COLUMN age INTEGER");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"ALTER TABLE\0".to_vec()));
    assert_eq!(single_row(&mut client, "SELECT age FROM users WHERE id = 1"), vec![None]);
    assert_sqlstate(&mut client, "ALTER TABLE users ADD COLUMN name VARCHAR(191)", b"42701");

    send_query(&mut client, "CREATE INDEX idx_users_name ON users (name)");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"CREATE INDEX\0".to_vec()));
    assert_sqlstate(&mut client, "CREATE INDEX idx_users_name ON users (name)", b"42P07");
    assert_sqlstate(&mut client, "DROP INDEX nope", b"42704");

    // DDL 失败同样把事务置为 aborted，后续语句必须是 `25P02`。
    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    assert_sqlstate_in_tx(&mut client, "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY)", b"42P07");
    assert_sqlstate_in_tx(&mut client, "SELECT 1", b"25P02");
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"ROLLBACK\0".to_vec()), (b'Z', b"I".to_vec())]);

    send_query(&mut client, "DROP TABLE users");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"DROP TABLE\0".to_vec()));
    assert_sqlstate(&mut client, "SELECT id FROM users", b"42P01");

    drop(client);
    server.join().unwrap();
}

#[test]
fn pgsql_runs_a_multi_statement_simple_query_batch() {
    let (mut client, server) = start_server_with_empty_schema();

    let commit = "CREATE TABLE IF NOT EXISTS \"_iris_commit\" (\"shard\" TEXT PRIMARY KEY NOT NULL, \"value\" TEXT NOT NULL)";
    let outbox = "CREATE TABLE IF NOT EXISTS \"_iris_outbox\" (\"id\" BIGSERIAL PRIMARY KEY, \"shard\" TEXT NOT NULL, \"seq\" BIGINT NOT NULL, \"operation_id\" TEXT NOT NULL, \"table_name\" TEXT NOT NULL, \"entity_id\" TEXT NOT NULL, \"entity_version\" BIGINT NOT NULL, \"effect\" TEXT NOT NULL, UNIQUE (\"operation_id\", \"entity_version\"))";
    let index = "CREATE INDEX IF NOT EXISTS \"_iris_outbox_seq\" ON \"_iris_outbox\" (\"shard\", \"seq\")";
    send_query(&mut client, &format!("{commit};\n{outbox};\n{index};"));

    let responses = receive_until_ready(&mut client);
    assert_eq!(responses.len(), 4);
    assert_eq!(responses[0], (b'C', b"CREATE TABLE\0".to_vec()));
    assert_eq!(responses[1], (b'C', b"CREATE TABLE\0".to_vec()));
    assert_eq!(responses[2], (b'C', b"CREATE INDEX\0".to_vec()));
    assert_eq!(responses[3], (b'Z', b"I".to_vec()));

    // 两条 DDL 都真实落到 registry：两个表都可查询。
    assert!(rows(&mut client, "SELECT \"shard\" FROM \"_iris_commit\"").is_empty());
    assert!(rows(&mut client, "SELECT \"seq\" FROM \"_iris_outbox\"").is_empty());
    assert_eq!(single_row(&mut client, "SELECT COUNT(*) FROM \"_iris_outbox\""), vec![Some(b"0".to_vec())]);

    drop(client);
    server.join().unwrap();
}

#[test]
fn pgsql_answers_an_empty_query_with_empty_query_response() {
    let (mut client, server) = start_server_with_empty_schema();

    send_query(&mut client, "");
    let responses = receive_until_ready(&mut client);
    assert_eq!(responses, vec![(b'I', Vec::new()), (b'Z', b"I".to_vec())]);

    // The connection stays usable: a probe must not abort the session.
    assert!(rows(&mut client, "SELECT 1").len() == 1);

    drop(client);
    server.join().unwrap();
}

#[test]
fn pgsql_batch_reports_statement_index_and_offset() {
    let (mut client, server) = start_server_with_empty_schema();

    let first = "CREATE TABLE IF NOT EXISTS \"_iris_commit\" (\"shard\" TEXT PRIMARY KEY NOT NULL)";
    let second = "CREATE TABLE \"_iris_outbox\" (\"id\" BIGSERIAL PRIMARY KEY) GENERATED ALWAYS AS IDENTITY";
    let third = "CREATE TABLE never_runs (\"id\" BIGINT)";
    let sql = format!("{first}; {second}; {third}");
    send_query(&mut client, &sql);

    let responses = receive_until_ready(&mut client);
    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0], (b'C', b"CREATE TABLE\0".to_vec()));
    assert_eq!(responses[1].0, b'E');
    let message = error_message(&responses[1].1);
    assert!(message.contains("statement 2 of 3 failed at batch offset"), "{message}");
    let expected_offset = first.len() + 2;
    assert!(message.contains(&format!("offset {expected_offset}")), "{message}");
    assert_eq!(responses[2], (b'Z', b"I".to_vec()));

    // 批次在失败语句处停止：第三条从未执行。
    assert_sqlstate(&mut client, "SELECT \"id\" FROM never_runs", b"42P01");
    // 事务外批次按自动提交语义保留第一条 DDL。
    assert!(rows(&mut client, "SELECT \"shard\" FROM \"_iris_commit\"").is_empty());

    drop(client);
    server.join().unwrap();
}

#[test]
fn pgsql_aborted_batch_discards_schema_inside_a_transaction() {
    let (mut client, server) = start_server_with_empty_schema();

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));

    let sql = "CREATE TABLE batch_users (\"id\" BIGINT PRIMARY KEY); CREATE TABLE batch_users (\"id\" BIGINT PRIMARY KEY)";
    send_query(&mut client, sql);
    let responses = receive_until_ready(&mut client);
    assert_eq!(responses[0], (b'C', b"CREATE TABLE\0".to_vec()));
    assert_eq!(responses[1].0, b'E');
    assert!(error_message(&responses[1].1).contains("statement 2 of 2"));
    assert_eq!(responses[2], (b'Z', b"E".to_vec()));

    // 事务被中止：SQL 一律 `25P02`。
    assert_sqlstate_in_tx(&mut client, "SELECT \"id\" FROM batch_users", b"25P02");
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"ROLLBACK\0".to_vec()));

    // 批次内第一条 DDL 也必须被回滚：catalog 与数据都不留痕迹。
    assert_sqlstate(&mut client, "SELECT \"id\" FROM batch_users", b"42P01");

    drop(client);
    server.join().unwrap();
}

/// `BEGIN; CREATE TABLE; INSERT; ROLLBACK;` 必须同时丢弃 schema 与数据。
#[test]
fn pgsql_rollback_discards_ddl_together_with_rows() {
    let (mut client, server) = start_server_with_empty_schema();

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "CREATE TABLE txn_users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"CREATE TABLE\0".to_vec()));
    send_query(&mut client, "INSERT INTO txn_users VALUES (1, 'alice')");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 1\0".to_vec()));
    // 事务内 schema 与数据都可见。
    assert_eq!(single_row(&mut client, "SELECT name FROM txn_users WHERE id = 1"), vec![value("alice")]);

    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"ROLLBACK\0".to_vec()));

    // 回滚后 catalog 与数据一致：表不存在，也就没有残留行。
    assert_sqlstate(&mut client, "SELECT name FROM txn_users", b"42P01");

    drop(client);
    server.join().unwrap();
}

/// 提交路径是回滚的对照：schema revision 随事务一起提交，数据和表都保留。
#[test]
fn pgsql_commit_keeps_ddl_and_rows_together() {
    let (mut client, server) = start_server_with_empty_schema();

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "CREATE TABLE committed_users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"CREATE TABLE\0".to_vec()));
    send_query(&mut client, "INSERT INTO committed_users VALUES (1, 'alice')");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 1\0".to_vec()));
    send_query(&mut client, "COMMIT");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"COMMIT\0".to_vec()));

    assert_eq!(single_row(&mut client, "SELECT name FROM committed_users WHERE id = 1"), vec![value("alice")]);

    drop(client);
    server.join().unwrap();
}

/// 断开连接等于隐式回滚：未提交的 DDL 不能留在共享 registry 里。
#[test]
fn pgsql_disconnect_discards_uncommitted_ddl_and_rows() {
    let (mut clients, server) = start_shared_schema_server(2);
    let mut writer = clients.remove(0);
    let mut reader = clients.remove(0);

    send_query(&mut writer, "BEGIN");
    assert_eq!(receive_until_ready(&mut writer).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut writer, "CREATE TABLE drop_on_disconnect (id BIGINT PRIMARY KEY)");
    assert_eq!(receive_until_ready(&mut writer)[0], (b'C', b"CREATE TABLE\0".to_vec()));
    send_query(&mut writer, "INSERT INTO drop_on_disconnect VALUES (1)");
    assert_eq!(receive_until_ready(&mut writer)[0], (b'C', b"INSERT 0 1\0".to_vec()));
    drop(writer);

    // 服务线程只能在客户端断开后回滚 schema，用有界轮询等待它完成。
    let mut gone = false;
    for _ in 0..100 {
        send_query(&mut reader, "SELECT id FROM drop_on_disconnect");
        if receive_until_ready(&mut reader).iter().any(|(tag, _)| *tag == b'E') {
            gone = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(gone, "disconnect must discard uncommitted DDL, catalog still reports the table");

    drop(reader);
    server.join().unwrap();
}

fn error_message(payload: &[u8]) -> String {
    let marker = b"\0M";
    let start = payload.windows(2).position(|window| window == marker).expect("ErrorResponse message field") + 2;
    let end = payload[start..].iter().position(|byte| *byte == 0).unwrap() + start;
    String::from_utf8_lossy(&payload[start..end]).into_owned()
}
