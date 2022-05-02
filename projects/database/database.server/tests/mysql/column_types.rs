//! 列类型上报 / 约束错误 / 精确聚合的 wire 级回归。
//!
//! 期望值 oracle（unverified-oracle：本测试不连真库，逐条对齐 MySQL 8.4 采集语料
//! `mysql-harness/corpus/truth.json` 的 `types` / `errors` / `aggregates` 场景，
//! 不拿本仓行为当期望值）：
//! - `types`：`DATE` 列上报 `MYSQL_TYPE_DATE` (0x0a)、`DATETIME` 列上报
//!   `MYSQL_TYPE_DATETIME` (0x0c)，文本协议值分别为 `2024-01-02` 与
//!   `2024-01-02 03:04:05`，mysql2 据此还原 JS Date。
//! - `errors`：`INSERT` 未点名 NOT NULL 且无 DEFAULT 的列报 errno 1364 / SQLSTATE
//!   `HY000`（`ER_NO_DEFAULT_FOR_FIELD`），显式 NULL 仍是 1048 / `23000`，
//!   失败语句不写入任何行。
//! - `aggregates`：整数列 `SUM` 上报 `MYSQL_TYPE_NEWDECIMAL` (0xf6) 且值为十进制
//!   文本（`SUM(score)` 为 `"80"`），`MIN`/`MAX` 保留列类型；`DECIMAL(p,s)` 列
//!   同样上报 NEWDECIMAL。

use std::{
    collections::HashMap,
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::Duration,
};

use database_cluster::topology::ShardMap;
use database_execution::cluster::LocalExecutor;
use database_query::SqlSchemaRegistry;
use database_server::yyds::mysql::connection::{MysqlSqlRuntime, serve_connection};
use database_storage::cluster_kv::FileShard;
use database_types::cluster::{ShardEpoch, ShardId};

const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
const COM_QUERY: u8 = 0x03;
const COM_STMT_PREPARE: u8 = 0x16;
const COM_STMT_EXECUTE: u8 = 0x17;

const MYSQL_TYPE_LONG: u8 = 0x03;
const MYSQL_TYPE_LONGLONG: u8 = 0x08;
const MYSQL_TYPE_DATE: u8 = 0x0A;
const MYSQL_TYPE_DATETIME: u8 = 0x0C;
const MYSQL_TYPE_NEWDECIMAL: u8 = 0xF6;
const NOT_NULL_FLAG: u16 = 0x0002;

/// 起一个 MySQL 网关并完成握手，返回客户端 socket。
///
/// 与 `affected_rows.rs` 同一接线：执行面用 `FileShard`，数据目录落系统临时目录，
/// 用例结束即清理。
fn connect_gateway(name: &str) -> (TcpStream, std::path::PathBuf) {
    let directory = std::env::temp_dir().join(format!("yyds-mysql-column-types-{name}"));
    let _ = fs::remove_file(directory.join("local-0.yykv"));
    fs::create_dir_all(&directory).unwrap();
    let shard = ShardId("local-0".into());
    let executor = Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
            HashMap::from([(shard, FileShard::open(directory.join("local-0.yykv")).unwrap())]),
            Vec::new(),
        )
        .unwrap(),
    );
    let runtime = Arc::new(MysqlSqlRuntime { schema: Arc::new(SqlSchemaRegistry::new_in_memory()), executor });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let _ = serve_connection(stream, Some(runtime));
    });
    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    client.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
    let (sequence, _) = read_packet(&mut client);
    assert_eq!(sequence, 0, "handshake must carry sequence 0");
    let mut response = Vec::new();
    response.extend_from_slice(&(CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION).to_le_bytes());
    response.extend_from_slice(&(16 * 1024 * 1024_u32).to_le_bytes());
    response.push(45);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(b"yyds\0");
    response.push(0);
    write_packet(&mut client, 1, &response);
    let (sequence, payload) = read_packet(&mut client);
    assert_eq!(sequence, 2);
    assert_eq!(payload.first(), Some(&0x00), "handshake must complete with OK, got {payload:?}");
    (client, directory)
}

fn read_packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 4];
    stream.read_exact(&mut header).unwrap();
    let length = usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).unwrap();
    (header[3], payload)
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) {
    let length = payload.len();
    let mut frame = Vec::with_capacity(4 + length);
    frame.extend_from_slice(&[length as u8, (length >> 8) as u8, (length >> 16) as u8, sequence]);
    frame.extend_from_slice(payload);
    stream.write_all(&frame).unwrap();
    stream.flush().unwrap();
}

/// 一条 `COM_QUERY` 的完整响应形态。
#[derive(Debug)]
enum Response {
    /// OK 包里的 length-encoded `affected_rows`。
    Ok { affected: u64 },
    /// ERR 包的 errno 与 SQLSTATE。
    Err { errno: u16, sql_state: String },
    /// 结果集：列定义（名字、类型码、NOT NULL 标志）与文本协议行值。
    ResultSet { columns: Vec<(String, u8, bool)>, rows: Vec<Vec<Option<String>>> },
}

fn query(client: &mut TcpStream, sql: &str) -> Response {
    let mut payload = vec![COM_QUERY];
    payload.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &payload);
    let (sequence, first) = read_packet(client);
    assert_eq!(sequence, 1, "COM_QUERY response must start at sequence 1");
    match first.first() {
        Some(0x00) => Response::Ok { affected: decode_lenenc(&first, 1).0 },
        Some(0xff) => {
            let errno = u16::from_le_bytes([first[1], first[2]]);
            assert_eq!(first[3], b'#', "ERR packet must carry the SQLSTATE marker");
            Response::Err { errno, sql_state: String::from_utf8_lossy(&first[4..9]).into_owned() }
        }
        _ => read_result_set(client, first),
    }
}

fn read_result_set(client: &mut TcpStream, count_packet: Vec<u8>) -> Response {
    let column_count = decode_lenenc(&count_packet, 0).0 as usize;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let (_, definition) = read_packet(client);
        columns.push(decode_column_definition(&definition));
    }
    let (_, eof) = read_packet(client);
    assert!(is_eof(&eof), "expected EOF after column definitions, got {eof:?}");
    let mut rows = Vec::new();
    loop {
        let (_, packet) = read_packet(client);
        if is_eof(&packet) {
            break;
        }
        let mut offset = 0;
        let mut row = Vec::with_capacity(columns.len());
        for _ in 0..columns.len() {
            let (value, next) = decode_lenenc_string(&packet, offset);
            row.push(value);
            offset = next;
        }
        assert_eq!(offset, packet.len(), "row payload must carry exactly one value per column: {packet:?}");
        rows.push(row);
    }
    Response::ResultSet { columns, rows }
}

/// ColumnDefinition：catalog/schema/table/org_table 四段 lenenc + 名字 + org_name
/// + `0x0C` 定长 + charset(2) + column_length(4) + type(1) + flags(2)。
fn decode_column_definition(payload: &[u8]) -> (String, u8, bool) {
    let mut offset = 0;
    for _ in 0..4 {
        offset = decode_lenenc_string(payload, offset).1;
    }
    let (name, next) = decode_lenenc_string(payload, offset);
    let (_, next) = decode_lenenc_string(payload, next); // org_name
    let (next_length, next) = decode_lenenc(payload, next);
    assert_eq!(next_length, 0x0C, "column definition must carry the fixed 12-byte tail");
    let mut offset = next + 2 + 4; // charset + column_length
    let column_type = payload[offset];
    offset += 1;
    let flags = u16::from_le_bytes([payload[offset], payload[offset + 1]]);
    (name.expect("column name"), column_type, flags & NOT_NULL_FLAG != 0)
}

fn is_eof(payload: &[u8]) -> bool {
    payload.first() == Some(&0xFE) && payload.len() == 5
}

fn decode_lenenc(payload: &[u8], offset: usize) -> (u64, usize) {
    match payload[offset] {
        0..=250 => (u64::from(payload[offset]), offset + 1),
        252 => (u64::from(u16::from_le_bytes([payload[offset + 1], payload[offset + 2]])), offset + 3),
        253 => (u64::from(u32::from_le_bytes([payload[offset + 1], payload[offset + 2], payload[offset + 3], 0])), offset + 4),
        254 => (u64::from_le_bytes(payload[offset + 1..offset + 9].try_into().unwrap()), offset + 9),
        other => panic!("invalid length-encoded prefix {other}"),
    }
}

fn decode_lenenc_string(payload: &[u8], offset: usize) -> (Option<String>, usize) {
    if payload[offset] == 0xFB {
        return (None, offset + 1);
    }
    let (length, start) = decode_lenenc(payload, offset);
    let end = start + length as usize;
    (Some(String::from_utf8_lossy(&payload[start..end]).into_owned()), end)
}

fn affected(client: &mut TcpStream, sql: &str) -> u64 {
    match query(client, sql) {
        Response::Ok { affected } => affected,
        other => panic!("query `{sql}` expected OK, got {other:?}"),
    }
}

fn result_set(client: &mut TcpStream, sql: &str) -> (Vec<(String, u8, bool)>, Vec<Vec<Option<String>>>) {
    match query(client, sql) {
        Response::ResultSet { columns, rows } => (columns, rows),
        other => panic!("query `{sql}` expected a result set, got {other:?}"),
    }
}

fn column_names(columns: &[(String, u8, bool)]) -> Vec<&str> {
    columns.iter().map(|(name, _, _)| name.as_str()).collect()
}

fn column_types(columns: &[(String, u8, bool)]) -> Vec<u8> {
    columns.iter().map(|(_, column_type, _)| *column_type).collect()
}

#[test]
fn date_and_datetime_columns_report_their_own_mysql_type_codes() {
    // truth.json `types` 场景：DATE/DATETIME 列的类型码与文本值。
    let (mut client, directory) = connect_gateway("dates");
    affected(&mut client, "CREATE TABLE truth_types (id INT NOT NULL PRIMARY KEY, d DATE, dt DATETIME)");
    assert_eq!(affected(&mut client, "INSERT INTO truth_types VALUES (1, '2024-01-02', '2024-01-02 03:04:05')"), 1);
    assert_eq!(affected(&mut client, "INSERT INTO truth_types (id) VALUES (2)"), 1);

    let (columns, rows) = result_set(&mut client, "SELECT id, d, dt FROM truth_types ORDER BY id");
    assert_eq!(column_names(&columns), vec!["id", "d", "dt"]);
    assert_eq!(column_types(&columns), vec![MYSQL_TYPE_LONG, MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME]);
    assert_eq!(rows, vec![vec![Some("1".into()), Some("2024-01-02".into()), Some("2024-01-02 03:04:05".into())], vec![Some("2".into()), None, None]]);

    // 日期谓词按文本比较命中写入的行。
    let (_, rows) = result_set(&mut client, "SELECT id FROM truth_types WHERE d = '2024-01-02'");
    assert_eq!(rows, vec![vec![Some("1".into())]]);

    drop(client);
    let _ = fs::remove_file(directory.join("local-0.yykv"));
    let _ = fs::remove_dir(&directory);
}

#[test]
fn insert_omitting_not_null_column_without_default_reports_1364() {
    // truth.json `errors` 场景：显式 NULL 仍是 1048，缺列且无 DEFAULT 报 1364，
    // 失败的 INSERT 不留行。
    let (mut client, directory) = connect_gateway("defaults");
    affected(&mut client, "CREATE TABLE truth_err (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)");
    assert_eq!(affected(&mut client, "INSERT INTO truth_err VALUES (1, 5)"), 1);

    match query(&mut client, "INSERT INTO truth_err VALUES (2, NULL)") {
        Response::Err { errno, sql_state } => {
            assert_eq!((errno, sql_state.as_str()), (1048, "23000"));
        }
        other => panic!("explicit NULL must fail with 1048, got {other:?}"),
    }
    match query(&mut client, "INSERT INTO truth_err (id) VALUES (4)") {
        Response::Err { errno, sql_state } => {
            assert_eq!((errno, sql_state.as_str()), (1364, "HY000"));
        }
        other => panic!("missing NOT NULL column must fail with 1364, got {other:?}"),
    }

    let (_, rows) = result_set(&mut client, "SELECT COUNT(*) AS n FROM truth_err");
    assert_eq!(rows, vec![vec![Some("1".into())]], "failed INSERTs must not add rows");

    drop(client);
    let _ = fs::remove_file(directory.join("local-0.yykv"));
    let _ = fs::remove_dir(&directory);
}

/// 走完 `COM_STMT_PREPARE` + `COM_STMT_EXECUTE`（零参数），返回列定义与
/// binary protocol 行报文原文。
fn prepare_and_execute(client: &mut TcpStream, sql: &str) -> (Vec<(String, u8, bool)>, Vec<Vec<u8>>) {
    let mut payload = vec![COM_STMT_PREPARE];
    payload.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &payload);
    let (sequence, ok) = read_packet(client);
    assert_eq!(sequence, 1, "prepare response must start at sequence 1");
    assert_eq!(ok.first(), Some(&0x00), "prepare must return OK, got {ok:?}");
    let statement_id = u32::from_le_bytes(ok[1..5].try_into().unwrap());
    let column_count = u16::from_le_bytes(ok[5..7].try_into().unwrap()) as usize;
    let parameter_count = u16::from_le_bytes(ok[7..9].try_into().unwrap()) as usize;
    assert_eq!(parameter_count, 0, "this helper only covers zero-parameter statements");
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let (_, definition) = read_packet(client);
        columns.push(decode_column_definition(&definition));
    }
    let (_, eof) = read_packet(client);
    assert!(is_eof(&eof), "expected EOF after prepare column definitions, got {eof:?}");

    let mut execute = vec![COM_STMT_EXECUTE];
    execute.extend_from_slice(&statement_id.to_le_bytes());
    execute.push(0); // flags
    execute.extend_from_slice(&1_u32.to_le_bytes()); // iteration count
    write_packet(client, 0, &execute);
    let (sequence, first) = read_packet(client);
    assert_eq!(sequence, 1, "execute response must start at sequence 1");
    assert_eq!(decode_lenenc(&first, 0).0 as usize, column_count, "execute column count must match prepare");
    // execute 响应会重发列定义，逐份与 prepare 时的声明比对。
    for index in 0..column_count {
        let (_, definition) = read_packet(client);
        assert_eq!(decode_column_definition(&definition), columns[index], "execute must re-send the prepared column definition");
    }
    let (_, eof) = read_packet(client);
    assert!(is_eof(&eof), "expected EOF after execute column definitions, got {eof:?}");
    let mut rows = Vec::new();
    loop {
        let (_, packet) = read_packet(client);
        if is_eof(&packet) {
            break;
        }
        rows.push(packet);
    }
    (columns, rows)
}

#[test]
fn prepared_select_encodes_date_and_datetime_in_binary_protocol() {
    // binary row 布局对齐真库：0x00 头 + null bitmap + 每列「长度字节 + 字段」，
    // DATE 为 4 字节（年 u16 LE + 月 + 日），DATETIME 为 7 字节（追加时/分/秒）。
    let (mut client, directory) = connect_gateway("binary-dates");
    affected(&mut client, "CREATE TABLE truth_types (id INT NOT NULL PRIMARY KEY, d DATE, dt DATETIME)");
    assert_eq!(affected(&mut client, "INSERT INTO truth_types VALUES (1, '2024-01-02', '2024-01-02 03:04:05')"), 1);
    assert_eq!(affected(&mut client, "INSERT INTO truth_types (id) VALUES (2)"), 1);

    let (columns, rows) = prepare_and_execute(&mut client, "SELECT d, dt FROM truth_types ORDER BY id");
    assert_eq!(column_names(&columns), vec!["d", "dt"]);
    assert_eq!(column_types(&columns), vec![MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME]);
    assert_eq!(rows.len(), 2);
    // 2024 = 0x07E8 → LE [0xE8, 0x07]；第一行两列都非 NULL（bitmap 0x00）。
    assert_eq!(rows[0], vec![0x00, 0x00, 0x04, 0xE8, 0x07, 0x01, 0x02, 0x07, 0xE8, 0x07, 0x01, 0x02, 0x03, 0x04, 0x05]);
    // 第二行两列都是 NULL：bitmap 位 2/3 置位（0x0C），无值段。
    assert_eq!(rows[1], vec![0x00, 0x0C]);

    drop(client);
    let _ = fs::remove_file(directory.join("local-0.yykv"));
    let _ = fs::remove_dir(&directory);
}

#[test]
fn exact_aggregates_and_decimal_columns_report_newdecimal_text() {
    // truth.json `aggregates` 场景：整数列 SUM 上报 NEWDECIMAL、值为十进制文本。
    let (mut client, directory) = connect_gateway("aggregates");
    affected(&mut client, "CREATE TABLE truth_agg (id INT NOT NULL PRIMARY KEY, team INT, score INT)");
    assert_eq!(affected(&mut client, "INSERT INTO truth_agg VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, NULL), (5, NULL, 20), (6, 3, NULL)"), 6);

    let (columns, rows) = result_set(&mut client, "SELECT SUM(score) AS s, MIN(score) AS mi, MAX(score) AS ma FROM truth_agg");
    assert_eq!(column_names(&columns), vec!["s", "mi", "ma"]);
    assert_eq!(column_types(&columns), vec![MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_LONG, MYSQL_TYPE_LONG]);
    assert_eq!(rows, vec![vec![Some("80".into()), Some("10".into()), Some("30".into())]]);

    let (_, rows) = result_set(&mut client, "SELECT SUM(score) AS s FROM truth_agg WHERE score > 1000");
    assert_eq!(rows, vec![vec![None]]);

    let (columns, rows) = result_set(&mut client, "SELECT team, COUNT(*) AS n, SUM(score) AS s FROM truth_agg GROUP BY team ORDER BY team");
    assert_eq!(column_types(&columns), vec![MYSQL_TYPE_LONG, MYSQL_TYPE_LONGLONG, MYSQL_TYPE_NEWDECIMAL]);
    assert_eq!(
        rows,
        vec![
            vec![None, Some("1".into()), Some("20".into())],
            vec![Some("1".into()), Some("2".into()), Some("30".into())],
            vec![Some("2".into()), Some("2".into()), Some("30".into())],
            vec![Some("3".into()), Some("1".into()), None],
        ]
    );

    // `DECIMAL(p,s)` 列同样落到 NEWDECIMAL + 十进制文本。
    affected(&mut client, "CREATE TABLE truth_dec (id INT NOT NULL PRIMARY KEY, price DECIMAL(10,2))");
    assert_eq!(affected(&mut client, "INSERT INTO truth_dec VALUES (1, '19.90')"), 1);
    let (columns, rows) = result_set(&mut client, "SELECT price FROM truth_dec");
    assert_eq!(column_types(&columns), vec![MYSQL_TYPE_NEWDECIMAL]);
    assert_eq!(rows, vec![vec![Some("19.90".into())]]);

    drop(client);
    let _ = fs::remove_file(directory.join("local-0.yykv"));
    let _ = fs::remove_dir(&directory);
}
