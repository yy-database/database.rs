//! MongoDB 基准骨架：固定数据规模、固定 seed、固定操作集合。
//!
//! 两种后端共用同一套操作定义，只换连接串即可切到真机：
//!
//! ```text
//! 本仓模式（mongo facade + .yykv 存储）：
//!   cargo run --release -p mongo --example mongo_bench
//!
//! 真机模式（先启动 mongod 8.0.9）：
//!   $env:MONGO_BENCH_URI = "mongodb://127.0.0.1:27017"
//!   cargo run --release -p mongo --example mongo_bench
//! ```
//!
//! 每一项跑 5 轮，先重置到同一初始状态再计时，输出绝对耗时的中位数与 p95。
//! 5 个样本下 p95 取最近秩，实际等于最大值。
//!
//! 本仓模式的数字只反映 facade 与 `.yykv` 存储自身，**不能**当作 `mongod` 的
//! 基准。真机数字必须由真机跑出，报告里不得用本仓输出填充。

use std::{
    io::Write,
    net::TcpStream,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use mongo::{
    bson::{BsonDocument, BsonValue},
    commands,
    cursor::CursorStore,
    store::Store,
    wire::{FrameLimits, decode_op_msg_command, encode_op_msg_body, read_frame, write_frame},
};

/// 固定数据规模。
const DATASET_SIZE: usize = 10_000;
/// 固定随机种子。
const SEED: u64 = 20_261_108;
/// 每项操作的轮数。
const ROUNDS: usize = 5;
/// 批量插入与游标分批的单批大小。
const BATCH_SIZE: usize = 100;
/// 批量插入操作的文档数。
const BULK_INSERT_SIZE: usize = 1_000;
/// 基准使用的库名。
const DATABASE: &str = "bench";
/// 基准使用的集合名。
const COLLECTION: &str = "items";
/// 索引名，创建与删除都用它。
const INDEX_NAME: &str = "bench_score";
/// 唯一索引名，`insert_unique` 用它测唯一性判定的开销。
const UNIQUE_INDEX_NAME: &str = "bench_id_kind";

fn main() {
    if let Err(error) = run() {
        eprintln!("benchmark failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let documents = seed_documents(DATASET_SIZE);
    let mut backend = open_backend()?;
    match &backend {
        Backend::Local { .. } => println!("backend: local (mongo facade + .yykv store)，本仓数字不代表 mongod"),
        Backend::Remote(remote) => println!("backend: remote {}", remote.uri),
    }
    println!("dataset: {DATASET_SIZE} docs, seed: {SEED}, rounds: {ROUNDS}, batch: {BATCH_SIZE}");
    println!();

    let mut results = Vec::new();
    for (name, operation) in OPERATIONS {
        let mut samples = Vec::with_capacity(ROUNDS);
        for round in 0..ROUNDS {
            let index = match *name {
                "index_create" => ResetIndex::None,
                "insert_unique" => ResetIndex::Unique,
                _ => ResetIndex::Score,
            };
            reset(&mut backend, &documents, index)?;
            let start = Instant::now();
            operation(&mut backend, round)?;
            samples.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        let (median, p95) = summarize(&mut samples);
        results.push((name.to_string(), median, p95));
    }

    println!("{:<20} {:>12} {:>12}", "operation", "median_ms", "p95_ms");
    for (name, median, p95) in results {
        println!("{name:<20} {median:>12.3} {p95:>12.3}");
    }
    Ok(())
}

/// 固定操作集合。顺序即报表顺序，每一项都必须可在两种后端上原样执行。
const OPERATIONS: &[(&str, fn(&mut Backend, usize) -> Result<(), String>)] = &[
    ("insert_single", insert_single),
    ("insert_bulk", insert_bulk),
    ("insert_unique", insert_unique),
    ("find_by_id", find_by_id),
    ("find_range", find_range),
    ("update_filter", update_filter),
    ("delete_filter", delete_filter),
    ("aggregate_group", aggregate_group),
    ("index_create", index_create),
    ("cursor_batches", cursor_batches),
];

fn insert_single(backend: &mut Backend, round: usize) -> Result<(), String> {
    let _ = round;
    let mut insert = command("insert", COLLECTION);
    insert.push(
        "documents",
        BsonValue::Array(vec![BsonValue::Document(document(&[
            ("_id", BsonValue::Int32(3_000_000)),
            ("kind", text("single")),
            ("score", BsonValue::Int32(1)),
            ("payload", text("single-insert")),
        ]))]),
    );
    check_write(&backend.run(&insert)?)
}

fn insert_bulk(backend: &mut Backend, round: usize) -> Result<(), String> {
    let mut documents = Vec::with_capacity(BULK_INSERT_SIZE);
    for index in 0..BULK_INSERT_SIZE {
        documents.push(BsonValue::Document(document(&[
            ("_id", BsonValue::Int32(4_000_000 + (round * BULK_INSERT_SIZE + index) as i32)),
            ("kind", text(if index % 2 == 0 { "a" } else { "b" })),
            ("score", BsonValue::Int32((index % 1_000) as i32)),
            ("payload", text("bulk-insert-payload")),
        ])));
    }
    let mut insert = command("insert", COLLECTION);
    insert.push("documents", BsonValue::Array(documents));
    check_write(&backend.run(&insert)?)
}

fn insert_unique(backend: &mut Backend, round: usize) -> Result<(), String> {
    let mut documents = Vec::with_capacity(BATCH_SIZE);
    for index in 0..BATCH_SIZE {
        documents.push(BsonValue::Document(document(&[
            ("_id", BsonValue::Int32(6_000_000 + (round * BATCH_SIZE + index) as i32)),
            ("kind", text(if index % 2 == 0 { "a" } else { "b" })),
            ("score", BsonValue::Int32((index % 1_000) as i32)),
            ("payload", text("unique-insert-payload")),
        ])));
    }
    let mut insert = command("insert", COLLECTION);
    insert.push("documents", BsonValue::Array(documents));
    check_write(&backend.run(&insert)?)
}

fn find_by_id(backend: &mut Backend, _round: usize) -> Result<(), String> {
    let mut find = command("find", COLLECTION);
    find.push("filter", BsonValue::Document(document(&[("_id", BsonValue::Int32(5_000))])));
    let reply = backend.run(&find)?;
    check_reply(&reply)?;
    expect_batch(&reply, 1)
}

fn find_range(backend: &mut Backend, _round: usize) -> Result<(), String> {
    let mut find = command("find", COLLECTION);
    find.push(
        "filter",
        BsonValue::Document(document(&[(
            "score",
            BsonValue::Document(document(&[("$gte", BsonValue::Int32(250)), ("$lt", BsonValue::Int32(350))])),
        )])),
    );
    find.push("sort", BsonValue::Document(document(&[("score", BsonValue::Int32(1))])));
    find.push("limit", BsonValue::Int32(100));
    let reply = backend.run(&find)?;
    check_reply(&reply)?;
    expect_batch(&reply, 100)
}

fn update_filter(backend: &mut Backend, _round: usize) -> Result<(), String> {
    let mut update = command("update", COLLECTION);
    update.push(
        "updates",
        BsonValue::Array(vec![BsonValue::Document(document(&[
            ("q", BsonValue::Document(document(&[("kind", text("b"))]))),
            ("u", BsonValue::Document(document(&[("$inc", BsonValue::Document(document(&[("counter", BsonValue::Int32(1))])))]))),
            ("multi", BsonValue::Bool(true)),
        ]))]),
    );
    check_write(&backend.run(&update)?)
}

fn delete_filter(backend: &mut Backend, _round: usize) -> Result<(), String> {
    let mut delete = command("delete", COLLECTION);
    delete.push(
        "deletes",
        BsonValue::Array(vec![BsonValue::Document(document(&[
            ("q", BsonValue::Document(document(&[("kind", text("b"))]))),
            ("limit", BsonValue::Int32(0)),
        ]))]),
    );
    check_write(&backend.run(&delete)?)
}

fn aggregate_group(backend: &mut Backend, _round: usize) -> Result<(), String> {
    let mut aggregate = command("aggregate", COLLECTION);
    aggregate.push(
        "pipeline",
        BsonValue::Array(vec![
            BsonValue::Document(document(&[("$match", BsonValue::Document(document(&[("kind", text("b"))])))])),
            BsonValue::Document(document(&[(
                "$group",
                BsonValue::Document(document(&[
                    ("_id", text("$kind")),
                    ("count", BsonValue::Document(document(&[("$sum", BsonValue::Int32(1))]))),
                    ("total", BsonValue::Document(document(&[("$sum", text("$score"))]))),
                ])),
            )])),
        ]),
    );
    aggregate.push("cursor", BsonValue::Document(document(&[("batchSize", BsonValue::Int32(BATCH_SIZE as i32))])));
    let reply = backend.run(&aggregate)?;
    check_reply(&reply)?;
    Ok(())
}

fn index_create(backend: &mut Backend, _round: usize) -> Result<(), String> {
    let mut create = command("createIndexes", COLLECTION);
    create.push(
        "indexes",
        BsonValue::Array(vec![BsonValue::Document(document(&[
            ("key", BsonValue::Document(document(&[("score", BsonValue::Int32(1))]))),
            ("name", text(INDEX_NAME)),
        ]))]),
    );
    check_reply(&backend.run(&create)?)
}

fn cursor_batches(backend: &mut Backend, _round: usize) -> Result<(), String> {
    let mut find = command("find", COLLECTION);
    find.push("filter", BsonValue::Document(BsonDocument::new()));
    find.push("batchSize", BsonValue::Int32(BATCH_SIZE as i32));
    let mut reply = backend.run(&find)?;
    check_reply(&reply)?;
    let mut seen = batch_len(&reply);
    let mut cursor = cursor_id(&reply);
    while cursor != 0 {
        let mut get_more = document(&[("getMore", BsonValue::Int64(cursor)), ("collection", text(COLLECTION)), ("$db", text(DATABASE))]);
        get_more.push("batchSize", BsonValue::Int32(BATCH_SIZE as i32));
        reply = backend.run(&get_more)?;
        check_reply(&reply)?;
        seen += batch_len(&reply);
        cursor = cursor_id(&reply);
    }
    if seen != DATASET_SIZE {
        return Err(format!("cursor drained {seen} documents, expected {DATASET_SIZE}"));
    }
    Ok(())
}

/// 重置时建哪一个索引。
#[derive(Clone, Copy)]
enum ResetIndex {
    /// 非唯一 `score` 索引，供索引扫描类操作使用。
    Score,
    /// 唯一 `{_id, kind}` 索引，供 `insert_unique` 使用。
    Unique,
    /// 不建索引，供 `index_create` 自己计时建索引。
    None,
}

/// 把集合重置到同一初始状态，重置耗时不计入样本。
fn reset(backend: &mut Backend, documents: &[BsonDocument], index: ResetIndex) -> Result<(), String> {
    // 集合不存在时 `drop` 会报错，这里忽略，重置本来就以删掉为准。
    let _ = backend.run(&command("drop", COLLECTION));
    for chunk in documents.chunks(BULK_INSERT_SIZE) {
        let mut insert = command("insert", COLLECTION);
        insert.push("documents", BsonValue::Array(chunk.iter().cloned().map(BsonValue::Document).collect()));
        check_write(&backend.run(&insert)?)?;
    }
    let specification = match index {
        ResetIndex::None => return Ok(()),
        ResetIndex::Score => document(&[("key", BsonValue::Document(document(&[("score", BsonValue::Int32(1))]))), ("name", text(INDEX_NAME))]),
        ResetIndex::Unique => document(&[
            ("key", BsonValue::Document(document(&[("_id", BsonValue::Int32(1)), ("kind", BsonValue::Int32(1))]))),
            ("name", text(UNIQUE_INDEX_NAME)),
            ("unique", BsonValue::Bool(true)),
        ]),
    };
    let mut create = command("createIndexes", COLLECTION);
    create.push("indexes", BsonValue::Array(vec![BsonValue::Document(specification)]));
    check_reply(&backend.run(&create)?)
}

/// 固定 seed 生成同一批文档，两种后端与多次运行都得到相同数据。
fn seed_documents(count: usize) -> Vec<BsonDocument> {
    let mut state = SEED;
    let mut documents = Vec::with_capacity(count);
    for index in 0..count {
        state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        let noise = (state >> 33) as i32;
        documents.push(document(&[
            ("_id", BsonValue::Int32(index as i32)),
            ("kind", text(if index % 2 == 0 { "a" } else { "b" })),
            ("score", BsonValue::Int32(noise.rem_euclid(1_000))),
            ("counter", BsonValue::Int32(0)),
            ("payload", text("benchmark-payload-0123456789")),
        ]));
    }
    documents
}

enum Backend {
    Local { store: Store, cursors: CursorStore },
    Remote(Remote),
}

impl Backend {
    fn run(&mut self, command: &BsonDocument) -> Result<BsonDocument, String> {
        match self {
            Self::Local { store, cursors } => {
                let name = command.command_name().ok_or_else(|| "command document has no name".to_string())?;
                let result = match name {
                    "insert" => commands::insert(command, store),
                    "find" => commands::find(command, store, cursors),
                    "getMore" => commands::get_more(command, cursors),
                    "update" => commands::update(command, store),
                    "delete" => commands::delete(command, store),
                    "aggregate" => commands::aggregate(command, store),
                    "createIndexes" => commands::create_indexes(command, store),
                    "drop" => commands::drop(command, store),
                    other => return Err(format!("unsupported local command: {other}")),
                };
                result.map_err(|error| error.to_string())
            }
            Self::Remote(remote) => remote.run(command),
        }
    }
}

fn open_backend() -> Result<Backend, String> {
    match std::env::var("MONGO_BENCH_URI") {
        Ok(uri) if !uri.trim().is_empty() => {
            let mut remote = Remote::connect(uri.trim())?;
            // 握手不计入样本，失败说明实例不可用或需要认证。
            let hello = document(&[("hello", BsonValue::Int32(1)), ("$db", text("admin"))]);
            remote.run(&hello).and_then(|reply| check_reply(&reply))?;
            Ok(Backend::Remote(remote))
        }
        _ => {
            let unique = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|error| error.to_string())?.as_nanos();
            let directory = std::env::temp_dir().join(format!("mongo-bench-{}-{unique}", std::process::id()));
            std::fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
            let store = Store::open(directory.join("store.yykv")).map_err(|error| error.to_string())?;
            Ok(Backend::Local { store, cursors: CursorStore::new() })
        }
    }
}

/// 极简 OP_MSG 客户端，只够发出命令与读回应答。
struct Remote {
    stream: TcpStream,
    uri: String,
    next_request_id: i32,
}

impl Remote {
    fn connect(uri: &str) -> Result<Self, String> {
        let (host, port) = parse_uri(uri)?;
        let stream = TcpStream::connect((host.as_str(), port)).map_err(|error| format!("connect {host}:{port}: {error}"))?;
        stream.set_nodelay(true).map_err(|error| error.to_string())?;
        Ok(Self { stream, uri: uri.to_string(), next_request_id: 1 })
    }

    fn run(&mut self, command: &BsonDocument) -> Result<BsonDocument, String> {
        // 预先编码一次，编码失败不应把半条帧写进连接。
        let body = encode_op_msg_body(command);
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        write_frame(&mut self.stream, request_id, 0, &body).map_err(|error| error.as_str().to_string())?;
        self.stream.flush().map_err(|error| error.to_string())?;
        let frame = read_frame(&mut self.stream, FrameLimits::default())
            .map_err(|error| error.as_str().to_string())?
            .ok_or_else(|| "connection closed before reply".to_string())?;
        decode_op_msg_command(&frame.body).map_err(|error| format!("decode reply: {error:?}"))
    }
}

fn parse_uri(uri: &str) -> Result<(String, u16), String> {
    let rest = uri.strip_prefix("mongodb://").unwrap_or(uri);
    let authority = rest.split('/').next().unwrap_or(rest);
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    match authority.rsplit_once(':') {
        Some((host, port)) => Ok((host.to_string(), port.parse::<u16>().map_err(|error| format!("bad port {port}: {error}"))?)),
        None => Ok((authority.to_string(), 27_017)),
    }
}

fn command(name: &str, collection: &str) -> BsonDocument {
    document(&[(name, text(collection)), ("$db", text(DATABASE))])
}

fn document(fields: &[(&str, BsonValue)]) -> BsonDocument {
    let mut document = BsonDocument::new();
    for (key, value) in fields {
        document.push(*key, value.clone());
    }
    document
}

fn text(value: &str) -> BsonValue {
    BsonValue::String(value.to_string())
}

fn check_reply(reply: &BsonDocument) -> Result<(), String> {
    let ok = match reply.get("ok") {
        Some(BsonValue::Int32(number)) => *number as i64,
        Some(BsonValue::Int64(number)) => *number,
        Some(BsonValue::Double(number)) => *number as i64,
        Some(BsonValue::Bool(flag)) => i64::from(*flag),
        _ => 0,
    };
    if ok == 1 { Ok(()) } else { Err(format!("command failed: ok={ok:?}, errmsg={:?}", reply.get("errmsg"))) }
}

fn check_write(reply: &BsonDocument) -> Result<(), String> {
    check_reply(reply)?;
    match reply.get("writeErrors") {
        Some(BsonValue::Array(errors)) if !errors.is_empty() => Err(format!("write error: {:?}", errors[0])),
        _ => Ok(()),
    }
}

fn cursor_id(reply: &BsonDocument) -> i64 {
    match reply.get("cursor") {
        Some(BsonValue::Document(cursor)) => match cursor.get("id") {
            Some(BsonValue::Int32(number)) => *number as i64,
            Some(BsonValue::Int64(number)) => *number,
            Some(BsonValue::Double(number)) => *number as i64,
            _ => 0,
        },
        _ => 0,
    }
}

fn batch_len(reply: &BsonDocument) -> usize {
    let Some(BsonValue::Document(cursor)) = reply.get("cursor")
    else {
        return 0;
    };
    let Some(BsonValue::Array(batch)) = cursor.get("firstBatch").or_else(|| cursor.get("nextBatch"))
    else {
        return 0;
    };
    batch.len()
}

fn expect_batch(reply: &BsonDocument, expected: usize) -> Result<(), String> {
    let found = batch_len(reply);
    if found == expected { Ok(()) } else { Err(format!("expected {expected} documents in the first batch, found {found}")) }
}

fn summarize(samples: &mut [f64]) -> (f64, f64) {
    samples.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let median = samples[samples.len() / 2];
    let rank = ((samples.len() as f64) * 0.95).ceil() as usize;
    let p95 = samples[rank.saturating_sub(1).min(samples.len() - 1)];
    (median, p95)
}
