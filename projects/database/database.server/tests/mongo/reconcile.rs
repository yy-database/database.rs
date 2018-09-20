//! 网关写读应答的冻结基线。
//!
//! 38 条固定语料喂给网关，游标 id 是服务端不透明值，先归一化为 `1`，再与冻结
//! 基线比对：37 条逐字节一致，1 条走结构化断言，0 条已知差异。
//!
//! 基线来源是真机 `mongod` 8.0.9，冻结于 2026-11-07。命令语义由上游 `mongo`
//! 门面提供，网关只做 OP_MSG 编解码与 `MongoStorage` 后端注入。
//!
//! `createIndexes` 的失败消息内含随机 operation id 与集合 UUID（真机同样随机），
//! 逐字节比较没有意义，改为只断言 `ok`、`code`、`codeName`、消息前缀与 namespace。

use std::collections::HashMap;
use std::sync::Mutex;

use database_cluster::topology::ShardMap;
use database_execution::cluster::{KeyValueExecutor, LocalExecutor};
use database_server::yyds::mongo::{
    bson::{encode_bson, parse_bson, BinaryData, BsonDocument, BsonValue},
    handle_op_msg,
};
use database_storage::cluster_kv::MemoryShard;
use database_types::cluster::{ShardEpoch, ShardId};
use mongo::cursor::CursorStore;

/// 结构化断言：真机 `createIndexes` 失败消息含随机 operation id 与集合 UUID，
/// 只冻结稳定字段，不比较完整字符串。
struct Structured {
    ok: f64,
    code: i32,
    code_name: &'static str,
    errmsg_prefix: &'static str,
    namespace: &'static str,
}

/// 一条语料的期望结果。
enum Expected {
    /// 冻结基线里该步骤的归一化字节，网关必须逐字节一致。
    Match(&'static str),
    /// 只断言稳定字段的步骤。
    Structured(Structured),
}

struct Step {
    scenario: &'static str,
    label: &'static str,
    document: BsonDocument,
    expected: Expected,
}

fn doc(fields: Vec<(&str, BsonValue)>) -> BsonDocument {
    let mut document = BsonDocument::new();
    for (key, value) in fields {
        document.push(key.to_string(), value);
    }
    document
}

fn d(fields: Vec<(&str, BsonValue)>) -> BsonValue {
    BsonValue::Document(doc(fields))
}

fn int(value: i32) -> BsonValue {
    BsonValue::Int32(value)
}

fn long(value: i64) -> BsonValue {
    BsonValue::Int64(value)
}

fn text(value: &str) -> BsonValue {
    BsonValue::String(value.to_string())
}

fn bin(subtype: u8, bytes: Vec<u8>) -> BsonValue {
    BsonValue::Binary(BinaryData::new(subtype, bytes))
}

fn array(items: Vec<BsonValue>) -> BsonValue {
    BsonValue::Array(items)
}

/// 38 条语料，顺序与对账时一致：语料在同一执行器上按序推进，跨步骤有状态依赖。
fn corpus() -> Vec<Step> {
    let mut steps = Vec::new();
    let mut push = |scenario: &'static str, label: &'static str, document: BsonDocument, expected: Expected| {
        steps.push(Step { scenario, label, document, expected });
    };

    // A. 基础 CRUD
    push(
        "crud",
        "insert two documents",
        doc(vec![
            ("insert", text("crud")),
            (
                "documents",
                array(vec![
                    d(vec![("_id", int(1)), ("a", text("x")), ("n", int(5))]),
                    d(vec![("_id", int(2)), ("a", text("y")), ("n", long(7))]),
                ]),
            ),
            ("$db", text("app")),
        ]),
        Expected::Match("18000000106e0002000000016f6b00000000000000f03f00"),
    );
    push(
        "crud",
        "find all",
        doc(vec![("find", text("crud")), ("filter", d(vec![])), ("$db", text("app"))]),
        Expected::Match(
            "92000000016f6b00000000000000f03f03637572736f7200790000000466697273744261746368004b0000000330001e0000\
            00105f69640001000000026100020000007800106e00050000000003310022000000105f6964000200000002610002000000\
            7900126e0007000000000000000000126964000100000000000000026e7300090000006170702e63727564000000",
        ),
    );
    push(
        "crud",
        "find by _id",
        doc(vec![("find", text("crud")), ("filter", d(vec![("_id", int(1))])), ("$db", text("app"))]),
        Expected::Match(
            "6d000000016f6b00000000000000f03f03637572736f720054000000046669727374426174636800260000000330001e0000\
            00105f69640001000000026100020000007800106e00050000000000126964000100000000000000026e7300090000006170\
            702e63727564000000",
        ),
    );
    push(
        "crud",
        "count all",
        doc(vec![("count", text("crud")), ("query", d(vec![])), ("$db", text("app"))]),
        Expected::Match("18000000106e0002000000016f6b00000000000000f03f00"),
    );
    push(
        "crud",
        "aggregate match+group",
        doc(vec![
            ("aggregate", text("crud")),
            (
                "pipeline",
                array(vec![
                    d(vec![("$match", d(vec![("a", text("x"))]))]),
                    d(vec![("$group", d(vec![("_id", BsonValue::Null), ("total", d(vec![("$sum", int(1))]))]))]),
                ]),
            ),
            ("$db", text("app")),
        ]),
        Expected::Match(
            "64000000016f6b00000000000000f03f03637572736f72004b0000000466697273744261746368001d000000033000150000\
            000a5f69640010746f74616c00010000000000126964000100000000000000026e7300090000006170702e63727564000000",
        ),
    );
    let q1 = d(vec![("_id", int(1))]);
    let inc = d(vec![("$inc", d(vec![("n", int(1))]))]);
    let update_spec = d(vec![("q", q1.clone()), ("u", inc)]);

    push(
        "crud",
        "update $inc",
        doc(vec![("update", text("crud")), ("updates", array(vec![update_spec])), ("$db", text("app"))]),
        Expected::Match("27000000106e0001000000106e4d6f6469666965640001000000016f6b00000000000000f03f00"),
    );
    push(
        "crud",
        "delete one",
        doc(vec![("delete", text("crud")), ("deletes", array(vec![d(vec![("q", q1), ("limit", int(1))])])), ("$db", text("app"))]),
        Expected::Match("18000000106e0001000000016f6b00000000000000f03f00"),
    );
    push(
        "crud",
        "find after delete",
        doc(vec![("find", text("crud")), ("filter", d(vec![])), ("$db", text("app"))]),
        Expected::Match(
            "71000000016f6b00000000000000f03f03637572736f7200580000000466697273744261746368002a000000033000220000\
            00105f69640002000000026100020000007900126e0007000000000000000000126964000100000000000000026e73000900\
            00006170702e63727564000000",
        ),
    );
    push(
        "crud",
        "delete all",
        doc(vec![("delete", text("crud")), ("deletes", array(vec![d(vec![("q", d(vec![])), ("limit", int(0))])])), ("$db", text("app"))]),
        Expected::Match("18000000106e0001000000016f6b00000000000000f03f00"),
    );
    push(
        "crud",
        "count after delete",
        doc(vec![("count", text("crud")), ("query", d(vec![])), ("$db", text("app"))]),
        Expected::Match("18000000106e0000000000016f6b00000000000000f03f00"),
    );

    // B. `_id` 重复键
    push(
        "dup",
        "insert duplicate _id ordered",
        doc(vec![
            ("insert", text("dup")),
            ("documents", array(vec![d(vec![("_id", int(1))]), d(vec![("_id", int(1))]), d(vec![("_id", int(2))])])),
            ("$db", text("app")),
        ]),
        Expected::Match(
            "d4000000106e00010000000477726974654572726f727300af000000033000a700000010696e646578000100000010636f64\
            6500f82a0000026572726d7367004f000000453131303030206475706c6963617465206b6579206572726f7220636f6c6c65\
            6374696f6e3a206170702e64757020696e6465783a205f69645f20647570206b65793a207b205f69643a2031207d00036b65\
            795061747465726e000e000000105f6964000100000000036b657956616c7565000e000000105f6964000100000000000001\
            6f6b00000000000000f03f00",
        ),
    );
    push(
        "dup",
        "insert duplicate _id unordered",
        doc(vec![
            ("insert", text("dup")),
            ("documents", array(vec![d(vec![("_id", int(3))]), d(vec![("_id", int(3))]), d(vec![("_id", int(4))])])),
            ("ordered", BsonValue::Bool(false)),
            ("$db", text("app")),
        ]),
        Expected::Match(
            "d4000000106e00020000000477726974654572726f727300af000000033000a700000010696e646578000100000010636f64\
            6500f82a0000026572726d7367004f000000453131303030206475706c6963617465206b6579206572726f7220636f6c6c65\
            6374696f6e3a206170702e64757020696e6465783a205f69645f20647570206b65793a207b205f69643a2033207d00036b65\
            795061747465726e000e000000105f6964000100000000036b657956616c7565000e000000105f6964000300000000000001\
            6f6b00000000000000f03f00",
        ),
    );
    push(
        "dup",
        "insert existing plus new",
        doc(vec![
            ("insert", text("dup")),
            ("documents", array(vec![d(vec![("_id", int(2))]), d(vec![("_id", int(5))])])),
            ("ordered", BsonValue::Bool(false)),
            ("$db", text("app")),
        ]),
        Expected::Match("18000000106e0002000000016f6b00000000000000f03f00"),
    );

    // C. `_id` 数字去重语义
    push(
        "numeric",
        "insert int 1",
        doc(vec![("insert", text("numeric")), ("documents", array(vec![d(vec![("_id", int(1)), ("tag", text("i"))])])), ("$db", text("app"))]),
        Expected::Match("18000000106e0001000000016f6b00000000000000f03f00"),
    );
    push(
        "numeric",
        "insert double 1.0",
        doc(vec![
            ("insert", text("numeric")),
            ("documents", array(vec![d(vec![("_id", BsonValue::Double(1.0)), ("tag", text("d"))])])),
            ("$db", text("app")),
        ]),
        Expected::Match(
            "de000000106e00000000000477726974654572726f727300b9000000033000b100000010696e646578000000000010636f64\
            6500f82a0000026572726d73670055000000453131303030206475706c6963617465206b6579206572726f7220636f6c6c65\
            6374696f6e3a206170702e6e756d6572696320696e6465783a205f69645f20647570206b65793a207b205f69643a20312e30\
            207d00036b65795061747465726e000e000000105f6964000100000000036b657956616c75650012000000015f6964000000\
            00000000f03f000000016f6b00000000000000f03f00",
        ),
    );
    push(
        "numeric",
        "insert NumberLong(1)",
        doc(vec![("insert", text("numeric")), ("documents", array(vec![d(vec![("_id", long(1)), ("tag", text("l"))])])), ("$db", text("app"))]),
        Expected::Match(
            "dc000000106e00000000000477726974654572726f727300b7000000033000af00000010696e646578000000000010636f64\
            6500f82a0000026572726d73670053000000453131303030206475706c6963617465206b6579206572726f7220636f6c6c65\
            6374696f6e3a206170702e6e756d6572696320696e6465783a205f69645f20647570206b65793a207b205f69643a2031207d\
            00036b65795061747465726e000e000000105f6964000100000000036b657956616c75650012000000125f69640001000000\
            00000000000000016f6b00000000000000f03f00",
        ),
    );
    push(
        "numeric",
        "find NumberLong(1)",
        doc(vec![("find", text("numeric")), ("filter", d(vec![("_id", long(1))])), ("$db", text("app"))]),
        Expected::Match(
            "6b000000016f6b00000000000000f03f03637572736f72005200000004666972737442617463680021000000033000190000\
            00105f6964000100000002746167000200000069000000126964000100000000000000026e73000c0000006170702e6e756d\
            65726963000000",
        ),
    );

    // D. 空数组触发的 `undefined` 键与 `_id: undefined`
    push(
        "undef",
        "insert two empty arrays",
        doc(vec![
            ("insert", text("undef")),
            ("documents", array(vec![d(vec![("_id", text("x1")), ("a", array(vec![]))]), d(vec![("_id", text("x2")), ("a", array(vec![]))])])),
            ("$db", text("app")),
        ]),
        Expected::Match("18000000106e0002000000016f6b00000000000000f03f00"),
    );
    push(
        "undef",
        "create unique index on a",
        doc(vec![
            ("createIndexes", text("undef")),
            ("indexes", array(vec![d(vec![("key", d(vec![("a", int(1))])), ("name", text("a_1")), ("unique", BsonValue::Bool(true))])])),
            ("$db", text("app")),
        ]),
        Expected::Structured(Structured {
            ok: 0.0,
            code: 11000,
            code_name: "DuplicateKey",
            errmsg_prefix: "Index build failed:",
            namespace: "app.undef",
        }),
    );
    push(
        "undef",
        "insert empty array after unique index",
        doc(vec![
            ("insert", text("undef")),
            ("documents", array(vec![d(vec![("_id", text("x3")), ("a", array(vec![]))])])),
            ("$db", text("app")),
        ]),
        Expected::Match("18000000106e0001000000016f6b00000000000000f03f00"),
    );
    push(
        "undef",
        "insert _id undefined",
        doc(vec![
            ("insert", text("undef")),
            ("documents", array(vec![d(vec![("_id", BsonValue::Undefined), ("b", int(1))])])),
            ("$db", text("app")),
        ]),
        Expected::Match("18000000106e0001000000016f6b00000000000000f03f00"),
    );
    push(
        "undef",
        "insert duplicate _id undefined",
        doc(vec![
            ("insert", text("undef")),
            ("documents", array(vec![d(vec![("_id", BsonValue::Undefined), ("b", int(2))])])),
            ("$db", text("app")),
        ]),
        Expected::Match(
            "da000000106e00000000000477726974654572726f727300b5000000033000ad00000010696e646578000000000010636f64\
            6500f82a0000026572726d73670059000000453131303030206475706c6963617465206b6579206572726f7220636f6c6c65\
            6374696f6e3a206170702e756e64656620696e6465783a205f69645f20647570206b65793a207b205f69643a20556e646566\
            696e6564207d00036b65795061747465726e000e000000105f6964000100000000036b657956616c7565000a000000065f69\
            6400000000016f6b00000000000000f03f00",
        ),
    );
    push(
        "undef",
        "find _id undefined",
        doc(vec![("find", text("undef")), ("filter", d(vec![("_id", BsonValue::Undefined)])), ("$db", text("app"))]),
        Expected::Match(
            "61000000016f6b00000000000000f03f03637572736f72004800000004666972737442617463680019000000033000110000\
            00065f696400106200010000000000126964000100000000000000026e73000a0000006170702e756e646566000000",
        ),
    );

    // E. 二进制子类型
    push(
        "binary",
        "insert binary subtypes",
        doc(vec![
            ("insert", text("binary")),
            (
                "documents",
                array(vec![
                    d(vec![("_id", text("b1")), ("b", bin(0x05, vec![1, 2, 3]))]),
                    d(vec![("_id", text("b2")), ("b", bin(0x80, vec![1, 2, 3]))]),
                    d(vec![("_id", text("b3")), ("b", bin(0x02, vec![9]))]),
                ]),
            ),
            ("$db", text("app")),
        ]),
        Expected::Match("18000000106e0003000000016f6b00000000000000f03f00"),
    );
    push(
        "binary",
        "find all binary",
        doc(vec![("find", text("binary")), ("filter", d(vec![])), ("$db", text("app"))]),
        Expected::Match(
            "ad000000016f6b00000000000000f03f03637572736f720094000000046669727374426174636800640000000330001c0000\
            00025f696400030000006231000562000300000005010203000331001c000000025f69640003000000623200056200030000\
            0080010203000332001e000000025f6964000300000062330005620005000000020100000009000012696400010000000000\
            0000026e73000b0000006170702e62696e617279000000",
        ),
    );
    push(
        "binary",
        "find legacy 0x02 document",
        doc(vec![("find", text("binary")), ("filter", d(vec![("_id", text("b3"))])), ("$db", text("app"))]),
        Expected::Match(
            "6f000000016f6b00000000000000f03f03637572736f720056000000046669727374426174636800260000000330001e0000\
            00025f69640003000000623300056200050000000201000000090000126964000100000000000000026e73000b0000006170\
            702e62696e617279000000",
        ),
    );
    push(
        "binary",
        "insert binary _id subtype 05",
        doc(vec![("insert", text("binary")), ("documents", array(vec![d(vec![("_id", bin(0x05, vec![7, 7]))])])), ("$db", text("app"))]),
        Expected::Match("18000000106e0001000000016f6b00000000000000f03f00"),
    );
    push(
        "binary",
        "insert binary _id subtype 80",
        doc(vec![("insert", text("binary")), ("documents", array(vec![d(vec![("_id", bin(0x80, vec![7, 7]))])])), ("$db", text("app"))]),
        Expected::Match("18000000106e0001000000016f6b00000000000000f03f00"),
    );
    push(
        "binary",
        "find binary _id subtype 05",
        doc(vec![("find", text("binary")), ("filter", d(vec![("_id", bin(0x05, vec![7, 7]))])), ("$db", text("app"))]),
        Expected::Match(
            "62000000016f6b00000000000000f03f03637572736f72004900000004666972737442617463680019000000033000110000\
            00055f696400020000000507070000126964000100000000000000026e73000b0000006170702e62696e617279000000",
        ),
    );

    // F. 错误结构
    push(
        "error",
        "find invalid namespace",
        doc(vec![("find", text("")), ("filter", d(vec![])), ("$db", text("app"))]),
        Expected::Match(
            "6b000000016f6b000000000000000000026572726d7367002500000066696e6420636f6d6d616e64206d697373696e672063\
            6f6c6c656374696f6e206e616d650010636f6465004900000002636f64654e616d650011000000496e76616c69644e616d65\
            73706163650000",
        ),
    );
    push(
        "error",
        "update replacement document",
        doc(vec![
            ("update", text("crud")),
            ("updates", array(vec![d(vec![("q", d(vec![])), ("u", d(vec![("a", int(1))]))])])),
            ("$db", text("app")),
        ]),
        Expected::Match("27000000106e0000000000106e4d6f6469666965640000000000016f6b00000000000000f03f00"),
    );
    push(
        "error",
        "aggregate unsupported stage",
        doc(vec![("aggregate", text("crud")), ("pipeline", array(vec![d(vec![("$out", text("crud_copy"))])])), ("$db", text("app"))]),
        Expected::Match(
            "4c000000016f6b00000000000000f03f03637572736f72003300000004666972737442617463680005000000001269640001\
            00000000000000026e7300090000006170702e63727564000000",
        ),
    );
    push(
        "error",
        "getMore unknown cursor",
        doc(vec![("getMore", long(99)), ("collection", text("crud")), ("batchSize", int(2)), ("$db", text("app"))]),
        Expected::Match(
            "5b000000016f6b000000000000000000026572726d73670017000000637572736f72206964203939206e6f7420666f756e64\
            0010636f6465002b00000002636f64654e616d65000f000000437572736f724e6f74466f756e640000",
        ),
    );

    // G. 游标
    push(
        "cursor",
        "seed five documents",
        doc(vec![
            ("insert", text("cursor")),
            ("documents", array((0..5).map(|value| d(vec![("_id", int(value)), ("v", int(value))])).collect())),
            ("$db", text("app")),
        ]),
        Expected::Match("18000000106e0005000000016f6b00000000000000f03f00"),
    );
    push(
        "cursor",
        "find batchSize 2",
        doc(vec![("find", text("cursor")), ("filter", d(vec![])), ("batchSize", int(2)), ("$db", text("app"))]),
        Expected::Match(
            "7e000000016f6b00000000000000f03f03637572736f72006500000004666972737442617463680035000000033000150000\
            00105f69640000000000107600000000000003310015000000105f6964000100000010760001000000000012696400010000\
            0000000000026e73000b0000006170702e637572736f72000000",
        ),
    );
    push(
        "cursor",
        "getMore 1",
        doc(vec![("getMore", long(1)), ("collection", text("cursor")), ("batchSize", int(2)), ("$db", text("app"))]),
        Expected::Match(
            "7d000000016f6b00000000000000f03f03637572736f720064000000046e6578744261746368003500000003300015000000\
            105f69640002000000107600020000000003310015000000105f696400030000001076000300000000001269640001000000\
            00000000026e73000b0000006170702e637572736f72000000",
        ),
    );
    push(
        "cursor",
        "getMore 1 again",
        doc(vec![("getMore", long(1)), ("collection", text("cursor")), ("batchSize", int(2)), ("$db", text("app"))]),
        Expected::Match(
            "65000000016f6b00000000000000f03f03637572736f72004c000000046e6578744261746368001d00000003300015000000\
            105f69640004000000107600040000000000126964000100000000000000026e73000b0000006170702e637572736f720000\
            00",
        ),
    );
    push(
        "cursor",
        "killCursors 1",
        doc(vec![("killCursors", text("cursor")), ("cursors", array(vec![long(1)])), ("$db", text("app"))]),
        Expected::Match(
            "6e00000004637572736f72734b696c6c656400050000000004637572736f72734e6f74466f756e6400100000001230000100\
            0000000000000004637572736f7273416c69766500050000000004637572736f7273556e6b6e6f776e000500000000016f6b\
            00000000000000f03f00",
        ),
    );

    steps
}

/// 把一个已解码的命令文档包成 OP_MSG 载荷（flags + kind `0x00` + BSON）。
fn op_msg_body(document: &BsonDocument) -> Vec<u8> {
    let mut body = vec![0_u8; 4];
    body.push(0x00);
    body.extend_from_slice(&encode_bson(document));
    body
}

fn execute(executor: &dyn KeyValueExecutor, cursors: &Mutex<CursorStore>, step: &Step) -> Vec<u8> {
    normalize(handle_op_msg(&op_msg_body(&step.document), Some(executor), cursors))
}

/// 游标 id 是服务端生成的不透明值：统一替换为 `1`，其余字节原样保留。
fn normalize(reply: Vec<u8>) -> Vec<u8> {
    let Ok(mut document) = parse_bson(&reply)
    else {
        return reply;
    };
    normalize_document(&mut document);
    encode_bson(&document)
}

fn normalize_document(document: &mut BsonDocument) {
    for (key, value) in document.fields.iter_mut() {
        match key.as_str() {
            "id" => {
                if matches!(value, BsonValue::Int64(_)) {
                    *value = BsonValue::Int64(1);
                }
            }
            "cursorsKilled" | "cursorsNotFound" => {
                if let BsonValue::Array(items) = value {
                    for item in items.iter_mut() {
                        if matches!(item, BsonValue::Int64(_)) {
                            *item = BsonValue::Int64(1);
                        }
                    }
                }
            }
            _ => {}
        }
        match value {
            BsonValue::Document(inner) => normalize_document(inner),
            BsonValue::Array(items) => {
                for item in items.iter_mut() {
                    if let BsonValue::Document(inner) = item {
                        normalize_document(inner);
                    }
                }
            }
            _ => {}
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn executor() -> LocalExecutor<MemoryShard> {
    let shard = ShardId("mongo-reconcile".into());
    LocalExecutor::new(ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(), HashMap::from([(shard, MemoryShard::new())]), Vec::new())
        .unwrap()
}

#[test]
fn reconcile_corpus_matches_frozen_wire_baseline() {
    let executor = executor();
    let steps = corpus();
    assert_eq!(steps.len(), 38, "对账语料条数漂移");

    // 语料在同一服务实例的游标表上按序推进，跨步骤保留游标状态。
    let cursors = Mutex::new(CursorStore::default());
    let mut matched = 0_usize;
    let mut structured = 0_usize;
    let mut drift = Vec::new();

    for (index, step) in steps.iter().enumerate() {
        let reply_bytes = execute(&executor, &cursors, step);
        let reply = hex(&reply_bytes);
        let position = index + 1;
        match &step.expected {
            Expected::Match(expected) => {
                if reply == *expected {
                    matched += 1;
                }
                else {
                    drift.push(format!("{position:02} MATCH 预期失效 {}/{}\n  预期 {expected}\n  实得 {reply}", step.scenario, step.label));
                }
            }
            Expected::Structured(expectation) => {
                if let Some(failure) = check_structured(position, step, expectation, &reply_bytes) {
                    drift.push(format!("{failure}\n  实得 {reply}"));
                }
                else {
                    structured += 1;
                }
            }
        }
    }

    assert!(drift.is_empty(), "对账基线与实跑不一致：\n{}", drift.join("\n"));
    assert_eq!(matched, 37, "MATCH 条数漂移");
    assert_eq!(structured, 1, "结构化断言条数漂移");
}

/// 只校验稳定字段。真机 `createIndexes` 失败消息里的 operation id 与集合 UUID
/// 每次构建都不同，不能逐字节比较。
fn check_structured(position: usize, step: &Step, expectation: &Structured, bytes: &[u8]) -> Option<String> {
    let Ok(document) = parse_bson(bytes)
    else {
        return Some(format!("{position:02} 结构化断言无法解码 BSON {}/{}", step.scenario, step.label));
    };
    let mut failures = Vec::new();
    match document.get("ok") {
        Some(BsonValue::Double(value)) if *value == expectation.ok => {}
        _ => failures.push(format!("ok 应为 {}", expectation.ok)),
    }
    match document.get("code") {
        Some(BsonValue::Int32(value)) if *value == expectation.code => {}
        _ => failures.push(format!("code 应为 {}", expectation.code)),
    }
    match document.get("codeName") {
        Some(BsonValue::String(value)) if value.as_str() == expectation.code_name => {}
        _ => failures.push(format!("codeName 应为 {}", expectation.code_name)),
    }
    match document.get("errmsg") {
        Some(BsonValue::String(value)) if value.starts_with(expectation.errmsg_prefix) && value.contains(expectation.namespace) => {}
        _ => failures.push(format!("errmsg 应以 {} 开头并包含 {}", expectation.errmsg_prefix, expectation.namespace)),
    }
    if failures.is_empty() {
        return None;
    }
    Some(format!("{position:02} 结构化断言失效 {}/{}：{}", step.scenario, step.label, failures.join("，")))
}
