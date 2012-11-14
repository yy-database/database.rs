//! 与 MongoDB 仓 `mongo-core` 的写读应答对账基线。
//!
//! 同一份 38 条固定语料分别喂给本网关与参考实现，先把参考实现的回复归一化
//! （游标 id 是服务端不透明值，统一替换为 `1`），再逐字节比对。基线结果：
//! 34 条逐字节一致，4 条为已记录在案的能力差异。
//!
//! 4 条差异在这里按「已知差异」断言：固定网关当前回复字节并写明原因，不写成
//! 通过。差异是否收口（把 `update` 或索引实现上收到命令层）属于结构裁决，
//! 不在网关里补实现来追平。

use std::collections::HashMap;

use database_cluster::topology::ShardMap;
use database_execution::cluster::{KeyValueExecutor, LocalExecutor};
use database_server::yyds::mongo::{
    bson::{BinaryData, BsonDocument, BsonValue, encode_bson, parse_bson},
    handle_op_msg,
};
use database_storage::cluster_kv::MemoryShard;
use database_types::cluster::{ShardEpoch, ShardId};

/// 一条语料的期望结果。
enum Expected {
    /// 参考实现回复的归一化字节，网关必须逐字节一致。
    Match(&'static str),
    /// 已知差异：网关当前的归一化回复字节，以及差异原因。
    KnownDifference { reply: &'static str, reason: &'static str },
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
            "8e000000106f6b000100000003637572736f7200790000000466697273744261746368004b0000000330001e000000105f696400010000000261000200\
            00007800106e00050000000003310022000000105f69640002000000026100020000007900126e0007000000000000000000126964000100000000000000026e7300090000006170702e63727564000000",
        ),
    );
    push(
        "crud",
        "find by _id",
        doc(vec![("find", text("crud")), ("filter", d(vec![("_id", int(1))])), ("$db", text("app"))]),
        Expected::Match(
            "69000000106f6b000100000003637572736f720054000000046669727374426174636800260000000330001e000000105f696400010000000261000200\
            00007800106e00050000000000126964000100000000000000026e7300090000006170702e63727564000000",
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
            "60000000106f6b000100000003637572736f72004b0000000466697273744261746368001d000000033000150000000a5f69640010746f74616c000100\
            00000000126964000100000000000000026e7300090000006170702e63727564000000",
        ),
    );
    let q1 = d(vec![("_id", int(1))]);
    let inc = d(vec![("$inc", d(vec![("n", int(1))]))]);
    let update_spec = d(vec![("q", q1.clone()), ("u", inc)]);

    push(
        "crud",
        "update $inc",
        doc(vec![("update", text("crud")), ("updates", array(vec![update_spec])), ("$db", text("app"))]),
        Expected::KnownDifference {
            reply: "2f000000106f6b0000000000026572726d7367000c000000756e737570706f727465640010636f6465003b00000000",
            reason: "网关没有 `update` 命令，只回通用 `59 unsupported`。参考实现对 `$inc` 回 `n: 1, nModified: 1, ok: 1.0`。",
        },
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
            "6d000000106f6b000100000003637572736f7200580000000466697273744261746368002a00000003300022000000105f696400020000000261000200\
            00007900126e0007000000000000000000126964000100000000000000026e7300090000006170702e63727564000000",
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
            "d4000000106e00010000000477726974654572726f727300af000000033000a700000010696e646578000100000010636f646500f82a0000026572726d7367\
            004f000000453131303030206475706c6963617465206b6579206572726f7220636f6c6c656374696f6e3a206170702e64757020696e6465783a205f69645f\
            20647570206b65793a207b205f69643a2031207d00036b65795061747465726e000e000000105f6964000100000000036b657956616c7565000e000000105f\
            69640001000000000000016f6b00000000000000f03f00",
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
            "d4000000106e00020000000477726974654572726f727300af000000033000a700000010696e646578000100000010636f646500f82a0000026572726d7367\
            004f000000453131303030206475706c6963617465206b6579206572726f7220636f6c6c656374696f6e3a206170702e64757020696e6465783a205f69645f\
            20647570206b65793a207b205f69643a2033207d00036b65795061747465726e000e000000105f6964000100000000036b657956616c7565000e000000105f\
            69640003000000000000016f6b00000000000000f03f00",
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
            "e6000000106e00000000000477726974654572726f727300c1000000033000b900000010696e646578000000000010636f646500f82a0000026572726d7367\
            005d000000453131303030206475706c6963617465206b6579206572726f7220636f6c6c656374696f6e3a206170702e6e756d6572696320696e6465783a20\
            5f69645f20647570206b65793a207b205f69643a20446f75626c6528312e3029207d00036b65795061747465726e000e000000105f6964000100000000036b\
            657956616c75650012000000015f696400000000000000f03f000000016f6b00000000000000f03f00",
        ),
    );
    push(
        "numeric",
        "insert NumberLong(1)",
        doc(vec![("insert", text("numeric")), ("documents", array(vec![d(vec![("_id", long(1)), ("tag", text("l"))])])), ("$db", text("app"))]),
        Expected::Match(
            "dc000000106e00000000000477726974654572726f727300b7000000033000af00000010696e646578000000000010636f646500f82a0000026572726d7367\
            0053000000453131303030206475706c6963617465206b6579206572726f7220636f6c6c656374696f6e3a206170702e6e756d6572696320696e6465783a20\
            5f69645f20647570206b65793a207b205f69643a2031207d00036b65795061747465726e000e000000105f6964000100000000036b657956616c7565001200\
            0000125f6964000100000000000000000000016f6b00000000000000f03f00",
        ),
    );
    push(
        "numeric",
        "find NumberLong(1)",
        doc(vec![("find", text("numeric")), ("filter", d(vec![("_id", long(1))])), ("$db", text("app"))]),
        Expected::Match(
            "67000000106f6b000100000003637572736f7200520000000466697273744261746368002100000003300019000000105f69640001000000027461670002\
            00000069000000126964000100000000000000026e73000c0000006170702e6e756d65726963000000",
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
        Expected::KnownDifference {
            reply: "2f000000106f6b0000000000026572726d7367000c000000756e737570706f727465640010636f6465003b00000000",
            reason: "网关没有索引实现，`createIndexes` 只回 `59 unsupported`。参考实现会在唯一索引 `a_1` 上撞到两条 `{a: []}` 产生的 `undefined` 键，\
                     回 `DuplicateKey`（含顶层 `keyPattern` / `keyValue`）。",
        },
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
            "da000000106e00000000000477726974654572726f727300b5000000033000ad00000010696e646578000000000010636f646500f82a0000026572726d7367\
            0059000000453131303030206475706c6963617465206b6579206572726f7220636f6c6c656374696f6e3a206170702e756e64656620696e6465783a205f69\
            645f20647570206b65793a207b205f69643a20556e646566696e6564207d00036b65795061747465726e000e000000105f6964000100000000036b65795661\
            6c7565000a000000065f696400000000016f6b00000000000000f03f00",
        ),
    );
    push(
        "undef",
        "find _id undefined",
        doc(vec![("find", text("undef")), ("filter", d(vec![("_id", BsonValue::Undefined)])), ("$db", text("app"))]),
        Expected::Match(
            "5d000000106f6b000100000003637572736f7200480000000466697273744261746368001900000003300011000000065f696400106200010000000000\
            126964000100000000000000026e73000a0000006170702e756e646566000000",
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
            "a9000000106f6b000100000003637572736f720094000000046669727374426174636800640000000330001c000000025f69640003000000623100056200\
            0300000005010203000331001c000000025f696400030000006232000562000300000080010203000332001e000000025f69640003000000623300056200\
            050000000201000000090000126964000100000000000000026e73000b0000006170702e62696e617279000000",
        ),
    );
    push(
        "binary",
        "find legacy 0x02 document",
        doc(vec![("find", text("binary")), ("filter", d(vec![("_id", text("b3"))])), ("$db", text("app"))]),
        Expected::Match(
            "6b000000106f6b000100000003637572736f720056000000046669727374426174636800260000000330001e000000025f69640003000000623300056200\
            050000000201000000090000126964000100000000000000026e73000b0000006170702e62696e617279000000",
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
            "5e000000106f6b000100000003637572736f7200490000000466697273744261746368001900000003300011000000055f6964000200000005070700\
            00126964000100000000000000026e73000b0000006170702e62696e617279000000",
        ),
    );

    // F. 错误结构
    push(
        "error",
        "find invalid namespace",
        doc(vec![("find", text("")), ("filter", d(vec![])), ("$db", text("app"))]),
        Expected::Match(
            "67000000106f6b0000000000026572726d7367002500000066696e6420636f6d6d616e64206d697373696e6720636f6c6c656374696f6e206e616d6500\
            10636f6465004900000002636f64654e616d650011000000496e76616c69644e616d6573706163650000",
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
        Expected::KnownDifference {
            reply: "2f000000106f6b0000000000026572726d7367000c000000756e737570706f727465640010636f6465003b00000000",
            reason: "网关没有 `update`，替换式更新同样落进通用 `59 unsupported`。参考实现回 `replacement updates are not supported`。",
        },
    );
    push(
        "error",
        "aggregate unsupported stage",
        doc(vec![("aggregate", text("crud")), ("pipeline", array(vec![d(vec![("$out", text("crud_copy"))])])), ("$db", text("app"))]),
        Expected::KnownDifference {
            reply: "28000000106f6b0000000000026572726d73670005000000246f75740010636f6465003b00000000",
            reason: "两者都是 `59` 三字段错误，但参考 `errmsg` 是 `unknown aggregation stage: $out (supported: ...)`，网关只回 stage 名 `$out`。",
        },
    );
    push(
        "error",
        "getMore unknown cursor",
        doc(vec![("getMore", long(99)), ("collection", text("crud")), ("batchSize", int(2)), ("$db", text("app"))]),
        Expected::Match("3a000000106f6b0000000000026572726d73670017000000637572736f72206964203939206e6f7420666f756e640010636f6465002b00000000"),
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
            "7a000000106f6b000100000003637572736f7200650000000466697273744261746368003500000003300015000000105f6964000000000010760000\
            0000000003310015000000105f69640001000000107600010000000000126964000100000000000000026e73000b0000006170702e637572736f72000000",
        ),
    );
    push(
        "cursor",
        "getMore 1",
        doc(vec![("getMore", long(1)), ("collection", text("cursor")), ("batchSize", int(2)), ("$db", text("app"))]),
        Expected::Match(
            "79000000106f6b000100000003637572736f720064000000046e6578744261746368003500000003300015000000105f6964000200000010760002\
            0000000003310015000000105f69640003000000107600030000000000126964000100000000000000026e73000b0000006170702e637572736f7200\
            0000",
        ),
    );
    push(
        "cursor",
        "getMore 1 again",
        doc(vec![("getMore", long(1)), ("collection", text("cursor")), ("batchSize", int(2)), ("$db", text("app"))]),
        Expected::Match(
            "61000000106f6b000100000003637572736f72004c000000046e6578744261746368001d00000003300015000000105f6964000400000010760004\
            0000000000126964000100000000000000026e73000b0000006170702e637572736f72000000",
        ),
    );
    push(
        "cursor",
        "killCursors 1",
        doc(vec![("killCursors", text("cursor")), ("cursors", array(vec![long(1)])), ("$db", text("app"))]),
        Expected::Match(
            "6a00000004637572736f72734b696c6c656400050000000004637572736f72734e6f74466f756e6400100000001230000100000000000000000463757273\
            6f7273416c69766500050000000004637572736f7273556e6b6e6f776e000500000000106f6b000100000000",
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

fn execute(executor: &dyn KeyValueExecutor, step: &Step) -> Vec<u8> {
    normalize(handle_op_msg(&op_msg_body(&step.document), Some(executor)))
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
fn reconcile_corpus_matches_reference_wire_bytes() {
    let executor = executor();
    let steps = corpus();
    assert_eq!(steps.len(), 38, "对账语料条数漂移");

    let mut matched = 0_usize;
    let mut known = 0_usize;
    let mut drift = Vec::new();

    for (index, step) in steps.iter().enumerate() {
        let reply = hex(&execute(&executor, step));
        let position = index + 1;
        match step.expected {
            Expected::Match(expected) => {
                if reply == expected {
                    matched += 1;
                }
                else {
                    drift.push(format!("{position:02} MATCH 预期失效 {}/{}\n  预期 {expected}\n  实得 {reply}", step.scenario, step.label));
                }
            }
            Expected::KnownDifference { reply: expected, reason } => {
                if reply == expected {
                    known += 1;
                }
                else {
                    drift.push(format!(
                        "{position:02} 已知差异的网关字节漂移 {}/{}\n  原因 {reason}\n  预期 {expected}\n  实得 {reply}",
                        step.scenario, step.label
                    ));
                }
            }
        }
    }

    assert!(drift.is_empty(), "对账基线与实跑不一致：\n{}", drift.join("\n"));
    assert_eq!(matched, 34, "MATCH 条数漂移");
    assert_eq!(known, 4, "MISMATCH 条数漂移");
}
