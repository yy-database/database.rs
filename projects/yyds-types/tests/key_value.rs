use yyds_types::{KeyValueAction, KeyValueCommand, KeyValueResult, Namespace};

#[test]
fn key_value_contract_preserves_namespace_and_opaque_bytes() {
    let command = KeyValueCommand {
        namespace: Namespace("redis:0".into()),
        key: vec![0, 0xff, b'k'],
        action: KeyValueAction::Put(vec![0, 0xfe, b'v']),
    };

    assert_eq!(command.namespace.0, "redis:0");
    assert_eq!(command.key, [0, 0xff, b'k']);
    assert_eq!(command.action, KeyValueAction::Put(vec![0, 0xfe, b'v']));
    assert_eq!(KeyValueAction::Exists, KeyValueAction::Exists);
    assert_eq!(KeyValueAction::GetDelete, KeyValueAction::GetDelete);
    assert_eq!(KeyValueAction::GetSet(vec![1]), KeyValueAction::GetSet(vec![1]));
    assert_eq!(KeyValueResult::Exists(true), KeyValueResult::Exists(true));
    assert_eq!(KeyValueResult::GetDelete(None), KeyValueResult::GetDelete(None));
    assert_eq!(KeyValueResult::GetSet(None), KeyValueResult::GetSet(None));
    match KeyValueResult::Get(Some(vec![0, 0xfe, b'v'])) {
        KeyValueResult::Get(Some(value)) => assert_eq!(value, [0, 0xfe, b'v']),
        result => panic!("unexpected result: {result:?}"),
    }
    assert_eq!(KeyValueResult::Put { revision: 9 }, KeyValueResult::Put { revision: 9 });
    assert_eq!(KeyValueResult::Delete { removed: true }, KeyValueResult::Delete { removed: true });
    assert_eq!(KeyValueAction::IncrementBy(-2), KeyValueAction::IncrementBy(-2));
    assert_eq!(KeyValueAction::PutIfAbsent(vec![1]), KeyValueAction::PutIfAbsent(vec![1]));
    assert_eq!(KeyValueAction::PutIfPresent(vec![1]), KeyValueAction::PutIfPresent(vec![1]));
    assert_eq!(KeyValueResult::Increment { value: -2 }, KeyValueResult::Increment { value: -2 });
    assert_eq!(KeyValueResult::PutIfAbsent { revision: None }, KeyValueResult::PutIfAbsent { revision: None });
    assert_eq!(KeyValueResult::PutIfPresent { revision: None }, KeyValueResult::PutIfPresent { revision: None });
}
