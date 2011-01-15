//! 从 `src/exec.rs` 迁出的测试。

use database_query::local::{CmpOp, cmp_values, compare_values};
use database_types::{FileDescriptor, FileManifestRef, ObjectKind, ObjectRef, Value, Vector};

fn file_value(digest: &str, name: Option<&str>, media_type: Option<&str>) -> Value {
    Value::File(FileDescriptor {
        digest: digest.to_owned(),
        octets: 4,
        name: name.map(str::to_owned),
        media_type: media_type.map(str::to_owned),
        manifest: FileManifestRef { hash: [7; 32], size: 4 },
    })
}

fn digest(byte: u8) -> String {
    std::iter::repeat_n(format!("{byte:02x}"), 32).collect()
}

/// File identity is the content digest: metadata labels never participate,
/// so two descriptors with the same digest are the same file.
#[test]
fn file_equality_is_content_hash_equality() {
    let left = file_value(&digest(0xaa), Some("report.pdf"), Some("application/pdf"));
    let same_content = file_value(&digest(0xaa), Some("renamed.bin"), None);
    let other_content = file_value(&digest(0xbb), Some("report.pdf"), Some("application/pdf"));

    assert_eq!(cmp_values(&left, CmpOp::Eq, &same_content), Some(true));
    assert_eq!(cmp_values(&left, CmpOp::Ne, &same_content), Some(false));
    assert_eq!(cmp_values(&left, CmpOp::Eq, &other_content), Some(false));
    assert_eq!(cmp_values(&left, CmpOp::Ne, &other_content), Some(true));
    assert_eq!(cmp_values(&left, CmpOp::Lt, &other_content), Some(true));
}

/// Vector / object pairs carry no filter comparison semantics: every
/// predicate on them stays unknown instead of silently matching.
#[test]
fn vector_and_object_filter_comparisons_stay_unknown() {
    let vector_a = Value::Vector(Vector::new(vec![1.0_f32, 2.0]).expect("vector"));
    let vector_b = Value::Vector(Vector::new(vec![1.0_f32, 2.0]).expect("vector"));
    for op in [CmpOp::Eq, CmpOp::Ne, CmpOp::Lt, CmpOp::Le, CmpOp::Gt, CmpOp::Ge] {
        assert_eq!(cmp_values(&vector_a, op, &vector_b), None, "vector `{op:?}` must stay unknown");
    }

    let object = Value::Object(ObjectRef { algo: database_types::HashAlgo::Blake3, hash: [1; 32], size: 8, kind: ObjectKind::Blob });
    assert_eq!(cmp_values(&object, CmpOp::Eq, &object), None);
    assert_eq!(cmp_values(&object, CmpOp::Ne, &object), None);
}

/// Sorting keeps a deterministic order for vectors (dimension, then bit
/// patterns) even though filters cannot compare them.
#[test]
fn vector_ordering_is_structural_and_total() {
    let small = Value::Vector(Vector::new(vec![1.0_f32]).expect("vector"));
    let pair = Value::Vector(Vector::new(vec![1.0_f32, 0.0]).expect("vector"));
    assert_eq!(compare_values(&small, &pair), std::cmp::Ordering::Less);
    assert_eq!(compare_values(&pair, &pair), std::cmp::Ordering::Equal);
}

/// Cross-kind pairs order by kind rank and never collapse into equality.
#[test]
fn cross_kind_pairs_never_compare_equal() {
    let file = file_value(&digest(0xaa), None, None);
    let vector = Value::Vector(Vector::new(vec![0.0_f32]).expect("vector"));
    assert_eq!(cmp_values(&file, CmpOp::Eq, &vector), Some(false));
    assert_eq!(cmp_values(&vector, CmpOp::Eq, &file), Some(false));
}
