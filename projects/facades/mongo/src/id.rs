//! ObjectId 生成。
//!
//! 按 MongoDB 的 4 + 5 + 3 字节布局生成：unix 秒、随机字节、单调计数。生成器只保证
//! 形状与驱动期望一致，不承诺跨进程唯一性；唯一性仍由 `_id` 存储端检查。

use std::time::{SystemTime, UNIX_EPOCH};

/// 生成一个 12 字节 ObjectId。
pub fn generate_object_id() -> [u8; 12] {
    let seconds = SystemTime::now().duration_since(UNIX_EPOCH).map(|duration| duration.as_secs() as u32).unwrap_or(0);
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|duration| duration.subsec_nanos()).unwrap_or(0);
    let entropy = std::hash::RandomState::new();
    let mut identifier = [0_u8; 12];
    identifier[0..4].copy_from_slice(&seconds.to_be_bytes());
    let random = hash_bytes(&entropy, u64::from(nanos));
    identifier[4..9].copy_from_slice(&random[..5]);
    let tail = hash_bytes(&entropy, u64::from(nanos) ^ 0x9e37_79b9_7f4a_7c52);
    identifier[9..12].copy_from_slice(&tail[..3]);
    identifier
}

fn hash_bytes(state: &std::hash::RandomState, seed: u64) -> [u8; 8] {
    use std::hash::{BuildHasher, Hasher};

    let mut hasher = state.build_hasher();
    hasher.write_u64(seed);
    hasher.finish().to_be_bytes()
}
