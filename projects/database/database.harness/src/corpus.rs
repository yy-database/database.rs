//! 语料身份与已知差异台账。
//!
//! 本模块只处理已经由调用方生成好的语料与观测值：固定 seed / count 的身份、
//! 摘要、已知差异登记与匹配结果。它不生成用例，也不判断用例本身是否应该通过。

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest as _, Sha256};

use crate::{HarnessError, Result};

/// 语料身份的公开参数。
///
/// `version` 由各产品仓的基线文件声明；`seed` 与 `count` 必须与生成语料时
/// 使用的值一致。三者任一变化都会让既有摘要失效。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorpusSpec {
    /// 基线文件版本。
    pub version: u32,
    /// 语料生成使用的固定种子。
    pub seed: u64,
    /// 语料条目数量。
    pub count: u64,
}

/// SHA-256 摘要。
///
/// 只对调用方给出的字节取哈希。调用方必须使用其既有的规范化序列化，
/// 本 crate 不重新排列字段或改写转义，避免迁移时摘要无故变化。
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest([u8; 32]);

impl Digest {
    /// 直接由 32 字节构造。
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// 对给定字节取 SHA-256。
    pub fn of(bytes: &[u8]) -> Self {
        let mut out = [0u8; 32];
        out.copy_from_slice(&Sha256::digest(bytes));
        Self(out)
    }

    /// 以小写十六进制输出。
    pub fn to_hex(&self) -> String {
        let mut out = String::with_capacity(64);
        for byte in self.0 {
            use fmt::Write as _;
            let _ = write!(out, "{byte:02x}");
        }
        out
    }

    /// 解析 64 位小写或大写十六进制。
    pub fn parse(value: &str) -> Result<Self> {
        if value.len() != 64 {
            return Err(HarnessError::InvalidDigest(value.to_owned()));
        }
        let mut bytes = [0u8; 32];
        for (index, slot) in bytes.iter_mut().enumerate() {
            let offset = index * 2;
            let pair = &value[offset..offset + 2];
            *slot = u8::from_str_radix(pair, 16).map_err(|_| HarnessError::InvalidDigest(value.to_owned()))?;
        }
        Ok(Self(bytes))
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("Digest").field(&self.to_hex()).finish()
    }
}

impl Serialize for Digest {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// 基线声明的身份：参数加摘要。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaselineIdentity {
    /// 基线声明的语料参数。
    pub spec: CorpusSpec,
    /// 基线声明的语料摘要。
    pub digest: Digest,
}

/// 基线中的一条已知差异。
///
/// `key` 由调用方从用例字段派生，必须稳定且在同一基线内唯一。`signal`
/// 是调用方观测到的差异信号（例如错误码、结构化字段名）；只有与本次观测
/// 完全相同时，才算命中已知差异。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnownDifference {
    /// 稳定的用例键。
    pub key: String,
    /// 差异信号。
    pub signal: Vec<String>,
    /// 归类原因，使用各仓原有的稳定字符串。
    pub reason: String,
}

/// 本次运行中实际观测到的一条差异。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DifferenceObservation {
    /// 稳定的用例键。
    pub key: String,
    /// 差异信号。
    pub signal: Vec<String>,
    /// 归类原因。
    pub reason: String,
}

/// 台账判定问题。
///
/// 判别值保持 kebab-case：影子迁移要求新旧两路输出逐字节一致，既有 TS 台账
/// 的问题词汇（`seed-drift` / `signal-changed` 等）就是本枚举的线名，属于
/// 迁移期契约，不随核心命名规则改写。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum BaselineProblem {
    /// 基线与当前语料的 seed 不一致。
    SeedDrift {
        /// 基线 seed。
        baseline: u64,
        /// 当前 seed。
        current: u64,
    },
    /// 基线与当前语料的 count 不一致。
    CountDrift {
        /// 基线 count。
        baseline: u64,
        /// 当前 count。
        current: u64,
    },
    /// 基线与当前语料的摘要不一致。
    DigestDrift {
        /// 基线摘要。
        baseline: String,
        /// 当前摘要。
        current: String,
    },
    /// 观测到基线未登记的差异。
    Unlisted {
        /// 用例键。
        key: String,
        /// 观测信号。
        signal: Vec<String>,
        /// 观测原因。
        reason: String,
    },
    /// 已知差异的信号与本次观测不一致。
    SignalChanged {
        /// 用例键。
        key: String,
        /// 基线信号。
        baseline: Vec<String>,
        /// 当前信号。
        current: Vec<String>,
    },
    /// 基线登记了本次运行未出现的差异。
    Stale {
        /// 用例键。
        key: String,
        /// 基线信号。
        signal: Vec<String>,
        /// 基线原因。
        reason: String,
    },
}

/// 台账判定结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaselineVerdict {
    /// 命中已知差异的条目数。
    pub matched: u64,
    /// 问题列表，顺序固定为身份漂移、逐条观测、最后是陈旧条目。
    pub problems: Vec<BaselineProblem>,
}

impl BaselineVerdict {
    /// 没有任何问题时才算干净。
    pub fn is_clean(&self) -> bool {
        self.problems.is_empty()
    }
}

/// 一条完整基线：身份加已知差异登记。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Baseline {
    /// 基线声明的身份。
    pub identity: BaselineIdentity,
    /// 已登记的已知差异。
    pub entries: Vec<KnownDifference>,
}

/// 比较基线身份与当前语料身份。
///
/// 问题顺序固定为 seed、count、digest，便于与既有实现逐条对齐。
pub fn check_identity(baseline: &BaselineIdentity, current: &BaselineIdentity) -> Vec<BaselineProblem> {
    let mut problems = Vec::new();
    if baseline.spec.seed != current.spec.seed {
        problems.push(BaselineProblem::SeedDrift { baseline: baseline.spec.seed, current: current.spec.seed });
    }
    if baseline.spec.count != current.spec.count {
        problems.push(BaselineProblem::CountDrift { baseline: baseline.spec.count, current: current.spec.count });
    }
    if baseline.digest != current.digest {
        problems.push(BaselineProblem::DigestDrift { baseline: baseline.digest.to_hex(), current: current.digest.to_hex() });
    }
    problems
}

/// 按既有台账规则判定一次运行。
///
/// 规则：身份漂移先入列；逐条观测按输入顺序检查，未登记记 `unlisted`，
/// 信号不同记 `signal-changed`，完全相同才计入 `matched`；最后按基线登记
/// 顺序补出本次未出现的 `stale` 条目。
pub fn check_baseline(baseline: &Baseline, current: &BaselineIdentity, observed: &[DifferenceObservation]) -> BaselineVerdict {
    let mut problems = check_identity(&baseline.identity, current);
    let known: BTreeMap<&str, &KnownDifference> = baseline.entries.iter().map(|entry| (entry.key.as_str(), entry)).collect();
    let mut seen = BTreeSet::new();
    let mut matched = 0u64;

    for observation in observed {
        match known.get(observation.key.as_str()) {
            None => problems.push(BaselineProblem::Unlisted {
                key: observation.key.clone(),
                signal: observation.signal.clone(),
                reason: observation.reason.clone(),
            }),
            Some(entry) => {
                seen.insert(observation.key.as_str());
                if entry.signal == observation.signal {
                    matched += 1;
                }
                else {
                    problems.push(BaselineProblem::SignalChanged {
                        key: observation.key.clone(),
                        baseline: entry.signal.clone(),
                        current: observation.signal.clone(),
                    });
                }
            }
        }
    }

    for entry in &baseline.entries {
        if !seen.contains(entry.key.as_str()) {
            problems.push(BaselineProblem::Stale { key: entry.key.clone(), signal: entry.signal.clone(), reason: entry.reason.clone() });
        }
    }

    BaselineVerdict { matched, problems }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SHA-256("abc") 的公开标准向量。
    const ABC: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    fn spec(seed: u64, count: u64) -> CorpusSpec {
        CorpusSpec { version: 1, seed, count }
    }

    fn known(key: &str, signal: &[&str]) -> KnownDifference {
        KnownDifference { key: key.to_owned(), signal: signal.iter().map(|item| (*item).to_owned()).collect(), reason: "known".to_owned() }
    }

    fn observation(key: &str, signal: &[&str]) -> DifferenceObservation {
        DifferenceObservation {
            key: key.to_owned(),
            signal: signal.iter().map(|item| (*item).to_owned()).collect(),
            reason: "observed".to_owned(),
        }
    }

    #[test]
    fn digest_matches_standard_vector() {
        assert_eq!(Digest::of(b"abc").to_hex(), ABC);
        assert_eq!(Digest::parse(ABC).unwrap(), Digest::of(b"abc"));
    }

    #[test]
    fn digest_rejects_bad_input() {
        assert!(Digest::parse("abc").is_err());
        assert!(Digest::parse(&"z".repeat(64)).is_err());
    }

    #[test]
    fn digest_serde_uses_hex() {
        let digest = Digest::of(b"abc");
        let json = serde_json::to_string(&digest).unwrap();
        assert_eq!(json, format!("\"{ABC}\""));
        let back: Digest = serde_json::from_str(&json).unwrap();
        assert_eq!(back, digest);
    }

    #[test]
    fn spec_drift_order_is_stable() {
        let baseline = BaselineIdentity { spec: spec(7, 3), digest: Digest::of(b"baseline") };
        let current = BaselineIdentity { spec: spec(9, 5), digest: Digest::of(b"current") };
        let problems = check_identity(&baseline, &current);
        assert_eq!(problems.len(), 3);
        assert!(matches!(problems[0], BaselineProblem::SeedDrift { .. }));
        assert!(matches!(problems[1], BaselineProblem::CountDrift { .. }));
        assert!(matches!(problems[2], BaselineProblem::DigestDrift { .. }));
    }

    #[test]
    fn ledger_clean_run_matches_all_entries() {
        let identity = BaselineIdentity { spec: spec(1, 2), digest: Digest::of(b"corpus") };
        let baseline = Baseline { identity, entries: vec![known("a", &["x"]), known("b", &["y"])] };
        let verdict = check_baseline(&baseline, &identity, &[observation("a", &["x"]), observation("b", &["y"])]);
        assert!(verdict.is_clean());
        assert_eq!(verdict.matched, 2);
    }

    #[test]
    fn ledger_reports_unlisted_signal_and_stale() {
        let identity = BaselineIdentity { spec: spec(1, 2), digest: Digest::of(b"corpus") };
        let baseline = Baseline { identity, entries: vec![known("a", &["x"]), known("b", &["y"]), known("c", &["z"])] };
        let verdict =
            check_baseline(&baseline, &identity, &[observation("a", &["x"]), observation("b", &["y", "extra"]), observation("d", &["w"])]);
        assert_eq!(verdict.matched, 1);
        assert_eq!(verdict.problems.len(), 3);
        assert!(matches!(verdict.problems[0], BaselineProblem::SignalChanged { .. }));
        assert!(matches!(verdict.problems[1], BaselineProblem::Unlisted { .. }));
        assert!(matches!(verdict.problems[2], BaselineProblem::Stale { .. }));
    }

    #[test]
    fn problem_serde_carries_kind() {
        let problem = BaselineProblem::Unlisted { key: "case-7".to_owned(), signal: vec!["code".to_owned()], reason: "unlisted".to_owned() };
        let json = serde_json::to_string(&problem).unwrap();
        assert!(json.contains("\"kind\":\"unlisted\""));
    }
}
