//! grammar 驱动的模糊测试输入生成。
//!
//! 本模块提供通用生成引擎：确定性 PRNG、产生式规则树、深度与类型约束、
//! 收缩状态机与复现信封。它不含任何语法词汇——SQL / RESP / BSON 的关键字
//! 与产生式内容由产品仓以 [`Grammar`] 数据形式提供。生成的是**输入文本**，
//! 不是期望值；期望值仍由真机 oracle 或冻结基线提供，失败判定谓词由调用方
//! 持有，本模块不执行任何输入。
//!
//! 复现口径：同一 `Grammar`（以 `version` 守护漂移）+ 同一 [`FuzzInput`]
//! （seed、depth、count、trace）必然重放出逐条相同的语句序列。trace 覆盖
//! 到的决策点直接采用信封记录；trace 之外的部分由 PRNG 流补齐，因此收缩
//! 候选的后缀同样是确定的。
//!
//! PRNG 跨语言移植口径（供产品仓 TS / Python 对拍，必须用 golden vector
//! 验证后才可接入）：
//!
//! - [`SplitMix64`]：`state += 0x9E3779B97F4A7C15`，随后
//!   `z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9`、
//!   `z = (z ^ (z >> 27)) * 0x94D049BB133111EB`、返回 `z ^ (z >> 31)`，
//!   全部为 u64 wrapping 算术。
//! - 有界抽取 [`SplitMix64::below`]：Lemire multiply-high，
//!   `(next_u64() as u128 * bound as u128) >> 64`，无取模、无分支。
//! - 规则作者的书写约定：每条规则的 alternatives 按「最简在前」排列，
//!   收缩状态机优先接受下标更小的 alternative。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{HarnessError, Result};

/// 确定性 PRNG：splitmix64。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SplitMix64 {
    /// 当前内部状态。
    state: u64,
}

impl SplitMix64 {
    /// 以给定种子构造。
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// 产出下一个 64 位值。
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// 在 `[0, bound)` 内均匀抽取（multiply-high，无取模）。`bound` 必须大于 0。
    pub fn below(&mut self, bound: u64) -> u64 {
        debug_assert!(bound > 0);
        (((self.next_u64() as u128) * (bound as u128)) >> 64) as u64
    }
}

/// 产生式符号：终结符、槽位抽取或非终端引用。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Symbol {
    /// 固定文本。
    Literal(String),
    /// 从命名槽位池抽取一个值（表名、列名等 schema 事实由调用方提供）。
    Slot {
        /// 槽位池名称。
        pool: String,
    },
    /// 引用具体规则。
    Rule(String),
    /// 引用某类型标签下的任意规则。
    Typed(String),
}

impl Symbol {
    fn is_terminal(&self) -> bool {
        matches!(self, Symbol::Literal(_) | Symbol::Slot { .. })
    }
}

/// 一条产生式规则。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    /// 稳定规则 id，进入复现信封。
    pub id: String,
    /// 类型标签，调用方用它约束 `Symbol::Typed` 与起点选择。
    pub type_tag: String,
    /// 候选展开式；约定「最简在前」。
    pub alternatives: Vec<Vec<Symbol>>,
}

/// 命名槽位池：调用方提供的取值集合。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotPool {
    /// 池名称。
    pub name: String,
    /// 候选值，必须非空。
    pub values: Vec<String>,
}

/// 完整语法：规则集 + 槽位池 + 起点类型标签。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grammar {
    /// 语法版本；规则内容任何变化都必须递增，复现信封以此守护漂移。
    pub version: u32,
    /// 起点类型标签。
    pub start: String,
    /// 规则列表，id 必须唯一。
    pub rules: Vec<Rule>,
    /// 槽位池列表，name 必须唯一且取值非空。
    pub pools: Vec<SlotPool>,
}

/// 生成参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FuzzPlan {
    /// PRNG 种子。
    pub seed: u64,
    /// 最大展开深度；深度为 0 时只允许纯终结符 alternative。
    pub depth: u32,
    /// 语句条数，至少为 1。
    pub count: u32,
}

/// 复现信封中的一个决策点记录。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceEntry {
    /// 本决策点选择的规则 id。
    pub rule: String,
    /// 选择的 alternative 下标（规则内绝对下标）。
    pub alternative: usize,
    /// 当时可选的 alternative 数量，用于重放时校验语法漂移。
    pub choices: usize,
}

/// 一条可复现的生成输入：参数 + 选择路径。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FuzzInput {
    /// 生成种子。
    pub seed: u64,
    /// 生成时的语法版本。
    pub grammar_version: u32,
    /// 生成深度。
    pub depth: u32,
    /// 语句条数。
    pub count: u32,
    /// 决策点选择路径，按展开顺序记录。
    pub trace: Vec<TraceEntry>,
}

/// 收缩动作类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShrinkMoveKind {
    /// 减少语句条数。
    ReduceCount,
    /// 把某个决策点换成下标更小的 alternative。
    LowerAlternative,
}

/// 一条收缩候选动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShrinkMove {
    /// 动作类型。
    pub kind: ShrinkMoveKind,
    /// `ReduceCount` 的目标条数。
    pub count: u32,
    /// `LowerAlternative` 的决策点下标。
    pub point: usize,
    /// `LowerAlternative` 的目标 alternative 下标。
    pub alternative: usize,
}

/// 收缩状态机：napi 面以 next / report 协议进出，回调谓词只存在于 Rust 侧。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShrinkSession {
    /// 当前已接受的最小失败输入。
    pub base: FuzzInput,
    /// 待尝试动作队列。
    pub moves: Vec<ShrinkMove>,
    /// 队列游标。
    pub cursor: usize,
    /// 上一次 next 产出、等待 report 的候选。
    pub pending: Option<FuzzInput>,
    /// 队列耗尽后置真。
    pub done: bool,
}

impl ShrinkSession {
    /// 当前已接受的最小失败输入。
    pub fn best(&self) -> &FuzzInput {
        &self.base
    }
}

/// 收缩状态机内部：由 base 重建动作队列。约定：先减条数（从大到小），
/// 再按决策点顺序把 alternative 下标往「最简」方向逐个下调。
fn rebuild_moves(input: &FuzzInput) -> Vec<ShrinkMove> {
    let mut moves = Vec::new();
    for count in (1..input.count).rev() {
        moves.push(ShrinkMove { kind: ShrinkMoveKind::ReduceCount, count, point: 0, alternative: 0 });
    }
    for (point, entry) in input.trace.iter().enumerate() {
        for alternative in 0..entry.alternative {
            moves.push(ShrinkMove { kind: ShrinkMoveKind::LowerAlternative, count: 0, point, alternative });
        }
    }
    moves
}

/// 语法索引与校验结果。
struct GrammarIndex<'g> {
    by_id: BTreeMap<&'g str, &'g Rule>,
    by_tag: BTreeMap<&'g str, Vec<&'g Rule>>,
    pools: BTreeMap<&'g str, &'g SlotPool>,
}

fn fuzz_error(message: impl Into<String>) -> HarnessError {
    HarnessError::Fuzz(message.into())
}

impl<'g> GrammarIndex<'g> {
    fn build(grammar: &'g Grammar) -> Result<Self> {
        if grammar.rules.is_empty() {
            return Err(fuzz_error("语法至少需要一条规则"));
        }
        let mut by_id: BTreeMap<&str, &Rule> = BTreeMap::new();
        let mut by_tag: BTreeMap<&str, Vec<&Rule>> = BTreeMap::new();
        for rule in &grammar.rules {
            if rule.alternatives.is_empty() {
                return Err(fuzz_error(format!("规则 {0} 没有任何 alternative", rule.id)));
            }
            if by_id.insert(rule.id.as_str(), rule).is_some() {
                return Err(fuzz_error(format!("规则 id 重复: {0}", rule.id)));
            }
            by_tag.entry(rule.type_tag.as_str()).or_default().push(rule);
        }
        if !by_tag.contains_key(grammar.start.as_str()) {
            return Err(fuzz_error(format!("起点类型标签 {0} 没有任何规则", grammar.start)));
        }
        let mut pools: BTreeMap<&str, &SlotPool> = BTreeMap::new();
        for pool in &grammar.pools {
            if pool.values.is_empty() {
                return Err(fuzz_error(format!("槽位池 {0} 取值为空", pool.name)));
            }
            if pools.insert(pool.name.as_str(), pool).is_some() {
                return Err(fuzz_error(format!("槽位池 name 重复: {0}", pool.name)));
            }
        }
        for rule in &grammar.rules {
            for alternative in &rule.alternatives {
                if alternative.is_empty() {
                    return Err(fuzz_error(format!("规则 {0} 存在空 alternative", rule.id)));
                }
                for symbol in alternative {
                    match symbol {
                        Symbol::Literal(_) => {}
                        Symbol::Slot { pool } => {
                            if !pools.contains_key(pool.as_str()) {
                                return Err(fuzz_error(format!("规则 {0} 引用了未知槽位池 {1}", rule.id, pool)));
                            }
                        }
                        Symbol::Rule(id) => {
                            if !by_id.contains_key(id.as_str()) {
                                return Err(fuzz_error(format!("规则 {0} 引用了未知规则 {1}", rule.id, id)));
                            }
                        }
                        Symbol::Typed(tag) => {
                            if !by_tag.contains_key(tag.as_str()) {
                                return Err(fuzz_error(format!("规则 {0} 引用了未知类型标签 {1}", rule.id, tag)));
                            }
                        }
                    }
                }
            }
        }
        Ok(Self { by_id, by_tag, pools })
    }
}

/// 展开器：一次生成或重放的内部状态。
struct Expander<'g, 'i> {
    index: GrammarIndex<'g>,
    rng: SplitMix64,
    forced: &'i [TraceEntry],
    point: usize,
    trace: Vec<TraceEntry>,
}

impl<'g, 'i> Expander<'g, 'i> {
    /// 展开一个类型标签：先选规则（决策点内），再进入 `expand_rule`。
    fn expand_tag(&mut self, tag: &str, remaining: u32) -> Result<Vec<String>> {
        let candidates = self
            .index
            .by_tag
            .get(tag)
            .ok_or_else(|| fuzz_error(format!("未知类型标签 {tag}")))?;
        let rule = if self.point < self.forced.len() {
            let entry = &self.forced[self.point];
            let found = candidates.iter().position(|candidate| candidate.id == entry.rule);
            match found {
                None => return Err(fuzz_error(format!("trace 第 {0} 点的规则 {1} 不属于标签 {2}", self.point, entry.rule, tag))),
                Some(position) => {
                    let _ = self.rng.below(candidates.len() as u64);
                    candidates[position]
                }
            }
        }
        else {
            let position = self.rng.below(candidates.len() as u64) as usize;
            candidates[position]
        };
        self.expand_rule(rule, remaining)
    }

    /// 展开一条规则：本模块唯一的决策点记录位置。
    fn expand_rule(&mut self, rule: &'g Rule, remaining: u32) -> Result<Vec<String>> {
        let eligible: Vec<usize> = rule
            .alternatives
            .iter()
            .enumerate()
            .filter(|(_, alternative)| remaining > 0 || alternative.iter().all(Symbol::is_terminal))
            .map(|(index, _)| index)
            .collect();
        if eligible.is_empty() {
            return Err(fuzz_error(format!("规则 {0} 在深度边界没有纯终结符 alternative", rule.id)));
        }
        let chosen = if self.point < self.forced.len() {
            let entry = &self.forced[self.point];
            if entry.rule != rule.id {
                return Err(fuzz_error(format!("trace 第 {0} 点期望规则 {1}，实际到达 {2}", self.point, entry.rule, rule.id)));
            }
            if entry.choices != eligible.len() {
                return Err(fuzz_error(format!("trace 第 {0} 点可选数量 {1} 与当前语法 {2} 不一致", self.point, entry.choices, eligible.len())));
            }
            if !eligible.contains(&entry.alternative) {
                return Err(fuzz_error(format!("trace 第 {0} 点 alternative {1} 不在当前可选集", self.point, entry.alternative)));
            }
            let _ = self.rng.below(eligible.len() as u64);
            entry.alternative
        }
        else {
            let position = self.rng.below(eligible.len() as u64) as usize;
            eligible[position]
        };
        self.trace.push(TraceEntry { rule: rule.id.clone(), alternative: chosen, choices: eligible.len() });
        self.point += 1;

        let child_depth = remaining.saturating_sub(1);
        let mut tokens = Vec::new();
        for symbol in &rule.alternatives[chosen] {
            tokens.extend(self.expand_symbol(symbol, child_depth)?);
        }
        Ok(tokens)
    }

    fn expand_symbol(&mut self, symbol: &Symbol, remaining: u32) -> Result<Vec<String>> {
        match symbol {
            Symbol::Literal(text) => Ok(vec![text.clone()]),
            Symbol::Slot { pool } => {
                let values = &self.index.pools.get(pool.as_str()).ok_or_else(|| fuzz_error(format!("未知槽位池 {pool}")))?.values;
                let position = self.rng.below(values.len() as u64) as usize;
                Ok(vec![values[position].clone()])
            }
            Symbol::Rule(id) => {
                let rule = self.index.by_id.get(id.as_str()).ok_or_else(|| fuzz_error(format!("未知规则 {id}")))?;
                self.expand_rule(rule, remaining)
            }
            Symbol::Typed(tag) => self.expand_tag(tag, remaining),
        }
    }
}

/// 按信封展开语句序列。`forced` 为前缀复现路径，其后由 PRNG 补齐。
fn expand(grammar: &Grammar, seed: u64, depth: u32, count: u32, forced: &[TraceEntry]) -> Result<(Vec<String>, Vec<TraceEntry>)> {
    let index = GrammarIndex::build(grammar)?;
    let mut expander = Expander { index, rng: SplitMix64::new(seed), forced, point: 0, trace: Vec::new() };
    let mut statements = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let tokens = expander.expand_tag(grammar.start.as_str(), depth)?;
        statements.push(tokens.join(" "));
    }
    Ok((statements, expander.trace))
}

/// 生成一条可复现信封；语法必须通过校验。
pub fn generate(grammar: &Grammar, plan: &FuzzPlan) -> Result<FuzzInput> {
    if plan.count == 0 {
        return Err(fuzz_error("count 必须至少为 1"));
    }
    let (_, trace) = expand(grammar, plan.seed, plan.depth, plan.count, &[])?;
    Ok(FuzzInput { seed: plan.seed, grammar_version: grammar.version, depth: plan.depth, count: plan.count, trace })
}

/// 按信封重放语句序列；版本不一致即语法漂移，直接拒绝。
pub fn render(grammar: &Grammar, input: &FuzzInput) -> Result<Vec<String>> {
    if input.grammar_version != grammar.version {
        return Err(fuzz_error(format!("语法版本不匹配：信封 {0}，当前 {1}", input.grammar_version, grammar.version)));
    }
    let (statements, _) = expand(grammar, input.seed, input.depth, input.count, &input.trace)?;
    Ok(statements)
}

/// 以失败输入初始化收缩状态机。
pub fn shrink_start(grammar: &Grammar, input: &FuzzInput) -> Result<ShrinkSession> {
    GrammarIndex::build(grammar)?;
    if input.grammar_version != grammar.version {
        return Err(fuzz_error(format!("语法版本不匹配：信封 {0}，当前 {1}", input.grammar_version, grammar.version)));
    }
    Ok(ShrinkSession { base: input.clone(), moves: rebuild_moves(input), cursor: 0, pending: None, done: false })
}

/// 产出下一个收缩候选及其文本；队列耗尽后返回 `None` 并置 `done`。
pub fn shrink_next(grammar: &Grammar, session: &mut ShrinkSession) -> Result<Option<(FuzzInput, Vec<String>)>> {
    if session.done || session.cursor >= session.moves.len() {
        session.done = true;
        session.pending = None;
        return Ok(None);
    }
    let move_ = session.moves[session.cursor];
    let candidate = match move_.kind {
        ShrinkMoveKind::ReduceCount => FuzzInput { count: move_.count, ..session.base.clone() },
        ShrinkMoveKind::LowerAlternative => {
            let mut trace = session.base.trace[..=move_.point].to_vec();
            trace[move_.point].alternative = move_.alternative;
            FuzzInput { trace, ..session.base.clone() }
        }
    };
    let texts = render(grammar, &candidate)?;
    session.pending = Some(candidate.clone());
    Ok(Some((candidate, texts)))
}

/// 回报当前候选是否仍失败：失败则接受为新的 base 并重建队列，否则前进一格。
pub fn shrink_report(session: &mut ShrinkSession, failing: bool) {
    if failing {
        if let Some(candidate) = session.pending.take() {
            session.base = candidate;
            session.moves = rebuild_moves(&session.base);
            session.cursor = 0;
        }
    }
    else {
        session.pending = None;
        session.cursor += 1;
        if session.cursor >= session.moves.len() {
            session.done = true;
        }
    }
}

/// Rust 侧便捷入口：用调用方谓词驱动收缩状态机，返回保持失败的最小输入。
/// 谓词返回 `true` 表示候选仍然失败（继续收缩）。
pub fn shrink<F>(grammar: &Grammar, input: &FuzzInput, mut predicate: F) -> Result<FuzzInput>
where
    F: FnMut(&FuzzInput, &[String]) -> bool,
{
    let mut session = shrink_start(grammar, input)?;
    while let Some((candidate, texts)) = shrink_next(grammar, &mut session)? {
        let failing = predicate(&candidate, &texts);
        shrink_report(&mut session, failing);
    }
    Ok(session.base)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(text: &str) -> Symbol {
        Symbol::Literal(text.to_owned())
    }

    fn typed(tag: &str) -> Symbol {
        Symbol::Typed(tag.to_owned())
    }

    fn slot(pool: &str) -> Symbol {
        Symbol::Slot { pool: pool.to_owned() }
    }

    fn rule(id: &str, tag: &str, alternatives: Vec<Vec<Symbol>>) -> Rule {
        Rule { id: id.to_owned(), type_tag: tag.to_owned(), alternatives }
    }

    fn pool(name: &str, values: &[&str]) -> SlotPool {
        SlotPool { name: name.to_owned(), values: values.iter().map(|item| (*item).to_owned()).collect() }
    }

    /// 小型 SQL 风格语法，仅用于测试引擎本身，不代表任何产品词汇归属。
    fn demo_grammar() -> Grammar {
        Grammar {
            version: 1,
            start: "stmt".to_owned(),
            rules: vec![
                rule("select", "stmt", vec![vec![lit("SELECT"), typed("expr")], vec![lit("SELECT"), lit("1")]]),
                rule("insert", "stmt", vec![vec![lit("INSERT"), slot("tbl")], vec![lit("INSERT"), lit("t0")]]),
                rule("arith", "expr", vec![vec![lit("1")], vec![lit("("), typed("expr"), lit("+"), typed("expr"), lit(")")]]),
            ],
            pools: vec![pool("tbl", &["t0", "t1", "t2"])],
        }
    }

    #[test]
    fn prng_matches_splitmix64_reference() {
        let mut rng = SplitMix64::new(0);
        assert_eq!(rng.next_u64(), 0xe220_a839_7b1d_cdaf);
        assert_eq!(rng.next_u64(), 0x6e78_9e6a_a1b9_65f4);
        assert_eq!(rng.next_u64(), 0x06c4_5d18_8009_454f);
        assert_eq!(rng.next_u64(), 0xf88b_b8a8_724c_81ec);
        let mut rng = SplitMix64::new(0x2D3B_8E4A_1F0C_9B7A);
        assert_eq!(rng.next_u64(), 0x6630_b98b_8257_0402);
        assert_eq!(rng.next_u64(), 0x388f_11ba_3698_effc);
    }

    #[test]
    fn prng_below_is_multiply_high() {
        let mut rng = SplitMix64::new(0);
        assert_eq!(rng.below(100), 88);
        let mut rng = SplitMix64::new(7);
        for _ in 0..64 {
            assert!(rng.below(3) < 3);
        }
    }

    #[test]
    fn generation_is_deterministic() {
        let grammar = demo_grammar();
        let plan = FuzzPlan { seed: 42, depth: 3, count: 5 };
        let first = generate(&grammar, &plan).unwrap();
        let second = generate(&grammar, &plan).unwrap();
        assert_eq!(first, second);
        assert_eq!(render(&grammar, &first).unwrap(), render(&grammar, &second).unwrap());
    }

    #[test]
    fn render_replays_generated_text() {
        let grammar = demo_grammar();
        let plan = FuzzPlan { seed: 1001, depth: 2, count: 4 };
        let input = generate(&grammar, &plan).unwrap();
        let once = render(&grammar, &input).unwrap();
        let twice = render(&grammar, &input).unwrap();
        assert_eq!(once, twice);
        assert_eq!(once.len(), 4);
        for statement in &once {
            assert!(statement.starts_with("SELECT") || statement.starts_with("INSERT"), "{statement}");
        }
    }

    #[test]
    fn depth_boundary_forces_terminal_alternatives() {
        let grammar = Grammar {
            version: 1,
            start: "stmt".to_owned(),
            rules: vec![
                rule("q", "stmt", vec![vec![lit("SELECT"), typed("expr")], vec![lit("SELECT")]]),
                rule("leaf", "expr", vec![vec![lit("*")], vec![lit("expr"), typed("expr")]]),
            ],
            pools: vec![],
        };
        let input = FuzzInput { seed: 5, grammar_version: 1, depth: 0, count: 1, trace: vec![] };
        assert_eq!(render(&grammar, &input).unwrap(), vec!["SELECT".to_owned()]);
        let deep = generate(&grammar, &FuzzPlan { seed: 5, depth: 2, count: 1 }).unwrap();
        let text = render(&grammar, &deep).unwrap().remove(0);
        let selects = text.split("SELECT").count() - 1;
        assert!(selects <= 3, "{text}");
    }

    #[test]
    fn shrink_accepts_everything_down_to_simplest() {
        let grammar = demo_grammar();
        let input = generate(&grammar, &FuzzPlan { seed: 42, depth: 3, count: 6 }).unwrap();
        let minimal = shrink(&grammar, &input, |_input, _texts| true).unwrap();
        assert_eq!(minimal.count, 1);
        assert!(minimal.trace.iter().all(|entry| entry.alternative == 0), "{:?}", minimal.trace);
        render(&grammar, &minimal).unwrap();
    }

    #[test]
    fn shrink_keeps_predicate_true_only_inputs() {
        let grammar = Grammar {
            version: 1,
            start: "stmt".to_owned(),
            rules: vec![rule("q", "stmt", vec![vec![lit("DELETE"), slot("tbl")], vec![lit("SELECT"), lit("1")]])],
            pools: vec![pool("tbl", &["t0"])],
        };
        let input = generate(&grammar, &FuzzPlan { seed: 3, depth: 1, count: 4 }).unwrap();
        let minimal = shrink(&grammar, &input, |_input, texts| texts.iter().any(|text| text.contains("DELETE"))).unwrap();
        assert_eq!(minimal.count, 1);
        assert_eq!(minimal.trace[0].alternative, 0);
        assert_eq!(render(&grammar, &minimal).unwrap(), vec!["DELETE t0".to_owned()]);
    }

    #[test]
    fn shrink_session_protocol_round_trips_through_json() {
        let grammar = demo_grammar();
        let input = generate(&grammar, &FuzzPlan { seed: 9, depth: 1, count: 3 }).unwrap();
        let mut session = shrink_start(&grammar, &input).unwrap();
        let session_json = serde_json::to_string(&session).unwrap();
        session = serde_json::from_str(&session_json).unwrap();
        let mut steps = 0;
        while let Some((_candidate, _texts)) = shrink_next(&grammar, &mut session).unwrap() {
            steps += 1;
            shrink_report(&mut session, false);
            assert!(steps <= session.moves.len() + 1);
        }
        assert!(session.done);
        assert_eq!(session.best(), &input);
    }

    #[test]
    fn render_rejects_grammar_version_drift() {
        let grammar = demo_grammar();
        let input = generate(&grammar, &FuzzPlan { seed: 1, depth: 1, count: 1 }).unwrap();
        let mut drifted = grammar.clone();
        drifted.version = 2;
        assert!(matches!(render(&drifted, &input), Err(HarnessError::Fuzz(_))));
        assert!(matches!(shrink_start(&drifted, &input), Err(HarnessError::Fuzz(_))));
    }

    #[test]
    fn validation_rejects_broken_grammars() {
        let cases: Vec<Grammar> = vec![
            Grammar { version: 1, start: "stmt".to_owned(), rules: vec![], pools: vec![] },
            Grammar {
                version: 1,
                start: "stmt".to_owned(),
                rules: vec![rule("a", "stmt", vec![]), rule("a", "other", vec![vec![lit("x")]])],
                pools: vec![],
            },
            Grammar { version: 1, start: "missing".to_owned(), rules: vec![rule("a", "stmt", vec![vec![lit("x")]])], pools: vec![] },
            Grammar {
                version: 1,
                start: "stmt".to_owned(),
                rules: vec![rule("a", "stmt", vec![vec![Symbol::Rule("nope".to_owned())]])],
                pools: vec![],
            },
            Grammar {
                version: 1,
                start: "stmt".to_owned(),
                rules: vec![rule("a", "stmt", vec![vec![slot("empty")]])],
                pools: vec![pool("empty", &[])],
            },
            Grammar {
                version: 1,
                start: "stmt".to_owned(),
                rules: vec![rule("a", "stmt", vec![vec![]])],
                pools: vec![],
            },
        ];
        for grammar in cases {
            assert!(matches!(generate(&grammar, &FuzzPlan { seed: 0, depth: 1, count: 1 }), Err(HarnessError::Fuzz(_))), "{grammar:?}");
        }
        assert!(matches!(generate(&demo_grammar(), &FuzzPlan { seed: 0, depth: 1, count: 0 }), Err(HarnessError::Fuzz(_))));
    }

    #[test]
    fn symbol_serde_uses_snake_case_tags() {
        assert_eq!(serde_json::to_string(&lit("SELECT")).unwrap(), "{\"literal\":\"SELECT\"}");
        assert_eq!(serde_json::to_string(&slot("tbl")).unwrap(), "{\"slot\":{\"pool\":\"tbl\"}}");
        assert_eq!(serde_json::to_string(&typed("expr")).unwrap(), "{\"typed\":\"expr\"}");
        let symbol: Symbol = serde_json::from_str("{\"rule\":\"arith\"}").unwrap();
        assert_eq!(symbol, Symbol::Rule("arith".to_owned()));
    }
}
