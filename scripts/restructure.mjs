#!/usr/bin/env node
/**
 * database.rs 一次性结构迁移脚本（默认 dry-run）。
 *
 * 目标：按设计文档最末版的 13 个能力包重排（六包方案作废）。
 *
 * 落位规则：
 * - 旧 crate 的 `src/*.rs` 进 `projects/database/database.<domain>/src/<module>/`；
 *   顶层 `src/lib.rs` 改名为 `src/mod.rs`。
 * - `tests/`、`readme.md`、旧 `Cargo.toml`、`build.rs`、`templates/` 等非源码素材
 *   进 `projects/database/database.<domain>/parts/<module>/`，`src/` 树里不留任何 `Cargo.toml`。
 * - 一个旧 crate 可以拆成多个目标；每个目标有自己的最终模块名。
 * - 已跟踪文件一律逐文件 `git mv`；未跟踪文件用 `fs.rename` 并单列。
 * - 每个文件搬前搬后 sha256 比对，任一侧丢失或不一致立即中止。
 * - 不删除目录、不 commit、不 push、不动 target/ 与 Cargo.lock。
 *
 * 用法：
 *   node scripts/restructure.mjs             # dry-run
 *   node scripts/restructure.mjs --apply     # 真正执行
 */

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '..');
const PLAN_FILE = path.join(HERE, 'restructure.plan.json');
const APPLY = process.argv.includes('--apply');

const WORKSPACE_DIR = 'projects/database';

/** 能力域 -> 目录（目录用点号，Cargo package 用连字符）。 */
const DOMAIN_DIR = {
  types: 'projects/database/database.types',
  input: 'projects/database/database.input',
  query: 'projects/database/database.query',
  execution: 'projects/database/database.execution',
  formats: 'projects/database/database.formats',
  storage: 'projects/database/database.storage',
  cluster: 'projects/database/database.cluster',
  protocol: 'projects/database/database.protocol',
  client: 'projects/database/database.client',
  server: 'projects/database/database.server',
  orm: 'projects/database/database.orm',
  generate: 'projects/database/database.generate',
};

/** 最终 13 个包：门面 + 12 个能力包。 */
const STUBS = [
  {
    domain: 'facade',
    dir: 'projects/database/database',
    pkg: 'database',
    summary: '`database` is the single stable public facade for the database family.',
    todo: 'Re-export abilities only: types, input, query, execution, formats, storage, cluster, protocol, client, server, orm, generate. Do not add brand features.',
  },
  { domain: 'types', dir: DOMAIN_DIR.types, pkg: 'database-types', summary: '`database-types` holds shared values, identities, errors and a few cross-domain contracts.', todo: 'Declare the merged modules under `src/` and drop the staged manifests in `parts/`.' },
  { domain: 'input', dir: DOMAIN_DIR.input, pkg: 'database-input', summary: '`database-input` owns VOS / SQL / command input semantics and lowering.', todo: 'Declare the merged modules under `src/` and drop the staged manifests in `parts/`.' },
  { domain: 'query', dir: DOMAIN_DIR.query, pkg: 'database-query', summary: '`database-query` owns binding, parameters, predicates, projections, plan validation and result description.', todo: 'Declare the merged modules under `src/` and drop the staged manifests in `parts/`.' },
  { domain: 'execution', dir: DOMAIN_DIR.execution, pkg: 'database-execution', summary: '`database-execution` owns connections, cursors, operators, writes, transactions and UDF invocation.', todo: 'Declare the merged modules under `src/` and drop the staged manifests in `parts/`.' },
  { domain: 'formats', dir: DOMAIN_DIR.formats, pkg: 'database-formats', summary: '`database-formats` owns on-disk byte layouts: pages, records, WAL, blobs, shard snapshots, replication logs, SQLite format-3.', todo: 'Declare the merged modules under `src/` and drop the staged manifests in `parts/`.' },
  { domain: 'storage', dir: DOMAIN_DIR.storage, pkg: 'database-storage', summary: '`database-storage` owns actual reads/writes, locks, snapshots, checkpoints, recovery and object storage.', todo: 'Declare the merged modules under `src/` and drop the staged manifests in `parts/`.' },
  { domain: 'cluster', dir: DOMAIN_DIR.cluster, pkg: 'database-cluster', summary: '`database-cluster` owns sharding, routing, ownership, fencing, replication and distributed orchestration.', todo: 'Declare the merged modules under `src/` and drop the staged manifests in `parts/`.' },
  { domain: 'protocol', dir: DOMAIN_DIR.protocol, pkg: 'database-protocol', summary: '`database-protocol` owns wire codecs, handshakes and protocol state only.', todo: 'Declare the merged modules under `src/` and drop the staged manifests in `parts/`.' },
  { domain: 'client', dir: DOMAIN_DIR.client, pkg: 'database-client', summary: '`database-client` owns connections, authentication, request/response transport and result reading.', todo: 'Declare the merged modules under `src/` and drop the staged manifests in `parts/`.' },
  { domain: 'server', dir: DOMAIN_DIR.server, pkg: 'database-server', summary: '`database-server` owns service sessions, auth ingress, prepared-statement lifecycle and request dispatch.', todo: 'Declare the merged modules under `src/` and drop the staged manifests in `parts/`.' },
  { domain: 'orm', dir: DOMAIN_DIR.orm, pkg: 'database-orm', summary: '`database-orm` owns object mapping, datasource adapters, sessions, migration planning, projection/hydrate/outbox.', todo: 'Declare the merged modules under `src/` and drop the staged manifests in `parts/`.' },
  { domain: 'generate', dir: DOMAIN_DIR.generate, pkg: 'database-generate', summary: '`database-generate` owns generation models, Dejavu rendering and multi-language emitters.', todo: 'Declare the merged modules under `src/` and drop the staged manifests in `parts/`.' },
];

/**
 * 搬移条目。`targets` 按顺序匹配 `src/` 下的相对路径：
 * - `match` 里以 `/` 结尾表示整棵子树，否则按精确文件名匹配；
 * - `rest: true` 的条目兜住剩余文件；
 * - `primary: true` 的条目同时接收该 crate 的非源码素材（parts）。
 */
const MOVES = [
  {
    id: 'yydb-types',
    src: 'projects/types/yydb-types',
    pkg: 'yydb-types',
    note: '通用值/标识/错误进 types；UDF 执行模型按职责进 execution。',
    targets: [
      { domain: 'types', module: 'yydb', rest: true, primary: true },
      { domain: 'execution', module: 'udf_contract', match: ['udf/'] },
    ],
  },
  {
    id: 'yyds-types',
    src: 'projects/types/yyds-types',
    pkg: 'yyds-types',
    note: '分片/复制/路由进 cluster；VOS UDF lowering 进 input；KV 命令/结果进 query。',
    targets: [
      { domain: 'types', module: 'yyds', rest: true, primary: true },
      { domain: 'cluster', module: 'yyds', match: ['distributed_execution.rs', 'replication.rs', 'replication_log.rs', 'routing.rs'] },
      { domain: 'input', module: 'yyds', match: ['vos_udf.rs'] },
      { domain: 'query', module: 'yyds_kv', match: ['key_value.rs'] },
    ],
  },
  {
    id: 'iris-types',
    src: 'projects/types/iris-types',
    pkg: 'iris-types',
    targets: [{ domain: 'orm', module: 'iris_types', rest: true, primary: true }],
  },
  {
    id: 'iris-ir',
    src: 'projects/types/iris-ir',
    pkg: 'iris-ir',
    note: 'plan/op/envelope 形态 -> query；orm 依赖 query，反向不允许。',
    targets: [{ domain: 'query', module: 'iris_ir', rest: true, primary: true }],
  },
  {
    id: 'yydb-query',
    src: 'projects/planning/yydb-query',
    pkg: 'yydb-query',
    note: 'SQL/lower/resolve 进 input；dml/exec/write 进 execution；绑定/计划/结果留 query。',
    targets: [
      { domain: 'query', module: 'yydb_query', rest: true, primary: true },
      { domain: 'input', module: 'yydb_query', match: ['sql.rs', 'lower.rs', 'resolve.rs'] },
      { domain: 'execution', module: 'yydb_ops', match: ['dml.rs', 'exec.rs', 'write.rs'] },
    ],
  },
  {
    id: 'yydb-formats',
    src: 'projects/formats/yydb-formats',
    pkg: 'yydb-formats',
    note: '页/记录/WAL/blob 字节布局留 formats；file/pager/memory/shm/append 的实际读写进 storage。',
    targets: [
      { domain: 'formats', module: 'yydb', rest: true, primary: true },
      { domain: 'storage', module: 'yydb_io', match: ['file.rs', 'pager.rs', 'memory.rs', 'shm.rs', 'wal_append.rs'] },
    ],
  },
  { id: 'yyds-formats', src: 'projects/formats/yyds-formats', pkg: 'yyds-formats', targets: [{ domain: 'formats', module: 'yyds', rest: true, primary: true }] },
  { id: 'sqlite-formats', src: 'projects/formats/sqlite-formats', pkg: 'sqlite-formats', targets: [{ domain: 'formats', module: 'sqlite', rest: true, primary: true }] },
  { id: 'yydb-storage', src: 'projects/storage/yydb-storage', pkg: 'yydb-storage', targets: [{ domain: 'storage', module: 'yydb', rest: true, primary: true }] },
  { id: 'yyds-kv', src: 'projects/storage/yyds-storage', pkg: 'yyds-kv', targets: [{ domain: 'storage', module: 'yyds_kv', rest: true, primary: true }] },
  { id: 'sqlite-storage', src: 'projects/storage/sqlite-storage', pkg: 'sqlite-storage', targets: [{ domain: 'storage', module: 'sqlite', rest: true, primary: true }] },
  {
    id: 'yydb-engine',
    src: 'projects/execution/yydb-engine',
    pkg: 'yydb-engine',
    note: '连接/事务/UDF/对象/KV 运行态留 execution；storage/ 与 journal 的持久化实现进 storage。',
    targets: [
      { domain: 'execution', module: 'yydb_engine', rest: true, primary: true },
      { domain: 'storage', module: 'yydb_engine_io', match: ['storage/', 'journal.rs'] },
    ],
  },
  { id: 'yydb-execution', src: 'projects/execution/yydb-execution', pkg: 'yydb-execution', targets: [{ domain: 'execution', module: 'yydb_execution', rest: true, primary: true }] },
  { id: 'yydb-udf', src: 'projects/execution/yydb-udf', pkg: 'yydb-udf', targets: [{ domain: 'execution', module: 'yydb_udf', rest: true, primary: true }] },
  { id: 'yyds-execution', src: 'projects/execution/yyds-execution', pkg: 'yyds-execution', targets: [{ domain: 'execution', module: 'yyds_execution', rest: true, primary: true }] },
  { id: 'yyds-catalog', src: 'projects/control/yyds-catalog', pkg: 'yyds-catalog', targets: [{ domain: 'cluster', module: 'yyds_catalog', rest: true, primary: true }] },
  { id: 'yyds-control', src: 'projects/control/yyds-control', pkg: 'yyds-control', targets: [{ domain: 'cluster', module: 'yyds_control', rest: true, primary: true }] },
  { id: 'yydb-protocol', src: 'projects/protocol/yydb-protocol', pkg: 'yydb-protocol', targets: [{ domain: 'protocol', module: 'yydb', rest: true, primary: true }] },
  {
    id: 'yyds-gateway',
    src: 'projects/gateways/yyds-gateway',
    pkg: 'yyds-gateway',
    note: 'SQL 输入进 input；catalog/planner/meta/ddl 进 query；sql_execute 进 execution；config/kind/serial 与网关基座留 protocol。',
    targets: [
      { domain: 'protocol', module: 'gateway', rest: true, primary: true },
      { domain: 'input', module: 'sql', match: ['sql.rs'] },
      { domain: 'query', module: 'sql', match: ['catalog_bind.rs', 'sql_batch.rs', 'sql_ddl.rs', 'sql_describe.rs', 'sql_meta.rs', 'sql_parameters.rs', 'sql_planner.rs', 'sql_schema.rs'] },
      { domain: 'execution', module: 'sql_exec', match: ['sql_execute.rs', 'sql_execute/'] },
    ],
  },
  {
    id: 'yyds-gateway-mysql',
    src: 'projects/gateways/yyds-gateway-mysql',
    pkg: 'yyds-gateway-mysql',
    targets: [
      { domain: 'server', module: 'mysql', rest: true, primary: true },
      { domain: 'client', module: 'mysql', match: ['client.rs'] },
      { domain: 'protocol', module: 'mysql_wire', match: ['wire.rs'] },
    ],
  },
  {
    id: 'yyds-gateway-pgsql',
    src: 'projects/gateways/yyds-gateway-pgsql',
    pkg: 'yyds-gateway-pgsql',
    targets: [
      { domain: 'server', module: 'pgsql', rest: true, primary: true },
      { domain: 'client', module: 'pgsql', match: ['client.rs'] },
      { domain: 'protocol', module: 'pgsql_wire', match: ['wire.rs'] },
    ],
  },
  {
    id: 'yyds-gateway-redis',
    src: 'projects/gateways/yyds-gateway-redis',
    pkg: 'yyds-gateway-redis',
    note: 'resp/bind 是 wire 编解码；bin 与服务会话进 server。',
    targets: [
      { domain: 'server', module: 'redis', rest: true, primary: true },
      { domain: 'client', module: 'redis', match: ['client.rs'] },
      { domain: 'protocol', module: 'redis_wire', match: ['resp.rs', 'bind.rs'] },
    ],
  },
  {
    id: 'yyds-gateway-mongo',
    src: 'projects/gateways/yyds-gateway-mongo',
    pkg: 'yyds-gateway-mongo',
    note: '现有文件没有独立 client；client 能力需合并后新写。',
    targets: [
      { domain: 'server', module: 'mongo', rest: true, primary: true },
      { domain: 'protocol', module: 'mongo_wire', match: ['bson.rs', 'wire.rs'] },
    ],
  },
  { id: 'yydb-client', src: 'projects/services/yydb-client', pkg: 'yydb-client', targets: [{ domain: 'client', module: 'yydb', rest: true, primary: true }] },
  { id: 'yydb-server', src: 'projects/services/yydb-server', pkg: 'yydb-server', note: 'host_wire 暂留 server，合并时再判断是否下沉到 protocol。', targets: [{ domain: 'server', module: 'yydb', rest: true, primary: true }] },
  { id: 'iris-adapter-d1', src: 'projects/adapters/iris-adapter-d1', pkg: 'iris-adapter-d1', targets: [{ domain: 'orm', module: 'adapter_d1', rest: true, primary: true }] },
  { id: 'iris-adapter-mysql', src: 'projects/adapters/iris-adapter-mysql', pkg: 'iris-adapter-mysql', targets: [{ domain: 'orm', module: 'adapter_mysql', rest: true, primary: true }] },
  { id: 'iris-adapter-postgres', src: 'projects/adapters/iris-adapter-postgres', pkg: 'iris-adapter-postgres', targets: [{ domain: 'orm', module: 'adapter_postgres', rest: true, primary: true }] },
  { id: 'iris-adapter-redis', src: 'projects/adapters/iris-adapter-redis', pkg: 'iris-adapter-redis', targets: [{ domain: 'orm', module: 'adapter_redis', rest: true, primary: true }] },
  { id: 'iris-adapter-sqlite', src: 'projects/adapters/iris-adapter-sqlite', pkg: 'iris-adapter-sqlite', targets: [{ domain: 'orm', module: 'adapter_sqlite', rest: true, primary: true }] },
  { id: 'iris-connector-yydb', src: 'projects/connectors/iris-connector-yydb', pkg: 'iris-connector-yydb', targets: [{ domain: 'orm', module: 'connector_yydb', rest: true, primary: true }] },
  { id: 'iris-connector-yyds', src: 'projects/connectors/iris-connector-yyds', pkg: 'iris-connector-yyds', targets: [{ domain: 'orm', module: 'connector_yyds', rest: true, primary: true }] },
  { id: 'iris-generator', src: 'projects/tools/iris-generator', pkg: 'iris-generator', targets: [{ domain: 'generate', module: 'iris', rest: true, primary: true }] },
];

/** 产品门面：被能力包取代，本轮不搬不删。 */
const DEFERRED = [
  { path: 'projects/facades/yydb', pkg: 'yydb', abilities: ['execution', 'storage', 'formats', 'query', 'protocol', 'client', 'server'], advice: '拆成能力引用后由 database 门面统一暴露；产品仓只保留绑定。' },
  { path: 'projects/facades/yyds', pkg: 'yyds', abilities: ['cluster', 'execution', 'storage', 'formats', 'protocol', 'client', 'server'], advice: '同上，分片/复制语义归 cluster。' },
  { path: 'projects/facades/sqlite', pkg: 'sqlite', abilities: ['input', 'query', 'formats', 'storage', 'execution'], advice: 'SQL 语义与 format-3 归能力包，产品门面退场。' },
  { path: 'projects/facades/iris', pkg: 'iris', abilities: ['orm', 'generate', 'query'], advice: 'ORM 与代码生成归能力包，产品仓只保留薄门面。' },
  { path: 'projects/facades/iris-host', pkg: 'iris-host', abilities: ['orm', 'execution', 'client'], advice: '宿主会话能力归 orm/execution。' },
  { path: 'projects/facades/iris-wasm', pkg: 'iris-wasm', abilities: ['orm', 'generate'], advice: 'wasm 只做绑定转换，语义归 orm/generate。' },
];

/** 已裁决：以设计文档最末版 13 包为准，六包方案作废。 */
const DESIGN_DECISION = {
  status: 'resolved',
  chosen: '13-packages',
  summary: '六包方案作废，按设计文档约 4109-4130 行的能力树重排。',
  facadeRule: '门面只按能力 re-export，不出现 database::yydb / yyds / iris / sqlite，也不建品牌 feature。',
};

/** 全量映射后仍属低置信度的落点，不阻塞搬移，但合并时要复核。 */
const PROVISIONAL = [
  { path: 'projects/planning/yydb-query/src/store.rs', placed: 'query', reason: '行存储假设与 placement hook 也可归 storage；暂随 query 走。' },
  { path: 'projects/types/yyds-types/src/key_value.rs', placed: 'query', reason: 'KV 命令与结果形状也可归 storage；按命令语义暂放 query。' },
  { path: 'projects/storage/yyds-storage/src/replication_log.rs', placed: 'storage', reason: '复制日志的字节格式归 formats，协调归 cluster；这里只留持久化读写。' },
  { path: 'projects/gateways/yyds-gateway-mongo', placed: 'server + protocol', reason: '现有文件没有独立 client 实现，client 能力需合并后补写。' },
  { path: 'projects/services/yydb-server/src/host_wire.rs', placed: 'server', reason: 'host wire 也可能属于 protocol；合并时复核。' },
];

/** 明确不动、需单列。 */
const UNTOUCHED = [
  { path: 'projects/searcher', reason: '空目录（未跟踪、无 Cargo.toml），不是 crate；保持原样等待人工裁决。' },
];

/** 无阻塞项：每个已存在文件都有唯一目标。 */
const UNRESOLVED = [];

// ---------------------------------------------------------------------------
// 基础工具
// ---------------------------------------------------------------------------

const posix = (p) => p.split(path.sep).join('/');
const abs = (rel) => path.resolve(REPO_ROOT, rel);
const exists = (rel) => fs.existsSync(abs(rel));

function git(args, { allowFail = false } = {}) {
  const r = spawnSync('git', ['-C', REPO_ROOT, ...args], { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024, windowsHide: true });
  if (r.error) throw new Error(`git 调用失败: ${r.error.message}`);
  if (!allowFail && r.status !== 0) throw new Error(`git ${args.join(' ')} 失败 (exit ${r.status}): ${(r.stderr || '').trim()}`);
  return r;
}

function branchName() {
  return (git(['branch', '--show-current'], { allowFail: true }).stdout || '').trim();
}

function isClean(relPath) {
  return (git(['status', '--porcelain', '--', relPath], { allowFail: true }).stdout || '').trim().length === 0;
}

function dirtyEntries() {
  return (git(['status', '--porcelain', '--', 'projects'], { allowFail: true }).stdout || '')
    .split('\n')
    .map((s) => s.trimEnd())
    .filter(Boolean);
}

function listTracked(relDir) {
  return git(['ls-files', '-z', '--', relDir]).stdout.split('\0').filter(Boolean).sort();
}

function sha256(absPath) {
  return crypto.createHash('sha256').update(fs.readFileSync(absPath)).digest('hex');
}

function walkFiles(absDir, out = [], skipped = []) {
  if (!fs.existsSync(absDir)) return out;
  for (const entry of fs.readdirSync(absDir, { withFileTypes: true })) {
    if (entry.name === 'target' || entry.name === '.git') {
      skipped.push(posix(path.relative(REPO_ROOT, path.join(absDir, entry.name))));
      continue;
    }
    const child = path.join(absDir, entry.name);
    if (entry.isDirectory()) walkFiles(child, out, skipped);
    else if (entry.isFile()) out.push(child);
  }
  return out;
}

// ---------------------------------------------------------------------------
// 落位计算
// ---------------------------------------------------------------------------

function targetRoots(m) {
  const roots = new Set();
  for (const t of m.targets) roots.add(`${DOMAIN_DIR[t.domain]}/src/${t.module}`);
  roots.add(`${DOMAIN_DIR[primary(m).domain]}/parts/${primary(m).module}`);
  return [...roots];
}

function primary(m) {
  return m.targets.find((t) => t.primary) || m.targets[0];
}

function findTarget(m, relSrc) {
  let rest = null;
  for (const t of m.targets) {
    if (t.rest) rest = t;
    for (const pat of t.match || []) {
      if (pat.endsWith('/') ? relSrc.startsWith(pat) : relSrc === pat) return t;
    }
  }
  return rest;
}

/** 返回 {kind:'src'|'parts', domain, module, toRel}，无法落位返回 null。 */
function place(m, relToCrate) {
  const isSrc = relToCrate.startsWith('src/');
  const relSrc = isSrc ? relToCrate.slice(4) : '';
  const isReadme = /^readme\.md$/i.test(path.posix.basename(relToCrate));
  if (isSrc && !isReadme) {
    const t = findTarget(m, relSrc);
    if (!t) return null;
    const rel = relSrc === 'lib.rs' ? 'mod.rs' : relSrc;
    return { kind: 'src', domain: t.domain, module: t.module, toRel: `${DOMAIN_DIR[t.domain]}/src/${t.module}/${rel}` };
  }
  const p = primary(m);
  return { kind: 'parts', domain: p.domain, module: p.module, toRel: `${DOMAIN_DIR[p.domain]}/parts/${p.module}/${relToCrate}` };
}

function analyze() {
  const moves = MOVES.map((m) => {
    const srcExists = exists(m.src);
    const roots = targetRoots(m);
    const presentRoots = roots.filter((r) => exists(r));
    const item = { ...m, srcExists, roots, state: 'ready', files: [], partsFiles: [], untracked: [], skippedIgnored: [], unmapped: [], bytes: 0, error: null };

    if (!srcExists) {
      if (presentRoots.length === roots.length) item.state = 'applied';
      else if (presentRoots.length === 0) item.state = 'missing';
      else { item.state = 'conflict'; item.error = `部分目标已存在: ${presentRoots.join(', ')}`; }
      return item;
    }
    if (presentRoots.length > 0) {
      item.state = 'conflict';
      item.error = `目标目录已存在: ${presentRoots.join(', ')}`;
      return item;
    }
    if (!isClean(m.src)) {
      item.state = 'skipped-dirty';
      item.error = '源路径有未提交改动（git status --porcelain 非空）';
      return item;
    }

    const tracked = listTracked(m.src);
    const trackedSet = new Set(tracked.map(posix));
    for (const rel of tracked) {
      const relToCrate = rel.slice(m.src.length + 1);
      const slot = place(m, relToCrate);
      if (!slot) { item.unmapped.push(rel); continue; }
      if (fs.existsSync(abs(slot.toRel))) {
        item.state = 'conflict';
        item.error = `目标文件已存在: ${slot.toRel}`;
        return item;
      }
      const size = fs.statSync(abs(rel)).size;
      const entry = { from: rel, to: slot.toRel, sha256: sha256(abs(rel)), bytes: size };
      (slot.kind === 'src' ? item.files : item.partsFiles).push(entry);
      item.bytes += size;
    }

    const walked = [];
    const skippedDirs = [];
    walkFiles(abs(m.src), walked, skippedDirs);
    for (const d of skippedDirs) item.skippedIgnored.push({ path: d, reason: 'target/ 构建产物不搬' });
    for (const absFile of walked) {
      const rel = posix(path.relative(REPO_ROOT, absFile));
      if (trackedSet.has(rel)) continue;
      if (path.basename(rel) === 'Cargo.lock') { item.skippedIgnored.push({ path: rel, reason: 'Cargo.lock 不搬' }); continue; }
      const relToCrate = rel.slice(m.src.length + 1);
      const slot = place(m, relToCrate);
      if (!slot) { item.unmapped.push(rel); continue; }
      const size = fs.statSync(absFile).size;
      item.untracked.push({ from: rel, to: slot.toRel, sha256: sha256(absFile), bytes: size });
      item.bytes += size;
    }

    if (item.unmapped.length) { item.state = 'conflict'; item.error = `有文件无法落位: ${item.unmapped.join(', ')}`; }
    return item;
  });

  const stubs = STUBS.map((s) => {
    const manifest = `${s.dir}/Cargo.toml`;
    const lib = `${s.dir}/src/lib.rs`;
    const state = !exists(manifest) && !exists(lib) ? (exists(s.dir) ? 'partial' : 'create') : (exists(manifest) && exists(lib) ? 'applied' : 'partial');
    const conflicts = [];
    if (state === 'partial') {
      if (exists(manifest) && fs.readFileSync(abs(manifest), 'utf8') !== renderManifest(s)) conflicts.push(manifest);
      if (exists(lib) && fs.readFileSync(abs(lib), 'utf8') !== renderLib(s)) conflicts.push(lib);
    }
    return { ...s, manifest, lib, state, conflicts };
  });

  const dupModules = [];
  const seen = new Set();
  for (const m of moves) for (const t of m.targets) {
    const key = `${t.domain}/${t.module}`;
    if (seen.has(key)) dupModules.push(key);
    seen.add(key);
  }
  return { moves, stubs, dupModules };
}

function renderManifest(s) {
  return [
    `# TODO: ${s.todo}`,
    '[package]',
    `name = "${s.pkg}"`,
    'version.workspace = true',
    'edition.workspace = true',
    'authors.workspace = true',
    'license.workspace = true',
    'repository.workspace = true',
    'publish = false',
    '',
    '[dependencies]',
    '',
  ].join('\n');
}

function renderLib(s) {
  return [
    `//! ${s.summary}`,
    '//!',
    `//! TODO: ${s.todo}`,
    '#![deny(missing_docs)]',
    '',
    '// TODO: declare merged modules.',
    '',
  ].join('\n');
}

// ---------------------------------------------------------------------------
// 执行
// ---------------------------------------------------------------------------

function createStubs(stubs) {
  let created = 0;
  for (const s of stubs) {
    if (s.state === 'applied') continue;
    if (s.conflicts.length) throw new Error(`桩文件内容不一致，拒绝覆盖: ${s.conflicts.join(', ')}`);
    fs.mkdirSync(abs(`${s.dir}/src`), { recursive: true });
    if (!exists(s.manifest)) { fs.writeFileSync(abs(s.manifest), renderManifest(s), 'utf8'); created += 1; }
    if (!exists(s.lib)) { fs.writeFileSync(abs(s.lib), renderLib(s), 'utf8'); created += 1; }
  }
  return created;
}

function applyMove(item) {
  for (const f of [...item.files, ...item.partsFiles, ...item.untracked]) {
    fs.mkdirSync(path.dirname(abs(f.to)), { recursive: true });
    const untracked = item.untracked.includes(f);
    if (untracked) fs.renameSync(abs(f.from), abs(f.to));
    else git(['mv', f.from, f.to]);
    if (fs.existsSync(abs(f.from))) throw new Error(`源文件仍在: ${f.from}`);
    if (!fs.existsSync(abs(f.to))) throw new Error(`目标文件消失: ${f.to}`);
    if (sha256(abs(f.to)) !== f.sha256) throw new Error(`哈希不一致: ${f.to}`);
  }
}

// ---------------------------------------------------------------------------
// 输出
// ---------------------------------------------------------------------------

function manualTodo() {
  const todo = [
    { file: 'Cargo.toml', action: 'members 用 "projects/database/*" 替换 13 条旧类别通配；default-members 指向 database 门面' },
    { file: 'Cargo.toml', action: 'workspace.dependencies 的 path 全部改指 projects/database/database.<domain>/；删除以旧 crate 名为键的依赖项' },
    { file: 'Cargo.toml', action: 'workspace.dependencies 不再暴露任何品牌 profile（yydb/yyds/iris/sqlite 相关键）' },
    { file: 'projects/database/database/Cargo.toml', action: '补 12 个能力包的 re-export 与 feature；只做模块入口，不写实现，不建品牌 feature' },
  ];
  for (const m of MOVES) {
    for (const t of m.targets) {
      todo.push({ file: `${DOMAIN_DIR[t.domain]}/src/${t.module}`, action: `把 ${m.pkg} 的这一部分并进 ${t.domain}，在 ${DOMAIN_DIR[t.domain]}/src/lib.rs 声明模块并把 crate:: 路径改成新模块路径` });
    }
    const p = primary(m);
    todo.push({ file: `${DOMAIN_DIR[p.domain]}/parts/${p.module}`, action: `旧 Cargo.toml/tests/readme 暂存，依赖迁入 ${p.domain}/Cargo.toml，合并后由人工删除该 parts 目录（脚本不删）` });
  }
  for (const d of DEFERRED) todo.push({ file: d.path, action: `deferred：职责拆到 ${d.abilities.join(' / ')}，产品仓只留绑定与重导出` });
  for (const u of UNTOUCHED) todo.push({ file: u.path, action: `不动：${u.reason}` });
  return todo;
}

function domainStats(moves) {
  const stats = {};
  for (const m of moves) {
    if (m.state !== 'ready') continue;
    for (const f of [...m.files, ...m.partsFiles, ...m.untracked]) {
      const rel = f.to.startsWith('projects/database/') ? f.to.slice('projects/database/'.length) : '';
      const domain = rel.split('/')[0].replace(/^database\./, '');
      const s = (stats[domain] ||= { files: 0, bytes: 0, crates: new Set() });
      s.files += 1; s.bytes += f.bytes; s.crates.add(m.id);
    }
  }
  return stats;
}

function formatSummary(result) {
  const { moves, stubs } = result;
  const byState = (s) => moves.filter((m) => m.state === s);
  const ready = byState('ready');
  const files = ready.reduce((n, m) => n + m.files.length, 0);
  const parts = ready.reduce((n, m) => n + m.partsFiles.length, 0);
  const untracked = ready.reduce((n, m) => n + m.untracked.length, 0);
  const bytes = ready.reduce((n, m) => n + m.bytes, 0);
  const lines = [];
  lines.push(`== database.rs restructure (${APPLY ? 'APPLY' : 'dry-run'}) ==`);
  lines.push(`repo root : ${REPO_ROOT}`);
  lines.push(`branch    : ${branchName()}`);
  lines.push(`decision  : ${DESIGN_DECISION.status} -> ${DESIGN_DECISION.chosen}`);
  lines.push('');
  lines.push(`moves     : ${moves.length} (ready ${ready.length} / applied ${byState('applied').length} / skipped-dirty ${byState('skipped-dirty').length} / conflict ${byState('conflict').length} / missing ${byState('missing').length})`);
  lines.push(`files     : ${files} src + ${parts} parts + ${untracked} untracked / ${bytes} bytes`);
  lines.push(`stubs     : ${stubs.filter((s) => s.state === 'create').length} create / ${stubs.filter((s) => s.state === 'applied').length} applied / ${stubs.filter((s) => s.state === 'partial').length} partial`);
  lines.push(`dup module: ${result.dupModules.length ? result.dupModules.join(', ') : 'none'}`);
  const dirty = dirtyEntries();
  lines.push(`dirty     : ${dirty.length ? dirty.join('; ') : 'none'}`);
  lines.push('');
  for (const m of moves) {
    const tag = { ready: 'ready ', applied: 'applied', 'skipped-dirty': 'skip  ', conflict: 'CONFL', missing: 'MISS ' }[m.state];
    let extra;
    if (m.state === 'ready') extra = `${m.files.length}s+${m.partsFiles.length}p+${m.untracked.length}u, ${m.bytes}B -> ${m.targets.map((t) => `${t.domain}:${t.module}`).join(', ')}`;
    else if (m.state === 'applied') extra = 'already moved';
    else extra = m.error;
    lines.push(`[${tag}] ${m.id.padEnd(22)} ${m.src} (${extra})`);
  }
  lines.push('');
  if (ready.length) {
    lines.push('未跟踪文件（fs.rename，单列）:');
    for (const m of ready) {
      for (const f of m.untracked) lines.push(`  ${f.from} -> ${f.to}`);
      for (const f of m.skippedIgnored) lines.push(`  [skip] ${f.path} (${f.reason})`);
    }
    lines.push('');
  }
  lines.push('按域聚合（仅 ready）:');
  const stats = domainStats(moves);
  for (const [d, s] of Object.entries(stats).sort(([a], [b]) => a.localeCompare(b))) {
    lines.push(`  ${d.padEnd(12)} ${String(s.files).padStart(3)} files ${String(s.crates.size).padStart(2)} crates ${s.bytes} bytes`);
  }
  lines.push('');
  lines.push('deferred（不搬）:');
  for (const d of DEFERRED) lines.push(`  ${d.path} -> ${d.abilities.join(' / ')}`);
  lines.push('');
  lines.push('untouched（不动）:');
  for (const u of UNTOUCHED) lines.push(`  ${u.path} — ${u.reason}`);
  lines.push('');
  lines.push('provisional（已落位但需复核）:');
  for (const p of PROVISIONAL) lines.push(`  ${p.path} -> ${p.placed} — ${p.reason}`);
  lines.push('');
  return lines.join('\n');
}

function writePlan(result) {
  const plan = {
    schema: 2,
    mode: APPLY ? 'apply' : 'dry-run',
    workspace: WORKSPACE_DIR,
    designDecision: DESIGN_DECISION,
    domains: DOMAIN_DIR,
    stubs: result.stubs.map(({ domain, dir, pkg, manifest, lib, state }) => ({ domain, dir, pkg, manifest, lib, state })),
    moves: result.moves.map(({ id, src, pkg, note, targets, state, error, bytes, files, partsFiles, untracked, skippedIgnored, unmapped }) => ({
      id, src, pkg, note, state, error, bytes, targets, files, partsFiles, untracked, skippedIgnored, unmapped,
    })),
    deferred: DEFERRED,
    unresolved: UNRESOLVED,
    untouched: UNTOUCHED,
    provisional: PROVISIONAL,
    manualTodo: manualTodo(),
  };
  fs.writeFileSync(PLAN_FILE, `${JSON.stringify(plan, null, 2)}\n`, 'utf8');
  return PLAN_FILE;
}

// ---------------------------------------------------------------------------
// 主流程
// ---------------------------------------------------------------------------

function main() {
  const branch = branchName();
  if (APPLY && branch !== 'dev') {
    console.error(`拒绝执行：当前分支是 "${branch}"，apply 只允许在 dev 上。`);
    process.exit(2);
  }

  const result = analyze();
  const conflicts = result.moves.filter((m) => m.state === 'conflict');
  const stubConflicts = result.stubs.filter((s) => s.conflicts.length > 0);

  if (conflicts.length || stubConflicts.length || result.dupModules.length) {
    console.error(formatSummary(result));
    console.error('发现冲突，已中止，未做任何改动：');
    for (const c of conflicts) console.error(`  ${c.src}: ${c.error}`);
    for (const s of stubConflicts) console.error(`  桩文件内容不同: ${s.conflicts.join(', ')}`);
    for (const d of result.dupModules) console.error(`  模块名重复: ${d}`);
    process.exit(2);
  }

  const planPath = writePlan(result);
  console.log(formatSummary(result));
  console.log(`plan written: ${planPath}`);

  if (!APPLY) {
    console.log('dry-run only; 加 --apply 才会真正移动。');
    return;
  }

  let moved = 0;
  for (const item of result.moves) {
    if (item.state !== 'ready') continue;
    console.log(`move ${item.id}: ${item.files.length}s + ${item.partsFiles.length}p + ${item.untracked.length}u`);
    applyMove(item);
    moved += item.files.length + item.partsFiles.length + item.untracked.length;
  }
  const created = createStubs(result.stubs);
  console.log(`done: ${moved} files moved, ${created} stub files created`);
}

main();
