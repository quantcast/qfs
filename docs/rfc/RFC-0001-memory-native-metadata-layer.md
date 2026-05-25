# RFC-0001: 内存原生元数据层（Memory-Native Metadata Layer）

| 字段 | 值 |
|------|-----|
| **状态** | Draft |
| **日期** | 2026-05-25 |
| **相关** | QFS `metatree`（B+ 树）、`LogWriter`、HDFS NameNode edit log + FSImage |
| **动机来源** | CREATE 延迟分析、与 HDFS NN 路径对比、绿场元数据设计讨论 |

---

## 摘要

本 RFC 提议为 QFS 类分布式文件系统定义一套**从 0 设计的内存原生元数据层**：命名空间用 **按目录分片的哈希索引 + 全局 inode 表** 维护，持久化采用 **edit log + HDFS 式用户快照（引用/COW）+ 周期性 Checkpoint（FSImage）**，与当前 **单一 B+ 树（`metatree`）+ 先 WAL 后改树** 的实现路线对比，并给出实现与分阶段交付路径（**不含**从现有 B+ 树/checkpoint 的迁移方案）。

目标是在可比持久化语义下，将 **`create` / `lookup` 的热路径** 从「多次 B+ 树 descent + 双 insert + 全局串行」收敛为「O(1) 内存索引 + 摊销组提交 fsync」，并保留 VR/幂等等生产特性。

---

## 1. 背景与动机

### 1.1 当前 QFS 元数据路径（CREATE）

空文件 `CREATE` 的典型路径：

```text
ClientThread → submit_request() [全局互斥]
  → MetaCreate::start()           [校验，不改树]
  → LogWriter::Enqueue()          [写 transaction log]
  → （log committed 后）MetaCreate::handle()
  → Tree::create()
       → getFattr(parent)          [B+ 树查找]
       → lookup(parent, name)      [B+ 树查找]
       → link()
            → insert(MetaDentry)   [B+ 树插入 #1]
            → insert(MetaFattr)     [B+ 树插入 #2]
```

特征：

- 命名空间、dentry、fattr、chunkinfo 混在同一棵 **B+ 树**（`kfstree.h` 明确为 B+ 树）。
- **先 durable log，再** 修改内存树（`SubmitBegin` + `LogWriter` 队列）。
- `MetaIdempotentRequest` 将 `logAction` 设为 **`kLogAlways`**，成功路径几乎必落 log。
- 核心处理在 **`submit_request` 全局锁** 下串行（见 `NetDispatch.cc` 注释）。

### 1.2 HDFS NameNode 对照

HDFS 将问题拆成两层：

| 层 | 实现要点 |
|----|----------|
| 运行时 | 全内存 inode 树；目录下按名索引（hash/map）；`create` 主要为内存挂接 |
| 持久化 | Edit log（操作记录）+ FSImage/checkpoint；fsync 可组提交 |

Chunk 分配通常在 **首次 write / addBlock**，而非 `create` 本身，因此 NN 上 `create` 常数项小。

### 1.3 结论

QFS **并非不能** 采用 HDFS NN 式布局；当前选择是 **统一 B+ 树 + 树形 checkpoint** 的历史工程路线。在 CREATE 延迟与元数据 QPS 成为瓶颈时，绿场或下一代元数据层值得单独设计，而非仅在 B+ 树上做局部锁优化（参见仓库内 `MetaTree-Lock-Optimization.md`）。

---

## 2. 目标与非目标

### 2.1 目标

1. **`create` / `lookup`（按 fid + name）**：小目录 **O(1) 均摊**（Small）；超大目录 **O(log N)**（Large，§4.2）；均无全局 `metatree` descent。
2. **持久化**：edit log + §6.3 用户快照 + §6.4 Checkpoint；支持 **`sync=none | batch | always`** 三档。
3. **吞吐**：通过 **目录分片锁 + log 单写线程组提交** 提升并行度。
4. **语义**：保留客户端 **父目录 fid**、幂等 `(session_id, op_id)`、VR/quorum 复制（与现有 MetaServer 部署模型兼容）。
5. **规模（单机）**：假定 **单个 MetaServer 进程、单机 RAM 容纳全部 namespace**（`DirIndex` + `InodeTable` + 可选 `BlockMap`）；容量规划为运维/部署话题，本 RFC 不定义上限模型。
6. **大目录（首版必做）**：单目录 **百万级** 子项时，`lookup` / `create` / `readdir` 不得退化为「单哈希桶长链遍历」；须使用 §4.2 的 **Small/Large 双布局** 与 **升格（promotion）** 机制。

### 2.2 非目标（本 RFC 首版）

- 不定义 chunk 数据路径、纠删码、LayoutManager 细节（仅要求 **BlockMap 与 namespace 解耦**）。
- 不替换现有 ChunkServer 协议。
- **不考虑**从现有 B+ 树 / checkpoint / transaction log 的**离线迁移、在线双写、回滚**（若需要，另起 RFC）。
- **不考虑**元数据 **水平分片**（多 MetaServer 各管一段 namespace）、**冷 inode / namespace 换出**、单机内存超限后的分级存储（另起 RFC）。
- 不实现完整 POSIX（符号链接、硬链接语义等可后续 RFC 补充）。

本 RFC 假定 **绿场部署**：新集群以 v2 snapshot + edit log **冷启动**，或与现行 `metatree` 并行存在、不互通。

文中 **「分片」** 若无特别说明，均指 **单进程内** 的锁分片 / `hash(fid) % N` 数据结构分片，**不是** 集群级 namespace 分片。

---

## 3. 提议架构

### 3.1 逻辑分层

```text
┌─────────────────────────────────────────────────────────┐
│  RPC 层：CREATE / LOOKUP / READDIR / REMOVE / RENAME …   │
└───────────────────────────┬─────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────┐
│  内存权威层（Authoritative In-Memory State）               │
│  • Namespace：DirTable[parent_fid] → DirNode（Small/Large）│
│  • InodeTable[fid] → Inode（属性、parent、类型、计数）      │
│  • BlockMap[fid] → chunk 列表（可选独立模块/服务）          │
│  • （无服务端 PathCache；路径缓存仅在客户端，见 §4.3）       │
└───────────────────────────┬─────────────────────────────┘
                            │ 仅追加操作记录
┌───────────────────────────▼─────────────────────────────┐
│  持久化层                                                 │
│  • Edit Log（二进制 op，组提交 fsync）                     │
│  • 用户快照：Ref + COW + 目录 diff（§6.3）                 │
│  • Checkpoint/FSImage：一致性点 N + 后台遍历（§6.4）          │
│  • Quorum / VR 复制（复用现有 LogWriter/VR 基础设施）       │
└─────────────────────────────────────────────────────────┘
```

### 3.2 与 QFS 现状的核心差异

| 维度 | QFS 现状 | 本 RFC |
|------|----------|--------|
| 主索引 | 全局 B+ 树，dentry/fattr 不同 key | DirNode（Small hash / Large **抽取/适配 `kfstree` 节点算法的目录局部 B+ 树**）+ InodeTable |
| CREATE 索引操作 | 2× 全局 `insert` + 多次 `findLeaf` | 1× DirNode insert + 1× InodeTable insert |
| 百万级单目录 | 同全局树叶子链/同桶冲突风险 | Large 布局 O(log N)，首版必做 promotion |
| 持久化顺序 | 先 WAL committed，再 `handle()` | 临界区内写入 **pending 版本** + append log buffer；commit 后进入对外可见视图 |
| 用户快照 | （现 QFS 无同等机制） | `InodeRef` + 文件级 COW + 目录 diff，创建 O(1)（§6.3） |
| checkpoint | B+ 树页/节点序列化 | 模糊 FSImage（§6.4）+ replay txn>N |
| chunk 元数据 | 同树 `KFS_CHUNKINFO` | BlockMap 分离，allocate 时再写 |

---

## 4. 内存数据结构

### 4.1 Inode

```text
Inode {
  fid:          u64          // 全局唯一，单调分配（单机）
  type:         file | dir | symlink
  parent_fid:   u64          // 父目录；根目录哨兵
  mode, uid, gid, size, mtime, ctime, atime
  nlink, flags               // 见 §8.4：WORM、dumpster 子树、striping 等
  snapshottable: bool        // 目录可打快照（§6.3）
  snap_ref_count: u32        // 被用户快照持有的 frozen 引用数（§6.3.4）；live inode 常为 0
  replication | ec_policy    // 或仅指针，详细布局在 allocate 时设置
  dir_child_count            // 仅目录；用于 readdir 分页提示
  generation:   u64          // 每次 rename/unlink/rmdir/promotion 递增，供 cache 失效
}
```

存储：`InodeTable` 为 `fid → Inode`，数组分片或 `flat_hash_map` 分片。

### 4.2 目录索引 DirIndex（含大目录，首版必做）

**问题**：若每个目录仅用一个全局 `HashMap` 且冲突用链表串接，则单目录 **百万文件** 时会出现极端哈希桶长链，`lookup` / `create` / `readdir` 退化为 **O(N)**，成为新瓶颈。

**决策**：每个目录一个 **`DirNode`**，首版即实现 **Small（哈希）+ Large（有序索引）** 两种布局，并在子项数超过阈值时 **强制升格**（promotion），不是后续可选优化。

#### 4.2.1 公共类型

```text
NameKey  = (name_hash: u64, name: string)   // name_hash = Hsieh(name)<<4，与现 MetaDentry 一致
DirEntry = { child_fid, name }
DirNode  = {
  state:       SMALL | PROMOTING | LARGE   // 见 §4.2.6
  generation:  u64                        // promotion 完成后递增，失效 readdir cookie
  child_count: u64
  body:        SmallDir | LargeDir        // PROMOTING 期间读者只访问 Small
  staging:     LargeDir?                  // 仅 PROMOTING：构建完成前对读者不可见
}
```

全局：`DirTable[parent_fid] → DirNode`（按 `parent_fid` 分片锁，§7；晋升见 §4.2.6）。

#### 4.2.2 Small 布局（子项数 < `dir_large_threshold`）

- 结构：**开放寻址** `flat_hash_map<NameKey, fid>`（Robin Hood 或等价），**禁止**无限链表冲突链。
- **`lookup` / `create`**：均摊 **O(1)**；探测次数有硬上限 `max_probe`（如 16），插入时若接近满负荷或探测失败则 **触发升格** 而非继续堆链。
- 默认阈值 **`dir_large_threshold = 4096`**（可配置 `meta.dir.largeThreshold`）。

#### 4.2.3 Large 布局（子项数 ≥ 阈值，或 Small 无法安全插入）

**决策：Large 布局复用当前 QFS B+ 树的节点/Key/迭代算法**（`kfstree.h` / `kfstree.cc`），但实现上需要抽取或适配为 **目录局部 B+ 树组件**，而不是把现有 `Tree` 类原封不动实例化。现有 `Tree` 仍带有全局 namespace、checkpoint、dumpster、path cache 等语义；LargeDir 只需要其中的有序 dentry 索引能力。

| 复用组件 | 路径 / 说明 |
|----------|-------------|
| 内部节点 | `Node`（`NKEY=170`，4096B 页式节点，`findplace` 二分，`split` / `merge`） |
| 树操作 | 复用/抽取 `insert`、`del`、`lowerBound` / `findLeaf`、`LeafIter` 的节点算法 |
| 键 | 现有 `Key` / `PartialMatch`；叶键 **`Key(KFS_DENTRY, parent_fid, name_hash)`**，与现 `MetaDentry::keySelf()` 一致 |
| 叶记录 | `MetaDentry`（或薄封装 `DirBTreeLeaf` 内嵌相同字段）；`matchSelf` 比对 `name` |
| 内存 | `MetaNode::allocate` / `PoolAllocator`（与现 meta 节点相同） |

```text
LargeDir {
  parent_fid:  fid_t
  tree:        DirBTree      // 从 kfstree 节点算法抽取/适配，非全局 metatree 单例
}
```

- **语义**：逻辑上仍是「该目录下 name → child_fid」；物理上用 **一棵子树** 存该目录全部 `MetaDentry` 叶，**不再**插入全局 `metatree` 的混合 key 空间。
- **`lookup` / `create`**：对该目录的 `DirBTree` 调用与现 `getDentry` / `insert` 相同逻辑（`findLeaf` + 叶链 `peer()` 扫同名 hash），**O(log N)**，无百万长链。
- **`readdir`**：`LeafIter` 逻辑序遍历 + §5.4 **逻辑位置 cookie**（禁止裸指针）。
- **升格（promotion）**：原子性与并发语义见 **§4.2.6**（`PROMOTING` 状态、写阻塞、读仍用 Small、staging 完成后一次性切换）。
- **checkpoint / fsck**：Large 目录可复用现 Node/Meta 的 **记录编码思路**，但需新增 `section_dirs.large` 外层元数据（`parent_fid`、layout、generation、child_count、记录数/校验和），不能直接把全局 `metatree` checkpoint 流嵌入；fsck 见 §8.5（`PROMOTING` 视为 transient，持久化快照中不应出现）。

**不重复造轮子**：split/merge、节点页大小、Key 排序、LeafIter 语义应尽量沿用 `kfstree`；但需要把全局 `Tree` 的非目录职责剥离出去。

#### 4.2.4 复杂度与验收（百万级单目录）

| 操作 | Small | Large |
|------|-------|-------|
| lookup | O(1) 均摊，探测有界 | O(log N) |
| create | O(1) 均摊或触发 O(N) 一次性 promotion | O(log N) |
| readdir 一页 | O(page) 或扫描有界桶 | O(log N + page) |

**禁止**：单桶链表长度 ∝ N、百万次指针追逐的「伪 O(1) 哈希表」。

#### 4.2.5 与全局 `metatree` 的关系

| | 全局 `metatree`（现 QFS） | Large `DirNode`（本 RFC） |
|--|---------------------------|---------------------------|
| 代码 | `kfstree` | **同一套** `kfstree` |
| 实例 | 单例 `metatree`，混放 dentry/fattr/chunk | **每超大目录一个 `DirBTree`** |
| create 副作用 | 可能 split 共享祖先内部节点 | 仅影响该目录子树 |
| 小目录 | 也走全局树 | **Small `flat_hash`**，不进 B+ 树 |

InodeTable、BlockMap **不再**进入任何 B+ 树；仅 **超大目录的子项列表** 使用 `DirBTree` 存 `MetaDentry` 叶。

#### 4.2.6 晋升（Promotion）的原子性与可见性（已决）

**问题**：§4.2.3 若在「半建成」的 Large `DirBTree` 上并发 `lookup`/`create`，可能看到 **不完整** 的 B+ 树或 Small/Large 双写混乱。

**决策**：`DirNode` 增加 **`state`**；晋升在 **staging** 中构建 Large，通过 **一次性发布** 切换；晋升期间 **读走 Small、写阻塞或排队**。

##### 状态机

```text
SMALL ──(触发晋升)──► PROMOTING ──(发布完成)──► LARGE
                         │
                         └── 失败回滚 ──► SMALL（见下）
```

| `state` | 读者 (`lookup`/`readdir`) | 写者 (`create`/`unlink`/`rename` 子项) |
|---------|---------------------------|----------------------------------------|
| **SMALL** | `body.small` | 正常；可能触发进入 PROMOTING |
| **PROMOTING** | **仅** `body.small`（不读 `staging`） | **阻塞**于 `promote_cv` 或同目录写队列，直到 `LARGE` |
| **LARGE** | `body.large.tree` | 正常 `kfstree` 路径 |

##### 晋升算法（持有 `DirTable[parent]` 互斥或写锁）

```text
promote_small_to_large(parent_fid):
  lock(dir)   // 目录分片写锁；阻塞其它写者，读者见下

  1. assert(state == SMALL)
  2. state = PROMOTING
  3. staging.large = new DirBTree()      // 读者不可见
  4. for entry in body.small:            // 只读 Small，不改 Small
       staging.large.insert(MetaDentry(...))
  5. // 一次性发布（原子切换可见布局）
     body.large   = move(staging.large)
     staging      = null
     free(body.small)
     state        = LARGE
     generation++                         // 失效 readdir cookie / 客户端 path 缓存
  6. broadcast(promote_cv)                // 唤醒排队写者
  7. append EditLog(DIR_PROMOTE, parent_fid, generation)
  unlock(dir)
```

- **「原子」含义**：在步骤 5 之前，任何 RPC **不可能** 观察到 `staging` 或半填充的 `body.large`；步骤 5 之后，**不可能** 再观察到 `body.small`。
- 实现上可用 **同一把目录锁** 包裹步骤 2–6；步骤 5 的字段赋值顺序：`staging` 清空 → `body.large` 生效 → `state=LARGE` → 释放 `small`（避免读者看到 `LARGE` 但 body 仍为空）。

##### 并发 `lookup` / `create`（与 §7 分片锁配合）

| 操作 | `state == PROMOTING` 时行为 |
|------|---------------------------|
| **lookup** | 获取目录 **读锁**（或与写互斥的 `shared_lock`）：读 **`body.small` 快照**，与晋升线程不共享写；晋升 **不修改** Small，只读遍历。 |
| **readdir** | 同 lookup；cookie 若带旧 `generation`，晋升完成后返回 **失效**，客户端重试。 |
| **create** | 需目录 **写锁**：若 `PROMOTING`，**等待** 晋升完成（`promote_cv`），不得在半成品 Large 上 insert。 |
| **触发晋升的 create** | 当前线程持写锁执行 `promote_small_to_large`，完成后在同一锁内对 **Large** 执行 insert。 |

**不采用**：晋升过程中对活动 RPC 暴露「部分迁移」的 Large；不采用无 `PROMOTING` 标记、原地边建树边切换 `layout` 字段。

##### 晋升期间的读性能与写者饥饿（已决）

- **读者**：`PROMOTING` 期间仍持目录 **读锁** 访问 `body.small`，可与其它 `lookup`/`readdir` **并发**；晋升线程 **只读遍历** Small，不修改 Small。
- **风险**：触发晋升时 Small 可能已接近阈值（如 **数千～4096** 项），步骤 4 的 `insert` 循环耗时可 **阻塞同目录所有写者**（`create`/`unlink` 等等待 `promote_cv`），极端情况下造成 **写者饥饿**。
- **决策**：单次 `promote_small_to_large` 须有 **墙上时钟上限**（默认 **`meta.dir.promoteMaxWallMs = 1000`**，可配置）：
  - 在循环中 **分批** `insert`（如每批 256/512 项）并检查超时；
  - **未超时**：正常完成步骤 5–7；
  - **超时**：中止本轮晋升 → **回滚 SMALL**（§失败与恢复），返回 `-EBUSY` / 可重试错误；**不**半发布 Large；客户端/写路径 **退避重试** 或稍后由下一次 `create` 再次触发。
- **观测**：对 `promote_wall_ms`、`promote_aborted_timeout` 打点；P2 验收：4096 项目录晋升 p99 墙钟 **≤ 配置上限**。

##### 失败与恢复

- 若步骤 4 失败：`state` 回滚 **SMALL**，丢弃 `staging`，`generation` 不变，唤醒等待者并返回错误。
- Edit log 仅在 **成功** 步骤 7 记录 `DIR_PROMOTE`；replay 时目录应已为 **LARGE**（或从 snapshot 还原 layout 字段）。
- §6.4 Checkpoint 扫描时：若发现 `PROMOTING`（崩溃中间态），按 **SMALL** 序列化并打标需 **重做 promotion** 或 fsck 修复（运维策略，首版可 panic 要求重放 log 修复）。

##### 与 §5 热路径的衔接

```text
DirTable[parent].lookup(name):
  lock_shared(dir)
  switch (state):
    SMALL | PROMOTING → return body.small.find(name)
    LARGE             → return body.large.tree.lookup(...)

DirTable[parent].insert(name, child_fid):
  lock_exclusive(dir)
  while (state == PROMOTING) wait(promote_cv)
  if state == SMALL && need_promote(): promote_small_to_large()  // 仍持写锁
  ... insert into active body ...
```

### 4.3 路径缓存（已决：仅客户端）

**决策：不在 MetaServer 集群内维护、复制或共享 PathCache**（对比现 QFS 可选的 `metaServer.enablePathToFidCache`，本设计 **不** 在服务端做路径→fid 缓存）。

| 侧 | 职责 |
|----|------|
| **客户端** | 维护 `path → fid`、`parent_fid` 等缓存；热路径用 **fid + name** 发 RPC，避免 `LOOKUP_PATH`。 |
| **服务端** | 不存 PathCache；`lookup` / `LOOKUP_PATH` 每次按 `DirIndex` 解析。通过 RPC 响应携带 **`generation`**（目录或 inode 上的单调版本），供客户端判断缓存是否失效。 |

**目录 `generation`**（§4.1 Inode）：在 `rename` / `rmdir` / `unlink` / 子树变更时递增；客户端比对 `(path, cached_fid, cached_generation)`，不一致则丢弃该路径缓存项并重新解析。

**失效规则（客户端本地）**：

- 单文件 `remove` / `rename`：失效该路径及已知子路径前缀（若有目录缓存树）。
- 目录 `rmdir`：失效以该路径为前缀的全部缓存项。
- 收到别客户端 mutating 成功且本地无 generation 时：可保守失效父目录缓存，或依赖后续 `LOOKUP` 失败再刷新。

**不采用**：MetaServer 间复制 path cache、standby 只读副本提供缓存命中、或全局 `PathToFidCacheMap`（避免一致性、失效广播与内存占用问题）。

### 4.4 BlockMap（与 namespace 分离）

```text
BlockMap : 按 fid 分片
  fid → [ ChunkInfo { chunk_id, offset, version, locations, tier } ]
```

- **`create` 不写入 BlockMap**（与 HDFS 一致）。
- **`allocate` / `append`** 才追加 chunk 记录；edit log 使用独立 op 类型。

---

## 5. 热路径算法

### 5.1 CREATE（空文件）

**前置**：客户端提供 `parent_fid` + `name`（已有 QFS `MetaCreate::dir`）。

```text
1. shard = hash(parent_fid) % N_SHARDS
2. lock(DirShard[shard])
3.   if DirIndex[parent].contains(name) → 处理 exclusive / truncate 语义
4.   txn_id = EditLog.reserve_txn()
5.   new_fid = FidAllocator.next()
6.   DirTable[parent].insert_pending(name, new_fid, create_txn=txn_id)   // §4.2，必要时 promotion
7.   InodeTable[new_fid] = Inode{ parent, attrs..., create_txn=txn_id, delete_txn=none, pending=true }
8.   update parent pending mtime / child_count version
9.   EditLog.append_buffer(txn_id, CREATE, parent, name, new_fid, attrs, op_id)
10. unlock
11. if sync_policy == always: wait(txn_id.committed)
12. return { new_fid, txn_id }
```

**树操作次数**：0。持久化：1 条 edit（组提交时与其他 op 共享一次 fsync）。

**可见性要求**：步骤 6–9 修改的是 **pending 版本**。普通 `LOOKUP`/`READDIR` 只暴露 `txn_id <= committed_txn_id` 的版本；同一客户端是否可读到自己的 pending create 由会话级 read-your-writes 选项单独定义，默认不向其它客户端暴露未提交 txn。

### 5.2 LOOKUP（单级）

```text
lock(DirShard[hash(parent)])
  entry = DirTable[parent].lookup_committed(name, committed_txn_id)
  if entry == null: return ENOENT
  fa = InodeTable[entry.fid]
  if fa.create_txn > committed_txn_id: return ENOENT
  if fa.delete_txn != none and fa.delete_txn <= committed_txn_id: return ENOENT
unlock
→ 权限检查
```

### 5.3 LOOKUP_PATH

**服务端**无 PathCache：按 `/` 分段，**每段一次 DirIndex 查找**，最后一段做 access check。  
**客户端**应先查本地 path 缓存（§4.3）；未命中再发 `LOOKUP_PATH` 或分段 `LOOKUP`（持 `parent_fid`）。

### 5.4 READDIR

```text
readdir(parent, cookie, max_entries) → 分页返回 DirEntry 列表
```

| `DirNode.state` | 遍历方式 | cookie 概要 |
|-----------------|----------|-------------|
| **SMALL** | 按 `NameKey` 逻辑序（Small 有界，必要时临时排序） | 逻辑 key 游标（§5.4.1） |
| **PROMOTING** | 仍按 Small 的 `NameKey` 逻辑序 | 同 SMALL；promotion 完成后 `generation++` |
| **LARGE** | B+ 树 key 序（`kfstree`） | **逻辑 key 游标**，禁止节点指针 |

- 每次 RPC 仅返回 **≤ max_entries**（默认上限如 1024，可配置）。
- 禁止：一次 RPC 返回百万项；禁止 Large 布局下无序全表扫描。

#### 5.4.1 Readdir Cookie 鲁棒性（已决）

**问题**：若 Large 布局 cookie 编码 **`LeafIter` 内部物理状态**（`Node*`、叶内下标），则两次 `readdir` 之间对该目录的 **`insert`/`del` 导致 B+ 树 split/merge** 后，cookie 可能 **失效或指错位置**（重复、漏项）。目录已为 **LARGE** 时不会发生 promotion，但 **树重平衡仍会发生**。

**决策**：cookie 表示 **逻辑遍历位置**，不绑定可变物理指针；对齐 HDFS「续传令牌 = 逻辑名 / 有序 key」思路。

##### 硬性规则

| 规则 | 说明 |
|------|------|
| **禁止** | cookie 中序列化 `Node*`、堆地址、`LeafIter` 内存指针 |
| **必须** | 可由 `(parent_fid, generation, resume_key)` 在当前树上 **重新定位** |
| **`generation` 不匹配** | 返回 `EINVAL` / 空 cookie 重启；客户端全量重扫该目录 |

##### SMALL / PROMOTING

```text
CookieSmall = {
  generation:   u64
  layout:       SMALL | PROMOTING
  last_key:     NameKey?  // 上一页最后一条；空表示从头
}
```

- Small 不把开放寻址的 `bucket_id`/`slot` 暴露给 cookie；rehash、删除后的 tombstone 清理、Robin Hood 位移都会改变物理桶位置。
- `readdir` 对 Small 使用 `NameKey` 逻辑序重定位；Small 有阈值上限（默认 4096），可在每页临时收集并排序，或维护有序 side index。
- Small 上任意 `create`/`delete`/rehash 必须 `generation++`，旧 cookie 返回 `EINVAL` 并要求客户端重扫；promotion 完成同样 `generation++`，旧 Small cookie 作废。

##### LARGE（推荐：逻辑 key 游标）

**首选**（实现简单、对 split/merge 最稳）：

```text
CookieLarge = {
  generation:   u64
  layout:       LARGE
  last_key:     NameKey      // 上一页最后一条的完整排序键 (name_hash, name)
}
// 字段名 after_hash/after_name 仅作实现别名，语义上必须是 NameKey 二元组
```

- **排序键**：与 §4.2.1 `NameKey` 一致；`kfstree` 叶序为 **先 `name_hash` 再 `name` 字典序**（同 `MetaDentry::matchSelf`）。单目录内 **不可能** 存在两个相同 `name`，但续扫仍须用 **`(hash, name)` 对**，不能仅用 `name`（不同 hash 桶下仅比 name 会错位）。
- **禁止**：cookie 仅编码 `name` 字符串而省略 `name_hash`。

恢复算法：

```text
readdir_resume(parent, cookie):
  if cookie.generation != DirNode.generation: INVALID
  it = lowerBound(tree, Key(KFS_DENTRY, parent, cookie.last_key.name_hash))
  skip entries where NameKey(hash,name) <= cookie.last_key lexicographically
  return next max_entries from it (LeafIter 仅作实现手段，不写入 cookie)
```

- B+ 树 **split/merge 不改变 key 的全序**；只要条目未被删除，续扫位置仍正确。
- **并发 insert**：新名可能插在已扫过区间之前 → 客户端可能漏扫；与 HDFS 一致，**不保证** 遍历期间快照隔离；强一致列举需 **`generation` 冻结** 或 copy-on-read（**非首版**）。
- **并发 delete**：已返回的名字可能已不存在；续扫 `lowerBound` 自然跳过。

**可选**（与 HDFS 部分实现类似，需稳定叶 id）：

```text
CookieLargeAlt = { generation, leaf_node_id, index_in_leaf }
```

- `leaf_node_id` 为 **分配的稳定叶标识**（split 时子叶继承/拆分规则须在 RFC 实现细则中定义），**不是**运行时指针。
- 恢复时若 `leaf_node_id` 已合并/分裂：**从该 id 映射节点的最小 key**，或 **`lowerBound(该 key)` 的下一个有效叶** 继续，**宁可少量重复不可漏**（与建议一致）。
- 首版 **优先 `last_key`（NameKey）游标**；`leaf_node_id` 方案可在性能优化阶段引入。

##### 与 promotion / mutation 的交互

| 事件 | cookie 行为 |
|------|-------------|
| **promotion 完成** | `generation++`；Small cookie **失效**；客户端用空 cookie 对 Large 重扫 |
| **Small 上 create/delete/rehash** | `generation++`；Small cookie **失效**，避免开放寻址物理位置变化造成漏扫/重复 |
| **rename/unlink/rmdir（目录）** | `generation++`；所有 cookie 失效 |
| **Large 上 create/delete** | `generation` 可不变；**`last_key` cookie 仍有效**（靠 `NameKey` 重定位）；若产品要求列举快照视图，另议 |
| **返回 `-EBADF`/`EINVAL`** | 客户端 **丢弃 cookie，从空重新开始** |

##### RPC 响应

- 每页返回：`entries[]`、`more_entries`、`next_cookie`（编码上述结构，版本号 `cookie_ver=1`）。
- 不把 `LeafIter` 状态暴露给客户端。

**验收（P2）**：在 Large 目录连续 `readdir` 分页过程中注入随机 `insert`/`del`，验证无指针 cookie 时 **无崩溃、无无限循环**；允许与 HDFS 相同的「并发修改下不保证严格快照列举」语义。

---

## 6. 持久化设计

### 6.1 Edit Log 记录格式（概念）

采用 **定长头 + 变长 payload** 的二进制编码（避免 QFS 部分文本 token 解析开销）：

```text
Record {
  magic, version
  txn_id:      u64      // 单调
  op:          u16      // CREATE=1, REMOVE=2, MKDIR=3, ...
  op_id:       u128     // 幂等键 (client_id, seq)
  payload:     op-specific
}
checksum per block / per record
```

**CREATE payload 示例字段**：`parent_fid, name, new_fid, mode, uid, gid, replication, ...`

### 6.2 组提交（Group Commit）

| 模式 | 行为 |
|------|------|
| `batch`（默认） | 每 `commit_interval_ms` 或 `commit_batch_bytes` 一次 `fdatasync` |
| `always` | 每个 txn 等待 fsync（兼容强一致测试） |
| `none` | 仅写 page cache，崩溃可能丢最近操作（需明确禁用场景） |

**Log 线程模型**：单写者 append + fsync；namespace 分片锁与 log 锁分离，缩短临界区。

### 6.3 用户快照（已决：HDFS 式引用 + 文件级 COW + 目录 Diff）

**放置说明**：本节描述 **Snapshottable 目录上的用户可见快照**（类比 HDFS `createSnapshot`），与 §6.4 **周期性 Checkpoint/FSImage**（NN 冷备）分工不同。实现可落在 **P3/P3.1**（§9）。

**决策：创建快照采用 HDFS 核心思路——引用（Rename/Reference）而非复制；文件内容修改采用文件级写时复制（COW）；目录项变化必须记录目录 diff。** 不采用对整棵树做全量内存扫描来「创建」用户快照（该做法保留给 §6.4 Checkpoint）。

#### 6.3.1 核心机制（对齐 HDFS）

| HDFS 概念 | 本 RFC 映射 |
|-----------|-------------|
| `INodeReference` | **`InodeRef`**：快照目录上的一个轻量引用，指向某 **`fid`（目录或文件根）** 在 `committed_txn_id = N` 时的逻辑视图 |
| 创建快照 O(1) | 在 snapshottable 目录 `D` 上新增 `Snapshot{s_id}` → 仅增加 **Ref → D 的 inode/目录状态**，**不**复制百万子项 |
| 读快照 | 沿 Ref 解析路径；**无锁读**（读快照侧为只读视图） |
| 首次修改被快照覆盖的文件 | **文件级 COW**：保留旧 `Inode`+`BlockMap` 给快照；活动命名空间新建 `Inode`（新 fid 或新 inode 行）并更新 **该名字** 在 `DirTable` 中的映射 |
| 修改目录下其他未触碰文件 | **零开销**（Ref 仍指向原 inode；活动 DirTable 不变） |

```text
allowSnapshot(dir_fid)     // 标记目录可打快照（类比 snapshottable）
createSnapshot(dir, name)  // 例如 /foo → s1
  → SnapshotRecord { id, parent_snap, root_ref → inode@N }   // O(1)，无子树复制

// 用户 delete / truncate / 覆盖写 / rename 活动树中的 file1，且 file1 在 s1 覆盖下：
mutate(file1):
  if inode_snapshotted(file1):
    cow_inode(file1):
      frozen = clone_inode_shallow(file1)   // 快照保留
      live   = new_inode_for_mutation()     // 活动命名空间
      DirTable[parent].replace_name(file1 → live)
      append EditLog(COW_SPLIT, ...)
  else:
    normal_mutate(file1)
```

- **目录级百万文件**：创建 `s1` **不遍历** `DirTable`；仅在被修改的单个文件上支付 COW（约一次 create + 后续 write 的元数据开销）。
- **Large 目录**：COW 只 **`replace_name` 一条 DirEntry**（Small 或 `DirBTree` 单键更新），不重扫整棵 per-dir `DirBTree`。

#### 6.3.1.1 目录 Diff（必需）

仅有 `InodeRef + 文件级 COW` **不足以**提供用户快照的时间点语义：快照创建后，live 目录里的 `create`、`unlink`、`rename` 若直接修改 `DirTable`，快照读会跟着变化。首版快照必须同时实现 **目录级 diff**（对齐 HDFS snapshot diff 思路），记录快照创建点之后每个 snapshottable 子树内的目录项变化。

```text
DirSnapshotDiff {
  dir_fid, snap_id, base_txn
  created:  set<NameKey>                 // 快照之后新建，快照视图不可见
  deleted:  map<NameKey, frozen_fid>      // 快照之后删除/rename out，快照视图仍可见
  renamed:  optional oldName -> newName   // 可展开为 deleted+created
}
```

规则：

- `createSnapshot(D)`：只创建根 `SnapshotRecord`，不遍历百万子项；目录 diff 延迟到后续 mutation 时按需创建。
- `create(parent, name)`：若 parent 被某个活跃快照覆盖，在对应 `DirSnapshotDiff.created` 记录 `name`，使该快照视图过滤掉新名字。
- `unlink/rename out(parent, name)`：若被快照覆盖，先冻结当前 `child_fid`（文件按 §6.3.4；目录需冻结目录引用和后续 diff 链），在 `deleted[name]` 记录 frozen 引用，快照视图继续返回旧条目。
- `rename across dirs`：按源目录 `deleted` + 目标目录 `created` 处理；必须与 §7.3 锁顺序一致。
- `readdir(snapshot)`：以 live DirIndex 为基底叠加 diff：过滤 `created`，补回 `deleted`，并按 `NameKey` 逻辑序输出；Large 目录仍使用 `DirBTree` lowerBound，再 merge diff 项。

没有目录 diff 时，§6.3 的用户快照只能算 inode 引用缓存，不能作为可恢复的目录快照交付。

#### 6.3.2 性能预期（与 HDFS 对照）

| 场景 | 性能 | 原因 |
|------|------|------|
| 读活动/读快照文件 | 快照读无额外锁；活动读与无快照相同 | Ref 只读解析 |
| 创建快照 | **O(1)** | 仅新增 `SnapshotRecord` / `InodeRef`，不遍历子树 |
| 删除快照 | O(本快照登记的 frozen/diff 项) | 释放 `cow_inodes`、`dir_diffs.deleted` 与倒排索引引用 |
| 首次修改快照覆盖下的文件 | 有开销（COW 一个 inode） | 与被修改文件数成正比，与目录总规模无关 |
| 再次修改已 COW 过的活动文件 | 与无快照相同 | 已操作活动侧新 inode |

#### 6.3.3 数据结构（与 §4.1 衔接）

```text
SnapshotRecord {
  snap_id, name, root_dir_fid, txn_id_at_create: N
  root_ref: InodeRef              // O(1) 创建：指向 snapshottable 根目录 inode
  cow_inodes:  set<fid_t>         // 本快照引用的 frozen fid，便于 delete 时递减
  dir_diffs:   map<dir_fid, DirSnapshotDiff> // 本快照目录项变化
}

InodeRef { target_fid, txn_id_cap }
```

- **`snap_ref_count`** 定义在 §4.1 `Inode` 上：表示有多少 **独立快照引用** 仍依赖该 **inode 对象**（通常为 COW 后的 **frozen** 副本；活动/live inode 在分裂后一般为 0）。目录项时间点语义由 §6.3.1.1 的 `DirSnapshotDiff` 维护，inode 引用计数只解决 frozen inode 生命周期。
- Edit log：`SNAPSHOT_CREATE`、`SNAPSHOT_DELETE`、`INODE_COW_SPLIT`（含 `frozen_fid`、`live_fid`、`snap_ref_delta`）、`DIR_SNAPSHOT_DIFF_UPDATE`，供 standby **确定性 replay**。

#### 6.3.4 Frozen inode 引用计数（已决）

**问题**：§6.3 删除快照时「仅回收本快照专属的 frozen inode」。若同一 frozen inode 被 **多个快照** 引用（例如 `/foo` 上连续创建 `s1`、`s2` 后才首次修改 `file1`），**不能在 `snap_ref_count > 0` 时释放**。

**决策**：在 `Inode` 上维护 **`snap_ref_count`**；在 **COW 分裂** 与 **删除快照** 时严格增减；减到 **0** 才可回收该 frozen inode（及对应 `BlockMap`）。

##### 何时增减（与 HDFS 文件级 COW 对齐）

| 事件 | `snap_ref_count` | 说明 |
|------|------------------|------|
| **`createSnapshot`** | 根目录 `root_ref.target` **+1**（可选） | 创建本身 O(1)；**不**遍历子树给每个文件 +1。未 COW 的文件仍与 live 共用同一 `fid`，读快照走解析路径。 |
| **首次 `COW_SPLIT`（file1）** | 对 **frozen_fid**（旧 inode 副本）设为 **覆盖该文件的所有活跃快照数** `K` | 例：存在 `s1`、`s2` 均可见 `file1` 时尚未修改 → `frozen.snap_ref_count = 2`。活动侧新 `live_fid`：`snap_ref_count = 0`。 |
| **再建快照 `s3`（已有 frozen file1）** | 若 `s3` 仍指向含 `file1` 的视图且 `file1` 已 frozen：对 `frozen_fid` **+1** | 仅影响 **已分裂** 的 frozen 对象；仍与 live 共用的路径在首次 COW 时一次性结算。 |
| **`deleteSnapshot(s)`** | 对该快照登记过的每个 `frozen_fid`：**-1** | 来自 `cow_inodes`、`dir_diffs.deleted` 或快照元数据索引；**仅当减到 0** 时 `free_inode(frozen_fid)` + 释放 BlockMap |
| **活动路径修改 live inode** | 不增减 | live 与快照引用解耦 |

```text
cow_split(file_fid, parent, name):
  frozen_fid = retain_or_clone_inode(file_fid)   // 旧版本留给快照
  live_fid   = allocate_new_inode(...)
  frozen.snap_ref_count = snapshot_ref_index.count(frozen_fid)   // 见下，禁止仅靠运行时全表扫描
  DirTable[parent].replace_name(name, live_fid)
  for each snap in snapshot_ref_index.ref_snapshots(frozen_fid):
    snap.cow_inodes.insert(frozen_fid)
  append EditLog(INODE_COW_SPLIT, frozen_fid, live_fid, snap_ref_count, ...)

deleteSnapshot(snap_id):
  for fid in snap.cow_inodes:
    if snapshot_ref_index.remove(fid, snap_id) and --InodeTable[fid].snap_ref_count == 0
      free_inode_and_blockmap(fid)
  for diff in snap.dir_diffs:
    for fid in diff.deleted.values:
      if snapshot_ref_index.remove(fid, snap_id) and --InodeTable[fid].snap_ref_count == 0
        free_inode_and_blockmap(fid)
  release snap.dir_diffs
  remove SnapshotRecord
  append EditLog(SNAPSHOT_DELETE, snap_id, ...)
```

##### 回收规则（「仅属于该快照」的精确定义）

- **可回收**：`snap_ref_count` 在 `deleteSnapshot` 后变为 **0** 的 inode（表示 **没有任何** 快照再引用该 frozen 版本）。
- **不可回收**：`snap_ref_count > 0`——即使本次删除的 `s_i` 不再引用，只要还有 `s_j` 引用同一 frozen 副本，就必须保留。
- **活动 inode**：`snap_ref_count == 0` 为常态；删除快照 **永不** 直接 `free` 当前 live `fid`（除非该 `fid` 本身也是某次 COW 的 frozen 且计数归零）。

##### 正确性验证（实现必须覆盖）

| 检查点 | 要求 |
|--------|------|
| **无双重释放** | 仅当 `snap_ref_count == 0` 入 free 队列；delete/replay 幂等 |
| **无泄漏** | 删除最后一个持有引用快照后，frozen 必入 free；fsck 扫描 `snap_ref_count==0` 且 unreachable |
| **Replay** | `INODE_COW_SPLIT` / `SNAPSHOT_DELETE` 重放后计数与主路径一致 |
| **并发** | COW 与 `deleteSnapshot` 在同 `fid` 或 snap 元数据锁下串行化计数更新 |
| **循环引用** | 命名空间为 **DAG**（父指针单父目录）；`InodeRef` 仅 **快照元数据 → inode**，inode **不** 指回 `SnapshotRecord`，图 **无环**。无需通用循环引用检测，但需在 code review / 单元测试中 **断言** 不建立 inode→snapshot 反向边 |

##### fsck（§8.5 扩展）

- 对每个 `snap_ref_count > 0` 的 inode：存在至少一条 `SnapshotRecord` / `cow_inodes` / `dir_diffs.deleted` / `SnapshotRefIndex` 反向引用。
- 对每个 `SnapshotRecord.cow_inodes` 中的 `fid`：`snap_ref_count >= 1`。
- 对每个 `DirSnapshotDiff.deleted` 中的 `frozen_fid`：inode 存在，`snap_ref_count >= 1`，且 `SnapshotRefIndex[frozen_fid]` 包含该 `snap_id`。
- `snap_ref_count == |SnapshotRefIndex[fid]|`；允许再与所有 `cow_inodes`、`dir_diffs.deleted` 的并集交叉校验。
- 删除快照后的 spot check：`cow_inodes` / `dir_diffs.deleted` 中不应出现已 free 的 `fid`。

##### `count_snapshots_covering` 与倒排索引（已决）

**问题**：`frozen.snap_ref_count = count_snapshots_covering(...)` 若在 COW 时 **扫描全部 SnapshotRecord** 或沿路径动态枚举，易错且 O(快照数)；多快照引用同一 frozen inode 时 **跨快照累计** 必须精确。

**决策**（二选一，首版至少实现其一）：

| 方案 | 做法 |
|------|------|
| **A. 倒排索引（推荐）** | 维护 `SnapshotRefIndex: frozen_fid → { snap_id... }`（及可选 `(parent,name) → frozen_fid`）。`createSnapshot`：对仍与 live 共用的路径 **不** 预遍历；**COW / 目录 diff 产生 frozen 引用时** 将 `frozen_fid` 登记到 **当前所有覆盖该 `(parent,name)` 的活跃快照**（由 snap 链/目录 Ref 解析一次，写入索引）。`deleteSnapshot`：对 `cow_inodes` 与 `dir_diffs.deleted` 中每个 `fid` 从索引移除 `snap_id`，再 `--snap_ref_count`。 |
| **B. 快照创建时预计算** | 在 `createSnapshot` O(1) 元数据之外，记录「该快照可见的 (parent,name)→fid 视图版本」；首次 COW 时用 **快照差分元数据** 得到 `K`，写入 `snap_ref_count` 与 `cow_inodes`。 |

- **禁止**：`deleteSnapshot` 或 replay 时依赖 **未持久化的** 临时扫描结果且与主路径不一致。
- **再建快照 `s3`（file1 已 frozen）**：`SnapshotRefIndex` 对 `frozen_fid` **insert(s3)** 并 `snap_ref_count++`（与上表「再建快照」行一致）。
- **fsck**：`snap_ref_count == |SnapshotRefIndex[fid]|`（允许索引与 `cow_inodes`、`dir_diffs.deleted` 并集交叉校验）。

**成熟度说明**：引用计数为业界成熟手段，但须在 **COW 初值 / 目录 diff frozen 引用 / 多快照叠加 / delete + replay / 索引一致性** 上做 **专项测试**，列入 **P3.1 验收**。

#### 6.3.5 与 §6.4 Checkpoint 的边界

| | §6.3 用户快照 | §6.4 Checkpoint/FSImage |
|--|----------------|-------------------------|
| 目的 | 时间点恢复、误删回滚、对比历史 | MetaServer **重启/冷备**、缩短 replay |
| 创建成本 | **O(1)** per snap | O(namespace) 后台扫描（可模糊） |
| 读路径 | 快照视图 | 正常命名空间 |
| 存储 | 内存 Ref + 目录 diff + 被 COW 分离的 inode | 磁盘 FSImage 文件 |

两者可同时存在：HDFS 亦区分 **Snapshot** 与 **Checkpoint（FSImage）**。

#### 6.3.6 未采纳为用户快照的方案

| 方案 | 结论 |
|------|------|
| 一致性点 + 全量遍历生成用户快照 | **否**；移至 §6.4，仅用于 Checkpoint |
| 全局 freeze 瞬时快照 | 阻塞写，不利于 CREATE 目标 |
| 目录级深拷贝百万子项 | O(N) 创建，不可接受 |

### 6.4 Checkpoint / FSImage（已决：一致性点 + 后台遍历）

**用途**：周期性 **MetaServer 冷备与启动加速**（类比 HDFS FSImage + edits），**不是** §6.3 的用户快照。

**决策**：采用 **一致性点 `N = committed_txn_id` + 后台遍历** 写出模糊 FSImage；恢复时 `load FSImage(N)` + `replay(txn_id > N)`。与 §6.3 HDFS 式快照 **正交**。

#### 6.4.1 流程

```text
triggerCheckpoint()   // 周期或 MetaCheckpoint RPC
  ├─ 记录 LAST_TXN_ID = committed_txn_id  （N）
  ├─ 后台线程遍历 InodeTable、DirTable（§4）、BlockMap（可选）
  │     允许与写并发，但只序列化 txn_id <= N 的 committed 视图
  │     忽略 pending txn>N；对 delete_txn>N 的旧版本仍按 N 时刻保留
  ├─ 写出 FSImage + footer(N)；每个 section 带 section checksum 和 max_txn_seen<=N
  └─ 原子 publish

冷启动：load FSImage(N) → replay Edit Log (txn_id > N) → 一致
```

正确性：依赖 §6.7 的版本化可见性边界。Checkpoint 可以与写并发，但 **不能**把 txn>N 的新 dentry/inode 写入 FSImage(N)，否则冷启动 replay(txn>N) 会重复 create、复活已删除对象或双加计数。实现必须在扫描时按 `create_txn <= N < delete_txn` 过滤，或在 FSImage 记录中携带版本并在 load 阶段过滤。

#### 6.4.2 FSImage 内容与 Large 目录

- `section_inodes`、`section_dirs`（Small 逻辑项或 Large `DirBTree` 记录流），记录必须带可过滤的 create/delete txn 或保证已经按 N 过滤。
- `section_snapshots`：只写 `create_txn <= N` 且未在 N 前删除的 `SnapshotRecord`，包括 `cow_inodes`、`dir_diffs` 与可重建 `SnapshotRefIndex` 的记录；`DIR_SNAPSHOT_DIFF_UPDATE` 中 txn>N 的变化不得进入 FSImage(N)。
- `section_blockmap`（可选）：若写入，则与 inode 一样按版本过滤，避免 replay 后重复块引用计数。
- log 截断：**可选**运维操作，非恢复前提。

#### 6.4.3 代价

快照扫描慢 → `txn_id > N` 的 log 段变长 → **重启 replay 变长**；需控制 checkpoint 周期（配置 `meta.checkpoint.interval` 等）。

#### 6.4.4 扫描期内存压力（已决）

**问题**：后台遍历 `InodeTable`、`DirTable`（含 Large 目录 `kfstree` 流式导出）、`BlockMap` 时，若 **每 inode/每目录项分配独立序列化 buffer**，峰值内存可与 **瞬时分配速率 × 对象数** 成正比，挤压热路径 RSS。

**决策**：

| 措施 | 说明 |
|------|------|
| **Buffer 池** | 后台线程 **复用** 固定大小写缓冲（如 1–4 MiB），`section_*` 写满再 flush 到 FSImage 文件，避免 per-object `malloc` |
| **扫描节流** | `meta.checkpoint.maxEntriesPerTick` / `maxBytesPerTick` 限制每时间片处理条数；`yield` 或短 sleep，避免与 mutating 抢满 CPU |
| **Large 目录** | 按 `LeafIter` **流式** 写出 checkpoint 记录，**禁止** 先将百万 `MetaDentry` 载入单一 `vector` |
| **背压** | 若 FSImage 写盘慢于扫描，队列深度有界；超限则 **拉长 checkpoint 周期** 而非无界堆内存 |
| **可观测** | `checkpoint_scan_rss_delta`、`checkpoint_buffer_pool_bytes` 指标；压测：全量 namespace 扫描期间 CREATE p99 退化 **≤ 约定比例**（如 20%，P3 验收） |

### 6.5 与 QFS LogWriter / VR 的关系

- **可复用**：quorum 复制、block 切分、primary lease、`MetaVrLogSeq` 序语义。
- **需替换**：`WriteLog` 序列化内容与 replay 解析器（`Replay.cc` / `replay_create` 文本格式 → 二进制 op）。
- **不再依赖**：`metatree.insert` 作为 redo 单元；redo 单元为 **edit op**。

### 6.6 内存修改与 log 的顺序（相对 QFS 的关键改进）

**提议默认顺序**：

```text
reserve txn_id
（分片锁内）写 pending 版本 → append 到 log 内存 buffer → 释放锁
（log 线程）buffer → 复制 → fsync → 推进 committed_txn_id
（发布阶段）txn_id <= committed_txn_id 的 pending 版本进入 committed 视图
```

对比 QFS：**先 log committed 再 `handle()`**，客户端等待包含「空窗期」内无法从内存读到结果的双重延迟。本 RFC 将内存修改拆成 **pending 版本** 与 **committed 视图**：写路径可以先构造 pending 状态并排队 fsync，但普通读路径只能读 committed 视图。发起方在 RPC 成功后的可见范围与 **lease / sync 策略** 对齐 HDFS 习惯，而非「未提交 txn 全网可见」。

### 6.7 读一致性（已决）

**决策：采用 (c) 跟随 HDFS 风格的 lease + 已提交命名空间模型**，并与 QFS 现有 **primary / VR / chunk lease** 语义衔接（`LEASE_ACQUIRE`、`LEASE_RENEW` 等，见 `MetaRequest`）。

| 场景 | 规则 |
|------|------|
| **命名空间变更**（CREATE / REMOVE / RENAME …） | 对其他客户端：仅在 edit **已 committed**（`committed_txn_id` 推进、quorum 复制完成）后可见；primary 内存中的 pending 版本 **不**进入普通读视图。 |
| **RPC 返回与 durable** | `sync=always`：成功返回 ≡ 命名空间变更已 durable，他客户端可见（在 primary 正常服务前提下）。`sync=batch`：返回表示 **已接受并分配 txn/fid**；他客户端可见时点不早于本批 **组提交 fsync**。若需要 read-your-writes，必须用会话 token 或等待 txn committed。 |
| **文件数据读写** | 命名空间登记（create 得 fid）与 **写数据** 分离；已打开文件的读写一致性由 **chunk lease** 保证写者独占/租约续期，读者看到已提交块版本，与 HDFS 「NN 管名字、DN 管块 + lease」分工一致。 |
| **Primary / standby** | 仅 primary 执行 namespace 变更并写 edit；standby 通过 log replay 追赶；客户端 mutating 与强一致命名空间读面向 primary（与现 VR 一致）。 |

**不采用**：

- **(a) 仅 primary 本地可见未提交变更**：不足以定义多客户端语义，且与 backup 复制模型冲突。
- **(b) 未提交 txn 全网可见**：破坏恢复与 fsck 假设，并引入跨客户端脏读。

**实现提示**：DirEntry/Inode 需要携带 `create_txn`、`delete_txn`（或等价版本区间）与 pending 标志；`lookup` / `readdir` 只暴露 `create_txn <= committed_txn_id < delete_txn` 的视图。commit 发布可以批量翻转 pending，也可以只推进全局 `committed_txn_id` 并在读路径过滤。写路径 lease 逻辑复用现有 QFS 实现，本层不新增第二套租约协议。

---

## 7. 并发模型

### 7.1 锁层次

| 资源 | 锁粒度 |
|------|--------|
| `DirIndex` | `hash(parent_fid) % N` 分片锁：**读锁**（lookup/readdir，`PROMOTING` 仍读 Small）；**写锁**（create/promotion，写者等待 `PROMOTING` 结束） |
| `InodeTable` | `hash(fid) % M` 分片；读多写少用 RW lock |
| `FidAllocator` | 无锁原子或独立 mutex |
| `EditLog buffer` | 单写者 + MPSC 队列 |
| 客户端 `PathCache` | 客户端本地缓存，不在 MetaServer 锁层次内 |

**禁止**：所有 mutating RPC 共用一个 `submit_request` 全局 mutex（现状瓶颈）。

### 7.2 与 B+ 树分片锁的区别

对 **全局 `metatree`（单例 B+ 树）**，「按 parent 加锁」**不安全**（不同目录可能 split 同一内部节点，见 `MetaTree-Lock-Optimization.md`）。  
对 **DirTable 分片**：按 `parent_fid` 加锁 **安全**——Small 为独立 `flat_hash`；Large 为 **该目录专属 `DirBTree` 实例**（抽取/适配 `kfstree` 节点算法，但不与别目录共享内部节点）。

### 7.3 跨目录操作锁顺序（已决）

`rename`、dumpster move、快照 COW / 目录 diff 更新会同时触碰多个目录、inode、BlockMap 与快照元数据，必须使用全局确定性锁顺序，禁止按调用路径临时加锁。

**锁顺序**：

```text
1. SnapshotRegistry / SnapshotRefIndex 元数据锁（仅快照相关操作）
2. DirShard locks，按 (shard_id, parent_fid) 升序；同一目录只加一次
3. InodeTable locks，按 fid 升序
4. BlockMap locks，按 fid 升序
5. EditLog append buffer（只追加内存 buffer，不在锁内等待 fsync）
```

规则：

- `RENAME(src_parent, name, dst_parent, new_name)`：先按 `(shard_id, parent_fid)` 顺序拿源/目标父目录写锁；在锁内重新校验源项存在、目标项冲突、权限和 generation；再写入同一个 txn 的 pending `delete(src)` + `create(dst)`，并更新 inode parent/name 与目录 diff。
- `remove(..., todumpster=true)`：视为从源父目录 rename 到 `dumpster_fid`，按同一 DirShard 顺序加锁，不给 dumpster 单独开后门锁。
- 快照 COW / diff：先拿 snapshot 元数据锁，确定受影响的 `snap_id` / `DirSnapshotDiff` / `SnapshotRefIndex`，再按目录和 fid 顺序加锁；不得持有低层锁后再回头等待 snapshot 元数据锁。
- 冲突处理：多资源操作使用 `try_lock` + 释放已持有锁 + 退避重试，避免 ABBA；禁止在持有另一把目录锁时做读锁升级为写锁。
- `EditLog` 只在已完成内存 pending 版本后 append buffer；`fsync` / quorum 等待发生在释放业务锁之后。

---

## 8. RPC 与客户端约定

### 8.1 保持兼容的字段

- `CREATE`：`P`（parent fid）、`N`（name）、`R`（replicas）等现有 QFS 头。
- 响应：`H`（新 fid）不变。

### 8.2 推荐客户端行为

1. **路径与父 fid 缓存（必选）**：在客户端维护 `path → { fid, generation }` 与 `parent_fid`；mutating 成功后更新或按 §4.3 失效；**不要依赖** MetaServer 路径缓存。
2. **批量 create**：`MULTI_CREATE` 一次 RPC 多条，log 一条 batch op 或连续 append 一次 fsync。
3. **幂等**：携带 `r`（reqId）；服务端 LRU 表 `op_id → result`（TTL 秒级）。
4. **响应字段**：`LOOKUP` / `CREATE` / `READDIR` 等返回目录 `generation`（或等价 epoch），供客户端校验本地 cache。

### 8.3 服务端可删减的 create 工作

将 **striping / tier / object-store 判定** 延后到 `SETATTR` 或 **首次 `ALLOCATE`**，使 `CREATE` 保持最小临界区（可选配置开关兼容旧语义）。

### 8.4 特殊路径：WORM、dumpster、虚拟 `/proc`（已决）

对齐现 QFS（`gWormMode`、`DUMPSTERDIR`、`/proc/invalid_chunks`），在 **DirIndex + InodeTable** 模型下的规则如下。

#### WORM

| 项 | 规则 |
|----|------|
| 开关 | 全局 `worm_mode`（等价 `TOGGLE_WORM` RPC），与现网一致。 |
| 拦截层 | **RPC / op 分发层** 统一校验：在 `worm_mode` 下，`REMOVE` / `RENAME` / 覆盖写等 mutating 若目标路径或文件名不满足 `IsWormMutationAllowed`，返回 `-EPERM`。 |
| DirIndex | **不**为 WORM 单独建索引类型；普通 `DirIndex` 操作不变。 |
| 持久化 | `worm_mode` 写入 edit / snapshot 元数据段（或专用 op），恢复后恢复开关状态。 |

#### dumpster（`/dumpster`）

| 项 | 规则 |
|----|------|
| 形态 | 根目录下 **普通目录**，启动时 `MKDIR(ROOT, "dumpster")` 得到固定 `dumpster_fid`；在 `Inode.flags` 标记 **`INODE_FLAG_DUMPSTER_ROOT`**（仅根下该目录）。 |
| 用途 | `remove(..., todumpster=true)` 语义为 **rename 到 `dumpster_fid` 下**（与现 `kfsops` 一致），不是额外隐藏表。 |
| 限制（RPC） | 禁止在 dumpster 内 **create/mkdir**；禁止任意 rename **进入或离开** dumpster（`mEnforceDumpsterRulesFlag` 等价配置）；禁止删除 dumpster 目录本身。 |
| DirIndex | 与普通目录相同：`DirIndex[dumpster_fid]` 存待清理文件；后台任务对非 busy 文件再 `remove`。 |

#### `/proc/invalid_chunks`（虚拟路径）

| 项 | 规则 |
|----|------|
| 形态 | **不进入 `DirIndex`**；无真实 `proc` 目录项。 |
| 解析 | RPC 层（如 `CREATE` / `LOOKUP_PATH` 入口）识别前缀 `/proc/invalid_chunks/`，解析 `chunkId` 后直查 **`BlockMap` / chunk 元数据**，用于诊断日志（对齐现 `MetaCreate::start` 中 `invalChunkFlag` 分支）。 |
| 客户端 | 不应缓存该路径为普通目录；不分配长期 fid。 |

### 8.5 fsck（已决）

**不再遍历 B+ 树叶子**；单机全量检查按以下顺序（可 fork 后台进程，对齐现 `MetaFsck` 工具链）：

```text
Phase A — InodeTable
  对每个 fid：
    - 类型合法；parent_fid 存在（或为 ROOT）
    - 若 type=file：BlockMap 条目可选校验（chunk 副本、版本，委托 LayoutManager 逻辑）
    - 标记 abandoned / 零长度策略（沿用现 fsck 配置项）

Phase B — DirTable（每个目录 fid）
  按 DirNode.layout 枚举：
    - **SMALL**：flat_hash 全桶扫描，校验无重复 NameKey、探测链有界
    - **LARGE**：遍历该目录专属 `DirBTree` 叶（同现 `kfstree` 迭代），校验 `Key(KFS_DENTRY, parent, hash)` 与 name 唯一
  对每条 DirEntry (name → child_fid)：
    - InodeTable[child_fid] 存在且 parent_fid == 当前目录 fid
  对 InodeTable 中 type=dir 的项：
    - 必须存在 DirTable[dir_fid]；`child_count` 与枚举数量一致
  - `state` 不得持久化为 **PROMOTING**；若 checkpoint 遇到则按 §4.2.6 修复或拒绝加载
  - 若 `child_count >= dir_large_threshold` 则 `state` 应为 **LARGE**

Phase C — 双向一致
  - 无「仅在 DirIndex 出现、Inode 无 parent」的孤儿
  - 无「Inode 有 parent 但父目录 DirIndex 无对应 name」的悬空项
  - dumpster 子项：仅允许 file 类型条目（可选策略检查）

Phase D — 与 edit committed 视图一致（可选在线 fsck）
  仅扫描 txn_id ≤ committed_txn_id 的视图（§6.7）

Phase E — 用户快照与目录 diff（§6.3.1.1 / §6.3.4）
  - 对每个 SnapshotRecord：cow_inodes 中 fid 存在且 snap_ref_count >= 1
  - 对每个 DirSnapshotDiff：所属 snap_id 存在，base_txn <= committed_txn_id，created/deleted 的 NameKey 无重复
  - 对每个 DirSnapshotDiff.deleted 中的 frozen_fid：inode 存在，snap_ref_count >= 1，SnapshotRefIndex 包含该 snap_id
  - 对每个 snap_ref_count > 0 的 inode：至少被 SnapshotRecord.cow_inodes、dir_diffs.deleted 或 SnapshotRefIndex 引用
  - snap_ref_count == |SnapshotRefIndex[fid]|；无 snap_ref_count == 0 且仅被快照元数据悬挂的 unreachable frozen
```

报告格式可继续兼容现 `MetaFsck` / `kfsfsck` 客户端字段；内部扫描源从 `metatree` 迭代改为 **InodeTable + DirIndex 枚举**。

---

## 9. 分阶段实施路线图

| 阶段 | 交付 | CREATE 预期收益 |
|------|------|-----------------|
| **P0** | 文档 + 基准：分解 QFS create = queue / fsync / btree / mutex | 基线数据 |
| **P1** | Log 组提交 + 缩小 `submit_request` 锁；客户端强制 parent fid | 中（不改索引） |
| **P2** | `DirTable`（§4.2 Small+Large+promotion）+ `InodeTable`；百万级单目录基准 | 高 |
| **P2.1** | §8.4 特殊路径 + §8.5 fsck（含两种 DirNode layout） | 可运维 |
| **P3** | v2 edit + §6.4 Checkpoint（FSImage N + replay）+ 冷启动闭环 | 很高 |
| **P3.1** | §6.3 用户快照 + 目录 Diff + §6.3.4 `snap_ref_count`（COW/delete/replay/fsck 测试） | 可回滚目录 |

（**范围外**：多 MetaServer namespace 分片、BlockMap 独立服务、inode 换出等，不列入本 RFC 路线图。）

---

## 10. 开放问题

本 RFC 范围内 **无剩余开放项**。已决事项索引：

| 主题 | 章节 |
|------|------|
| 读一致性 | §6.7 |
| PathCache | §4.3 |
| 单机内存范围 | §2.1 / §2.2 |
| WORM / dumpster / `/proc/invalid_chunks` | §8.4 |
| fsck | §8.5 |
| 大目录索引（Small/Large + promotion） | §4.2 |
| Promotion 原子性与可见性 | §4.2.6 |
| Promotion 墙钟上限（写者饥饿） | §4.2.6 |
| Readdir cookie 逻辑位置（`NameKey`） | §5.4.1 |
| 用户快照（HDFS 式 Ref + 文件级 COW + 目录 Diff） | §6.3 |
| Frozen inode `snap_ref_count` + 倒排索引 | §6.3.4 |
| 目录快照 Diff | §6.3.1.1 |
| Pending / committed 视图 | §6.6 / §6.7 |
| 跨目录锁顺序 | §7.3 |
| Checkpoint/FSImage（一致性点 + 后台遍历） | §6.4 |
| Checkpoint 扫描内存与节流 | §6.4.4 |

后续若扩展 **多机分片、inode 换出**，另起 RFC。

---

## 11. 备选方案（已否决或延后）

| 方案 | 结论 |
|------|------|
| 保留全局 B+ 树，仅优化锁 | 无法消除双 insert 与树分裂；并发上限低（见 `MetaTree-Lock-Optimization.md`） |
| 仅全局 B+ 树 | 已否决；见 §4.2.5 |
| 单目录百万项仍用平铺 HashMap+链表 | **已否决**；首版必须 Large 布局 + promotion |
| 每目录一棵 B+ 树（Large 布局） | **已采纳**，**抽取/适配 `kfstree` 节点算法**，仅用于 `child_count ≥ threshold` 的目录 |
| 自研另一套目录 B-tree 实现 | **已否决**，与现网重复且难保持 checkpoint 一致 |
| 纯 tmpfs、无持久化 | 不符合 QFS 定位 |
| 完全照搬 RocksDB/LSM 存 namespace | 写放大与 create 延迟不如 hash + edit log 直接 |

---

## 12. 成功指标（建议验收）

在相同硬件与 `sync=batch`（如 1ms 组提交）下，相对当前 QFS main：

| 指标 | 目标（示例，需基准标定） |
|------|--------------------------|
| 空文件 create p50 | 降低 ≥ 50% |
| 空文件 create p99 | 降低 ≥ 40%（/fsync 尾延迟） |
| create QPS（单 meta，多客户端线程） | 提升 ≥ 3×，且随线程数近线性至磁盘/log 瓶颈 |
| 单目录 10⁶ 子项 lookup p99 | < 50µs 量级（Large 布局，无长链；以基准为准） |
| 单目录 10⁶ 子项 readdir（每页 1k） | 稳定延迟，不随 N 线性恶化 |
| 恢复时间 | `FSImage(N)` + `replay(txn>N)`（§6.4）；与用户快照创建 O(1)（§6.3）无关 |
| 创建用户快照 | O(1)，与目录子项数无关（§6.3） |

---

## 13. 参考文献（仓库内）

- `src/cc/meta/kfstree.h` / `kfstree.cc` — B+ 树（Large 目录 **抽取/适配** 节点算法；全局 `metatree` 不再用于 namespace dentry）
- `src/cc/meta/kfsops.cc` — `Tree::create` / `link` 双 `insert`
- `src/cc/meta/MetaRequest.cc` — `MetaCreate::start` / `handle`，`SubmitBegin`
- `src/cc/meta/LogWriter.cc` — `Enqueue`、`WriteLog`、`fsync`
- `src/cc/meta/NetDispatch.cc` — `submit_request` 全局串行注释
- `src/cc/meta/MetaRequest.cc` — `gWormMode`、`/proc/invalid_chunks`、`MetaFsck`
- `src/cc/meta/kfsops.cc` — `DUMPSTERDIR`、dumpster rename/remove 规则
- `MetaTree-Lock-Optimization.md` — B+ 树分片锁不安全分析
- `wiki/Performance-Comparison-to-HDFS.md` — 历史 metaserver 对比背景

---

## 修订历史

| 版本 | 日期 | 说明 |
|------|------|------|
| 0.1 | 2026-05-25 | 初稿：绿场内存原生元数据层，对照 QFS/HDFS |
| 0.2 | 2026-05-25 | 移除 § 迁移与兼容；明确绿场/冷启动范围，路线图去掉迁移工具 |
| 0.3 | 2026-05-25 | 读一致性决策：§6.6 采用 HDFS 风格 lease + 已提交命名空间 |
| 0.4 | 2026-05-25 | PathCache 决策：§4.3 仅客户端缓存，MetaServer 不维护/复制路径 cache |
| 0.5 | 2026-05-25 | 范围限定单机内存；去掉 namespace 水平分片/换出开放项与 P4 路线图 |
| 0.6 | 2026-05-25 | §8.4 特殊路径、§8.5 fsck 已决；§10 无剩余开放项 |
| 0.7 | 2026-05-25 | §4.2 大目录首版必做：Small flat_hash + Large 每目录 B+ 树 + promotion |
| 0.8 | 2026-05-25 | Large 布局明确抽取/适配现 `kfstree` 节点算法（`Node`/`Key`/`MetaDentry`），不新写 B-tree |
| 0.9 | 2026-05-25 | §6.3 已决：一致性点 + 后台模糊 FSImage + replay(txn>N) |
| 1.0 | 2026-05-25 | §6.3 改为 HDFS 式用户快照（InodeRef+文件级COW）；§6.4 为 Checkpoint/FSImage |
| 1.1 | 2026-05-25 | §4.2.6 Promotion：`PROMOTING` 状态、staging、读 Small/写等待、原子发布 |
| 1.2 | 2026-05-25 | §6.3.4：`snap_ref_count`、COW/删快照维护、fsck 与无环不变量 |
| 1.3 | 2026-05-25 | §5.4.1：readdir cookie 用逻辑 key 游标，禁止 LeafIter/节点指针 |
| 1.4 | 2026-05-25 | §4.2.6 晋升墙钟上限；§5.4.1 `last_key`；§6.3.4 倒排索引；§6.4.4 checkpoint 内存 |
| 1.5 | 2026-05-25 | 补充 pending/committed 视图、目录快照 diff、checkpoint 版本过滤、跨目录锁顺序 |
