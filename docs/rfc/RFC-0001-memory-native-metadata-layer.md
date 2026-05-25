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
│  • 用户快照：HDFS 式引用 + 文件级 COW（§6.3）               │
│  • Checkpoint/FSImage：一致性点 N + 后台遍历（§6.4）          │
│  • Quorum / VR 复制（复用现有 LogWriter/VR 基础设施）       │
└─────────────────────────────────────────────────────────┘
```

### 3.2 与 QFS 现状的核心差异

| 维度 | QFS 现状 | 本 RFC |
|------|----------|--------|
| 主索引 | 全局 B+ 树，dentry/fattr 不同 key | DirNode（Small hash / Large **复用 `kfstree` 每目录一棵 `Tree`**）+ InodeTable |
| CREATE 索引操作 | 2× 全局 `insert` + 多次 `findLeaf` | 1× DirNode insert + 1× InodeTable insert |
| 百万级单目录 | 同全局树叶子链/同桶冲突风险 | Large 布局 O(log N)，首版必做 promotion |
| 持久化顺序 | 先 WAL committed，再 `handle()` | 临界区内改内存 + append log buffer；fsync 摊销 |
| 用户快照 | （现 QFS 无同等机制） | `InodeRef` + 文件级 COW，创建 O(1)（§6.3） |
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
  snap_ref_count: u32        // 被用户快照持有的 frozen 引用数（§6.3.6）；live inode 常为 0
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

**决策：Large 布局直接复用当前 QFS B+ 树实现**（`kfstree.h` / `kfstree.cc`），不新写一套目录 B-tree。与全局 `metatree` 的差异仅是 **每目录一棵独立 `Tree` 实例**，键空间 scoped 在该 `parent_fid` 下。

| 复用组件 | 路径 / 说明 |
|----------|-------------|
| 内部节点 | `Node`（`NKEY=170`，4096B 页式节点，`findplace` 二分，`split` / `merge`） |
| 树操作 | `Tree::insert`、`Tree::del`、`lowerBound` / `findLeaf`、`LeafIter` |
| 键 | 现有 `Key` / `PartialMatch`；叶键 **`Key(KFS_DENTRY, parent_fid, name_hash)`**，与现 `MetaDentry::keySelf()` 一致 |
| 叶记录 | `MetaDentry`（或薄封装 `DirBTreeLeaf` 内嵌相同字段）；`matchSelf` 比对 `name` |
| 内存 | `MetaNode::allocate` / `PoolAllocator`（与现 meta 节点相同） |

```text
LargeDir {
  parent_fid:  fid_t
  tree:        Tree          // 现 kfstree.Tree，非全局 metatree 单例
}
```

- **语义**：逻辑上仍是「该目录下 name → child_fid」；物理上用 **一棵子树** 存该目录全部 `MetaDentry` 叶，**不再**插入全局 `metatree` 的混合 key 空间。
- **`lookup` / `create`**：对该目录的 `Tree` 调用与现 `getDentry` / `insert` 相同逻辑（`findLeaf` + 叶链 `peer()` 扫同名 hash），**O(log N)**，无百万长链。
- **`readdir`**：`LeafIter` 逻辑序遍历 + §5.4 **逻辑位置 cookie**（禁止裸指针）。
- **升格（promotion）**：原子性与并发语义见 **§4.2.6**（`PROMOTING` 状态、写阻塞、读仍用 Small、staging 完成后一次性切换）。
- **checkpoint / fsck**：Large 目录序列化可 **复用现 Node/Meta checkpoint 格式**；fsck 见 §8.5（`PROMOTING` 视为 transient，持久化快照中不应出现）。

**不新写**：单独的目录 B-tree 节点类型、另一套 split/merge 或不同于 `Node` 页大小的树实现。

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
| 实例 | 单例 `metatree`，混放 dentry/fattr/chunk | **每超大目录一个 `Tree`** |
| create 副作用 | 可能 split 共享祖先内部节点 | 仅影响该目录子树 |
| 小目录 | 也走全局树 | **Small `flat_hash`**，不进 B+ 树 |

InodeTable、BlockMap **不再**进入任何 B+ 树；仅 **超大目录的子项列表** 使用 `Tree` 存 `MetaDentry` 叶。

#### 4.2.6 晋升（Promotion）的原子性与可见性（已决）

**问题**：§4.2.3 若在「半建成」的 Large `Tree` 上并发 `lookup`/`create`，可能看到 **不完整** 的 B+ 树或 Small/Large 双写混乱。

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
  3. staging.large = new Tree()          // 读者不可见
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
4.   new_fid = FidAllocator.next()
5.   DirTable[parent].insert(name, new_fid)   // §4.2，必要时 promotion
6.   InodeTable[new_fid] = Inode{ parent, attrs... }
7.   update parent.mtime, parent.file_count
8.   txn = EditLog.append(CREATE, parent, name, new_fid, attrs, op_id)
9. unlock
10. if sync_policy == always: wait(txn.committed)
11. return new_fid
```

**树操作次数**：0。持久化：1 条 edit（组提交时与其他 op 共享一次 fsync）。

### 5.2 LOOKUP（单级）

```text
lock(DirShard[hash(parent)])
  entry = DirTable[parent].lookup(name)
  fa = InodeTable[entry.fid]
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
| **SMALL** | 桶序 + 桶内序 | 逻辑位置（§5.4.1） |
| **PROMOTING** | 仍按 Small | 同 SMALL；`generation` 未变 |
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
  bucket_id:    u32      // 开放寻址桶序号（稳定枚举顺序）
  slot:         u32      // 桶内下一起始槽位
}
```

- 仅在 **同一 `generation`、同一 Small 布局** 下有效；**promotion 完成** 后 `generation++`，旧 cookie **作废**（切换为 Large cookie 或从头）。

##### LARGE（推荐：逻辑 key 游标）

**首选**（实现简单、对 split/merge 最稳）：

```text
CookieLarge = {
  generation:   u64
  layout:       LARGE
  after_hash:   u64          // 上一页最后一条的 name_hash
  after_name:   bytes        // 上一页最后一条的文件名（字典序续扫）
}
```

恢复算法：

```text
readdir_resume(parent, cookie):
  if cookie.generation != DirNode.generation: INVALID
  key = Key(KFS_DENTRY, parent, cookie.after_hash)
  it = lowerBound(tree, key)                    // 现 kfstree
  skip entries where (hash,name) <= cookie.after_name lexicographically
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
- 首版 **优先 `after_name` 游标**；`leaf_node_id` 方案可在性能优化阶段引入。

##### 与 promotion / mutation 的交互

| 事件 | cookie 行为 |
|------|-------------|
| **promotion 完成** | `generation++`；Small cookie **失效**；客户端用空 cookie 对 Large 重扫 |
| **rename/unlink/rmdir（目录）** | `generation++`；所有 cookie 失效 |
| **Large 上 create/delete** | `generation` 可不变；**`after_name` cookie 仍有效**（靠 key 重定位）；若产品要求列举快照视图，另议 |
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

### 6.3 用户快照（已决：HDFS 式引用 + 文件级 COW）

**放置说明**：本节描述 **Snapshottable 目录上的用户可见快照**（类比 HDFS `createSnapshot`），与 §6.4 **周期性 Checkpoint/FSImage**（NN 冷备）分工不同。实现可落在 **P3/P3.1**（§9）。

**决策：创建快照采用 HDFS 核心思路——引用（Rename/Reference）而非复制；修改采用文件级写时复制（COW）。** 不采用对整棵树做全量内存扫描来「创建」用户快照（该做法保留给 §6.4 Checkpoint）。

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
- **Large 目录**：COW 只 **`replace_name` 一条 DirEntry**（Small 或 `kfstree` 单键更新），不重扫整棵 per-dir `Tree`。

#### 6.3.2 性能预期（与 HDFS 对照）

| 场景 | 性能 | 原因 |
|------|------|------|
| 读活动/读快照文件 | 快照读无额外锁；活动读与无快照相同 | Ref 只读解析 |
| 创建 / 删除快照 | **近似 O(1)** | 仅增删 `SnapshotRecord` / `InodeRef` |
| 首次修改快照覆盖下的文件 | 有开销（COW 一个 inode） | 与被修改文件数成正比，与目录总规模无关 |
| 再次修改已 COW 过的活动文件 | 与无快照相同 | 已操作活动侧新 inode |

#### 6.3.3 数据结构（与 §4.1 衔接）

```text
SnapshotRecord {
  snap_id, name, root_dir_fid, txn_id_at_create: N
  root_ref: InodeRef              // O(1) 创建：指向 snapshottable 根目录 inode
  cow_inodes:  set<fid_t>         // 可选：本快照触发的 frozen fid 登记，便于 delete 时递减
}

InodeRef { target_fid, txn_id_cap }
```

- **`snap_ref_count`** 定义在 §4.1 `Inode` 上：表示有多少 **独立快照引用** 仍依赖该 **inode 对象**（通常为 COW 后的 **frozen** 副本；活动/live inode 在分裂后一般为 0）。
- Edit log：`SNAPSHOT_CREATE`、`SNAPSHOT_DELETE`、`INODE_COW_SPLIT`（含 `frozen_fid`、`live_fid`、`snap_ref_delta`），供 standby **确定性 replay**。

#### 6.3.6 Frozen inode 引用计数（已决）

**问题**：§6.3 删除快照时「仅回收本快照专属的 frozen inode」。若同一 frozen inode 被 **多个快照** 引用（例如 `/foo` 上连续创建 `s1`、`s2` 后才首次修改 `file1`），**不能在 `snap_ref_count > 0` 时释放**。

**决策**：在 `Inode` 上维护 **`snap_ref_count`**；在 **COW 分裂** 与 **删除快照** 时严格增减；减到 **0** 才可回收该 frozen inode（及对应 `BlockMap`）。

##### 何时增减（与 HDFS 文件级 COW 对齐）

| 事件 | `snap_ref_count` | 说明 |
|------|------------------|------|
| **`createSnapshot`** | 根目录 `root_ref.target` **+1**（可选） | 创建本身 O(1)；**不**遍历子树给每个文件 +1。未 COW 的文件仍与 live 共用同一 `fid`，读快照走解析路径。 |
| **首次 `COW_SPLIT`（file1）** | 对 **frozen_fid**（旧 inode 副本）设为 **覆盖该文件的所有活跃快照数** `K` | 例：存在 `s1`、`s2` 均可见 `file1` 时尚未修改 → `frozen.snap_ref_count = 2`。活动侧新 `live_fid`：`snap_ref_count = 0`。 |
| **再建快照 `s3`（已有 frozen file1）** | 若 `s3` 仍指向含 `file1` 的视图且 `file1` 已 frozen：对 `frozen_fid` **+1** | 仅影响 **已分裂** 的 frozen 对象；仍与 live 共用的路径在首次 COW 时一次性结算。 |
| **`deleteSnapshot(s)`** | 对该快照登记过的每个 `frozen_fid`：**-1** | 来自 `cow_inodes` 或快照元数据索引；**仅当减到 0** 时 `free_inode(frozen_fid)` + 释放 BlockMap |
| **活动路径修改 live inode** | 不增减 | live 与快照引用解耦 |

```text
cow_split(file_fid, parent, name):
  frozen_fid = retain_or_clone_inode(file_fid)   // 旧版本留给快照
  live_fid   = allocate_new_inode(...)
  frozen.snap_ref_count = count_snapshots_covering(parent, name, frozen_fid)
  DirTable[parent].replace_name(name, live_fid)
  for each snap covering this path:
    snap.cow_inodes.insert(frozen_fid)
  append EditLog(INODE_COW_SPLIT, frozen_fid, live_fid, snap_ref_count, ...)

deleteSnapshot(snap_id):
  for fid in snap.cow_inodes:
    if (--InodeTable[fid].snap_ref_count == 0)
      free_inode_and_blockmap(fid)
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

- 对每个 `snap_ref_count > 0` 的 inode：存在至少一条 `SnapshotRecord` / `cow_inodes` 反向引用。
- 对每个 `SnapshotRecord.cow_inodes` 中的 `fid`：`snap_ref_count >= 1`。
- 删除快照后的 spot check：`cow_inodes` 中不应出现已 free 的 `fid`。

**成熟度说明**：引用计数为业界成熟手段（HDFS snapshot diff、 btrfs 等同类问题），但本实现须在 **COW 分裂计数初值**、**多快照叠加**、**delete + replay** 三条路径上做 **专项测试**（属性测试或模拟并发删除），列入 **P3.1 验收**。

#### 6.3.4 与 §6.4 Checkpoint 的边界

| | §6.3 用户快照 | §6.4 Checkpoint/FSImage |
|--|----------------|-------------------------|
| 目的 | 时间点恢复、误删回滚、对比历史 | MetaServer **重启/冷备**、缩短 replay |
| 创建成本 | **O(1)** per snap | O(namespace) 后台扫描（可模糊） |
| 读路径 | 快照视图 | 正常命名空间 |
| 存储 | 内存 Ref + 被 COW 分离的 inode | 磁盘 FSImage 文件 |

两者可同时存在：HDFS 亦区分 **Snapshot** 与 **Checkpoint（FSImage）**。

#### 6.3.5 未采纳为用户快照的方案

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
  │     允许与写并发；图像可「模糊」
  ├─ 写出 FSImage + footer(N)
  └─ 原子 publish

冷启动：load FSImage(N) → replay Edit Log (txn_id > N) → 一致
```

正确性：依赖 §6.7 之「先 Edit Log 再内存 / committed 边界」；模糊项由 replay 修正（同原 §6.3.2 论证）。

#### 6.4.2 FSImage 内容与 Large 目录

- `section_inodes`、`section_dirs`（Small 桶或嵌入 **`kfstree` checkpoint 流**）。
- log 截断：**可选**运维操作，非恢复前提。

#### 6.4.3 代价

快照扫描慢 → `txn_id > N` 的 log 段变长 → **重启 replay 变长**；需控制 checkpoint 周期（配置 `meta.checkpoint.interval` 等）。

### 6.5 与 QFS LogWriter / VR 的关系

- **可复用**：quorum 复制、block 切分、primary lease、`MetaVrLogSeq` 序语义。
- **需替换**：`WriteLog` 序列化内容与 replay 解析器（`Replay.cc` / `replay_create` 文本格式 → 二进制 op）。
- **不再依赖**：`metatree.insert` 作为 redo 单元；redo 单元为 **edit op**。

### 6.6 内存修改与 log 的顺序（相对 QFS 的关键改进）

**提议默认顺序**：

```text
（分片锁内）改内存 → append 到 log 内存 buffer → 释放锁
（log 线程）buffer → 复制 → fsync → 推进 committed_txn_id
```

对比 QFS：**先 log committed 再 `handle()`**，客户端等待包含「空窗期」内无法从内存读到结果的双重延迟。本 RFC 的可见性边界见 **§6.7**：其他客户端以 **已提交命名空间** 为准；发起方在 RPC 成功后的可见范围与 **lease / sync 策略** 对齐 HDFS 习惯，而非「未提交 txn 全网可见」。

### 6.7 读一致性（已决）

**决策：采用 (c) 跟随 HDFS 风格的 lease + 已提交命名空间模型**，并与 QFS 现有 **primary / VR / chunk lease** 语义衔接（`LEASE_ACQUIRE`、`LEASE_RENEW` 等，见 `MetaRequest`）。

| 场景 | 规则 |
|------|------|
| **命名空间变更**（CREATE / REMOVE / RENAME …） | 对其他客户端：仅在 edit **已 committed**（`committed_txn_id` 推进、quorum 复制完成）后可见；primary 内存中未 fsync 的 buffer **不**对外暴露。 |
| **RPC 返回与 durable** | `sync=always`：成功返回 ≡ 命名空间变更已 durable，他客户端可见（在 primary 正常服务前提下）。`sync=batch`：返回表示 **已接受**；他客户端可见时点不早于本批 **组提交 fsync**（类比 HDFS edit 组提交窗口）。 |
| **文件数据读写** | 命名空间登记（create 得 fid）与 **写数据** 分离；已打开文件的读写一致性由 **chunk lease** 保证写者独占/租约续期，读者看到已提交块版本，与 HDFS 「NN 管名字、DN 管块 + lease」分工一致。 |
| **Primary / standby** | 仅 primary 执行 namespace 变更并写 edit；standby 通过 log replay 追赶；客户端 mutating 与强一致命名空间读面向 primary（与现 VR 一致）。 |

**不采用**：

- **(a) 仅 primary 本地可见未提交变更**：不足以定义多客户端语义，且与 backup 复制模型冲突。
- **(b) 未提交 txn 全网可见**：破坏恢复与 fsck 假设，并引入跨客户端脏读。

**实现提示**：可在 `Inode` 或目录上保留 `last_committed_txn`；`lookup` / `readdir` 仅暴露 `txn_id ≤ committed_txn_id` 的视图；写路径 lease 逻辑复用现有 QFS 实现，本层不新增第二套租约协议。

---

## 7. 并发模型

### 7.1 锁层次

| 资源 | 锁粒度 |
|------|--------|
| `DirIndex` | `hash(parent_fid) % N` 分片锁：**读锁**（lookup/readdir，`PROMOTING` 仍读 Small）；**写锁**（create/promotion，写者等待 `PROMOTING` 结束） |
| `InodeTable` | `hash(fid) % M` 分片；读多写少用 RW lock |
| `FidAllocator` | 无锁原子或独立 mutex |
| `EditLog buffer` | 单写者 + MPSC 队列 |
| `PathCache` | RCU 或 per-shard 锁 |

**禁止**：所有 mutating RPC 共用一个 `submit_request` 全局 mutex（现状瓶颈）。

### 7.2 与 B+ 树分片锁的区别

对 **全局 `metatree`（单例 B+ 树）**，「按 parent 加锁」**不安全**（不同目录可能 split 同一内部节点，见 `MetaTree-Lock-Optimization.md`）。  
对 **DirTable 分片**：按 `parent_fid` 加锁 **安全**——Small 为独立 `flat_hash`；Large 为 **该目录专属 `Tree` 实例**（仍用 `kfstree`，但不与别目录共享内部节点）。

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
    - **LARGE**：遍历该目录专属 `Tree` 叶（同现 `kfstree` 迭代），校验 `Key(KFS_DENTRY, parent, hash)` 与 name 唯一
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

Phase E — 用户快照引用计数（§6.3.6）
  - 对每个 SnapshotRecord：cow_inodes 中 fid 存在且 snap_ref_count >= 1
  - 对每个 snap_ref_count > 0 的 inode：至少被一个 SnapshotRecord.cow_inodes 引用
  - 无 snap_ref_count == 0 且仅被快照元数据悬挂的 unreachable frozen
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
| **P3.1** | §6.3 用户快照 + §6.3.6 `snap_ref_count`（COW/delete/replay/fsck 测试） | 可回滚目录 |

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
| Readdir cookie 逻辑位置 | §5.4.1 |
| 用户快照（HDFS 式 Ref + 文件级 COW） | §6.3 |
| Frozen inode `snap_ref_count` | §6.3.6 |
| Checkpoint/FSImage（一致性点 + 后台遍历） | §6.4 |

后续若扩展 **多机分片、inode 换出**，另起 RFC。

---

## 11. 备选方案（已否决或延后）

| 方案 | 结论 |
|------|------|
| 保留全局 B+ 树，仅优化锁 | 无法消除双 insert 与树分裂；并发上限低（见 `MetaTree-Lock-Optimization.md`） |
| 仅全局 B+ 树 | 已否决；见 §4.2.5 |
| 单目录百万项仍用平铺 HashMap+链表 | **已否决**；首版必须 Large 布局 + promotion |
| 每目录一棵 B+ 树（Large 布局） | **已采纳**，**复用 `kfstree`**，仅用于 `child_count ≥ threshold` 的目录 |
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

- `src/cc/meta/kfstree.h` / `kfstree.cc` — B+ 树（Large 目录 **复用** 本实现；全局 `metatree` 不再用于 namespace dentry）
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
| 0.8 | 2026-05-25 | Large 布局明确复用现 `kfstree`（`Tree`/`Node`/`Key`/`MetaDentry`），不新写 B-tree |
| 0.9 | 2026-05-25 | §6.3 已决：一致性点 + 后台模糊 FSImage + replay(txn>N) |
| 1.0 | 2026-05-25 | §6.3 改为 HDFS 式用户快照（InodeRef+文件级COW）；§6.4 为 Checkpoint/FSImage |
| 1.1 | 2026-05-25 | §4.2.6 Promotion：`PROMOTING` 状态、staging、读 Small/写等待、原子发布 |
| 1.2 | 2026-05-25 | §6.3.6：`snap_ref_count`、COW/删快照维护、fsck 与无环不变量 |
| 1.3 | 2026-05-25 | §5.4.1：readdir cookie 用逻辑 key 游标，禁止 LeafIter/节点指针 |
