# PolySI 验证器使用手册

## 一、项目概述

PolySI 是一个基于 **Snapshot Isolation (SI)** 的黑盒历史检测器，支持检测数据库历史是否满足 **Serializable Snapshot Isolation (SER/PSI)**。

### 1.1 核心验证模式

项目支持两种验证模式：

| 模式 | 配置 | 验证方法 | 适用场景 |
|------|------|----------|----------|
| **SER 模式** | `SIVerifier.verifySer = true`（默认） | PR_* 边推导 + SAT 编码（MonoSAT） | 精确的 predicate-aware SER 验证 |
| **SI 模式** | `SIVerifier.verifySer = false` | 拓扑排序（Kahn 算法） | 快速纯 SI 验证，无 predicate |

### 1.2 架构总览

```
┌─────────────────────────────────────────────────────┐
│                   Main (CLI 入口)                    │
│  verifier audit | stat | convert | dump              │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│              HistoryLoader (加载器)                   │
│  CobraHistoryLoader / DBCopHistoryLoader            │
│  ElleHistoryLoader / TextHistoryLoader              │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│           KnownGraph (依赖图构建)                     │
│  knownGraphA: SO, WR, WW, PR_WR                      │
│  knownGraphB: RW, PR_RW                              │
│  readFrom: 点读的写来源                              │
└────────────────────┬────────────────────────────────┘
                     │
         ┌───────────┴───────────┐
         │                       │
┌────────▼────────┐    ┌────────▼────────┐
│ SER 模式         │    │ SI 模式          │
│                 │    │                 │
│ generateConstraints│  │ checkSIWith    │
│ generateConstraints│  │ TopologicalSort│
│ (WW/RW约束)      │    │ (Kahn算法)     │
│         ↓        │    │                 │
│ refreshDerived   │    └─────────────────┘
│ PredicateEdges   │
│ (PR_WR/PR_RW)    │
│         ↓        │
│ Pruning          │
│ (可达性剪枝)      │
│         ↓        │
│ SISolver         │
│ (MonoSAT求解)    │
└──────────────────┘
```

---

## 二、核心概念

### 2.1 历史记录 (History)

历史记录由以下元素组成：

```
History
├── Session[]         — 会话序列（按序执行的事务）
│   └── Transaction[]  — 事务序列
│       └── Event[]   — 事件序列 (READ/WRITE/PREDICATE_READ)
```

**事件类型：**

| 类型 | 说明 | 字段 |
|------|------|------|
| `READ` | 点读 | key, value |
| `WRITE` | 写 | key, value |
| `PREDICATE_READ` | 谓词读 | predicate函数, results列表 |

### 2.2 依赖边类型

| 边类型 | 符号 | 说明 | 放入哪个图 |
|--------|------|------|------------|
| **SO** | `T₁ → T₂` | Session Order：同一会话中 T₁ 在 T₂ 之前 | knownGraphA |
| **WR** | `T₁ → T₂` | Write-Read：T₁ 写了，T₂ 读了 T₁ 的值 | knownGraphA |
| **WW** | `T₁ → T₂` | Write-Write：T₁ 和 T₂ 都写了同一 key，T₁ 的写更早 | knownGraphA |
| **RW** | `T₁ → T₂` | Read-Write：T₁ 读了某值，T₂ 写了同一 key | knownGraphB |
| **PR_WR** | `T₁ → T₂` | Predicate Write-Read：T₁ 是 flip witness，T₂ 的 predicate read 受其影响 | knownGraphA |
| **PR_RW** | `T₁ → T₂` | Predicate Read-Write：T₁ 的 predicate read 阻止了 T₂ 的写入可见 | knownGraphB |

### 2.3 谓词读相关概念

**Flip Witness（翻转见证者）**：
- 定义：写事件 w 是 predicate P 的 flip witness，当且仅当 `match(prev(w), P) XOR match(w, P) = true`
- 即 w 及其前驱对 predicate P 的满足状态不同
- 语义：w 的写入改变了 predicate 的真值

**PR_WR 边**：
- 形式：`T_flip → S`（flip witness 的事务 → predicate read 的事务）
- 语义：T_flip 是 S 观察到的最新 flip witness

**PR_RW 边**：
- 形式：`S → T_winner`（predicate read 的事务 → WW winner 事务）
- 语义：S 的 predicate read 状态由 T_winner 决定（S 的可见写被 T_flip 支配）

---

## 三、模块详细说明

### 3.1 历史加载 (history/)

**支持格式：**

1. **Cobra 格式** (`CobraHistoryLoader`) — 二进制 .log 文件
2. **DBCop 格式** (`DBCopHistoryLoader`) — LittleEndian 二进制
3. **Elle 格式** (`ElleHistoryLoader`) — EDN 文本格式
4. **Text 格式** (`TextHistoryLoader`) — 简单文本 `r(key,val,sess,txn)` / `w(key,val,sess,txn)`

**文本格式示例：**

```
w(x,10,0,0)    # Session 0, Txn 0: 写 x=10
r(x,10,0,1)    # Session 0, Txn 1: 读 x=10
w(x,20,0,1)    # Session 0, Txn 1: 写 x=20
p(v>5,[(x,20)],0,1)  # Session 0, Txn 1: 谓词读，条件 v>5，结果 [(x,20)]
```

### 3.2 图结构 (graph/)

**KnownGraph** — 从历史构建的依赖图：

```java
KnownGraph(History h) {
    knownGraphA  // SO, WR, WW, PR_WR 边
    knownGraphB  // RW, PR_RW 边
    readFrom    // 点读来源（WR 边的来源信息）
}
```

**MatrixGraph** — 高效矩阵图：

```java
MatrixGraph
├── nodes() → 所有节点
├── edges() → 所有边
├── successors(n) → n 的后继
├── predecessors(n) → n 的前驱
├── union(MatrixGraph) → 图并集
├── composition(MatrixGraph) → 图合成 (A∘B)
├── reachability() → 传递闭包（RoaringBitmap）
├── hasLoops() → 检测环
└── topologicalSort() → 拓扑排序
```

### 3.3 验证器核心 (verifier/)

#### 3.3.1 SIVerifier — 主验证流程

```java
SIVerifier.verifySer = true;  // 默认 SER 模式
// SIVerifier.verifySer = false;  // 切换到 SI 模式

SIVerifier<KeyType, ValueType> verifier = new SIVerifier<>(loader);
boolean accepted = verifier.audit();
```

**SER 模式完整流程（`audit()` 方法）：**

```
audit()
├── 1. verifyInternalConsistency()     — 内部一致性检查
│   └── 点读/谓词读是否来自最新写
├── 2. new KnownGraph(history)         — 构建已知依赖图
├── 3. generateConstraints()            — 生成 WW/RW 约束
│   └── 对每个 key 的写对生成互斥选择约束
├── 4. refreshDerivedPredicateEdges()    — 推导 PR_WR/PR_RW 边
│   └── 基于 flip witness 和 Δμ 条件
├── 5. pruneConstraints()               — 约束剪枝
│   └── 可达性分析 + 冲突检测
├── 6. refreshDerivedPredicateEdges()    — 再次推导（剪枝后）
├── 7. new SISolver(history, graph, constraints) — SAT 编码
│   ├── createKnownGraph()             — 图 A/B
│   ├── 构建可达性矩阵 AC = A ∪ (A∘B)
│   ├── getUnknownEdges()              — A∘B 中的未知边
│   ├── getKnownEdges()                — 已知边
│   ├── addConstraints()               — WW choice 编码
│   └── monoGraph.acyclic()            — 无环性断言
└── 8. solver.solve()                  — SAT 求解
    └── acyclic → ACCEPT / 有环 → REJECT
```

**SI 模式（`verifySer=false`）：**

```
audit() → checkSIWithTopologicalSort()
└── Kahn 算法：GSO ∪ GWR 是否有环
    └── 有环 → REJECT / 无环 → ACCEPT
```

#### 3.3.2 PR_WR / PR_RW 推导（Algorithm 1 对齐）

**`refreshDerivedPredicateEdges()` — 完整推导流程：**

```
Phase 1: PR_WR 推导
  for each predicate observation S ⊢ PR(P, M):
    for each key x with confirmed total write order:
      1. resolveObservationIndex(x, S, P, M)
         → obsIdx: S 看到哪个写（index in orderedWrites）
      2. PredCand: collect { U | U is flip witness visible to S }
         → scan [0..obsIdx], check isFlipWitness(U, prev, P)
      3. MaxCand: unique maximal element under AR order
         → unique topological maximum of candidates
      4. if MaxCand = T ≠ ⊥ and T ≠ S:
         → emit PR_WR(T → S, x)

Phase 2: PR_RW 推导
  for each emitted PR_WR(T → S, x):
    for each WW(T → U, x) in knownGraphA:
      if deltaMu(T, S, U, x, P):
         → emit PR_RW(S → U, x)
```

**关键辅助方法：**

```java
// 判断 w 是否为 flip witness
isFlipWitness(writeRef, predecessor, predicateReadEvent)
→ matchesPredicate(prev) XOR matchesPredicate(w) = true

// MaxCand: AR-order 下唯一的最大候选
maxCand(candidates, orderedWrites, knownGraphA)
→ unique maximal element, or null if not unique

// Δμ 条件：T 的 flip 是否改变 S 对 U 的可见性
deltaMu(T, S, U, x, pr, resultSourceByKey, orderedWritesByKey, graph)
→ (a) T 是 flip witness AND
     (b) (x ∉ result AND T 的 flip 使 x 满足 P) OR
         (S 的可见 writer 被 T 支配)

// 确定 predicate read 的观察点
resolveObservationIndex(key, b, pr, orderedWrites, resultSourceByKey, graph)
→ case 1: key 在结果中 → 使用结果来源的 index
→ case 2: b 自己写了 key → 用程序序确定
→ case 3: 其他 → 用最新有直接边到 b 的写

// 基于确认边建立 key 的写全序
buildConfirmedWriteOrder(key, writesOnKey, graph)
→ 收集 key 上所有写的事务间边（SO/WR/WW/PR_WR）
→ 唯一拓扑排序 → total order
→ null if 无唯一顺序（保守跳过）
```

#### 3.3.3 约束生成与剪枝

**`generateConstraints()` — WW/RW 约束：**

```
对于每个 key k 的所有写事务对 (T₁, T₂):
  生成互斥选择约束:
    { T₁ WW(k)→T₂,  T₁ RW→T₂ 的所有冲突写 }
    或
    { T₂ WW(k)→T₁ }

  例如: T₁ 和 T₂ 都写 key x
    edges1: [WW(T₁→T₂, x)] ∪ [RW(T₁→T₂, x) for each T₂ reads from some write before T₂]
    edges2: [WW(T₂→T₁, x)]
```

**`Pruning.pruneConstraints()` — 剪枝循环：**

```
repeat
  1. refreshDerivedPredicateEdges()  — 重建 PR_* 边
  2. 构建可达性矩阵 Reach = A ∪ (A∘B)
  3. for each constraint ⟨either, or⟩ in CG:
       if neither consistent: return false (不可满足)
       if only 'either' consistent: GWW += either, CG -= ⟨either,or⟩
       if only 'or' consistent: GWW += or, CG -= ⟨either,or⟩
  4. changed = true if any constraint was forced
until no constraint is forced

其中 Consistent(H, e):
  G'WW = GWW ∪ {e}
  RefreshTmp(H, G'WW)  — 临时派生边
  Dep' = GSO ∪ GWR ∪ G'WW ∪ G'RW ∪ G'PWR ∪ G'PRW
  return Acyclic(Dep')
```

#### 3.3.4 SISolver — SAT 编码

**图 A 和图 B：**

```
图 A (knownGraphA): SO, WR, WW, PR_WR 边
图 B (knownGraphB): RW, PR_RW 边
```

**SAT 编码步骤：**

```
SISolver(history, graph, constraints):
  1. createKnownGraph(history, knownGraphA) → graphA
     对每条已知边创建 Lit 变量
  2. createKnownGraph(history, knownGraphB) → graphB
  3. 构建可达性矩阵:
     matA = MatrixGraph(graphA)
     matAC = reduceEdges(matA.union(matA.composition(graphB)))
  4. getKnownEdges(graphA, graphB, matAC) → knownEdges
     AC 中的直达边（来自 A 或 A∘B 合成路径）
  5. getUnknownEdges(graphA, graphB, matAC, solver) → unknownEdges
     不在 AC 中的边（用于 acyclicity 约束）
  6. addConstraints(constraints, graphA, graphB)
     对每个 ⟨either, or⟩:
       encode as (either AND ¬or) OR (or AND ¬either)
       each edge → Lit variable
  7. 全部加入 MonoSAT:
     monoGraph.addEdge(from, to, lit) for all known/unknown edges
     solver.assertTrue(monoGraph.acyclic())
  8. solver.solve() → SAT/UNSAT
     UNSAT → 有环，历史不满足 SER
     SAT → 无环，历史满足 SER
```

**PR_RW 边的特殊处理：**

```
PR_RW 边不参与 A∘B 合成！
原因：PR_RW 是 anti-dependency（reader → writer），不应通过 A∘B 推导。

在 getUnknownEdges 中:
  - 跳过 PR_RW 边（不在 graphB 的 RW 路径中）
  - PR_RW 边通过 addConstraints 或直接加入 knownEdges

在 getKnownEdges 中:
  - PR_RW 边若不在 AC 中，通过约束系统处理
```

### 3.4 SI → SER 转换 (history/transformers/)

**`SnapshotIsolationToSerializable.java` — 拆分读写事务：**

```
SI → SER 转换策略：
  对于每个只读事务 T_read:
    保持不变
  对于每个读写事务 T_rw:
    拆分为: T_read(RO部分) + T_write(WO部分)
    通过额外的 key-value 对引入冲突来模拟串行化

  例如: T₁: R(x) W(y) → 拆分为:
    T₁a: R(x)     (RO 事务)
    T₁b: W(y)     (WO 事务，依赖 T₁a)
```

---

## 四、使用方法

### 4.1 命令行接口

```bash
# SER 模式验证（默认）
java -jar polysi.jar verifier audit -m SER input.cobra

# SI 模式验证（跳过 PR_* 推导）
java -jar polysi.jar verifier audit -m SI input.cobra

# 输出 DOT 格式冲突图
java -jar polysi.jar verifier audit -o dot input.cobra

# 统计信息
java -jar polysi.jar verifier stat input.elle

# 格式转换
java -jar polysi.jar verifier convert -i Elle -o Text input.elle output.txt

# 打印历史内容
java -jar polysi.jar verifier dump input.cobra
```

### 4.2 Java API

```java
// 加载历史
HistoryLoader<String, Integer> loader = new ElleHistoryLoader("history.elle");
SIVerifier<String, Integer> verifier = new SIVerifier<>(loader);

// 切换模式
SIVerifier.setVerifySer(false);  // SI 模式
SIVerifier.setVerifySer(true);   // SER 模式（默认）

// 验证
boolean accepted = verifier.audit();

// 输出格式
SIVerifier.setDotOutput(true);   // DOT 格式
SIVerifier.setCoalesceConstraints(true);  // 合并约束
```

---

## 五、测试案例设计

### 5.1 设计原则

测试案例分为两类：

1. **SI 满足性测试** — 历史应被 SI 接受
2. **SER 可转化性测试** — 历史能否从 SI 转化为 SER

### 5.2 测试覆盖维度

| 维度 | 说明 | 测试点 |
|------|------|--------|
| 基础 WW 冲突 | 两个事务写同一 key | WW ordering |
| 基础 RW 冲突 | T1 读 X，T2 写 X | RW anti-dependency |
| 谓词 flip | 谓词满足状态改变 | Flip witness |
| PR_WR 推导 | flip witness → reader | MaxCand |
| PR_RW 推导 | reader → WW winner | Δμ condition |
| 环检测 | SER 违规历史 | PR_* 边组合形成环 |
| SI 模式 | 跳过 PR_* | 纯拓扑排序 |

---

## 六、文件清单

```
artifact/PolySI/
├── build.gradle                    # Gradle 构建配置
├── src/main/java/
│   ├── Main.java                   # CLI 入口 (picocli)
│   ├── Launcher.java               # JarClassLoader 启动器
│   ├── graph/
│   │   ├── Edge.java              # 边数据结构
│   │   ├── EdgeType.java          # 边类型枚举
│   │   ├── KnownGraph.java        # 已知依赖图
│   │   └── MatrixGraph.java       # 高效矩阵图
│   ├── history/
│   │   ├── Event.java             # 事件（READ/WRITE/PREDICATE_READ）
│   │   ├── History.java           # 历史容器
│   │   ├── Transaction.java       # 事务
│   │   ├── Session.java           # 会话
│   │   ├── HistoryLoader.java     # 加载器接口
│   │   ├── HistoryParser.java     # 解析器接口
│   │   ├── HistoryConverter.java  # 转换器接口
│   │   ├── HistoryDumper.java     # 输出器接口
│   │   ├── HistoryTransformer.java# 变换器接口
│   │   ├── InvalidHistoryError.java# 错误类
│   │   ├── loaders/
│   │   │   ├── CobraHistoryLoader.java    # Cobra 二进制格式
│   │   │   ├── DBCopHistoryLoader.java   # DBCop 二进制格式
│   │   │   ├── ElleHistoryLoader.java     # Elle 文本格式
│   │   │   ├── TextHistoryLoader.java     # 简单文本格式
│   │   │   └── Utils.java                 # 转换工具
│   │   └── transformers/
│   │       ├── Identity.java              # 恒等变换
│   │       └── SnapshotIsolationToSerializable.java  # SI→SER 转换
│   ├── verifier/
│   │   ├── SIVerifier.java        # 主验证器（含 PR_* 推导）
│   │   ├── SISolver.java          # SAT 求解器（MonoSAT）
│   │   ├── SISolver2.java         # SAT 求解器替代实现
│   │   ├── SIConstraint.java      # 约束数据结构
│   │   ├── SIEdge.java            # 约束中的边
│   │   ├── Pruning.java           # 约束剪枝
│   │   └── Utils.java             # 验证器工具
│   └── util/
│       ├── Profiler.java          # 性能分析
│       ├── QuadConsumer.java      # 函数接口
│       ├── TriConsumer.java       # 函数接口
│       └── UnimplementedError.java# 错误类
├── src/test/java/
│   ├── TestLoader.java            # 测试用 HistoryLoader
│   ├── TestVerifier.java          # 核心验证测试
│   ├── TestMatrixGraph.java       # 矩阵图测试
│   ├── TestSI2SER.java            # SI→SER 转换测试
│   └── verifier/
│       ├── SIVerifierPredicateTest.java        # PR_* 单元测试
│       └── SIVerifierPredicateIntegrationTest.java  # PR_* 集成测试
└── polysi-[version].jar           # 构建产物
```

---

## 七、关键算法索引

| 算法 | 位置 | 说明 |
|------|------|------|
| PR_WR 推导 | `SIVerifier.refreshDerivedPredicateEdges()` Phase 1 | 基于 MaxCand 选择 flip witness |
| PR_RW 推导 | `SIVerifier.refreshDerivedPredicateEdges()` Phase 2 | 基于 PR_WR + WW + Δμ |
| Flip Witness 判断 | `SIVerifier.isFlipWitness()` | match(prev) XOR match(w) |
| MaxCand 选择 | `SIVerifier.maxCand()` | AR-order 唯一最大候选 |
| Δμ 条件 | `SIVerifier.deltaMu()` | flip 是否改变可见性 |
| 观察点确定 | `SIVerifier.resolveObservationIndex()` | predicate read 看到哪个写 |
| 写全序建立 | `SIVerifier.buildConfirmedWriteOrder()` | 唯一拓扑排序 |
| 约束生成 | `SIVerifier.generateConstraints()` | WW/RW 互斥选择 |
| 剪枝 | `Pruning.pruneConstraints()` | 可达性 + 冲突检测 |
| SAT 编码 | `SISolver.<init>()` | 图 A/B + 约束字面量 |
| SI 拓扑验证 | `SIVerifier.checkSIWithTopologicalSort()` | Kahn 算法 |
