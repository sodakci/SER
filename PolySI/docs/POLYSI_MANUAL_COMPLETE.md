# PolySI 完整技术手册

**版本**: 1.0.0  
**日期**: 2026-04-12  
**项目**: PolySI - 可串行化快照隔离的黑盒历史检测器  

---

## 目录

1. [项目概述](#1-项目概述)
2. [核心概念与术语](#2-核心概念与术语)
3. [系统架构](#3-系统架构)
4. [数据模型](#4-数据模型)
5. [历史加载器](#5-历史加载器)
6. [图结构](#6-图结构)
7. [验证器核心](#7-验证器核心)
8. [谓词读边推导](#8-谓词读边推导)
9. [约束生成与剪枝](#9-约束生成与剪枝)
10. [SAT求解](#10-sat求解)
11. [SI到SER转换](#11-si到ser转换)
12. [命令行接口](#12-命令行接口)
13. [Java API](#13-java-api)
14. [构建与安装](#14-构建与安装)
15. [测试方案](#15-测试方案)
16. [文件清单](#16-文件清单)

---

## 1. 项目概述

### 1.1 项目背景

PolySI（Poly Serializable Isolation）是一个基于 **Snapshot Isolation (SI)** 的黑盒历史检测器，专门用于检测数据库执行历史是否满足 **Serializable Snapshot Isolation (SER/PSI)**。

该工具在 **PVLDB 2023** 发表，是数据库理论与工程实践结合的产物。

### 1.2 核心功能

PolySI 的核心功能是验证给定的事务历史记录是否满足可串行化快照隔离级别。具体包括：

1. **SI 模式验证**：使用拓扑排序快速验证纯 SI 约束（GSO ∪ GWR 无环）
2. **SER 模式验证**：通过谓词感知的 PR_* 边推导，精确检测 predicate-aware SER 违规
3. **格式转换**：支持多种历史格式之间的转换
4. **SI→SER 转换**：将 SI 历史转换为 SER 历史

### 1.3 技术特点

| 特点 | 描述 |
|------|------|
| 黑盒检测 | 无需了解数据库内部实现，仅分析历史记录 |
| 谓词感知 | 支持谓词读（Predicate Read）的 SER 验证 |
| SAT 求解 | 使用 MonoSAT 求解器进行图无环性约束求解 |
| 高性能 | 矩阵图结构 + 可达性剪枝优化 |
| 多格式支持 | Cobra、DBCop、Elle、Text 四种格式 |

### 1.4 依赖组件

```
PolySI
├── MonoSAT           # SAT Modulo Theory 求解器
├── Guava             # Google Java 工具库
├── Apache Commons    # Apache 通用工具
├── Picocli           # 命令行参数解析
└── RoaringBitmap     # 高效位图实现
```

---

## 2. 核心概念与术语

### 2.1 隔离级别基础

**Snapshot Isolation (SI)**：
- 每个事务从一个一致的快照开始读取数据
- 写冲突检测：两个并发事务不能写同一个 key
- 可能违反串行化：Write Skew 问题

**Serializable Snapshot Isolation (SER/PSI)**：
- 在 SI 基础上增加谓词读冲突检测
- 保证真正的串行化等价性
- PolySI 检测的核心目标

### 2.2 事务历史结构

```
History（历史记录）
├── Session[]         # 会话序列
│   ├── Session 0    # 客户端会话
│   │   ├── Transaction 0
│   │   │   ├── Event: READ(x, 10)
│   │   │   └── Event: WRITE(y, 20)
│   │   └── Transaction 1
│   │       └── Event: READ(y, 20)
│   └── Session 1
│       └── Transaction 0
│           └── Event: WRITE(x, 15)
```

### 2.3 事件类型

| 事件类型 | 符号 | 说明 |
|----------|------|------|
| `READ` | `R` | 点读：读取单个 key 的值 |
| `WRITE` | `W` | 写：写入单个 key 的值 |
| `PREDICATE_READ` | `P` | 谓词读：读取满足谓词条件的记录集合 |

### 2.4 依赖边类型

依赖边是构建事务间偏序关系的核心数据结构：

| 边类型 | 符号 | 说明 | 放入哪个图 |
|--------|------|------|------------|
| **SO** (Session Order) | `T₁ → T₂` | 同一会话中 T₁ 在 T₂ 之前执行 | knownGraphA |
| **WR** (Write-Read) | `T₁ → T₂` | T₁ 写了 key，T₂ 读了 T₁ 的值 | knownGraphA |
| **WW** (Write-Write) | `T₁ → T₂` | T₁ 和 T₂ 都写了同一 key，T₁ 的写更早 | knownGraphA |
| **RW** (Read-Write) | `T₁ → T₂` | T₁ 读了某值，T₂ 写了同一 key（反依赖） | knownGraphB |
| **PR_WR** | `T₁ → T₂` | Predicate Write-Read：T₁ 是 flip witness，T₂ 的 predicate read 受其影响 | knownGraphA |
| **PR_RW** | `T₁ → T₂` | Predicate Read-Write：T₁ 的 predicate read 阻止了 T₂ 的写入可见 | knownGraphB |

### 2.5 关键术语

**Flip Witness（翻转见证者）**：
- 定义：写事件 w 是 predicate P 的 flip witness，当且仅当 `match(prev(w), P) XOR match(w, P) = true`
- 即 w 及其前驱对 predicate P 的满足状态不同
- 语义：w 的写入改变了 predicate 的真值

**Observation Point（观察点）**：
- predicate read S 看到哪个写版本的索引
- 用于确定 S 受到哪些 flip witness 的影响

**MaxCand（最大候选）**：
- AR-order（所有已知边确定的偏序）下唯一的最大 flip witness
- 用于选择最新的有效 flip witness

**Δμ 条件**：
- 判断 T 的 flip 是否改变 S 对 U 的可见性
- 形式：`Δμ(T, S, U, x) = true` 当且仅当：
  - T 是 flip witness AND
  - (x ∉ result AND T 的 flip 使 x 满足 P) OR (S 的可见 writer 被 T 支配)

---

## 3. 系统架构

### 3.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                        Main (CLI 入口)                          │
│             verifier audit | stat | convert | dump               │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│                     HistoryLoader (加载器)                        │
│  CobraHistoryLoader │ DBCopHistoryLoader │ ElleHistoryLoader    │
│                   └──── TextHistoryLoader ────┘                  │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│                    KnownGraph (依赖图构建)                        │
│                                                                 │
│   knownGraphA: SO, WR, WW, PR_WR 边                             │
│   knownGraphB: RW, PR_RW 边                                     │
│   readFrom: 点读的写来源                                        │
└──────────────────────────────┬──────────────────────────────────┘
                               │
               ┌───────────────┴───────────────┐
               │                               │
┌──────────────▼──────────────┐  ┌───────────▼─────────────┐
│          SER 模式            │  │         SI 模式           │
│                             │  │                          │
│  generateConstraints        │  │  checkSIWith            │
│  (WW/RW 互斥选择约束)        │  │  TopologicalSort        │
│          ↓                  │  │  (Kahn 算法)            │
│  refreshDerivedPredicateEdges│  │                          │
│  (PR_WR/PR_RW 推导)         │  └──────────────────────────┘
│          ↓                  │
│  pruneConstraints           │
│  (可达性剪枝)               │
│          ↓                  │
│  SISolver                   │
│  (MonoSAT SAT 编码)         │
│          ↓                  │
│  solver.solve()             │
│  (acyclic → ACCEPT)        │
└─────────────────────────────┘
```

### 3.2 数据流

```
输入历史文件
    │
    ▼
HistoryLoader 解析
    │
    ▼
verifyInternalConsistency()  ──► 内部一致性检查
    │
    ▼
KnownGraph 构建  ──► SO/WR 边 + 写索引
    │
    ▼
generateConstraints()  ──► WW/RW 互斥选择约束
    │
    ▼
refreshDerivedPredicateEdges()  ──► PR_WR/PR_RW 边推导
    │
    ▼
pruneConstraints()  ──► 可达性分析 + 约束简化
    │
    ▼
SISolver SAT 编码  ──► 图 A/B + 约束字面量
    │
    ▼
solver.solve()  ──► acyclicity 约束求解
    │
    ▼
REJECT / ACCEPT  ◄── 冲突信息输出
```

### 3.3 图 A 和图 B

MonoSAT SAT 编码中的两个核心图结构：

| 图 | 包含边 | 用途 |
|----|--------|------|
| **图 A** | SO, WR, WW, PR_WR | 直接优先关系 + 可达性计算 |
| **图 B** | RW, PR_RW | 反依赖关系（用于 A∘B 合成） |
| **图 C = A∘B** | 传递依赖 | 通过图 A 和图 B 合成得到 |

**A∘B 合成**：
- 如果 P→Q 在 A 中，且 Q→R 在 B 中
- 则 P→R 在 C 中（传递边）

---

## 4. 数据模型

### 4.1 History 类

历史记录的主容器，管理所有会话和事务。

```java
public class History<KeyType, ValueType> {
    private final Map<Long, Session<KeyType, ValueType>> sessions;
    private final Map<Long, Transaction<KeyType, ValueType>> transactions;
    private final Set<Pair<KeyType, ValueType>> writes;  // 已提交的写

    // 核心方法
    public Session<KeyType, ValueType> addSession(long id);
    public Transaction<KeyType, ValueType> addTransaction(Session, long id);
    public Event<KeyType, ValueType> addEvent(Transaction, EventType, KeyType, ValueType);
    public Event<KeyType, ValueType> addPredicateReadEvent(Transaction, PredEval, Collection<PredResult>);

    // 查询方法
    public Collection<Session<KeyType, ValueType>> getSessions();
    public Collection<Transaction<KeyType, ValueType>> getTransactions();
    public Collection<Event<KeyType, ValueType>> getEvents();
}
```

### 4.2 Session 类

会话，表示来自同一客户端的一系列事务。

```java
public class Session<KeyType, ValueType> {
    final long id;                                    // 会话 ID
    private final List<Transaction<KeyType, ValueType>> transactions;
}
```

### 4.3 Transaction 类

事务，包含一系列事件。

```java
public class Transaction<KeyType, ValueType> {
    public enum TransactionStatus {
        ONGOING,    // 正在进行
        COMMIT      // 已提交
    }

    final long id;                                    // 事务 ID
    final Session<KeyType, ValueType> session;        // 所属会话
    final List<Event<KeyType, ValueType>> events;     // 事件列表
    private TransactionStatus status;                 // 状态
}
```

### 4.4 Event 类

事件，事务中的基本操作单元。

```java
public class Event<KeyType, ValueType> {
    public enum EventType {
        READ,           // 点读
        WRITE,          // 写
        PREDICATE_READ  // 谓词读
    }

    // 通用字段
    final Transaction<KeyType, ValueType> transaction;  // 所属事务
    final EventType type;                               // 事件类型
    final KeyType key;                                  // key（READ/WRITE）
    final ValueType value;                              // value（READ/WRITE）

    // 谓词读专用字段
    final PredEval<KeyType, ValueType> predicate;       // 谓词函数
    final Collection<PredResult<KeyType, ValueType>> predResults;  // 结果列表
}
```

### 4.5 Edge 类

依赖边，表示事务间的优先关系。

```java
public class Edge<KeyType> {
    final EdgeType type;    // 边类型
    final KeyType key;     // 关联的 key
}
```

---

## 5. 历史加载器

### 5.1 加载器接口

```java
public interface HistoryLoader<KeyType, ValueType> {
    History<KeyType, ValueType> loadHistory();
}

public interface HistoryParser<KeyType, ValueType> extends HistoryLoader<KeyType, ValueType> {
    History<KeyType, ValueType> convertFrom(History<?, ?> history);
    void dumpHistory(History<KeyType, ValueType> history);
}
```

### 5.2 支持的格式

#### 5.2.1 Text 格式

简单的文本格式，每行一个操作：

```
# 格式: <r/w>(key, value, session_id, transaction_id)
# r = read, w = write

# 示例：Session 0, Txn 0 写 x=10
w(1, 10, 0, 0)

# Session 0, Txn 1 读 x=10
r(1, 10, 0, 1)

# Session 0, Txn 1 写 x=20
w(1, 20, 0, 1)

# Session 1, Txn 0 读 y=5
r(2, 5, 1, 0)
```

**特点**：
- 简单易读，适合手工编写测试用例
- 自动为每个新 key 添加初始写（key=0）
- key 和 value 都是 64 位整数

#### 5.2.2 Elle 格式

EDN 格式的列表操作历史：

```
{:type :ok, :f :txn, :value [[:append 1 100] [:r 2 nil]], :process 0}
{:type :ok, :f :txn, :value [[:append 2 200]], :process 1}
{:type :ok, :f :txn, :value [[:r 1 [100]]], :process 0}
```

**特点**：
- 使用列表（List）而非 key-value 对
- `:append` 表示写操作
- `:r` 表示读操作，可指定读取的列表版本
- 支持 nil（空列表）

**ElleValue 内部表示**：

```java
public static class ElleValue {
    Integer lastElement;  // 列表最后一个元素（用于比较）
    List<Integer> list;   // 完整列表（读取时）
}
```

#### 5.2.3 Cobra 格式

二进制格式，目录中包含多个 `T*.log` 文件：

```
/path/to/history/
├── T0.log    # Session 0 的日志
├── T1.log    # Session 1 的日志
└── T2.log    # Session 2 的日志
```

**特点**：
- 高效的二进制格式
- 适合大规模历史记录
- 每个会话一个文件

#### 5.2.4 DBCop 格式

单个二进制文件 `history.bincode`：

**特点**：
- LittleEndian 编码
- 紧凑存储
- 适合特定工具生成的历史

### 5.3 格式转换

```bash
# Elle → Text
java -jar polysi.jar convert -f ELLE -o TEXT input.elle output.txt

# Cobra → DBCop
java -jar polysi.jar convert -f COBRA -o DBCOP input_dir/ output.bincode
```

### 5.4 历史导出

```bash
# 打印历史内容
java -jar polysi.jar dump -t TEXT input.txt

# 统计信息
java -jar polysi.jar stat input.cobra
```

---

## 6. 图结构

### 6.1 KnownGraph 类

已知依赖图的构建和管理：

```java
public class KnownGraph<KeyType, ValueType> {
    // 读来源信息：每个事务读了哪些写
    private final MutableValueGraph<Transaction, Collection<Edge<KeyType>>> readFrom;

    // 已知图 A：SO, WR, WW, PR_WR
    private final MutableValueGraph<Transaction, Collection<Edge<KeyType>>> knownGraphA;

    // 已知图 B：RW, PR_RW
    private final MutableValueGraph<Transaction, Collection<Edge<KeyType>>> knownGraphB;

    // 写索引：(key, value) → WriteRef
    private final Map<Pair<KeyType, ValueType>, WriteRef<KeyType, ValueType>> writes;

    // 事务写索引：(txn, key) → [write indices]
    private final Map<Pair<Transaction, KeyType>, List<Integer>> txnWrites;

    // 谓词观察列表
    private final List<PredicateObservation<KeyType, ValueType>> predicateObservations;
}
```

**KnownGraph 构造函数流程**：

```
1. 为每个事务创建节点（knownGraphA, knownGraphB, readFrom）
    │
    ▼
2. 添加 SO 边（同一会话中事务的顺序）
    │
    ▼
3. 构建写索引（writes, txnWrites）
    │
    ▼
4. 从点读添加 WR 边
    │
    ▼
5. 收集谓词读观察（不生成 PR_* 边）
```

### 6.2 MatrixGraph 类

高效的矩阵图实现，使用 RoaringBitmap 存储邻接关系：

```java
public class MatrixGraph<T> implements MutableGraph<T> {
    private final ImmutableBiMap<T, Integer> nodeMap;   // 节点 → 索引映射
    private final RoaringBitmap[] adjacency;            // 邻接矩阵
}
```

**核心方法**：

| 方法 | 功能 | 时间复杂度 |
|------|------|------------|
| `nodes()` | 获取所有节点 | O(1) |
| `edges()` | 获取所有边 | O(n²) |
| `successors(n)` | 获取 n 的后继节点 | O(outDegree) |
| `predecessors(n)` | 获取 n 的前驱节点 | 未实现 |
| `union(MatrixGraph)` | 图并集 | O(n²) |
| `composition(MatrixGraph)` | 图合成（A∘B） | O(n³) |
| `reachability()` | 计算传递闭包 | O(n²·avgDegree) |
| `hasLoops()` | 检测环 | O(n+m) |
| `topologicalSort()` | 拓扑排序 | O(n+m) |

**可达性矩阵计算**：

```java
public MatrixGraph<T> reachability() {
    var result = allNodesBfs();  // BFS 计算传递闭包
    for (int i = 0; i < nodeMap.size(); i++) {
        result.set(i, i);  // 自环（可达性矩阵包含对角线）
    }
    return result;
}
```

### 6.3 EdgeType 枚举

```java
public enum EdgeType {
    WW,      // Write-Write
    RW,      // Read-Write（反依赖）
    WR,      // Write-Read
    SO,      // Session Order
    PR_WR,   // Predicate Write-Read（flip witness → reader）
    PR_RW    // Predicate Read-Write（reader → post-read writer）
}
```

---

## 7. 验证器核心

### 7.1 SIVerifier 类

主验证器类，协调整个验证流程：

```java
public class SIVerifier<KeyType, ValueType> {
    // SER 模式开关（默认 true）
    // true:  推导 PR_* 边，使用 SAT 编码
    // false: 纯 SI 拓扑排序
    private static boolean verifySer = true;

    // 约束合并开关（默认 true）
    // true:  合并相同事务对的约束
    // false: 每个读写冲突单独约束
    private static boolean coalesceConstraints = true;

    // DOT 输出开关
    private static boolean dotOutput = false;

    private final History<KeyType, ValueType> history;

    public boolean audit() { ... }
}
```

### 7.2 SER 模式验证流程

```java
public boolean audit() {
    // 1. 内部一致性检查
    if (!Utils.verifyInternalConsistency(history)) {
        return false;
    }

    // 2. 构建已知依赖图
    var graph = new KnownGraph<>(history);

    // 3. 生成 WW/RW 约束
    var constraints = generateConstraints(history, graph);

    // 4. 推导 PR_* 边
    refreshDerivedPredicateEdges(history, graph);

    // 5. 约束剪枝
    var hasLoop = Pruning.pruneConstraints(graph, constraints, history);

    // 6. 再次推导 PR_* 边
    refreshDerivedPredicateEdges(history, graph);

    // 7. SAT 编码与求解
    var solver = new SISolver<>(history, graph, constraints);
    return solver.solve();
}
```

### 7.3 SI 模式验证流程

当 `verifySer=false` 时，使用简单的拓扑排序：

```java
private boolean checkSIWithTopologicalSort(KnownGraph<KeyType, ValueType> graph) {
    var siGraph = graph.getKnownGraphA();  // 只包含 SO, WR, WW
    var inDegree = new HashMap<Transaction, Integer>();

    // 初始化入度
    for (var n : siGraph.nodes()) {
        inDegree.put(n, 0);
    }
    for (var e : siGraph.edges()) {
        inDegree.merge(e.target(), 1, Integer::sum);
    }

    // Kahn 算法
    var queue = new ArrayDeque<>(
        siGraph.nodes().stream()
            .filter(n -> inDegree.get(n) == 0)
            .collect(Collectors.toList())
    );

    int processed = 0;
    while (!queue.isEmpty()) {
        var node = queue.poll();
        processed++;
        for (var succ : siGraph.successors(node)) {
            int d = inDegree.get(succ) - 1;
            inDegree.put(succ, d);
            if (d == 0) {
                queue.add(succ);
            }
        }
    }

    // 有环则 processed < nodes.size()
    return processed == siGraph.nodes().size();
}
```

### 7.4 内部一致性检查

`Utils.verifyInternalConsistency()` 检查：

1. **点读来源正确性**：
   - 读的值必须来自已提交的写
   - 必须读取最新的写版本
   - 不能读自己事务中未来的写

2. **谓词读结果正确性**：
   - 结果中的每条记录都必须满足谓词
   - 满足谓词的记录不能被遗漏
   - 不能读自己未来事务的写

---

## 8. 谓词读边推导

### 8.1 PR_* 边的重要性

在没有谓词读的情况下，SI 可以通过简单的拓扑排序验证。但谓词读引入了复杂的依赖关系：

```
T1: W(x, 10)    // 满足 x > 5
T2: P(x > 5)    // 应该看到 x=10
T3: W(x, 3)     // 不满足 x > 5
```

这里 T1 的写入改变了谓词 `x > 5` 的真值，T1 是 **flip witness**。T2 的 predicate read 受 T1 影响，需要建立 PR_WR 边。

### 8.2 refreshDerivedPredicateEdges 算法

```java
static void refreshDerivedPredicateEdges(History history, KnownGraph graph) {
    // 1. 清空已有的 PR_* 边
    graph.clearDerivedPredicateEdges();

    // 2. 构建 per-key 写索引
    var writesByKey = buildWritesByKey(graph);

    // 3. 建立 per-key 确认的写全序
    var orderedWritesByKey = new HashMap<>();
    for (var entry : writesByKey.entrySet()) {
        var order = buildConfirmedWriteOrder(entry.getKey(), entry.getValue(), graph);
        if (order != null) {
            orderedWritesByKey.put(entry.getKey(), order);
        }
    }

    // 4. Phase 1: 推导 PR_WR 边
    for (var obs : observations) {
        var pr = obs.getPredicateReadEvent();
        var b = obs.getTxn();  // predicate read 所在事务

        for (var entry : orderedWritesByKey.entrySet()) {
            var key = entry.getKey();
            var orderedWrites = entry.getValue();

            // 4.1 确定观察点
            int obsIdx = resolveObservationIndex(key, b, pr, orderedWrites, ...);
            if (obsIdx == OBS_UNDETERMINED) continue;

            // 4.2 收集 flip witnesses
            var candidates = new ArrayList<WriteRef>();
            for (int i = 0; i <= obsIdx; i++) {
                var w = orderedWrites.get(i);
                var prev = (i > 0) ? orderedWrites.get(i - 1) : null;
                if (isFlipWitness(w, prev, pr)) {
                    candidates.add(w);
                }
            }

            // 4.3 选择 MaxCand
            var maxFlip = maxCand(candidates, orderedWrites, graph.getKnownGraphA());
            if (maxFlip != null && maxFlip.getTxn() != b) {
                graph.putEdge(maxFlip.getTxn(), b, new Edge<>(EdgeType.PR_WR, key));
            }
        }
    }

    // 5. Phase 2: 推导 PR_RW 边
    for (var emitted : emittedPrWr) {
        var T = emitted.getLeft();      // flip witness writer
        var S = emitted.getMiddle();     // reader (PR_WR 目标)
        var x = emitted.getRight();      // key

        // 对于每个 WW(T → U, x)
        for (var wRef : writesOnKey) {
            var U = wRef.getTxn();
            if (U.equals(T) || U.equals(S)) continue;
            if (!graph.getKnownGraphA().hasEdgeConnecting(T, U)) continue;

            // 检查 Δμ 条件
            if (deltaMu(T, S, U, x, pr, ...)) {
                graph.putEdge(S, U, new Edge<>(EdgeType.PR_RW, x));
            }
        }
    }
}
```

### 8.3 Flip Witness 判断

```java
private static <KeyType, ValueType> boolean isFlipWitness(
        WriteRef<KeyType, ValueType> writeRef,
        WriteRef<KeyType, ValueType> predecessor,
        Event<KeyType, ValueType> predicateReadEvent) {

    // prevMatch: 前驱是否满足谓词
    boolean prevMatch = predecessor != null
            && matchesPredicate(predecessor, predicateReadEvent);

    // curMatch: 当前写是否满足谓词
    boolean curMatch = matchesPredicate(writeRef, predicateReadEvent);

    // flip = prevMatch XOR curMatch
    return prevMatch ^ curMatch;
}

private static <KeyType, ValueType> boolean matchesPredicate(
        WriteRef<KeyType, ValueType> writeRef,
        Event<KeyType, ValueType> predicateReadEvent) {

    var ev = writeRef.getEvent();
    return predicateReadEvent.getPredicate().test(ev.getKey(), ev.getValue());
}
```

### 8.4 MaxCand 选择

```java
private static <KeyType, ValueType> WriteRef<KeyType, ValueType> maxCand(
        List<WriteRef<KeyType, ValueType>> candidates,
        List<WriteRef<KeyType, ValueType>> orderedWrites,
        ValueGraph<Transaction, Collection<Edge<KeyType>>> knownGraphA) {

    if (candidates.isEmpty()) return null;

    WriteRef<KeyType, ValueType> maxCandidate = null;
    boolean unique = true;

    for (var c : candidates) {
        var cTxn = c.getTxn();

        if (maxCandidate == null) {
            maxCandidate = c;
            continue;
        }

        var maxTxn = maxCandidate.getTxn();

        // 检查 AR-order：c 是否严格在 maxCandidate 之后
        boolean cAfterMax = knownGraphA.hasEdgeConnecting(maxTxn, cTxn)
                && !knownGraphA.hasEdgeConnecting(cTxn, maxTxn);
        boolean maxAfterC = knownGraphA.hasEdgeConnecting(cTxn, maxTxn)
                && !knownGraphA.hasEdgeConnecting(maxTxn, cTxn);

        if (cAfterMax && !maxAfterC) {
            // c 在 maxCandidate 之后，更新最大值
            maxCandidate = c;
            unique = true;
        } else if (!maxAfterC && !cAfterMax) {
            // 不可比较，无唯一最大值
            unique = false;
        }
        // else: maxCandidate 支配 c，保持不变
    }

    return unique ? maxCandidate : null;
}
```

### 8.5 Δμ 条件

```java
private static <KeyType, ValueType> boolean deltaMu(
        Transaction T,         // flip-witness writer
        Transaction S,         // reader (PR_WR 源)
        Transaction U,         // WW winner
        KeyType x,
        Event<KeyType, ValueType> predicateRead,
        Map<KeyType, WriteRef<KeyType, ValueType>> resultSourceByKey,
        Map<KeyType, List<WriteRef<KeyType, ValueType>>> orderedWritesByKey,
        KnownGraph<KeyType, ValueType> graph) {

    // 找到 T 在 orderedWrites 中的位置
    var orderedWrites = orderedWritesByKey.get(x);
    int tIdx = -1;
    for (int i = 0; i < orderedWrites.size(); i++) {
        if (orderedWrites.get(i).getTxn().equals(T)) {
            tIdx = i;
            break;
        }
    }
    if (tIdx < 0) return false;

    var prevT = tIdx > 0 ? orderedWrites.get(tIdx - 1) : null;

    // (a) T 必须是 flip witness
    if (!isFlipWitness(orderedWrites.get(tIdx), prevT, predicateRead)) {
        return false;
    }

    // (b) 条件 (i): x 不在结果中 AND T 的 flip 使 x 满足 P
    boolean inResult = resultSourceByKey.containsKey(x);
    if (!inResult) {
        boolean prevMatch = prevT != null && matchesPredicate(prevT, predicateRead);
        boolean tMatch = matchesPredicate(orderedWrites.get(tIdx), predicateRead);
        if (!prevMatch && tMatch) {
            return true;  // T 的 flip 使 x 满足谓词
        }
    }

    // (c) 条件 (ii): S 的可见 writer 被 T 支配
    var wRef = resultSourceByKey.get(x);
    if (wRef != null) {
        var W = wRef.getTxn();
        // W 被 T 支配 = T AR→ W
        if (graph.getKnownGraphA().hasEdgeConnecting(T, W)
                && !graph.getKnownGraphA().hasEdgeConnecting(W, T)) {
            return true;
        }
    }

    return false;
}
```

### 8.6 观察点确定

```java
private static <KeyType, ValueType> int resolveObservationIndex(
        KeyType key,
        Transaction b,                    // predicate read 所在事务
        Event<KeyType, ValueType> pr,   // predicate read 事件
        List<WriteRef<KeyType, ValueType>> orderedWrites,
        Map<KeyType, WriteRef<KeyType, ValueType>> resultSourceByKey,
        KnownGraph<KeyType, ValueType> graph) {

    // Case 1: key 在谓词结果中
    var resultSource = resultSourceByKey.get(key);
    if (resultSource != null) {
        int idx = orderedWrites.indexOf(resultSource);
        return (idx >= 0) ? idx : OBS_UNDETERMINED;
    }

    // Case 2: b 自己写了这个 key
    var bWriteIndices = graph.getTxnWrites().getOrDefault(Pair.of(b, key), ...);
    if (!bWriteIndices.isEmpty()) {
        int prIdx = b.getEvents().indexOf(pr);

        // 找 pr 之前最近的一次写
        int latestBefore = -1;
        for (int idx : bWriteIndices) {
            if (idx < prIdx && idx > latestBefore) {
                latestBefore = idx;
            }
        }

        if (latestBefore >= 0) {
            for (int i = 0; i < orderedWrites.size(); i++) {
                var w = orderedWrites.get(i);
                if (w.getTxn() == b && w.getIndex() == latestBefore) {
                    return i;
                }
            }
            return OBS_UNDETERMINED;
        }

        // pr 在所有写之前
        for (int i = 0; i < orderedWrites.size(); i++) {
            if (orderedWrites.get(i).getTxn() == b) {
                return (i > 0) ? i - 1 : OBS_INITIAL_STATE;
            }
        }
        return OBS_UNDETERMINED;
    }

    // Case 3: 保守策略：找最近的有直接边到 b 的写
    for (int i = orderedWrites.size() - 1; i >= 0; i--) {
        var w = orderedWrites.get(i);
        if (w.getTxn() == b) continue;
        if (graph.getKnownGraphA().hasEdgeConnecting(w.getTxn(), b)) {
            return i;
        }
    }
    return OBS_UNDETERMINED;
}
```

---

## 9. 约束生成与剪枝

### 9.1 约束概念

对于每个 key 上的写事务对 (T₁, T₂)，它们的 WW 顺序是不确定的。约束表达了这种互斥关系：

```java
SIConstraint<KeyType, ValueType>
├── edges1: List<SIEdge>   // 如果选择 T₁ WW→ T₂ 的边
├── edges2: List<SIEdge>   // 如果选择 T₂ WW→ T₁ 的边
├── txn1: Transaction      // 事务 1
├── txn2: Transaction      // 事务 2
└── id: int
```

**约束语义**：
```
(EITHER edges1) OR (OR edges2)
```
即：要么 T₁ 的写在 T₂ 之前，要么 T₂ 的写在 T₁ 之前，且两者互斥。

### 9.2 合并约束（Coalescing）

当多个约束涉及相同的 (T₁, T₂) 对时，可以合并以减少约束数量：

```java
// 合并前：多个约束
constraint(T1→T3, {T1 WW→T3})
constraint(T1→T3, {T1 WW→T3, T2 RW→T3})

// 合并后：一个约束
constraint(T1→T3, {T1 WW→T3})
           (T2→T3, {T2 RW→T3})
```

### 9.3 约束生成算法

```java
private static <KeyType, ValueType> Collection<SIConstraint<KeyType, ValueType>>
        generateConstraintsCoalesce(History history, KnownGraph graph) {

    var writes = new HashMap<KeyType, Set<Transaction>>();

    // 收集每个 key 的所有写事务
    history.getEvents().stream()
        .filter(e -> e.getType() == EventType.WRITE)
        .forEach(ev -> {
            writes.computeIfAbsent(ev.getKey(), k -> new HashSet<>())
                  .add(ev.getTransaction());
        });

    var constraintEdges = new HashMap<Pair<Transaction, Transaction>, Collection<SIEdge>>();

    // 对每个 key 上的写对生成约束边
    for (var p : writes.entrySet()) {
        var key = p.getKey();
        var list = new ArrayList<>(p.getValue());
        for (int i = 0; i < list.size(); i++) {
            for (int j = i + 1; j < list.size(); j++) {
                var a = list.get(i);
                var c = list.get(j);
                // edges1: a WW→c | edges2: c WW→a
                constraintEdges.get(Pair.of(a, c)).add(new SIEdge(a, c, EdgeType.WW, key));
                constraintEdges.get(Pair.of(c, a)).add(new SIEdge(c, a, EdgeType.WW, key));
            }
        }
    }

    // 添加 RW 边到约束
    for (var a : history.getTransactions()) {
        for (var b : readFrom.successors(a)) {
            for (var edge : readFrom.edgeValue(a, b).get()) {
                for (var c : writes.get(edge.getKey())) {
                    if (a == c || b == c) continue;
                    // 如果 a WW→c，则 b RW→c 冲突
                    // 所以要么 a WW→c，要么 b RW→c
                    constraintEdges.get(Pair.of(a, c))
                        .add(new SIEdge(b, c, EdgeType.RW, edge.getKey()));
                }
            }
        }
    }

    // 构建约束
    var constraints = new HashSet<SIConstraint<KeyType, ValueType>>();
    for (var entry : constraintEdges.entrySet()) {
        var pair = entry.getKey();
        var a = pair.getLeft();
        var c = pair.getRight();
        // 构建互斥约束
        constraints.add(new SIConstraint<>(
            entry.getValue(),                    // edges1
            constraintEdges.get(Pair.of(c, a)), // edges2
            a, c, constraintId++
        ));
    }

    return constraints;
}
```

### 9.4 约束剪枝

通过可达性分析提前确定某些约束的取值：

```java
public static <KeyType, ValueType> boolean pruneConstraints(
        KnownGraph knownGraph,
        Collection<SIConstraint<KeyType, ValueType>> constraints,
        History<KeyType, ValueType> history) {

    int rounds = 1;
    while (!hasCycle) {
        // 1. 重新推导 PR_* 边
        SIVerifier.refreshDerivedPredicateEdges(history, knownGraph);

        // 2. 构建可达性矩阵
        var graphA = new MatrixGraph<>(knownGraph.getKnownGraphA().asGraph());
        var graphB = new MatrixGraph<>(knownGraph.getKnownGraphB().asGraph(), ...);
        var reachability = graphA.union(graphA.composition(graphB)).reachability();

        // 3. 剪枝约束
        for (var c : constraints) {
            // 检查 edges1 是否与可达性冲突
            var conflict = checkConflict(c.getEdges1(), reachability, knownGraph);
            if (conflict.isPresent()) {
                // edges1 会导致环，只能选 edges2
                addToKnownGraph(knownGraph, c.getEdges2());
                constraints.remove(c);
                continue;
            }

            // 检查 edges2 是否与可达性冲突
            conflict = checkConflict(c.getEdges2(), reachability, knownGraph);
            if (conflict.isPresent()) {
                addToKnownGraph(knownGraph, c.getEdges1());
                constraints.remove(c);
            }
        }

        rounds++;
    }

    return hasCycle;
}
```

### 9.5 冲突检测

```java
private static <KeyType, ValueType> Optional<SIEdge<KeyType, ValueType>> checkConflict(
        Collection<SIEdge<KeyType, ValueType>> edges,
        MatrixGraph<Transaction<KeyType, ValueType>> reachability,
        KnownGraph<KeyType, ValueType> knownGraph) {

    for (var e : edges) {
        switch (e.getType()) {
        case WW:
            // 如果 edges1 中有 T₁ WW→T₂，但可达性显示 T₂→*→T₁
            if (reachability.hasEdgeConnecting(e.getTo(), e.getFrom())) {
                return Optional.of(e);  // 冲突！
            }
            break;

        case RW:
        case PR_RW:
            // 如果 T₁ RW→T₂，但可达性显示存在 Tₓ 使得 T₂→Tₓ 且 T₁→Tₓ
            for (var n : knownGraph.getKnownGraphA().predecessors(e.getFrom())) {
                if (reachability.hasEdgeConnecting(e.getTo(), n)) {
                    return Optional.of(e);  // 冲突！
                }
            }
            break;
        }
    }

    return Optional.empty();
}
```

---

## 10. SAT求解

### 10.1 SISolver 概述

SISolver 使用 MonoSAT 进行 SAT Modulo Graphs 求解：

```java
class SISolver<KeyType, ValueType> {
    private final Solver solver = new Solver();
    private final Map<Lit, Pair<EndpointPair<Transaction>, Collection<Edge>>> knownLiterals;
    private final Map<Lit, SIConstraint<KeyType, ValueType>> constraintLiterals;

    boolean solve() {
        var lits = Stream.concat(
            knownLiterals.keySet().stream(),
            constraintLiterals.keySet().stream()
        ).collect(Collectors.toList());

        return solver.solve(lits);  // 返回 SAT/UNSAT
    }
}
```

### 10.2 SAT 编码流程

```java
SISolver(History history, KnownGraph precedenceGraph, Collection<SIConstraint> constraints) {
    // 1. 重新推导 PR_* 边（Pruning 可能清除了它们）
    SIVerifier.refreshDerivedPredicateEdges(history, precedenceGraph);

    // 2. 创建图 A 和图 B 的字面量
    var graphA = createKnownGraph(history, precedenceGraph.getKnownGraphA());
    var graphB = createKnownGraph(history, precedenceGraph.getKnownGraphB());

    // 3. 构建可达性矩阵
    var matA = new MatrixGraph<>(graphA.asGraph());
    var matAC = Utils.reduceEdges(
        matA.union(matA.composition(new MatrixGraph<>(graphB.asGraph(), matA.getNodeMap()))),
        orderInSession
    );
    var reachability = matAC.reachability();

    // 4. 收集已知边和未知边
    var knownEdges = Utils.getKnownEdges(graphA, graphB, matAC);
    var unknownEdges = Utils.getUnknownEdges(graphA, graphB, reachability, solver, knownPrRwPairs);

    // 5. 添加约束
    addConstraints(constraints, graphA, graphB);

    // 6. 构建 MonoSAT 图
    var monoGraph = new monosat.Graph(solver);
    for (var txn : history.getTransactions()) {
        nodeMap.put(txn, monoGraph.addNode());
    }

    // 添加已知边
    for (var e : knownEdges) {
        solver.assertEqual(e.getRight(),
            monoGraph.addEdge(nodeMap.get(e.getLeft()), nodeMap.get(e.getMiddle())));
    }

    // 添加未知边
    for (var e : unknownEdges) {
        solver.assertEqual(e.getRight(),
            monoGraph.addEdge(nodeMap.get(e.getLeft()), nodeMap.get(e.getMiddle())));
    }

    // 7. 添加无环性约束
    solver.assertTrue(monoGraph.acyclic());

    // 8. 添加全序约束（SER 要求）
    // SER (严格串行化) 要求任意两个事务必须可比较
    // 即：对于任意 T_i ≠ T_j，要么 T_i → T_j，要么 T_j → T_i
    var txnList = new ArrayList<Transaction<KeyType, ValueType>>(history.getTransactions());
    var edgeLitIndex = new HashMap<Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>, Lit>();
    for (var e : knownEdges) {
        edgeLitIndex.put(Pair.of(e.getLeft(), e.getMiddle()), e.getRight());
    }
    for (var e : unknownEdges) {
        edgeLitIndex.put(Pair.of(e.getLeft(), e.getMiddle()), e.getRight());
    }

    for (int i = 0; i < txnList.size(); i++) {
        for (int j = i + 1; j < txnList.size(); j++) {
            var txnI = txnList.get(i);
            var txnJ = txnList.get(j);
            var litIJ = edgeLitIndex.get(Pair.of(txnI, txnJ));
            var litJI = edgeLitIndex.get(Pair.of(txnJ, txnI));
            if (litIJ != null && litJI != null) {
                // 至少一个方向成立：lit(I→J) OR lit(J→I)
                solver.assertTrue(Logic.or(litIJ, litJI));
            }
            // 如果一对事务在 edgeLitIndex 中没有任何边，说明它们在 A ∪ B 中不可达
            // 这将导致 UNSAT，符合 SER 要求（全序必须覆盖所有事务对）
        }
    }
}
```

### 10.3 约束编码

```java
private void addConstraints(
        Collection<SIConstraint<KeyType, ValueType>> constraints,
        MutableValueGraph<Transaction, Collection<Lit>> graphA,
        MutableValueGraph<Transaction, Collection<Lit>> graphB) {

    for (var c : constraints) {
        // 对 edges1 和 edges2 分别构建 "all" 和 "none" 字面量
        var p1 = addEdges.apply(c.getEdges1());  // (all1, none1)
        var p2 = addEdges.apply(c.getEdges2());  // (all2, none2)

        // 互斥约束: (all1 AND none2) OR (all2 AND none1)
        // 即: 要么全选 edges1，要么全选 edges2
        constraintLiterals.put(
            Logic.or(
                Logic.and(p1.getLeft(), p2.getRight()),
                Logic.and(p2.getLeft(), p1.getRight())
            ),
            c
        );
    }
}
```

### 10.5 已知边与未知边

```java
// 已知边：在可达性矩阵中已经存在的边
// 来自: 图 A 直连边，或 A∘B 合成路径
static <K,V> List<Triple<Transaction, Transaction, Lit>> getKnownEdges(
        MutableValueGraph<Transaction, Collection<Lit>> graphA,
        MutableValueGraph<Transaction, Collection<Lit>> graphB,
        MatrixGraph<Transaction> AC) {

    var result = new ArrayList<Triple<Transaction, Transaction, Lit>>();

    for (var e : AC.edges()) {
        var n = e.source();
        var m = e.target();

        if (graphA.hasEdgeConnecting(n, m)) {
            // 直接边
            result.add(Triple.of(n, m, firstEdge.apply(graphA.edgeValue(n, m))));
        } else {
            // A∘B 合成边
            var middle = Sets.intersection(graphA.successors(n), graphB.predecessors(m))
                             .iterator().next();
            result.add(Triple.of(n, m, Logic.and(
                firstEdge.apply(graphA.edgeValue(n, middle)),
                firstEdge.apply(graphB.edgeValue(middle, m))
            )));
        }
    }

    return result;
}

// 未知边：不在可达性矩阵中的边，用于 acyclicity 约束
static <K,V> List<Triple<Transaction, Transaction, Lit>> getUnknownEdges(...) {
    var edges = new ArrayList<Triple<Transaction, Transaction, Lit>>();

    for (var p : graphA.nodes()) {
        for (var n : graphA.successors(p)) {
            // 如果 p→n 不在可达性矩阵中，是未知边
            if (p == n || !reachability.hasEdgeConnecting(p, n)) {
                for (var lit : graphA.edgeValue(p, n).get()) {
                    edges.add(Triple.of(p, n, lit));
                }
            }

            // A∘B 合成边
            for (var s : graphB.successors(n)) {
                if (p == s || !reachability.hasEdgeConnecting(p, s)) {
                    for (var lit1 : graphA.edgeValue(p, n).get()) {
                        for (var lit2 : graphB.edgeValue(n, s).get()) {
                            edges.add(Triple.of(p, s, Logic.and(lit1, lit2)));
                        }
                    }
                }
            }
        }
    }

    return edges;
}
```

### 10.6 PR_RW 边的特殊处理

PR_RW 边**不参与 A∘B 合成**：

```java
// 在 getUnknownEdges 中过滤掉 PR_RW
var txns = graphB.successors(n).stream()
    .filter(t -> ...)
    .filter(t -> !knownPrRwPairs.contains(Pair.of(n, t)))  // 排除 PR_RW
    .collect(Collectors.toList());

// PR_RW 边通过约束系统处理，或直接加入 knownEdges
```

---

## 11. SI到SER转换

### 11.1 转换器接口

```java
public interface HistoryTransformer {
    <T, U> History<Object, Object> transformHistory(History<T, U> history);
}
```

### 11.2 SnapshotIsolationToSerializable

将 SI 历史转换为 SER 历史：

```java
public <T, U> History<Object, Object> transformHistory(History<T, U> history) {
    var newHistory = new History<>();

    for (var session : history.getSessions()) {
        var newSession = newHistory.addSession(session.getId());

        for (var txn : session.getTransactions()) {
            // 拆分事务为 RO 和 WO 部分
            var readTxn = newHistory.addTransaction(newSession, txn.getId() * 2);
            var writeTxn = newHistory.addTransaction(newSession, txn.getId() * 2 + 1);

            for (var op : txn.getEvents()) {
                if (op.getType() == EventType.READ) {
                    // 只读操作保留在 readTxn
                    newHistory.addEvent(readTxn, EventType.READ, op.getKey(), op.getValue());
                } else {
                    // 写操作移到 writeTxn
                    newHistory.addEvent(writeTxn, EventType.WRITE, op.getKey(), op.getValue());

                    // 为所有写同一 key 的事务引入冲突
                    for (var txn2 : writes.get(op.getKey())) {
                        if (txn2 == txn) continue;
                        // 添加冲突操作
                        var conflictKey = conflictKeys.get(Pair.of(txn, txn2)).getLeft();
                        newHistory.addEvent(readTxn, EventType.WRITE, conflictKey, ...);
                        newHistory.addEvent(writeTxn, EventType.READ, conflictKey, ...);
                    }
                }
            }
        }
    }

    return newHistory;
}
```

### 11.3 转换策略示例

```
原始 SI 历史:
T1: R(x) W(y)
T2: R(y) W(x)

转换后 SER 历史:
T1a: R(x)          (只读部分)
T1b: W(y)          (写部分)
T2a: R(y)
T2b: W(x)

冲突注入:
- T1 和 T2 都访问 y → 引入冲突 key y_conflict
- T1 和 T2 都访问 x → 引入冲突 key x_conflict
```

---

## 12. 命令行接口

### 12.1 验证命令

```bash
# 基本用法
java -jar polysi.jar audit [OPTIONS] <path>

# 选项
-t, --type=<type>        历史类型: COBRA, DBCOP, TEXT, ELLE（默认: COBRA）
--no-pruning             禁用约束剪枝
--no-coalescing          禁用约束合并
--dot-output             DOT 格式输出
-h, --help               显示帮助
-V, --version            显示版本
```

### 12.2 示例

```bash
# 验证 Cobra 格式历史
java -jar polysi.jar audit --type=cobra /path/to/history/

# 验证 Text 格式历史
java -jar polysi.jar audit --type=text history.txt

# 禁用剪枝验证
java -jar polysi.jar audit --no-pruning input.elle

# DOT 格式输出
java -jar polysi.jar audit --dot-output input.txt > output.dot

# 输出示例
Sessions count: 3
Transactions count: 10
Events count: 25
Known edges: 15
Constraints count: 5
Mode: SER (verifySer=true), deriving PR_* edges
Pruning round 1
Pruning round 2
After Prune:
Constraints count: 3
ENTIRE_EXPERIMENT: 45ms
ONESHOT_CONS: 12ms
SI_PRUNE: 20ms
SI_SOLVER_GEN: 10ms
SI_SOLVER_SOLVE: 3ms
[[[[ ACCEPT ]]]]
```

### 12.3 统计命令

```bash
# 基本用法
java -jar polysi.jar stat [OPTIONS] <path>

java -jar polysi.jar stat --type=text history.txt
```

**输出示例**：

```
Sessions: 3
Transactions: 10, read-only: 2, write-only: 3, read-modify-write: 5
Events: total 25, read 10, write 15
Variables: 8
(writes, #keys):
1...3: 2
4...6: 3
7...9: 3
```

### 12.4 转换命令

```bash
# 基本用法
java -jar polysi.jar convert [OPTIONS] <in-path> <out-path>

# 选项
-f, --from=<type>       输入类型（默认: COBRA）
-o, --output=<type>      输出类型（默认: DBCOP）
-t, --transform=<type>  变换类型: IDENTITY, SI2SER
```

### 12.5 转换示例

```bash
# Elle → Text
java -jar polysi.jar convert -f ELLE -o TEXT input.elle output.txt

# Cobra → DBCop
java -jar polysi.jar convert -f COBRA -o DBCOP input_dir/ output.bincode

# SI → SER 转换
java -jar polysi.jar convert -t SI2SER input.txt output_ser.txt
```

### 12.6 转储命令

```bash
# 基本用法
java -jar polysi.jar dump [OPTIONS] <path>

java -jar polysi.jar dump --type=text history.txt
```

**输出示例**：

```
Transaction (0, 0)
READ(1, 10)
WRITE(2, 20)

Transaction (0, 1)
READ(2, 20)

Transaction (1, 0)
WRITE(1, 15)
```

---

## 13. Java API

### 13.1 基本用法

```java
import history.History;
import history.loaders.TextHistoryLoader;
import verifier.SIVerifier;

public class Example {
    public static void main(String[] args) {
        // 1. 加载历史
        HistoryLoader<String, Integer> loader = new TextHistoryLoader("history.txt");
        History<String, Integer> history = loader.loadHistory();

        // 2. 创建验证器（默认 SER 模式）
        SIVerifier<String, Integer> verifier = new SIVerifier<>(loader);

        // 3. 验证
        boolean accepted = verifier.audit();

        // 4. 输出结果
        if (accepted) {
            System.out.println("History satisfies SER");
        } else {
            System.out.println("History violates SER");
        }
    }
}
```

### 13.2 SI 模式

```java
// 切换到 SI 模式（跳过 PR_* 推导）
SIVerifier.setVerifySer(false);

SIVerifier<String, Integer> verifier = new SIVerifier<>(loader);
boolean accepted = verifier.audit();
```

### 13.3 高级选项

```java
// 禁用约束合并
SIVerifier.setCoalesceConstraints(false);

// 禁用剪枝
Pruning.setEnablePruning(false);

// DOT 格式输出
SIVerifier.setDotOutput(true);

// 自定义剪枝阈值
Pruning.setStopThreshold(0.05);  // 5%
```

### 13.4 Elle 格式加载

```java
import history.loaders.ElleHistoryLoader;

Path path = Paths.get("history.elle");
HistoryLoader<Integer, ElleHistoryLoader.ElleValue> loader =
    new ElleHistoryLoader(path);
SIVerifier<Integer, ElleHistoryLoader.ElleValue> verifier =
    new SIVerifier<>(loader);
```

### 13.5 SI→SER 转换

```java
import history.transformers.SnapshotIsolationToSerializable;

HistoryTransformer transformer = new SnapshotIsolationToSerializable();
History<Object, Object> serHistory = transformer.transformHistory(history);

// 然后验证转换后的历史
SIVerifier<Object, Object> verifier = new SIVerifier<>(serHistory);
boolean accepted = verifier.audit();
```

---

## 14. 构建与安装

### 14.1 系统要求

- **操作系统**: Ubuntu 22.04 LTS（已测试）
- **Java**: OpenJDK 11 或更高
- **C++ 编译器**: g++ 支持 C++11
- **CMake**: 3.x
- **GMP**: >= 5.1.3
- **zlib**: 开发库

### 14.2 安装依赖

```bash
sudo apt update
sudo apt install g++ openjdk-11-jdk cmake libgmp-dev zlib1g-dev git
```

### 14.3 构建项目

```bash
# 克隆仓库（包含子模块）
git clone --recurse-submodules https://github.com/amnore/PolySI.git
cd PolySI

# 构建 MonoSAT
./gradlew buildMonoSAT

# 构建 PolySI
./gradlew jar

# 运行测试
./gradlew test
```

### 14.4 构建产物

```
build/libs/
└── PolySI-1.0.0-SNAPSHOT.jar
```

### 14.5 常见问题

**问题**: MonoSAT 构建失败  
**解决**: 确保安装了 GMP 5.1.3+ 和 C++11 支持

**问题**: 找不到 `libmonosat.so`  
**解决**: 检查 `build/monosat` 目录，确保构建成功

**问题**: 测试失败  
**解决**: 确保设置 `java.library.path` 指向 MonoSAT 库目录

---

## 15. 测试方案

### 15.1 测试维度

| 维度 | 说明 | 测试点 |
|------|------|--------|
| 基础 WW 冲突 | 两个事务写同一 key | WW ordering |
| 基础 RW 冲突 | T1 读 X，T2 写 X | RW anti-dependency |
| 谓词 flip | 谓词满足状态改变 | Flip witness |
| PR_WR 推导 | flip witness → reader | MaxCand |
| PR_RW 推导 | reader → WW winner | Δμ condition |
| 环检测 | SER 违规历史 | PR_* 边组合形成环 |
| SI 模式 | 跳过 PR_* | 纯拓扑排序 |

### 15.2 测试场景

#### 场景 1：简单 WW 冲突 — SER 满足

```
History:
  T1: W(x,1)
  T2: W(x,2)
  T3: R(x,2)    // T3 reads T2's write

Expected: SER = true
Reason: T1 和 T2 的 WW 顺序确定后，无环
```

#### 场景 2：WW 冲突形成环 — SER 违规

```
History:
  T1: W(x,1)
  T2: W(y,1)
  T3: R(x,1) W(y,2)
  T4: R(y,1) W(x,2)

Expected: SER = false
Reason: Write-Dependency 环存在
```

#### 场景 3：Predicate flip — flip witness 识别

```
History:
  T1: W(x,10)     // x=10 满足 v>5
  T2: PRED_READ(v>5, results=[(x,10)])
  T3: W(x,3)      // x=3 不满足 v>5

Expected:
  - Flip witness = T1 (false→true)
  - PR_WR(T1→T2, x) 应存在
```

#### 场景 4：多 flip witness — MaxCand 选择

```
History:
  T1: W(x,10)     // 满足 v>5
  T2: W(x,3)      // 不满足 — flip1
  T3: W(x,20)     // 满足 v>5 — flip2
  T4: PRED_READ(v>5, results=[(x,20)])

Expected:
  - candidates = [T2, T3]
  - MaxCand = T3 (AR-order 下最大)
  - PR_WR(T3→T4, x)
```

#### 场景 5：Δμ 条件

```
History:
  T1: W(x,10)      // 满足 v>5
  T2: W(x,20)      // 满足 v>5
  T3: PRED_READ(v>5, results=[(x,20)])
  T4: W(x,3)       // 不满足 v>5

Expected:
  - PR_WR: T1→T3
  - WW(x): T1→T2, T2→T4
  - Δμ(T1, T3, T4, x) = true
  - PR_RW(T3→T4, x) = true
```

### 15.3 测试执行矩阵

| # | 场景 | SER 预期 | SI 预期 | 测试类型 |
|----|------|---------|---------|---------|
| 1 | 简单 WW — 无环 | true | true | 满足性 |
| 2 | WW 冲突 — 有环 | false | false | 违规性 |
| 3 | 简单 RW — 无环 | true | true | 满足性 |
| 4 | Write-Dependency 环 | false | false | 违规性 |
| 5 | Predicate flip 识别 | PR_WR 存在 | (无 PR_*) | 推导正确性 |
| 6 | 多 flip — MaxCand | PR_WR(最大) | (无 PR_*) | 推导正确性 |
| 7 | PR_WR 基本推导 | PR_WR 存在 | (无 PR_*) | 推导正确性 |
| 8 | PR_WR — 自己写 key | PR_WR 存在 | (无 PR_*) | 推导正确性 |
| 9 | PR_RW — Δμ 满足 | PR_RW 存在 | (无 PR_*) | 推导正确性 |
| 10 | PR_RW — 自环防护 | 无自环 | (无 PR_*) | 边界条件 |
| 11 | SI 满足 SER 违规 | false | true | 模式差异 |

---

## 16. 文件清单

### 16.1 目录结构

```
artifact/PolySI/
├── build.gradle                    # Gradle 构建配置
├── gradlew                         # Gradle 包装脚本
├── settings.gradle
├── src/
│   ├── main/
│   │   └── java/
│   │       ├── Main.java                   # CLI 入口 (picocli)
│   │       ├── Launcher.java               # JarClassLoader 启动器
│   │       ├── graph/                      # 图结构
│   │       │   ├── Edge.java               # 边数据结构
│   │       │   ├── EdgeType.java           # 边类型枚举
│   │       │   ├── KnownGraph.java         # 已知依赖图
│   │       │   └── MatrixGraph.java        # 高效矩阵图
│   │       ├── history/                    # 历史记录
│   │       │   ├── Event.java              # 事件（READ/WRITE/PREDICATE_READ）
│   │       │   ├── History.java            # 历史容器
│   │       │   ├── Transaction.java        # 事务
│   │       │   ├── Session.java            # 会话
│   │       │   ├── HistoryLoader.java     # 加载器接口
│   │       │   ├── HistoryParser.java     # 解析器接口
│   │       │   ├── HistoryConverter.java   # 转换器接口
│   │       │   ├── HistoryDumper.java     # 输出器接口
│   │       │   ├── HistoryTransformer.java # 变换器接口
│   │       │   ├── InvalidHistoryError.java# 错误类
│   │       │   ├── loaders/                # 加载器实现
│   │       │   │   ├── CobraHistoryLoader.java
│   │       │   │   ├── DBCopHistoryLoader.java
│   │       │   │   ├── ElleHistoryLoader.java
│   │       │   │   ├── TextHistoryLoader.java
│   │       │   │   └── Utils.java
│   │       │   └── transformers/           # 变换器实现
│   │       │       ├── Identity.java
│   │       │       └── SnapshotIsolationToSerializable.java
│   │       ├── verifier/                   # 验证器
│   │       │   ├── SIVerifier.java         # 主验证器
│   │       │   ├── SISolver.java           # SAT 求解器
│   │       │   ├── SISolver2.java         # SAT 求解器（备选）
│   │       │   ├── SIConstraint.java      # 约束
│   │       │   ├── SIEdge.java             # 约束中的边
│   │       │   ├── Pruning.java            # 约束剪枝
│   │       │   └── Utils.java              # 验证器工具
│   │       └── util/
│   │           ├── Profiler.java           # 性能分析
│   │           ├── QuadConsumer.java       # 函数接口
│   │           ├── TriConsumer.java        # 函数接口
│   │           └── UnimplementedError.java
│   └── test/
│       └── java/
│           ├── TestLoader.java             # 测试用加载器
│           ├── TestVerifier.java          # 核心验证测试
│           ├── TestMatrixGraph.java        # 矩阵图测试
│           ├── TestSI2SER.java            # SI→SER 转换测试
│           └── verifier/
│               ├── SIVerifierPredicateTest.java
│               ├── SIVerifierPredicateIntegrationTest.java
│               └── SERDetectabilityTest.java
└── monosat/                        # MonoSAT 子模块
    ├── CMakeLists.txt
    ├── README.md
    ├── src/
    │   └── monosat/
    │       ├── Core/                      # SAT 核心
    │       ├── Graph/                     # 图理论求解器
    │       ├── DGL/                       # 动态图算法库
    │       └── API/
    │           ├── java/                  # Java 绑定
    │           └── python/                # Python 绑定
    └── examples/
        ├── java/
        │   └── Tutorial.java
        └── python/
            └── tutorial.py
```

### 16.2 关键文件说明

| 文件 | 行数 | 功能 |
|------|------|------|
| `Main.java` | ~280 | CLI 入口，定义 audit/convert/stat/dump 命令 |
| `SIVerifier.java` | ~870 | 主验证器，包含 PR_* 推导核心算法 |
| `SISolver.java` | ~270 | SAT 编码与求解 |
| `KnownGraph.java` | ~185 | 依赖图构建 |
| `MatrixGraph.java` | ~450 | 矩阵图实现 |
| `Pruning.java` | ~160 | 约束剪枝算法 |
| `verifier/Utils.java` | ~440 | 验证器工具函数 |

---

## 附录 A：算法索引

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

---

## 附录 B：配置选项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `SIVerifier.verifySer` | `true` | SER/SI 模式切换 |
| `SIVerifier.coalesceConstraints` | `true` | 约束合并开关 |
| `SIVerifier.dotOutput` | `false` | DOT 格式输出开关 |
| `Pruning.enablePruning` | `true` | 剪枝开关 |
| `Pruning.stopThreshold` | `0.01` | 剪枝停止阈值 |

---

**文档版本**: 1.0  
**最后更新**: 2026-04-12
