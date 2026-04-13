# PolySI SER 可转化性检测测试方案

## 测试目标

验证 PolySI 验证器能否正确检测数据库历史是否满足 **Serializable Snapshot Isolation (SER/PSI)**。

---

## 测试维度与场景设计

### 维度一：基础 WW 冲突（无 predicate）

**场景 1：简单 WW 冲突 — SER 满足**

```
History:
  T1: W(x,1)
  T2: W(x,2)
  T3: R(x,2)    // T3 reads T2's write

Expected: SER = true
Reason: T1 和 T2 的 WW 顺序确定后，无环
```

**场景 2：WW 冲突形成环 — SER 违规**

```
History:
  T1: W(x,1)
  T2: W(y,1)
  T3: R(x,1) W(y,2)
  T4: R(y,1) W(x,2)

Expected: SER = false
Reason:
  WW(x): T1→T2 (或 T2→T1)
  WW(y): T2→T4 (或 T4→T2)
  存在 SER 违规
```

---

### 维度二：基础 RW 冲突（无 predicate）

**场景 3：简单 RW anti-dependency — SER 满足**

```
History:
  T1: W(x,1)
  T2: R(x,1) W(y,1)
  T3: R(y,1)

Expected: SER = true
Reason: RW 冲突不形成环
```

**场景 4：RW 形成 Write-Dependency 环 — SER 违规**

```
History (G-SER violation):
  T1: W(x,1)
  T2: R(x,1) W(y,1)
  T3: R(y,1) W(x,2)

Expected: SER = false
Reason:
  GSO: T1→T2→T3
  WR: T1→T2, T2→T3
  WW(x): T1→T3
  可构造: T1→T2(T1的写)→T3(T2的写)→?→T1
  Write-Dependency 环存在
```

---

### 维度三：谓词 Flip Witness

**场景 5：Predicate flip — flip witness 识别**

```
History:
  T1: W(x,10)     // x=10 满足 v>5
  T2: PRED_READ(v>5, results=[(x,10)])
  T3: W(x,3)      // x=3 不满足 v>5

Expected:
  - Flip witness = T1 (初始false→T1满足=true)
  - PR_WR(T1→T2, x) 应存在
Reason: T1 是离 T2 最近观察点的 flip witness
```

**场景 6：多 flip witness — MaxCand 选择**

```
History:
  T1: W(x,10)     // 满足 v>5
  T2: W(x,3)      // 不满足 — flip1
  T3: W(x,20)     // 满足 v>5 — flip2
  T4: PRED_READ(v>5, results=[(x,20)])

Expected:
  - Flip1: T2 (满足→不满足)
  - Flip2: T3 (不满足→满足)
  - candidates = [T2, T3]
  - MaxCand = T3 (AR-order 下最大)
  - PR_WR(T3→T4, x)
Reason: MaxCand 选择 AR-order 下唯一的最大 flip witness
```

---

### 维度四：PR_WR 推导

**场景 7：PR_WR 基本推导**

```
History:
  T1: W(x,10)     // 满足 v>5
  T2: PRED_READ(v>5, results=[(x,10)])
  (无 T2 自己写的 x)

Expected:
  - obsIdx = 0 (T1 是 result source)
  - Flip witness = T1 (初始不满足)
  - PR_WR(T1→T2, x) = true
Reason: T1 是 flip witness，且唯一最大
```

**场景 8：T2 自己写了 key — 排除自环**

```
History:
  T1: W(x,10)     // 满足
  T2: W(x,20) PRED_READ(v>5, results=[(x,20)])
  // T2 看到自己的 x=20

Expected:
  - obsIdx = 1 (T2 的写是 result source)
  - Flip: T1 (初始不满足 → T1满足 → flip)
  - candidates = [T1]
  - MaxCand = T1
  - T1 ≠ T2 → PR_WR(T1→T2, x) = true
Reason: 虽然 T2 自己写了，但 flip witness T1 仍然支配 T2 的可见值
```

---

### 维度五：PR_RW 推导（Δμ 条件）

**场景 9：PR_RW — Δμ 条件满足**

```
History:
  T1: W(x,10)      // 满足 v>5
  T2: W(x,20)      // 满足 v>5
  T3: PRED_READ(v>5, results=[(x,20)])
  T4: W(x,3)       // 不满足 v>5

Expected:
  - PR_WR: T1→T3 (flip witness)
  - WW(x): T1→T2, T2→T4
  - Δμ(T1, T3, T4, x)?
    → T3 的 visible writer = T2 (x=20)
    → T1 dominates T2? YES (via WW T1→T2)
    → Δμ = true
  - PR_RW(T3→T4, x) = true
Reason: T1 的 flip 支配了 T3 的 visible writer，使 T4 对 T3 不可见
```

**场景 10：PR_RW — Δμ 条件不满足**

```
History:
  T1: W(x,10)      // 满足 v>5
  T2: W(x,20)      // 满足 v>5
  T3: PRED_READ(v>5, results=[(x,20)])
  T4: W(x,3)       // 不满足 v>5

Expected:
  - PR_WR: T1→T3 (flip witness)
  - WW(x): T1→T2, T2→T4
  - Δμ(T1, T3, T4, x)?
    → T3 的 visible writer = T2 (x=20)
    → T1 dominates T2? YES (via WW T1→T2)
    → Δμ = true
  - PR_RW(T3→T4, x) = true
Reason: Δμ 条件满足

变体: 如果 T2 在 T3 之前提交，T1 不支配 T2?
  (需要更精细的 AR-order 判断)
```

**场景 11：PR_RW — 自环防护**

```
History:
  T1: W(x,10)      // 满足 v>5
  T2: PRED_READ(v>5, results=[(x,10)]) W(x,20)
  // T2 读了 x=10，然后自己写 x=20

Expected:
  - PR_WR(T1→T2, x) = true
  - 检查 PR_RW 时: U = T2 (T2 写了 x=20)
  - U == T2 == S (reader) → 跳过！
  - PR_RW(T2→T2, x) 不应出现（自环防护）
Reason: 不能对自己建立 anti-dependency
```

---

### 维度六：SER 违规（PR_* 形成环）

**场景 12：PR_WR + WW 形成环**

```
History:
  T1: W(x,10)
  T2: W(y,1)
  T3: PRED_READ(x+y>10, results=[])   // 看到初始状态
  T4: W(x,3) W(y,8)                  // x+y=11, 满足

Expected: SER = false
Reason: PR_WR + WW 组合可能形成环（具体取决于 predicate 语义）
```

**场景 13：PR_RW 形成环（Write Skew）**

```
History (Write Skew 经典案例):
  T1: R(x,0) R(y,0) W(x,1)
  T2: R(x,0) R(y,0) W(y,1)
  约束: x+y<2 (允许同时更新)

Expected: SER = false under predicate-aware SER
Reason: T1 和 T2 各自满足谓词，但并发执行违反了 SER
```

**场景 14：PR_WR + PR_RW 组合形成环**

```
History:
  T1: W(x,10)      // 满足 v>5
  T2: PRED_READ(v>5, results=[(x,10)]) W(y,1)
  T3: W(x,3)       // 不满足 — flip
  T4: R(y,1) W(x,20)

Expected: SER = false
Reason:
  PR_WR(T1→T2, x) + PR_RW(T2→T3, x)
  + 后续依赖可能形成环
```

---

### 维度七：SI 模式（跳过 PR_*）

**场景 15：SI 满足但 SER 违规**

```
History:
  T1: W(x,1) W(y,1)
  T2: R(x,1) W(y,2)
  T3: R(y,1) W(x,2)

Expected (SI 模式):
  SER = true (跳过 PR_*)
Reason: 纯 SI 下 GSO ∪ GWR 无环

Expected (SER 模式):
  SER = false (含 PR_*)
Reason: predicate 相关的 SER 违规被检测
```

---

### 维度八：内部一致性检查

**场景 16：点读不是来自最新写**

```
History:
  T1: W(x,10)
  T2: W(x,20)
  T3: R(x,10)   // T3 应该读到 T2 的 x=20

Expected: verifyInternalConsistency = false
Reason: T3 没有读到 x 的最新写
```

**场景 17：谓词读结果不满足谓词**

```
History:
  T1: W(x,10)     // 满足 v>5
  T2: PRED_READ(v>5, results=[(x,3)])  // 但 x=3 不满足 v>5

Expected: verifyInternalConsistency = false
Reason: predicate result 必须满足 predicate 条件
```

**场景 18：读自己的未来写**

```
History:
  T1: PRED_READ(v>5, results=[]) W(x,10)   // 先读后写
  // 读的时候 x 不存在，应看到初始状态

Expected: verifyInternalConsistency = true
Reason: 自己的写在读之后，初始状态满足谓词
```

---

## 测试执行矩阵

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
| 10 | PR_RW — Δμ 不满足 | PR_RW 不存在 | (无 PR_*) | 推导正确性 |
| 11 | PR_RW — 自环防护 | 无自环 | (无 PR_*) | 边界条件 |
| 12 | PR_WR + WW 环 | false | true | SER 违规 |
| 13 | Write Skew (predicate) | false | true | SER 违规 |
| 14 | PR_WR + PR_RW 环 | false | true | SER 违规 |
| 15 | SI 满足 SER 违规 | false | true | 模式差异 |
| 16 | 点读非最新写 | 内部检查失败 | 内部检查失败 | 一致性 |
| 17 | 谓词结果不满足谓词 | 内部检查失败 | 内部检查失败 | 一致性 |
| 18 | 读自己的未来写 | 内部检查通过 | 内部检查通过 | 一致性 |

---

## 测试结果判断标准

### SER 验证通过（返回 true）
- 图中所有边（SO + WR + WW + RW + PR_WR + PR_RW）无环
- SAT 求解器返回 SAT（存在一个 WW ordering 使得无环）

### SER 验证失败（返回 false）
- 图中存在环
- 或 SAT 求解器返回 UNSAT（无论哪种 WW ordering 都有环）

### SI 验证通过（verifySer=false）
- GSO ∪ GWR 无环（Kahn 拓扑排序成功）

### SI 验证失败（verifySer=false）
- GSO ∪ GWR 有环
