# Lakehouse Table Format（本地数据湖表，Go）

用**本地文件系统**模拟一套数据湖表格式，提供按快照（snapshot）原子提交的写入、
表结构演进（schema evolution）、小文件合并（compaction）与按主键增量更新
（upsert）。数据与元数据都是普通 JSON 文件，不依赖任何外部服务；并发写入通过
`flock(2)` 文件锁 + 进程内互斥串行化，因此多个 goroutine、多个进程共用同一表
目录时结论一致。

## 1. 快照模型

每次提交（`Append` / `Upsert` / `Compact` / `Evolve`）都在不可变的快照链上
追加一个新版本，快照 1 是建表时的空根快照：

```
snap(N) { id, parent_id -> snap(N-1), schema_version, operation,
          added_files[], removed_files[], timestamp_ms, summary }
```

- **当前视图（HEAD）**：`meta/HEAD` 指向当前快照 id。某快照的"活文件集合"由
  根快照沿父指针重放 `added_files / removed_files` 增量得到。
- **数据文件不可变、物理不删除**：合并或 upsert 只是在新快照里把旧文件标记为
  `removed_files`。旧快照仍引用旧文件，因此**历史快照永远可读**（时间旅行）。
- **读取快照隔离**：`Read()` / `ReadAt(id)` 只触碰不可变文件与不可变元数据，
  读取期间发生的提交不会影响已拿到的 `ResultSet`；同一快照重复读取结果确定、
  可复现（结果按主键确定性排序）。
- **原子提交顺序**（失败任何一步都不会出现半可见状态）：
  1. 数据文件先写入 `txn/` 临时目录，`fsync` 后 `rename` 到 `data/`；
  2. 写快照元数据 `meta/snap-<id>.json`（原子 rename）；
  3. 最后才把 `meta/HEAD` 原子切换到新 id。
  在第 3 步前崩溃：新提交对外不可见，`txn/` 残留由下次 `Open` 在持锁状态下
  清理；第 3 步是单次 rename，要么旧版本要么新版本，没有中间态。
- **并发**：所有提交先取进程内互斥锁，再取目录下 `LOCK` 的 `flock` 排他锁；
  取锁成功后重新从磁盘加载元数据（`reload`），保证提交总是基于最新 HEAD，
  快照 id 单调连续，不丢写。

### 目录布局

```
<table-dir>/
├── LOCK                     # flock 提交锁
├── meta/
│   ├── HEAD                 # 当前快照 id
│   ├── schema_v<N>.json     # 不可变的 schema 版本
│   └── snap-<id>.json       # 不可变的快照元数据
├── data/<snapid>-<n>.json   # 不可变数据文件（记录按稳定列 id 存储）
└── txn/*.tmp                # 提交暂存，崩溃残留于下次打开时清理
```

## 2. 结构兼容规则

列在数据文件里按**稳定列 id**存储，不按列名/列位置存储，因此改名、删列、
删后重建同名列都不会错列：

| 变更 | 规则 | 历史快照语义 |
| --- | --- | --- |
| 新增列 `AddColumn` | 只允许**可空**列（老行无法回填）；自动分配从未使用过的列 id（高水位，含已删除列，**不回收 id**） | 老快照不含该列；新视图中老行读为 `null` |
| 删列 `DropColumn` | 主键列禁止删除 | 新视图不暴露该列；老快照仍能读出 |
| 改名 `RenameColumn` | 列 id 不变，新名不得与现有列重名 | 数据按 id 连续；老快照保留老名字 |
| 改类型 `AlterColumnType` | 仅接受**拓宽**：`int→long`、`int→double`、`long→double`；主键列禁止；同类型空变更拒绝 | 读取时按快照自己的 schema 投影：老数据在新视图中确定性转换（如 int `20` → double `20.0`）；老快照仍以原类型读出 |

被明确拒绝（返回可 `errors.Is` 区分的哨兵错误，绝不静默错列/丢字段）的情况：

- 不兼容的类型变更、未知目标类型 → `ErrTypeMismatch`
- 列不存在 → `ErrColumnNotFound`；列名/id 已存在 → `ErrColumnExists`
- 删主键列、改主键列类型、新增主键列 → `ErrCannotChangePK`
- 新增非空列 → `ErrIncompatibleSchema`
- 一批变更中任一非法 → **整批回滚**，schema 版本不变
- 写入行带未知列 → `ErrUnknownColumn`；值与列物理类型不符 → `ErrTypeMismatch`；
  非空列为空或缺值 → `ErrNullNotNull`
- 同一批写入/upsert 输入内主键重复，或表内存量数据已存在重复主键却要 upsert
  → `ErrDuplicatePK`（错误信息带出行号/主键值，**不静默取舍**）

`SchemaError` 同时携带操作名（`add-column` / `drop-column` / `alter-type` /
`rename-column` / `validate-row`）、列名、被包装的哨兵错误与可读说明。

## 3. 小文件合并与增量更新

- `Compact(threshold)`：当 HEAD 的活文件数 ≥ 阈值（默认 2）时，把全部有效记录
  投影到当前 schema、按主键排序后重写为**一个**文件，并在新快照中把旧文件标记
  移除。合并前后行数与逐行内容签名必须一致；低于阈值返回 `NoOp`，不产生新快照。
- `Upsert(rows)`：先校验输入（严格类型、非空、批内主键唯一），再校验**存量数据
  主键唯一**（若已重复则拒绝，避免在多个旧值间静默选一个）；按主键替换同键旧行、
  追加新键，整表重写为一个新文件。`summary` 给出 `inserted/updated` 计数。
- `Append(rows)`：仅新增，不去重；同一批次内主键重复会被拒绝。跨快照的重复主键
  会在 `Upsert` 时被显式拦截。
- 合并与 upsert 都只让旧文件在新快照里"逻辑失效"，历史快照照旧可读。

## 4. 快速上手

```go
sch := &lakehouse.Schema{Fields: []lakehouse.Field{
    {ID: 1, Name: "id",   Type: lakehouse.TypeLong,   Nullable: false, IsPK: true},
    {ID: 2, Name: "name", Type: lakehouse.TypeString, Nullable: false},
    {ID: 3, Name: "age",  Type: lakehouse.TypeInt,    Nullable: true},
}}
t, _ := lakehouse.CreateTable(ctx, "/tmp/t1", sch, nil)

snap, _ := t.Append(ctx, []lakehouse.Row{{"id": 1, "name": "a", "age": 30}})
_, _, _ = t.Evolve(ctx, []lakehouse.SchemaChange{
    lakehouse.AlterColumnType("age", lakehouse.TypeDouble), // 拓宽
    lakehouse.AddColumn(lakehouse.Field{Name: "city", Type: lakehouse.TypeString, Nullable: true}),
})
t.Upsert(ctx, []lakehouse.Row{{"id": 1, "name": "A", "age": 31.0, "city": "LA"}})
t.Compact(ctx, 0)

cur, _ := t.Read(ctx)        // HEAD
old, _ := t.ReadAt(ctx, snap) // 时间旅行，结果稳定可复现
```

## 5. 本地验证

```bash
# 全部单元测试（无外部依赖，需要 Linux/macOS 的 flock）
go test ./...

# 详细日志：先打印三大业务域与覆盖用例清单，再逐用例打印执行/拒绝原因
go test -v ./...

# 并发安全验证
go test -race -count=1 ./...
```

测试按业务域组织，共 21 个顶层用例 + 5 个表式子用例：

- `snapshot_concurrency_test.go`：原子提交与快照链、8 写者并发 append、
  读快照隔离、失败提交无半可见状态、时间旅行可复现、读者/写入者/合并并发压测、
  重开一致性；
- `schema_evolution_test.go`：增/删/改名列、类型拓宽、不兼容变更可区分拒绝、
  主键保护、整批回滚、同名删后加列不复用旧 id；
- `compaction_upsert_test.go`：合并前后内容一致、阈值 NoOp、upsert 更新+插入、
  输入/存量重复主键拒绝、append/upsert/compact 往返、append 与 upsert 并发混合。

`go test -v` 日志中每个用例都以 `【业务域】执行用例: …` 开头，拒绝路径会额外
打印具体原因（错误类型 + 列名 + 说明）。
