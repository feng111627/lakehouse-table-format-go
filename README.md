# lakehouse-table-format-go

用本地文件系统模拟的数据湖表格式实现，提供：快照式原子提交、结构演进、
小文件合并与按主键增量更新。并发写入、结构变更与历史查询在不同入口下
得到一致且可解释的结论。

## 目录结构

```
lakehouse/        核心包
  types.go        类型定义与可区分错误
  storage.go      本地存储与原子写（临时文件 + rename）
  table.go        建表、快照提交、读取
  schema.go       结构兼容校验
  compact.go      小文件合并与按主键更新
```

## 快照模型

表目录即存储：

```
<dir>/
  metadata/
    v1.json, v2.json, ...   每份快照一份不可变元数据（结构 + 数据文件清单 + 操作类型）
    current                 当前版本号指针
  data/
    part-*.json             数据文件（行数组，按列名存 JSON）
```

- **提交协议**：先写数据文件，再写 `metadata/v{N}.json`，最后用
  「临时文件 + rename」原子替换 `metadata/current`。任意时刻崩溃，
  `current` 要么指向旧版本要么指向新版本，绝不出现半可见状态；
  未被引用的文件只是垃圾，不影响一致性。
- **并发控制**：进程内互斥锁串行化提交，并做乐观并发校验——持锁期间
  基版本被外部推进时放弃并基于最新快照重试，冲突以
  `ErrConcurrentModification` 表达。
- **快照隔离**：读取只经由某个 `v{N}.json` 解析数据文件清单，与并发
  写入互不影响；同一版本重复读取结果确定可复现（文件按名排序、
  行按文件内顺序）。
- **历史可读**：合并/更新只新增数据文件、不删除旧文件，历史快照引用的
  文件始终存在。

## 结构兼容规则

`EvolveSchema` 提交前校验，兼容才生效：

| 变更 | 结果 |
|---|---|
| 新增可选列 | 允许；历史数据文件无该列，读为缺失 |
| 新增 Required 列 | 拒绝，原因 `required-new-column` |
| 删除非主键列 | 允许；历史快照仍按各自结构读出该列 |
| 删除主键列 / 变更主键集合 | 拒绝，原因 `drop-primary-key` |
| 类型加宽 int → float | 允许 |
| 类型收窄 float → int | 拒绝，原因 `type-narrowing` |
| 跨类变更（如 string → int） | 拒绝，原因 `type-mismatch` |

不兼容变更返回 `ErrIncompatibleSchema`，`Reason` 字段可区分具体原因；
被拒绝的变更不推进版本。历史快照永远按各自快照内保存的结构读取，
不会错列或丢字段。

## 合并与更新语义

- `Compact()`：把当前快照全部数据文件重写为少量文件；定义了主键时按
  「后提交者覆盖先提交者」的确定规则去重。
- `Upsert(rows)`：按主键合并——已存在的主键原位覆盖，新主键按批内
  顺序追加。**单批内重复主键返回 `ErrDuplicatePrimaryKey`，拒绝整个
  提交，不做静默取舍**（`Append` 在主键表上同样校验）。
- 两者都是「读当前快照 → 计算 → 整体重写提交」，并发冲突时基于最新
  快照重算重试，最新快照的有效数据保持一致。

## 本地验证

```bash
go build ./...
go test ./... -v          # 日志中可见各业务场景与用例
go test ./... -race       # 并发安全
```

测试覆盖三类业务，`-v` 日志按「业务场景 → 用例」输出：

- **并发提交**（`TestConcurrentCommits`、失败原子性
  `TestFailedCommitLeavesNoPartialState`、快照读写
  `TestSnapshotCommitAndRead`）
- **结构演进**（`TestSchemaEvolution`、
  `TestIncompatibleSchemaRejected`）
- **合并与更新**（`TestCompact`、`TestUpsert`、
  `TestConcurrentUpsertAndCompact`）

## 最小示例

```go
tbl, _ := lakehouse.Create("/tmp/mytable", lakehouse.Schema{
    Fields: []lakehouse.Field{
        {Name: "id", Type: lakehouse.TypeInt, Required: true},
        {Name: "name", Type: lakehouse.TypeString},
    },
    PrimaryKey: []string{"id"},
})
v1, _ := tbl.Append([]lakehouse.Row{{"id": 1, "name": "a"}})
tbl.Upsert([]lakehouse.Row{{"id": 1, "name": "a2"}})
rows, _ := tbl.Read(v1) // 历史快照：仍为 name=a
latest, _ := tbl.Read(0) // 最新快照：name=a2
```
