# lakehouse-table-format-go

用本地文件系统模拟的数据湖表格式实现，提供：快照式原子提交、结构演进、
小文件合并、按主键增量更新，以及面向“线上长期运行”的三项能力：

- **跨进程并发**：多个后台进程可各自打开同一表目录一起读写；
- **历史管理**：给提交命名（tag）、查看历史列表、回滚到任意历史版本；
- **空间回收（Vacuum）**：按保留窗口安全清理过期快照与无引用文件。

并发写入、结构变更、回滚、回收与历史查询在不同入口下得到一致且可解释的结论。

## 目录结构

```
lakehouse/
  types.go        类型定义与可区分错误
  storage.go      本地存储与原子写（临时文件 + rename）
  lock.go         跨进程排他锁（flock）
  table.go        建表、快照提交、读取、乐观并发执行器
  schema.go       结构兼容校验
  compact.go      小文件合并与按主键更新
  tags.go         命名标记（tag）
  history.go      历史列表与回滚
  vacuum.go       保留窗口回收与墓碑日志
```

## 磁盘布局

```
<dir>/
  metadata/
    lock                 flock 跨进程排他锁文件
    current              当前版本号指针（原子替换）
    v1.json, v2.json,... 每份快照一份不可变元数据（结构 + 数据文件清单 + 操作）
    gc-log.json          已回收版本的墓碑日志（追加、原子覆盖）
    tags/<name>.json     命名标记：一个名字一个文件，原子改指向
  data/
    part-*.json          数据文件（行数组，按列名存 JSON）
```

## 快照与提交模型

- **提交协议**：先写数据文件，再写 `metadata/v{N}.json`，最后用
  「临时文件 + rename」原子替换 `metadata/current`。任何时刻崩溃，
  `current` 要么指向旧版本要么指向新版本，绝不出现半可见状态；
  未被引用的文件只是垃圾，不影响一致性，由 Vacuum 回收。
- **跨进程并发**：所有提交在「进程内互斥锁 + 目录级 flock 排他锁」下完成。
  执行器分两段：第一次锁外乐观准备、锁内 CAS；CAS 发现基版本被推进则持锁
  重算一次（协作进程无法再插队，必然成功，不会饿死）。若连持锁期间都观察到
  基版本变化（存在不遵守锁协议的外部写者），返回 **`ErrConcurrentModification`**
  ——这是一个可与普通错误区分的结果，调用方自行决定是否重试，而不是稀里糊涂失败。
- **多进程建表**：并发 `Create` 同一目录只有一个成功，其余得到明确错误。
- **快照隔离**：读取只经由某个 `v{N}.json` 解析不可变数据文件清单，不取写锁，
  与并发写入/回收互不阻塞；同一版本重复读取结果确定可复现。

> 锁基于 `flock(2)`，在类 Unix 本地文件系统（Linux/macOS）验证；
> 进程崩溃时内核自动释放锁，不会死锁。Windows 构建提供占位但不受支持。

## 跨进程并发怎么用

不同进程各自 `Open(dir)` 得到独立句柄即可并发读写。冲突处理：

```go
for {
    v, err := tbl.Append(rows)
    if err == nil {
        _ = v
        break
    }
    var cm lakehouse.ErrConcurrentModification
    if errors.As(err, &cm) {
        continue // 版本被别的进程抢先：你来决定重试
    }
    return err // 其它错误不重试
}
```

## 命名标记（tag）

- `Tag(name, version)`：给某次提交命名；`version<=0` 表示当前最新。
  `tags/<name>.json` 是一个独立文件，**同名再打即原子覆盖**，旧指向立即作废，
  任何读者都不可能看到“一个名字两个版本”。
- `ResolveTag(name)`：解析名字当前指向的版本；名字不存在返回 `ErrTagNotFound`。
- `DeleteTag(name)`：删除标记。
- 标记名允许 `[A-Za-z0-9._-]`、长度 1..128；非法返回 `ErrInvalidTag`。
- **不允许把名字钉在读不到的版本上**：目标从未存在返回
  `ErrSnapshotNotFound`，已被回收返回 `ErrSnapshotExpired`。

## 历史列表

`History()` 返回从新到旧的 `[]HistoryEntry`，每条含：版本号、提交时间、
操作类型（create/append/upsert/compact/schema/rollback）、指向它的标记名，
以及 `Expired`（是否已被 Vacuum 回收——已回收版本仍以墓碑形式留在列表里，
便于事后追溯）。

## 回滚

`Rollback(version)` 复用目标版本的**结构与数据文件清单**，提交为一个新快照
（`Operation=rollback`，并记录 `RollbackFromVersion` 便于审计）：

- 回滚**本身是一次新提交**，中间发生过的版本不被抹掉，仍可在历史中看到/读到；
- 回滚后读取即得到目标版本的**结构和数据语义**（老快照没有的列读为缺失）；
- 回滚之后的新提交沿用回滚后的结构；可以再次做结构演进（重新加列仍合法）；
- `version<=0` 返回 `*ErrInvalidRollback`；目标从未存在返回
  `ErrSnapshotNotFound`；**目标已被 Vacuum 回收返回 `ErrSnapshotExpired`，
  明确报错而不是静默成功**。

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
被拒绝的变更不推进版本。历史快照永远按各自快照内保存的结构读取。

回滚把结构也一并恢复，因此不存在“新数据配老结构”的错配；回滚后再加回曾删除
的可选列，老数据该列读为缺失。

## 合并与更新语义

- `Compact()`：把当前快照全部数据文件重写为少量文件；定义了主键时按
  「后提交者覆盖先提交者」的确定规则去重。
- `Upsert(rows)`：按主键合并——已存在的主键原位覆盖，新主键按批内
  顺序追加。**单批内重复主键返回 `ErrDuplicatePrimaryKey`，拒绝整个
  提交，不做静默取舍**。

## 空间回收（Vacuum）

```go
rep, err := tbl.Vacuum(lakehouse.RetentionPolicy{
    MaxVersions: 100,             // 保留最近 100 个快照；0 = 不按数量限制
    OlderThan:   7 * 24 * time.Hour, // 同时只清理该时间点之前的；0 = 不按时间
    MinFileAge:  time.Hour,       // 默认 1h；负值=关闭文件年龄闸门（测试用）
})
```

**保留判定（两个窗口取并集，落在任一维度内即保留）**：

- 当前版本永远保留；
- 任何 tag 指向的版本永远保留（回滚快照复用的老文件因此也被保住）；
- `MaxVersions`：保留最近 N 个**逻辑版本**（含已回收位置，窗口不会因重跑滑动）；
- `OlderThan`：提交时间晚于 `now-OlderThan` 的保留。
- 只有同时落在两个窗口之外、且不被当前/tag 引用的快照才过期。

**文件安全**：

- 数据文件只要还被任一**保留**快照引用就不删；脱离引用链的（例如 compact
  重写后的旧文件）才回收；
- `MinFileAge`（默认 1 小时）保护“落盘不久、可能正被并发提交使用”的数据/
  临时文件；
- 进行中的写入使用 `.inflight-*` 临时名，Vacuum **永不删除**；只有崩溃遗留的
  `.tmp-*`（足够老）才清理；
- 提交在锁内 CAS 时会复核自己的数据文件仍在，若恰好被上一轮回收清掉则本次
  作废、锁内重算，绝不引用一个已删除的文件。

**崩溃安全与幂等**：Vacuum 全程持 flock；先把过期版本**原子写入墓碑日志
`gc-log.json`**，再删除数据文件与元数据。中途崩溃后重跑：依据墓碑与现存文件
重新收敛，墓碑不重复写、孤儿文件被补删，不越清越乱。

**回收之后读得到什么**：

- 仍在保留范围内的快照（含当前、tag 指向）**照常可读**，数据完整；
- 已过期版本读取返回 **`ErrSnapshotExpired`**（区别于从未存在的
  `ErrSnapshotNotFound`），但仍出现在 `History()` 中并标记 `expired`；
- `VacuumReport.DanglingTags` 会报告指向“既不在磁盘也不在墓碑”版本的标记
  （正常使用不会产生，出现通常意味着目录被外部破坏）。

## 错误一览（均可 errors.As 区分）

| 错误 | 含义 |
|---|---|
| `ErrConcurrentModification` | 提交基版本被（非协作）写者推进，调用方可重试 |
| `ErrIncompatibleSchema` | 结构变更不兼容，`Reason` 区分子原因 |
| `ErrDuplicatePrimaryKey` | 单批内主键重复，整批拒绝 |
| `ErrSnapshotNotFound` | 版本从未存在 |
| `ErrSnapshotExpired` | 版本曾存在，已被 Vacuum 回收 |
| `ErrTagNotFound` / `ErrInvalidTag` | 标记不存在 / 标记名非法 |
| `ErrInvalidRollback` | 回滚参数非法（如版本号非正） |

## 本地验证

```bash
go build ./...
go vet ./...
gofmt -l .                 # 应无输出
go test ./... -v           # 日志按“业务场景 -> 用例”输出
go test ./... -race -count=10   # 并发/竞态压力
```

跨进程用例（`TestMultiProcessConcurrentWriters`、
`TestMultiProcessCreateRaces`）通过以环境变量重启测试二进制、拉起真实子进程
的方式验证：多个进程各自 `Open` 同一目录并发追加，断言无覆盖、无丢失、
版本链连续、无半写状态。

测试覆盖的业务场景（`-v` 日志按「业务场景 → 用例」打印）：

- **快照与并发**：`TestSnapshotCommitAndRead`、`TestFailedCommitLeavesNoPartialState`、
  `TestConcurrentCommits`、`TestMultiProcessConcurrentWriters`、
  `TestMultiProcessCreateRaces`、`TestConcurrentModificationDistinguishable`、
  `TestReadWhileOthersWrite`
- **结构演进**：`TestSchemaEvolution`、`TestIncompatibleSchemaRejected`
- **合并与更新**：`TestCompact`、`TestUpsert`、`TestConcurrentUpsertAndCompact`
- **命名标记 / 历史 / 回滚**：`TestTagBasics`、`TestTagCrossProcess`、
  `TestTagRefusesExpiredTarget`、`TestHistoryList`、`TestHistoryTimestampsMonotonic`、
  `TestRollback`、`TestRollbackThenResume`
- **空间回收**：`TestVacuumMaxVersions`、`TestVacuumProtectsTagsAndCurrent`、
  `TestVacuumOlderThan`、`TestVacuumIdempotentAndCrashConvergent`、
  `TestVacuumRollbackFileProtection`、`TestVacuumCleansTempFiles`、
  `TestVacuumRollbackExpiredTarget`、`TestVacuumConcurrentWithWriter`

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
tbl.Tag("batch-1", v1)                  // 给某次提交命名
hist, _ := tbl.History()                 // 查看历史（时间/操作/标记/过期）
tbl.Rollback(v1)                         // 回滚（一次新提交，历史保留）
tbl.Vacuum(lakehouse.RetentionPolicy{MaxVersions: 100}) // 只留最近 100 个
rows, _ := tbl.Read(v1)  // 若已回收 -> ErrSnapshotExpired；否则历史快照可读
latest, _ := tbl.Read(0) // 最新快照
```
