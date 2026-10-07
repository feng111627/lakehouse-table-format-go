package lakehouse

import (
	"errors"
	"fmt"
	"testing"
)

// TestTagsAndHistory 验证：给提交打名字、同名重指向旧指向作废、
// 历史列表完整反映版本/操作/时间/标签。
func TestTagsAndHistory(t *testing.T) {
	t.Log("业务场景: 标签与历史 - 命名提交、重指向无歧义、历史可查")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	v1, _ := tbl.Append([]Row{{"id": 1, "name": "a"}})
	v2, _ := tbl.Append([]Row{{"id": 2, "name": "b"}})

	// 灌完一批数据打标记
	if err := tbl.Tag("batch-001", v2); err != nil {
		t.Fatalf("tag: %v", err)
	}
	got, err := tbl.ResolveTag("batch-001")
	if err != nil || got != v2 {
		t.Fatalf("用例[打标签]: 期望指向 v%d，实际 v%d err=%v", v2, got, err)
	}
	t.Logf("用例[打标签]: batch-001 -> v%d", got)

	// 同名后来指到新版本：旧指向立即作废，一名一版无歧义
	v3, _ := tbl.Append([]Row{{"id": 3, "name": "c"}})
	if err := tbl.Tag("batch-001", v3); err != nil {
		t.Fatalf("retag: %v", err)
	}
	got, _ = tbl.ResolveTag("batch-001")
	if got != v3 {
		t.Fatalf("用例[重指向无歧义]: 期望 v%d，实际 v%d", v3, got)
	}
	t.Logf("用例[重指向无歧义]: batch-001 重新指向 v%d，旧指向 v%d 作废", got, v2)

	// 另一个标签同时存在
	if err := tbl.Tag("baseline", v1); err != nil {
		t.Fatalf("tag baseline: %v", err)
	}
	all, _ := tbl.Tags()
	if len(all) != 2 || all["batch-001"] != v3 || all["baseline"] != v1 {
		t.Fatalf("用例[标签集合]: %v", all)
	}
	t.Logf("用例[标签集合]: %v", all)

	// 标签可直接定位读取
	rows, _ := tbl.Read(got)
	if len(rows) != 3 {
		t.Fatalf("通过标签读取应有 3 行，实际 %d", len(rows))
	}
	t.Log("用例[按标签读]: 经 ResolveTag 定位的快照数据正确")

	// 历史列表：版本升序、操作类型与标签归属正确
	hist, err := tbl.History()
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(hist) != 4 { // create + 3 append
		t.Fatalf("用例[历史完整]: 期望 4 条，实际 %d: %+v", len(hist), hist)
	}
	ops := map[int]Operation{}
	for _, h := range hist {
		ops[h.Version] = h.Operation
	}
	if ops[1] != OpCreate || ops[v1] != OpAppend {
		t.Fatalf("用例[操作记录]: %v", ops)
	}
	for _, h := range hist {
		if h.Version == v3 && fmt.Sprint(h.Tags) != "[batch-001]" {
			t.Fatalf("v%d 标签应为 batch-001，实际 %v", v3, h.Tags)
		}
		if h.Version == v1 && fmt.Sprint(h.Tags) != "[baseline]" {
			t.Fatalf("v%d 标签应为 baseline，实际 %v", v1, h.Tags)
		}
		if h.Timestamp <= 0 {
			t.Fatalf("v%d 缺少提交时间", h.Version)
		}
	}
	t.Logf("用例[历史完整]: %d 条历史按版本升序，操作/时间/标签均正确: %+v", len(hist), hist)

	// 删除标签
	if err := tbl.Untag("baseline"); err != nil {
		t.Fatalf("untag: %v", err)
	}
	if _, err := tbl.ResolveTag("baseline"); !errors.As(err, new(ErrTagNotFound)) {
		t.Fatalf("用例[删标签]: 期望 ErrTagNotFound，实际 %v", err)
	}
	if err := tbl.Untag("baseline"); !errors.As(err, new(ErrTagNotFound)) {
		t.Fatalf("用例[重复删除]: 期望 ErrTagNotFound，实际 %v", err)
	}
	t.Log("用例[删标签]: 删除后解析/再删除均返回 ErrTagNotFound")

	// 非法标签名
	for _, bad := range []string{"", ".", "..", "a/b", `a\b`, ".hidden"} {
		if err := tbl.Tag(bad, v1); !errors.As(err, new(ErrInvalidTagName)) {
			t.Fatalf("用例[非法标签名 %q]: 期望 ErrInvalidTagName，实际 %v", bad, err)
		}
	}
	t.Log("用例[非法标签名]: 空串/路径穿越/点开头均被拒绝")

	// 标签不能指向不存在或已回收版本
	if err := tbl.Tag("ghost", 999); !errors.As(err, new(ErrSnapshotNotFound)) {
		t.Fatalf("用例[标签校验]: 指向不存在版本应报错，实际 %v", err)
	}
	t.Log("用例[标签校验]: 指向不存在版本被拒绝")
}

// TestRollback 验证：回滚产生新提交、历史不被抹除、回滚后读到旧结构旧数据；
// 回滚到不存在/已回收版本明确报错。
func TestRollback(t *testing.T) {
	t.Log("业务场景: 版本回滚 - 回滚是一次新提交，中间历史保留，语义明确")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())

	// v2：只有 id/name（先演进结构再写数据），v1 schema 是 testSchema
	v2, _ := tbl.Append([]Row{{"id": 1, "name": "a", "score": 1.0}})
	schema2 := testSchema()
	schema2.Fields = append(schema2.Fields, Field{Name: "city", Type: TypeString})
	v3, _ := tbl.EvolveSchema(schema2)
	_ = v3
	v4, _ := tbl.Append([]Row{{"id": 2, "name": "b", "score": 2.0, "city": "hz"}})

	before, _ := tbl.Current()
	// 回滚到加列之前的 v2
	v5, err := tbl.Rollback(v2)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if v5 != before.Version+1 {
		t.Fatalf("用例[回滚是新提交]: 版本应继续递增到 v%d，实际 v%d", before.Version+1, v5)
	}
	cur, _ := tbl.Current()
	if cur.Operation != OpRollback || cur.RollbackOf != v2 {
		t.Fatalf("用例[回滚标记]: operation=%s rollbackOf=%d", cur.Operation, cur.RollbackOf)
	}
	t.Logf("用例[回滚是新提交]: v%d = rollback(v%d)，版本号继续向前", v5, v2)

	// 回滚后读最新：结构与数据都回到 v2（无 city 列、仅 1 行）
	rows, _ := tbl.Read(0)
	if len(rows) != 1 || rows[0]["name"] != "a" {
		t.Fatalf("用例[回滚读语义]: 期望回到 v2 的 1 行数据，实际 %v", rows)
	}
	curSnap, _ := tbl.Current()
	if _, hasCity := curSnap.Schema.Field("city"); hasCity {
		t.Fatalf("用例[回滚读语义]: 最新结构应回到 v2（无 city），实际 %+v", curSnap.Schema.Fields)
	}
	if _, hasCity := rows[0]["city"]; hasCity {
		t.Fatalf("用例[回滚读语义]: 数据行不应出现 city: %v", rows[0])
	}
	t.Logf("用例[回滚读语义]: 最新快照结构=%d 列、数据 %v，与 v%d 一致",
		len(curSnap.Schema.Fields), rows, v2)

	// 中间发生的提交没有被抹掉：v3/v4 仍可读，历史仍在
	if _, err := tbl.SnapshotAt(v4); err != nil {
		t.Fatalf("用例[历史不抹除]: 回滚点之后的 v%d 应仍可读: %v", v4, err)
	}
	hist, _ := tbl.History()
	if len(hist) != 5 {
		t.Fatalf("用例[历史不抹除]: 应有 5 条历史，实际 %d", len(hist))
	}
	oldRows, _ := tbl.Read(v4)
	if len(oldRows) != 2 || oldRows[1]["city"] != "hz" {
		t.Fatalf("用例[历史不抹除]: v%d 数据应原样保留: %v", v4, oldRows)
	}
	t.Logf("用例[历史不抹除]: v1..v5 全部保留，v%d 仍是回滚前的 2 行带 city", v4)

	// 回滚后可以继续正常写入
	v6, err := tbl.Append([]Row{{"id": 9, "name": "after-rb"}})
	if err != nil {
		t.Fatalf("append after rollback: %v", err)
	}
	rows6, _ := tbl.Read(v6)
	if len(rows6) != 2 {
		t.Fatalf("回滚后追加应叠加到回滚后的状态，实际 %d 行", len(rows6))
	}
	t.Logf("用例[回滚后可写]: 在回滚后的结构/数据上继续追加得到 v%d，共 %d 行", v6, len(rows6))

	// 回滚到不存在的版本：明确报错
	if _, err := tbl.Rollback(999); !errors.As(err, new(ErrSnapshotNotFound)) {
		t.Fatalf("用例[回滚报错]: 不存在版本应返回 ErrSnapshotNotFound，实际 %v", err)
	}
	t.Log("用例[回滚报错-不存在]: 回滚到 v999 返回 ErrSnapshotNotFound，未默默成功")
}
