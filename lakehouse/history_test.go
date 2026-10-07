package lakehouse

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestTagBasics 验证：给提交命名、按名解析、重名改指向旧指向作废、删除。
func TestTagBasics(t *testing.T) {
	t.Log("业务场景: 命名标记 - 一个名字只对应一个版本，改指向后无歧义")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	v1, _ := tbl.Append([]Row{{"id": 1, "name": "a"}})
	v2, _ := tbl.Append([]Row{{"id": 2, "name": "b"}})

	// 给 v1 打标记
	if _, err := tbl.Tag("batch-2026-10-08", v1); err != nil {
		t.Fatalf("tag: %v", err)
	}
	got, err := tbl.ResolveTag("batch-2026-10-08")
	if err != nil || got != v1 {
		t.Fatalf("resolve tag -> %d, err=%v, want v%d", got, err, v1)
	}
	t.Logf("用例[打标解析]: batch-2026-10-08 -> v%d", got)

	// 同名改指向 v2：旧指向立即作废，不可能一名两版本
	if _, err := tbl.Tag("batch-2026-10-08", v2); err != nil {
		t.Fatalf("re-tag: %v", err)
	}
	got, _ = tbl.ResolveTag("batch-2026-10-08")
	if got != v2 {
		t.Fatalf("tag should repoint to v%d, got v%d", v2, got)
	}
	tags, _ := tbl.listTags()
	if len(tags) != 1 {
		t.Fatalf("repoint must overwrite, not create a second tag, got %d", len(tags))
	}
	t.Logf("用例[改指向唯一]: 重打后 batch-2026-10-08 -> v%d，标记总数仍为 %d", got, len(tags))

	// 0 表示当前最新
	cur, _ := tbl.Current()
	if _, err := tbl.Tag("latest_mark", 0); err != nil {
		t.Fatalf("tag latest: %v", err)
	}
	got, _ = tbl.ResolveTag("latest_mark")
	if got != cur.Version {
		t.Fatalf("tag(0) should point at current v%d, got v%d", cur.Version, got)
	}
	t.Logf("用例[最新打标]: Tag(name,0) -> 当前 v%d", got)

	// 非法名字
	for _, bad := range []string{"", "a/b", "带中文", ".."} {
		var inv ErrInvalidTag
		if _, err := tbl.Tag(bad, v1); !errors.As(err, &inv) {
			t.Fatalf("tag %q should be ErrInvalidTag, got %v", bad, err)
		}
	}
	t.Log("用例[名字校验]: 空名/斜杠/中文/.. 均返回 ErrInvalidTag")

	// 不存在的标记
	var nf ErrTagNotFound
	if _, err := tbl.ResolveTag("nope"); !errors.As(err, &nf) {
		t.Fatalf("expected ErrTagNotFound, got %v", err)
	}
	if err := tbl.DeleteTag("nope"); !errors.As(err, &nf) {
		t.Fatalf("delete missing tag expected ErrTagNotFound, got %v", err)
	}
	t.Log("用例[标记缺失]: 解析/删除不存在的名字返回 ErrTagNotFound")

	// 删除后解析失败
	if err := tbl.DeleteTag("latest_mark"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := tbl.ResolveTag("latest_mark"); !errors.As(err, &nf) {
		t.Fatalf("deleted tag should be unresolvable, got %v", err)
	}
	t.Log("用例[删除标记]: 删除后再解析返回 ErrTagNotFound")
}

// TestTagCrossProcess 验证：不同进程/句柄看到同一份标记视图。
func TestTagCrossProcess(t *testing.T) {
	t.Log("业务场景: 标记跨进程可见 - 一个进程打标，另一个进程立刻可解析")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	v, _ := tbl.Append([]Row{{"id": 1, "name": "a"}})
	if _, err := tbl.Tag("imported", v); err != nil {
		t.Fatalf("tag: %v", err)
	}
	other, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := other.ResolveTag("imported")
	if err != nil || got != v {
		t.Fatalf("other process view: %d, err=%v, want v%d", got, err, v)
	}
	t.Logf("用例[跨句柄可见]: 重新 Open 后 imported -> v%d", got)
}

// TestHistoryList 验证：历史列表按版本倒序，带时间/操作/标记/过期标记。
func TestHistoryList(t *testing.T) {
	t.Log("业务场景: 历史列表 - 每个版本的时间、操作、名字一目了然")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	va, _ := tbl.Append([]Row{{"id": 1, "name": "a"}})
	tbl.EvolveSchema(Schema{
		Fields:     append(fieldCopies(testSchema().Fields), Field{Name: "note", Type: TypeString}),
		PrimaryKey: []string{"id"},
	})
	vb, _ := tbl.Append([]Row{{"id": 2, "name": "b"}})
	tbl.Tag("mark-a", va)
	tbl.Tag("mark-b", vb)

	hist, err := tbl.History()
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(hist) != 4 { // create + append + schema + append
		t.Fatalf("expected 4 history entries, got %d: %+v", len(hist), hist)
	}
	if hist[0].Version != vb || hist[len(hist)-1].Version != 1 {
		t.Fatalf("history must be newest-first: %+v", hist)
	}
	opByV := map[int]Operation{}
	tagsByV := map[int][]string{}
	for _, e := range hist {
		if e.Timestamp <= 0 {
			t.Fatalf("v%d missing timestamp", e.Version)
		}
		opByV[e.Version] = e.Operation
		tagsByV[e.Version] = e.Tags
		if e.Expired {
			t.Fatalf("v%d should not be expired before vacuum", e.Version)
		}
	}
	if opByV[1] != OpCreate || opByV[va] != OpAppend || opByV[vb] != OpAppend {
		t.Fatalf("unexpected operations: %+v", opByV)
	}
	if s := hist[1].Operation; s != OpSchema && hist[2].Operation != OpSchema {
		t.Fatalf("schema evolution missing in history: %+v", hist)
	}
	if fmt.Sprint(tagsByV[va]) != "[mark-a]" || fmt.Sprint(tagsByV[vb]) != "[mark-b]" {
		t.Fatalf("tags in history wrong: %+v", tagsByV)
	}
	t.Logf("用例[历史内容]: 倒序 %d 条，操作/时间齐全，v%d=[mark-a] v%d=[mark-b]", len(hist), va, vb)
	for _, e := range hist {
		t.Logf("    v%d op=%s tags=%v expired=%v", e.Version, e.Operation, e.Tags, e.Expired)
	}
}

func fieldCopies(fs []Field) []Field {
	out := make([]Field, len(fs))
	copy(out, fs)
	return out
}

// TestRollback 验证：回滚是一次新提交，中间历史保留，回滚后按目标版本的
// 结构与数据读取；目标不存在/已回收必须明确报错。
func TestRollback(t *testing.T) {
	t.Log("业务场景: 表回滚 - 回滚产生新提交、历史不丢、结构数据回到目标版本")
	dir := t.TempDir()
	tbl, _ := Create(dir, Schema{
		Fields:     []Field{{Name: "id", Type: TypeInt, Required: true}, {Name: "name", Type: TypeString}},
		PrimaryKey: []string{"id"},
	})
	v1data, _ := tbl.Append([]Row{{"id": 1, "name": "old"}}) // v2
	// 演进结构：新增可选列 note
	evolved := Schema{
		Fields:     []Field{{Name: "id", Type: TypeInt, Required: true}, {Name: "name", Type: TypeString}, {Name: "note", Type: TypeString}},
		PrimaryKey: []string{"id"},
	}
	vSchema, err := tbl.EvolveSchema(evolved)
	if err != nil {
		t.Fatalf("evolve: %v", err)
	}
	vNew, _ := tbl.Append([]Row{{"id": 2, "name": "new", "note": "n2"}}) // v4
	rows, _ := tbl.Read(0)
	if len(rows) != 2 {
		t.Fatalf("pre-rollback expect 2 rows, got %d", len(rows))
	}
	cur, _ := tbl.Current()
	if len(cur.Schema.Fields) != 3 {
		t.Fatalf("pre-rollback schema should have 3 fields, got %d", len(cur.Schema.Fields))
	}
	t.Logf("用例[准备状态]: 数据提交 v%d, 结构演进 v%d, 新数据 v%d（3 列）", v1data, vSchema, vNew)

	// 回滚到 v2：结构与数据都应回到那一刻
	vrb, err := tbl.Rollback(v1data)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if vrb != vNew+1 {
		t.Fatalf("rollback must be a new commit: got v%d, want v%d", vrb, vNew+1)
	}
	rbCur, _ := tbl.Current()
	if rbCur.Operation != OpRollback {
		t.Fatalf("rollback snapshot operation = %q", rbCur.Operation)
	}
	if rbCur.RollbackFromVersion != v1data {
		t.Fatalf("rollback snapshot should record source v%d, got %d", v1data, rbCur.RollbackFromVersion)
	}
	if len(rbCur.Schema.Fields) != 2 {
		t.Fatalf("after rollback schema should be the 2-field schema, got %d fields", len(rbCur.Schema.Fields))
	}
	rbRows, err := tbl.Read(0)
	if err != nil || len(rbRows) != 1 || rbRows[0]["name"] != "old" {
		t.Fatalf("after rollback expect 1 row {old}, got %v err=%v", rbRows, err)
	}
	if _, hasNote := rbRows[0]["note"]; hasNote {
		t.Fatalf("rolled-back schema has no note column")
	}
	t.Logf("用例[回滚生效]: 新提交 v%d 使用 v%d 的 2 列结构与数据，读出 %v", vrb, v1data, rbRows)

	// 中间历史没有被抹掉：v4（新结构数据）仍然可读
	midRows, err := tbl.Read(vNew)
	if err != nil || len(midRows) != 2 {
		t.Fatalf("intermediate history must survive rollback: v%d rows=%v err=%v", vNew, midRows, err)
	}
	midSnap, _ := tbl.SnapshotAt(vSchema)
	if len(midSnap.Schema.Fields) != 3 {
		t.Fatalf("intermediate schema snapshot altered: %+v", midSnap.Schema)
	}
	t.Logf("用例[历史保留]: 回滚后 v%d 仍读出 %d 行的 3 列结构数据", vNew, len(midRows))

	// 回滚后再写：按回滚后的结构校验（note 列已不存在，数据文件仍合法）
	vAfter, err := tbl.Append([]Row{{"id": 3, "name": "after"}})
	if err != nil {
		t.Fatalf("append after rollback: %v", err)
	}
	if s, _ := tbl.SnapshotAt(vAfter); len(s.Schema.Fields) != 2 {
		t.Fatalf("post-rollback commit should carry 2-field schema")
	}
	t.Logf("用例[回滚后写入]: 新提交 v%d 沿用回滚后的 2 列结构", vAfter)

	// 历史中能看到 rollback 操作
	hist, _ := tbl.History()
	found := false
	for _, e := range hist {
		if e.Version == vrb && e.Operation == OpRollback {
			found = true
		}
	}
	if !found {
		t.Fatalf("rollback entry missing in history: %+v", hist)
	}
	t.Log("用例[历史可追溯]: rollback 作为一次操作出现在历史列表")

	// 不存在的版本：明确报错，不静默成功
	var nf ErrSnapshotNotFound
	if _, err := tbl.Rollback(999); !errors.As(err, &nf) {
		t.Fatalf("rollback to nonexistent version should fail with ErrSnapshotNotFound, got %v", err)
	}
	if cur2, _ := tbl.Current(); cur2.Version != vAfter {
		t.Fatalf("failed rollback must not advance version")
	}
	t.Log("用例[目标缺失]: 回滚不存在的版本返回 ErrSnapshotNotFound 且不推进版本")

	// 非正数参数
	var ir *ErrInvalidRollback
	if _, err := tbl.Rollback(0); !errors.As(err, &ir) {
		t.Fatalf("rollback(0) expected *ErrInvalidRollback, got %v", err)
	}
	t.Log("用例[参数非法]: Rollback(0) 返回 *ErrInvalidRollback")
}

// TestRollbackThenResume 验证：回滚到老结构后，后续提交基于老结构叠加，
// 再次演进可恢复新列。
func TestRollbackThenResume(t *testing.T) {
	t.Log("业务场景: 回滚后继续演进 - 结构语义随快照走，不影响后续演进")
	dir := t.TempDir()
	tbl, _ := Create(dir, Schema{
		Fields:     []Field{{Name: "id", Type: TypeInt, Required: true}},
		PrimaryKey: []string{"id"},
	})
	tbl.Append([]Row{{"id": 1}})
	vOld, _ := tbl.Append([]Row{{"id": 2}}) // v3
	evolved := Schema{
		Fields:     []Field{{Name: "id", Type: TypeInt, Required: true}, {Name: "tag", Type: TypeString}},
		PrimaryKey: []string{"id"},
	}
	tbl.EvolveSchema(evolved) // v4
	tbl.Append([]Row{{"id": 3, "tag": "x"}})

	vr, err := tbl.Rollback(vOld)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	rows, _ := tbl.Read(0)
	if len(rows) != 2 {
		t.Fatalf("expect 2 rows after rollback, got %d", len(rows))
	}
	// 回滚之后再次加列，仍然合法（历史快照里没有 tag 列，新增可选列允许）
	v2, err := tbl.EvolveSchema(evolved)
	if err != nil {
		t.Fatalf("re-evolve after rollback: %v", err)
	}
	if v2 <= vr {
		t.Fatalf("re-evolve must produce newer version")
	}
	cur, _ := tbl.Current()
	if len(cur.Schema.Fields) != 2 {
		t.Fatalf("re-evolved schema should have 2 fields, got %d", len(cur.Schema.Fields))
	}
	rows, _ = tbl.Read(0)
	if len(rows) != 2 { // 数据仍是回滚时的两行；新列对老数据读为缺失
		t.Fatalf("data after re-evolve changed unexpectedly: %d rows", len(rows))
	}
	if _, ok := rows[0]["tag"]; ok {
		t.Fatalf("old rows should read tag as missing")
	}
	t.Log("用例[再演进]: 回滚后可再次加列，老数据新列读为缺失")
}

// TestTagRefusesExpiredTarget 验证：不能把名字钉在已回收或不存在的版本上。
func TestTagRefusesExpiredTarget(t *testing.T) {
	t.Log("业务场景: 标记与回收联动 - 不允许给已回收/不存在的版本打标")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	v, _ := tbl.Append([]Row{{"id": 1}})
	for i := 0; i < 3; i++ {
		tbl.Append([]Row{{"id": i + 2}})
	}
	if _, err := tbl.Vacuum(RetentionPolicy{MaxVersions: 2, MinFileAge: -1}); err != nil {
		t.Fatal(err)
	}
	var expired ErrSnapshotExpired
	if _, err := tbl.Tag("ghost", v); !errors.As(err, &expired) {
		t.Fatalf("tag to vacuumed v%d expected ErrSnapshotExpired, got %v", v, err)
	}
	var nf ErrSnapshotNotFound
	if _, err := tbl.Tag("ghost2", 9999); !errors.As(err, &nf) {
		t.Fatalf("tag to nonexistent v9999 expected ErrSnapshotNotFound, got %v", err)
	}
	// 失败的打标不应留下文件
	var nf2 ErrTagNotFound
	if _, err := tbl.ResolveTag("ghost"); !errors.As(err, &nf2) {
		t.Fatalf("failed tag must not persist: %v", err)
	}
	t.Log("用例[拒绝悬空打标]: 给已回收版本打标返回 Expired、不存在返回 NotFound，且不留文件")
}

// TestHistoryTimestampsMonotonic 快速冒烟：时间戳可用且大体单调。
func TestHistoryTimestampsMonotonic(t *testing.T) {
	t.Log("业务场景: 历史时间 - 每个版本带提交时间且非减")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	var last int64
	for i := 0; i < 3; i++ {
		tbl.Append([]Row{{"id": i + 1}})
		time.Sleep(time.Millisecond)
	}
	hist, _ := tbl.History()
	// hist 倒序，检查时间戳非减（从旧到新）
	for i := len(hist) - 1; i >= 0; i-- {
		if hist[i].Timestamp < last {
			t.Fatalf("timestamps not non-decreasing: v%d %d < %d", hist[i].Version, hist[i].Timestamp, last)
		}
		last = hist[i].Timestamp
	}
	t.Logf("用例[时间非减]: %d 个版本时间戳从旧到新非减", len(hist))
}
