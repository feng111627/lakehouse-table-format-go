package lakehouse

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func testSchema() Schema {
	return Schema{
		Fields: []Field{
			{Name: "id", Type: TypeInt, Required: true},
			{Name: "name", Type: TypeString},
			{Name: "score", Type: TypeFloat},
		},
		PrimaryKey: []string{"id"},
	}
}

// TestSnapshotCommitAndRead 验证：按快照提交写入、原子生效、历史快照可复现读取。
func TestSnapshotCommitAndRead(t *testing.T) {
	t.Log("业务场景: 快照提交与读取 - 每次提交原子生效，历史快照可复现")
	dir := t.TempDir()
	tbl, err := Create(dir, testSchema())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	v1, err := tbl.Append([]Row{{"id": 1, "name": "a", "score": 1.5}})
	if err != nil {
		t.Fatalf("append1: %v", err)
	}
	v2, err := tbl.Append([]Row{{"id": 2, "name": "b", "score": 2.5}})
	if err != nil {
		t.Fatalf("append2: %v", err)
	}
	t.Logf("用例[连续提交]: 提交版本 v%d, v%d", v1, v2)
	if v2 != v1+1 {
		t.Fatalf("versions should be sequential: v1=%d v2=%d", v1, v2)
	}

	rows1a, _ := tbl.Read(v1)
	rows1b, _ := tbl.Read(v1)
	if len(rows1a) != 1 || len(rows1b) != 1 {
		t.Fatalf("snapshot v%d should have exactly 1 row, got %d/%d", v1, len(rows1a), len(rows1b))
	}
	if fmt.Sprint(rows1a) != fmt.Sprint(rows1b) {
		t.Fatalf("repeated reads of v%d diverged: %v vs %v", v1, rows1a, rows1b)
	}
	t.Logf("用例[快照隔离]: v%d 重复读取结果一致: %v", v1, rows1a)

	rows2, _ := tbl.Read(0) // 最新
	if len(rows2) != 2 {
		t.Fatalf("latest snapshot should have 2 rows, got %d", len(rows2))
	}
	t.Logf("用例[最新快照]: 最新快照共 %d 行", len(rows2))

	if _, err := tbl.SnapshotAt(999); !errors.As(err, new(ErrSnapshotNotFound)) {
		t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
	}
	t.Log("用例[缺失快照]: 读取不存在的版本返回 ErrSnapshotNotFound")
}

// TestFailedCommitLeavesNoPartialState 验证：校验失败的提交不推进版本、不留半可见状态。
func TestFailedCommitLeavesNoPartialState(t *testing.T) {
	t.Log("业务场景: 提交中途失败 - 不产生半可见状态")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	v1, _ := tbl.Append([]Row{{"id": 1, "name": "a"}})

	// 类型不合法的行，提交应失败
	if _, err := tbl.Append([]Row{{"id": 2, "name": 123}}); err == nil {
		t.Fatal("append with wrong type should fail")
	}
	// 批内重复主键，提交应失败
	var dupErr ErrDuplicatePrimaryKey
	if _, err := tbl.Append([]Row{{"id": 2}, {"id": 2}}); !errors.As(err, &dupErr) {
		t.Fatalf("expected ErrDuplicatePrimaryKey, got %v", err)
	}
	t.Log("用例[非法提交]: 类型错误与批内重复主键均被拒绝")

	cur, _ := tbl.Current()
	if cur.Version != v1 {
		t.Fatalf("failed commits must not advance version: got v%d, want v%d", cur.Version, v1)
	}
	rows, _ := tbl.Read(0)
	if len(rows) != 1 {
		t.Fatalf("failed commits must not be visible: got %d rows", len(rows))
	}
	t.Logf("用例[原子性]: 失败后当前版本仍为 v%d，可见行数 %d", cur.Version, len(rows))
}

// TestConcurrentCommits 验证：多 goroutine 并发提交，全部生效且版本链连续、读取稳定。
func TestConcurrentCommits(t *testing.T) {
	t.Log("业务场景: 并发提交 - 无丢失、无半可见、版本链连续")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())

	const writers = 8
	const perWriter = 5
	var wg sync.WaitGroup
	versions := make([][]int, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				id := w*perWriter + i + 1
				v, err := tbl.Append([]Row{{"id": id, "name": fmt.Sprintf("w%d", w)}})
				if err != nil {
					t.Errorf("writer %d append: %v", w, err)
					return
				}
				versions[w] = append(versions[w], v)
			}
		}(w)
	}
	wg.Wait()

	cur, err := tbl.Current()
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	want := 1 + writers*perWriter // 建表 v1 + 全部提交
	if cur.Version != want {
		t.Fatalf("expected final version %d, got %d (lost commits?)", want, cur.Version)
	}
	t.Logf("用例[无丢失]: %d 个并发写者各提交 %d 次，最终版本 v%d", writers, perWriter, cur.Version)

	// 版本链连续：每个版本都可读
	for v := 1; v <= cur.Version; v++ {
		if _, err := tbl.SnapshotAt(v); err != nil {
			t.Fatalf("snapshot chain broken at v%d: %v", v, err)
		}
	}
	t.Logf("用例[版本链]: v1..v%d 全部可读", cur.Version)

	rows, err := tbl.Read(0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(rows) != writers*perWriter {
		t.Fatalf("expected %d rows, got %d", writers*perWriter, len(rows))
	}
	seen := map[any]bool{}
	for _, r := range rows {
		seen[r["id"]] = true
	}
	if len(seen) != writers*perWriter {
		t.Fatalf("duplicate or missing ids: %d unique of %d", len(seen), writers*perWriter)
	}
	t.Logf("用例[一致性]: 最新快照共 %d 行，主键无重复无丢失", len(rows))
}
