package lakehouse

import (
	"errors"
	"fmt"
	"testing"
)

// TestUpsert 验证：按主键更新去重，最新快照数据一致，批内重复主键被拒绝。
func TestUpsert(t *testing.T) {
	t.Log("业务场景: 增量更新 - 按主键覆盖/插入，重复主键不静默取舍")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	tbl.Append([]Row{{"id": 1, "name": "a", "score": 1.0}})
	tbl.Append([]Row{{"id": 2, "name": "b", "score": 2.0}})
	vBefore, _ := tbl.Current()

	// 更新 id=2，插入 id=3
	vUp, err := tbl.Upsert([]Row{
		{"id": 2, "name": "b2", "score": 22.0},
		{"id": 3, "name": "c", "score": 3.0},
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	rows, _ := tbl.Read(0)
	if len(rows) != 3 {
		t.Fatalf("after upsert expect 3 rows, got %d: %v", len(rows), rows)
	}
	if rows[1]["name"] != "b2" || rows[1]["score"] != 22.0 {
		t.Fatalf("id=2 should be overwritten in place: %v", rows[1])
	}
	t.Logf("用例[覆盖+插入]: v%d 后最新快照 %v", vUp, rows)

	// 历史快照不受 upsert 影响
	oldRows, _ := tbl.Read(vBefore.Version)
	if len(oldRows) != 2 || oldRows[1]["name"] != "b" {
		t.Fatalf("historical snapshot v%d changed after upsert: %v", vBefore.Version, oldRows)
	}
	t.Logf("用例[历史不变]: v%d 仍为 %v", vBefore.Version, oldRows)

	// 批内重复主键：拒绝且不生效
	var dupErr ErrDuplicatePrimaryKey
	if _, err := tbl.Upsert([]Row{{"id": 9, "name": "x"}, {"id": 9, "name": "y"}}); !errors.As(err, &dupErr) {
		t.Fatalf("expected ErrDuplicatePrimaryKey, got %v", err)
	}
	rows, _ = tbl.Read(0)
	if len(rows) != 3 {
		t.Fatalf("rejected upsert must not take effect: %v", rows)
	}
	t.Logf("用例[重复主键]: 批内重复 id=9 被拒绝(%v)，数据未变", dupErr)
}

// TestCompact 验证：小文件合并后最新快照有效数据一致，历史快照仍可读。
func TestCompact(t *testing.T) {
	t.Log("业务场景: 小文件合并 - 有效数据一致，历史快照可读")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())

	// 制造多个小文件，其中 id=1 被多次追加（后写覆盖先写）
	var versions []int
	for i := 0; i < 6; i++ {
		v, err := tbl.Append([]Row{{"id": i % 3, "name": fmt.Sprintf("n%d", i), "score": float64(i)}})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		versions = append(versions, v)
	}
	before, _ := tbl.Current()
	t.Logf("用例[小文件]: 6 次提交产生 %d 个数据文件", len(before.DataFiles))
	if len(before.DataFiles) != 6 {
		t.Fatalf("expected 6 data files, got %d", len(before.DataFiles))
	}

	vC, err := tbl.Compact()
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	after, _ := tbl.Current()
	if len(after.DataFiles) >= len(before.DataFiles) {
		t.Fatalf("compact should reduce file count: %d -> %d", len(before.DataFiles), len(after.DataFiles))
	}
	t.Logf("用例[合并]: v%d 后文件数 %d -> %d", vC, len(before.DataFiles), len(after.DataFiles))

	// 合并后按主键去重，id 0/1/2 各保留最后写入
	rows, _ := tbl.Read(0)
	if len(rows) != 3 {
		t.Fatalf("after compact expect 3 deduped rows, got %d: %v", len(rows), rows)
	}
	byID := map[float64]Row{}
	for _, r := range rows {
		byID[r["id"].(float64)] = r
	}
	if byID[0]["name"] != "n3" || byID[1]["name"] != "n4" || byID[2]["name"] != "n5" {
		t.Fatalf("compact must keep last write per key: %v", rows)
	}
	t.Logf("用例[去重]: 后主键覆盖先主键，结果 %v", rows)

	// 历史快照仍按原样可读（含去重前的全部行）
	hist, _ := tbl.Read(versions[5])
	if len(hist) != 6 {
		t.Fatalf("historical snapshot v%d should still have 6 rows, got %d", versions[5], len(hist))
	}
	t.Logf("用例[历史可读]: v%d 仍读出合并前 %d 行", versions[5], len(hist))
}

// TestConcurrentUpsertAndCompact 验证：更新与合并并发进行时结果仍一致。
func TestConcurrentUpsertAndCompact(t *testing.T) {
	t.Log("业务场景: 并发维护 - upsert 与 compact 并发，最终数据一致")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	for i := 1; i <= 10; i++ {
		if _, err := tbl.Append([]Row{{"id": i, "name": fmt.Sprintf("n%d", i)}}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	done := make(chan error, 2)
	go func() {
		var err error
		for i := 1; i <= 10; i++ {
			if _, err = tbl.Upsert([]Row{{"id": i, "name": fmt.Sprintf("u%d", i), "score": float64(i * 10)}}); err != nil {
				break
			}
		}
		done <- err
	}()
	go func() {
		_, err := tbl.Compact()
		done <- err
	}()
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatalf("concurrent op: %v", err)
		}
	}

	rows, err := tbl.Read(0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(rows) != 10 {
		t.Fatalf("expect 10 rows after concurrent maintenance, got %d", len(rows))
	}
	for _, r := range rows {
		if r["name"] != fmt.Sprintf("u%v", r["id"].(float64)) {
			t.Fatalf("row not fully upserted: %v", r)
		}
	}
	t.Logf("用例[并发一致]: upsert 与 compact 交错后 10 行全部为更新后数据")
}
