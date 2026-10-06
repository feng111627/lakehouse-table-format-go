package lakehouse

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
)

func rowsSignature(rs *ResultSet) string {
	pairs := make([]string, 0, len(rs.Rows))
	for _, r := range rs.Rows {
		pairs = append(pairs, fmt.Sprintf("%v|%v|%v|%v",
			r["id"], r["name"], r["age"], r["city"]))
	}
	sort.Strings(pairs)
	return fmt.Sprint(pairs)
}

func TestCompactPreservesData(t *testing.T) {
	logCase(t, "C 小文件合并与主键增量更新", "多次 append 产生多小文件，compact 后行数与内容一致")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		if _, err := tbl.Append(ctx, []Row{
			{"id": int64(i), "name": fmt.Sprintf("n%d", i), "age": int64(i)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	filesBefore, err := tbl.liveFiles(tbl.CurrentSnapshotID())
	if err != nil {
		t.Fatal(err)
	}
	if len(filesBefore) != 5 {
		t.Fatalf("live files before compact = %d, want 5", len(filesBefore))
	}
	rsBefore, _ := tbl.Read(ctx)
	sigBefore := rowsSignature(rsBefore)

	cr, err := tbl.Compact(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cr.NoOp || cr.FilesAdded != 1 || cr.FilesRemoved != 5 || cr.Records != 5 {
		t.Fatalf("compact result = %+v", cr)
	}
	filesAfter, _ := tbl.liveFiles(tbl.CurrentSnapshotID())
	if len(filesAfter) != 1 {
		t.Fatalf("live files after compact = %d, want 1", len(filesAfter))
	}
	rsAfter, _ := tbl.Read(ctx)
	if rowsSignature(rsAfter) != sigBefore {
		t.Fatalf("data changed by compaction:\nbefore=%s\nafter =%s", sigBefore, rowsSignature(rsAfter))
	}
	t.Logf("compact 合并 %d 个小文件 -> %d，行数 %d，内容签名一致",
		cr.FilesRemoved, cr.FilesAdded, cr.Records)

	// History of every pre-compact snapshot stays readable.
	for _, sid := range tbl.ids {
		rs, err := tbl.ReadAt(ctx, sid)
		if err != nil {
			t.Fatalf("history %d unreadable after compact: %v", sid, err)
		}
		_ = rs
	}
}

func TestCompactNoOpUnderThreshold(t *testing.T) {
	logCase(t, "C 小文件合并与主键增量更新", "小文件数低于阈值时 compact 为 NoOp")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	_, _ = tbl.Append(ctx, []Row{{"id": 1, "name": "a"}})
	headBefore := tbl.CurrentSnapshotID()
	cr, err := tbl.Compact(ctx, 5) // threshold 5, only 1 live file
	if err != nil {
		t.Fatal(err)
	}
	if !cr.NoOp || cr.SnapshotID != headBefore {
		t.Fatalf("expected no-op, got %+v", cr)
	}
	t.Logf("低于阈值不产生新快照: head 仍为 %d", headBefore)
}

func TestUpsertMerge(t *testing.T) {
	logCase(t, "C 小文件合并与主键增量更新", "upsert 按主键更新已有行并插入新行")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	s1, _ := tbl.Append(ctx, []Row{
		{"id": 1, "name": "old-1", "age": 11},
		{"id": 2, "name": "old-2"},
	})
	s2, err := tbl.Upsert(ctx, []Row{
		{"id": 1, "name": "new-1", "age": 12}, // update
		{"id": 3, "name": "new-3"},            // insert
	})
	if err != nil {
		t.Fatal(err)
	}
	if s2 != s1+1 {
		t.Fatalf("upsert snapshot = %d, want %d", s2, s1+1)
	}
	rs, _ := tbl.Read(ctx)
	if len(rs.Rows) != 3 {
		t.Fatalf("rows after upsert = %d, want 3", len(rs.Rows))
	}
	got := map[int64]Row{}
	for _, r := range rs.Rows {
		got[r["id"].(int64)] = r
	}
	if got[1]["name"] != "new-1" || got[1]["age"] != int64(12) {
		t.Fatalf("update not applied: %v", got[1])
	}
	if got[3]["name"] != "new-3" {
		t.Fatalf("insert missing: %v", got[3])
	}
	// Pre-upsert snapshot unchanged.
	old, _ := tbl.ReadAt(ctx, s1)
	old0 := map[int64]Row{}
	for _, r := range old.Rows {
		old0[r["id"].(int64)] = r
	}
	if old0[1]["name"] != "old-1" {
		t.Fatalf("history mutated by upsert: %v", old0[1])
	}
	snap := tbl.Snapshots()
	last := snap[len(snap)-1]
	if last.Operation != OpUpdate || last.Summary["updated"] != "1" || last.Summary["inserted"] != "1" {
		t.Fatalf("upsert summary = %+v", last.Summary)
	}
}

func TestUpsertRejectsInputDuplicates(t *testing.T) {
	logCase(t, "C 小文件合并与主键增量更新", "upsert 输入内重复主键被显式拒绝")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	headBefore := tbl.CurrentSnapshotID()
	_, err := tbl.Upsert(ctx, []Row{
		{"id": 1, "name": "a"},
		{"id": 1, "name": "b"},
	})
	if !errors.Is(err, ErrDuplicatePK) {
		t.Fatalf("want ErrDuplicatePK, got %v", err)
	}
	t.Logf("输入重复主键拒绝原因: %v", err)
	if tbl.CurrentSnapshotID() != headBefore {
		t.Fatal("HEAD advanced on rejected upsert")
	}
}

func TestUpsertRejectsExistingDuplicates(t *testing.T) {
	logCase(t, "C 小文件合并与主键增量更新", "表内已存在重复主键时 upsert 被拒绝，不静默取舍")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	// Two separate append snapshots are allowed by the append semantics and
	// can together contain a repeated key; upsert must refuse to pick one.
	_, _ = tbl.Append(ctx, []Row{{"id": 1, "name": "first"}})
	_, _ = tbl.Append(ctx, []Row{{"id": 1, "name": "second"}})
	headBefore := tbl.CurrentSnapshotID()

	_, err := tbl.Upsert(ctx, []Row{{"id": 1, "name": "winner"}})
	if !errors.Is(err, ErrDuplicatePK) {
		t.Fatalf("want ErrDuplicatePK for pre-existing dup, got %v", err)
	}
	t.Logf("存量重复主键拒绝原因: %v", err)
	if tbl.CurrentSnapshotID() != headBefore {
		t.Fatal("HEAD advanced despite ambiguous upsert")
	}
	// Both rows remain visible: nothing was silently dropped.
	rs, _ := tbl.Read(ctx)
	count := 0
	for _, r := range rs.Rows {
		if r["id"] == int64(1) {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("duplicate rows count = %d, want 2", count)
	}
}

func TestAppendUpsertCompactRoundTrip(t *testing.T) {
	logCase(t, "C 小文件合并与主键增量更新", "append 与 upsert 交替后再 compact，最终数据确定一致")
	tbl, dir := openTestTable(t, testSchema())
	ctx := context.Background()
	_, _ = tbl.Append(ctx, []Row{
		{"id": 1, "name": "a", "age": 1},
		{"id": 2, "name": "b", "age": 2},
	})
	_, _ = tbl.Upsert(ctx, []Row{{"id": 2, "name": "B", "age": 22}})
	_, _ = tbl.Append(ctx, []Row{{"id": 3, "name": "c"}})
	_, _ = tbl.Upsert(ctx, []Row{
		{"id": 1, "name": "A", "age": 11},
		{"id": 4, "name": "d"},
	})
	// The last upsert rewrites everything into one file; add a second small
	// file so compaction has work to do.
	_, _ = tbl.Append(ctx, []Row{{"id": 5, "name": "e", "age": 5}})
	before, _ := tbl.Read(ctx)
	sig := rowsSignature(before)
	want := "[1|A|11|<nil> 2|B|22|<nil> 3|c|<nil>|<nil> 4|d|<nil>|<nil> 5|e|5|<nil>]"
	if sig != want {
		t.Fatalf("before compact = %s\nwant %s", sig, want)
	}

	cr, err := tbl.Compact(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cr.NoOp {
		t.Fatal("expected compaction to commit")
	}
	after, _ := tbl.Read(ctx)
	if rowsSignature(after) != sig {
		t.Fatalf("after compact = %s", rowsSignature(after))
	}
	// Deterministic: compaction again is a no-op (single live file).
	cr2, _ := tbl.Compact(ctx, 0)
	if !cr2.NoOp {
		t.Fatalf("second compact should be no-op, got %+v", cr2)
	}

	// Reopen cross-process and read the same final content.
	tbl.Close()
	t2, err := OpenTable(ctx, dir+"/table")
	if err != nil {
		t.Fatal(err)
	}
	defer t2.Close()
	reopened, err := t2.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rowsSignature(reopened) != sig {
		t.Fatalf("reopened content differs: %s", rowsSignature(reopened))
	}

	// History snapshot count includes root + 6 commits.
	if len(t2.Snapshots()) != 7 {
		t.Fatalf("snapshot count = %d, want 7", len(t2.Snapshots()))
	}
	t.Logf("交替 append/upsert/compact 最终签名: %s", sig)
}

func TestConcurrentAppendsAndUpsertsSerialize(t *testing.T) {
	logCase(t, "C 小文件合并与主键增量更新", "并发 append 与 upsert 提交全部原子串行，最终每个主键唯一")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	_, _ = tbl.Append(ctx, []Row{
		{"id": 1, "name": "seed", "age": 0},
		{"id": 2, "name": "seed", "age": 0},
	})

	var wg sync.WaitGroup
	errs := make(chan error, 6)
	// Appenders add unique keys; updaters repeatedly rewrite keys 1 and 2.
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				id := int64(100 + g*5 + i)
				if _, err := tbl.Append(ctx, []Row{{"id": id, "name": "x"}}); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				if _, err := tbl.Upsert(ctx, []Row{
					{"id": 1, "name": fmt.Sprintf("u%d-%d", g, i), "age": int64(g)},
					{"id": 2, "name": fmt.Sprintf("v%d-%d", g, i)},
				}); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent mutation: %v", err)
	}

	// After the last upsert, keys 1/2 are unique and 15 appends succeeded.
	rs, _ := tbl.Read(ctx)
	seen := map[int64]int{}
	for _, r := range rs.Rows {
		seen[r["id"].(int64)]++
	}
	if seen[1] != 1 || seen[2] != 1 {
		t.Fatalf("upserted keys not unique: 1=%d 2=%d", seen[1], seen[2])
	}
	if len(rs.Rows) != 17 { // 2 seeds + 15 appended
		t.Fatalf("final rows = %d, want 17", len(rs.Rows))
	}
	t.Logf("并发混合提交后总行数=%d, HEAD=%d", len(rs.Rows), tbl.CurrentSnapshotID())
}
