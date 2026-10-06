package lakehouse

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
)

func logCase(t *testing.T, biz, caseName string) {
	t.Helper()
	t.Logf("【%s】执行用例: %s", biz, caseName)
}

func TestSnapshotChainAndAtomicAppend(t *testing.T) {
	logCase(t, "A 快照提交与并发写入", "单写者多次 append 各自产生原子快照，快照链父指针连续")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()

	first := tbl.CurrentSnapshotID() // root snapshot 1
	if first != 1 {
		t.Fatalf("initial snapshot = %d, want 1", first)
	}
	s2, err := tbl.Append(ctx, []Row{{"id": 1, "name": "a", "age": 10}})
	if err != nil {
		t.Fatal(err)
	}
	s3, err := tbl.Append(ctx, []Row{{"id": 2, "name": "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if s3 != s2+1 {
		t.Fatalf("snapshot ids not contiguous: %d -> %d", s2, s3)
	}
	snaps := tbl.Snapshots()
	if snaps[len(snaps)-1].ParentID != s2 {
		t.Fatalf("parent pointer broken: %+v", snaps[len(snaps)-1])
	}
	if tbl.CurrentSnapshotID() != s3 {
		t.Fatalf("HEAD = %d, want %d", tbl.CurrentSnapshotID(), s3)
	}
	rs, err := tbl.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustRowIDs(t, rs); fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("rows = %v, want [1 2]", got)
	}
}

func TestConcurrentAppends(t *testing.T) {
	logCase(t, "A 快照提交与并发写入", "并发 append 提交串行化且全部成功，HEAD 单调递增")
	tbl, dir := openTestTable(t, testSchema())
	ctx := context.Background()

	const writers = 8
	const perWriter = 12
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				id := int64(w*perWriter + i + 1)
				if _, err := tbl.Append(ctx, []Row{
					{"id": id, "name": fmt.Sprintf("n%d", id), "age": int64(i)},
				}); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent append failed: %v", err)
	}

	rs, err := tbl.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != writers*perWriter {
		t.Fatalf("row count = %d, want %d", len(rs.Rows), writers*perWriter)
	}
	ids := mustRowIDs(t, rs)
	sorted := append([]int64(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for i, v := range sorted {
		if v != int64(i+1) {
			t.Fatalf("missing/duplicate id around %d: got %d", i, v)
		}
	}
	if tbl.CurrentSnapshotID() != int64(1+writers*perWriter) {
		t.Fatalf("HEAD = %d", tbl.CurrentSnapshotID())
	}

	// Cross-process consistency: reopen and compare.
	tbl.Close()
	t2, err := OpenTable(ctx, dir+"/table")
	if err != nil {
		t.Fatal(err)
	}
	defer t2.Close()
	rs2, err := t2.Read(ctx)
	if err != nil || len(rs2.Rows) != writers*perWriter {
		t.Fatalf("reopen read: %v rows=%d", err, len(rs2.Rows))
	}
}

func TestSnapshotIsolationReadStable(t *testing.T) {
	logCase(t, "A 快照提交与并发写入", "读快照隔离：读取期间其他提交不影响已拿到的 ResultSet")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	if _, err := tbl.Append(ctx, []Row{{"id": 1, "name": "v1"}}); err != nil {
		t.Fatal(err)
	}
	before, err := tbl.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	snapBefore := before.SnapshotID

	if _, err := tbl.Append(ctx, []Row{{"id": 2, "name": "v2"}}); err != nil {
		t.Fatal(err)
	}
	if len(before.Rows) != 1 || before.Rows[0]["id"] != int64(1) {
		t.Fatalf("old ResultSet mutated: %+v", before.Rows)
	}
	again, err := tbl.ReadAt(ctx, snapBefore)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Rows) != len(before.Rows) {
		t.Fatalf("time-travel result unstable: %d vs %d", len(again.Rows), len(before.Rows))
	}
}

func TestFailedCommitLeavesNoPartialState(t *testing.T) {
	logCase(t, "A 快照提交与并发写入", "失败的提交不留下半可见状态：校验失败行数与 HEAD 不变")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	okSnap, _ := tbl.Append(ctx, []Row{{"id": 1, "name": "a"}})
	headBefore := tbl.CurrentSnapshotID()

	bad := [][]Row{
		{{"id": 2, "name": 123}},                         // wrong type
		{{"id": 2, "name": "b", "extra": 1}},             // unknown column
		{{"id": 2, "name": nil}},                         // null non-nullable
		{{"id": 3, "name": "c"}, {"id": 3, "name": "d"}}, // duplicate PK
	}
	for i, rows := range bad {
		_, err := tbl.Append(ctx, rows)
		if err == nil {
			t.Fatalf("bad batch %d unexpectedly accepted", i)
		}
		t.Logf("坏数据批次 %d 被拒绝原因: %v", i+1, err)
		if tbl.CurrentSnapshotID() != headBefore {
			t.Fatalf("HEAD changed after failed commit: %d -> %d", headBefore, tbl.CurrentSnapshotID())
		}
	}
	rs, _ := tbl.Read(ctx)
	if len(rs.Rows) != 1 {
		t.Fatalf("rows after failed commits = %d, want 1", len(rs.Rows))
	}
	if files, _ := tbl.liveFiles(tbl.CurrentSnapshotID()); len(files) != 1 {
		t.Fatalf("live files = %d, want 1", len(files))
	}
	if okSnap != 2 {
		t.Fatalf("first append snapshot = %d", okSnap)
	}
}

func TestTimeTravelReproducible(t *testing.T) {
	logCase(t, "A 快照提交与并发写入", "时间旅行：任意历史快照可重复读出相同结果")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	s1, _ := tbl.Append(ctx, []Row{{"id": 1, "name": "a"}})
	s2, _ := tbl.Append(ctx, []Row{{"id": 2, "name": "b"}})
	s3, _ := tbl.Append(ctx, []Row{{"id": 3, "name": "c"}})

	for _, id := range []int64{s1, s2, s3, 1} {
		r1, err := tbl.ReadAt(ctx, id)
		if err != nil {
			t.Fatalf("read snapshot %d: %v", id, err)
		}
		r2, err := tbl.ReadAt(ctx, id)
		if err != nil {
			t.Fatalf("reread snapshot %d: %v", id, err)
		}
		if fmt.Sprint(r1.Rows) != fmt.Sprint(r2.Rows) {
			t.Fatalf("snapshot %d not reproducible: %v vs %v", id, r1.Rows, r2.Rows)
		}
		t.Logf("快照 %d 稳定行数=%d", id, len(r1.Rows))
	}

	_, err := tbl.ReadAt(ctx, 9999)
	if !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("want ErrSnapshotNotFound, got %v", err)
	}
}

func TestConcurrentReadersWritersAndCompaction(t *testing.T) {
	logCase(t, "A 快照提交与并发写入", "读者/写入者/合并并发：无数据竞争，固定快照读取始终稳定")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()

	var workers sync.WaitGroup
	stop := make(chan struct{})

	for w := 0; w < 4; w++ {
		workers.Add(1)
		go func(w int) {
			defer workers.Done()
			for i := 0; i < 25; i++ {
				id := int64(w*100 + i + 1)
				if _, err := tbl.Append(ctx, []Row{
					{"id": id, "name": fmt.Sprintf("n%d", id), "age": int64(i)},
				}); err != nil {
					t.Errorf("writer %d: %v", w, err)
					return
				}
			}
		}(w)
	}

	for r := 0; r < 4; r++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 40; i++ {
				pinned := tbl.CurrentSnapshotID()
				rs1, err := tbl.ReadAt(ctx, pinned)
				if err != nil {
					t.Errorf("read1: %v", err)
					return
				}
				rs2, err := tbl.ReadAt(ctx, pinned)
				if err != nil {
					t.Errorf("read2: %v", err)
					return
				}
				if len(rs1.Rows) != len(rs2.Rows) {
					t.Errorf("pinned snapshot %d row count drifted: %d vs %d",
						pinned, len(rs1.Rows), len(rs2.Rows))
					return
				}
			}
		}()
	}

	var compactor sync.WaitGroup
	compactor.Add(1)
	go func() {
		defer compactor.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := tbl.Compact(ctx, 0); err != nil {
					t.Errorf("compact: %v", err)
					return
				}
			}
		}
	}()

	workers.Wait()
	close(stop)
	compactor.Wait()

	final, err := tbl.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(final.Rows) != 100 {
		t.Fatalf("final rows = %d, want 100", len(final.Rows))
	}
	t.Logf("并发读写压测结束，最终行数=%d HEAD=%d", len(final.Rows), tbl.CurrentSnapshotID())
}
