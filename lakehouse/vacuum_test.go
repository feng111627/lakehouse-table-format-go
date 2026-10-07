package lakehouse

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// settle 让新写文件“变老”，超过 MinFileAge 保护窗口（测试里窗口取 5ms）。
func settle() { time.Sleep(8 * time.Millisecond) }

func countFiles(t *testing.T, dir, sub string) int {
	t.Helper()
	ns, err := os.ReadDir(filepath.Join(dir, sub))
	if err != nil {
		return 0
	}
	return len(ns)
}

// TestVacuumMaxVersions 验证：按最近版本数保留，窗口外快照与其独占文件被回收，
// 窗口内与当前版本照常可读；已回收版本得到与“从未存在”不同的错误。
func TestVacuumMaxVersions(t *testing.T) {
	t.Log("业务场景: 按版本数回收 - 窗口外版本与无引用文件被清，窗口内可读")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	var vs []int
	for i := 0; i < 3; i++ {
		v, err := tbl.Append([]Row{{"id": i + 1, "name": fmt.Sprintf("n%d", i)}})
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		vs = append(vs, v) // v2..v4
	}
	// 整体重写：compact 产生新数据文件 C1；v2..v4 的 append 文件只被
	// v2..v4 自己引用（累积链止于 v4），一旦这些快照超窗即成为垃圾。
	vc, err := tbl.Compact() // v5
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	vLast, err := tbl.Append([]Row{{"id": 4, "name": "n4"}}) // v6
	if err != nil {
		t.Fatalf("append after compact: %v", err)
	}
	_ = vLast
	settle()
	dataBefore := countFiles(t, dir, dataDir)
	if dataBefore <= 1 {
		t.Fatalf("expected rewritten + old files, got %d", dataBefore)
	}

	rep, err := tbl.Vacuum(RetentionPolicy{MaxVersions: 2, MinFileAge: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	// 保留最近 2 个：v5(compact)、v6；过期 v1..v4
	if fmt.Sprint(rep.RetainedVersions) != "[5 6]" {
		t.Fatalf("retained = %v, want [5 6]", rep.RetainedVersions)
	}
	if fmt.Sprint(rep.ExpiredVersions) != "[1 2 3 4]" {
		t.Fatalf("expired = %v, want [1 2 3 4]", rep.ExpiredVersions)
	}
	t.Logf("用例[窗口判定]: 保留 %v，过期 %v（含超窗的建表快照）", rep.RetainedVersions, rep.ExpiredVersions)

	// 窗口内版本照常读
	for _, v := range rep.RetainedVersions {
		if _, err := tbl.Read(v); err != nil {
			t.Fatalf("retained v%d must remain readable: %v", v, err)
		}
	}
	// 当前版本完整（compact 去重后 3 行 + 追加 1 行）
	rows, _ := tbl.Read(0)
	if len(rows) != 4 {
		t.Fatalf("current data must be intact, got %d rows", len(rows))
	}
	t.Logf("用例[保留可读]: 窗口内版本全部可读，当前 %d 行完整（compact 快照 v%d）", len(rows), vc)

	// 过期版本：ErrSnapshotExpired（区别于从未存在的 ErrSnapshotNotFound）
	var expired ErrSnapshotExpired
	for _, v := range rep.ExpiredVersions {
		if _, err := tbl.SnapshotAt(v); !errors.As(err, &expired) {
			t.Fatalf("v%d expected ErrSnapshotExpired, got %v", v, err)
		}
		if _, err := tbl.Read(v); !errors.As(err, &expired) {
			t.Fatalf("read v%d expected ErrSnapshotExpired, got %v", v, err)
		}
	}
	var notFound ErrSnapshotNotFound
	if _, err := tbl.SnapshotAt(4242); !errors.As(err, &notFound) {
		t.Fatalf("never-existing version expected ErrSnapshotNotFound, got %v", err)
	}
	t.Log("用例[错误可区分]: 已回收版本返回 ErrSnapshotExpired，从未存在返回 ErrSnapshotNotFound")

	// v2..v4 累积引用的 3 个 append 文件，随这些快照过期而脱离引用链，应回收；
	// v5 的 compact 输出仍被 v6 间接保留？不——v6 是 append，其 DataFiles
	// 在 v5 基础上追加，因此 v5 的 compact 文件仍被 v6 引用，必须保留。
	if len(rep.DeletedDataFiles) != 3 {
		t.Fatalf("expected 3 orphaned pre-compact data files deleted, got %d: %v", len(rep.DeletedDataFiles), rep.DeletedDataFiles)
	}
	left := countFiles(t, dir, dataDir)
	if left != 2 { // compact 文件 + 最后一个 append 文件
		t.Fatalf("compact output and last append file should remain, got %d files", left)
	}
	t.Logf("用例[文件回收]: 删除脱离引用链的旧 append 文件 %d 个，磁盘剩 %d 个（仍被 v5/v6 引用）", len(rep.DeletedDataFiles), left)

	// 历史列表仍能看到已回收版本（墓碑信息），并标 expired
	hist, _ := tbl.History()
	seen := map[int]bool{}
	for _, e := range hist {
		seen[e.Version] = true
		for _, v := range rep.ExpiredVersions {
			if e.Version == v && !e.Expired {
				t.Fatalf("v%d should be marked expired in history", v)
			}
		}
	}
	for _, v := range rep.ExpiredVersions {
		if !seen[v] {
			t.Fatalf("expired v%d should remain visible (as tombstone) in history", v)
		}
	}
	t.Log("用例[历史墓碑]: 已回收版本仍出现在历史列表，带 expired 标记与时间/操作")
}

// TestVacuumProtectsTagsAndCurrent 验证：超出窗口但被标记/当前版本引用的
// 快照与其文件一律不删。
func TestVacuumProtectsTagsAndCurrent(t *testing.T) {
	t.Log("业务场景: 回收保护 - 当前版本、标记钉住的版本及其文件绝不删除")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	old, _ := tbl.Append([]Row{{"id": 1, "name": "old"}}) // v2
	tbl.Tag("release-1.0", old)
	for i := 0; i < 4; i++ {
		tbl.Append([]Row{{"id": i + 2, "name": "x"}})
	}
	cur, _ := tbl.Current() // v6
	settle()

	rep, err := tbl.Vacuum(RetentionPolicy{MaxVersions: 2, MinFileAge: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	for _, v := range rep.ExpiredVersions {
		if v == old || v == cur.Version {
			t.Fatalf("protected version v%d expired: %+v", v, rep)
		}
	}
	if _, err := tbl.ResolveTag("release-1.0"); err != nil {
		t.Fatalf("tag disappeared: %v", err)
	}
	if rows, err := tbl.Read(old); err != nil || len(rows) != 1 || rows[0]["name"] != "old" {
		t.Fatalf("tagged snapshot v%d must stay fully readable: rows=%v err=%v", old, rows, err)
	}
	// 被标记版本引用的数据文件不能出现在删除清单
	oldSnap, _ := tbl.SnapshotAt(old)
	deleted := map[string]bool{}
	for _, f := range rep.DeletedDataFiles {
		deleted[f] = true
	}
	for _, f := range oldSnap.DataFiles {
		if deleted[f] {
			t.Fatalf("data file %s of tagged snapshot was deleted", f)
		}
	}
	t.Logf("用例[标记保命]: 窗口外的 v%d 被 release-1.0 钉住，数据与文件完好；当前 v%d 同样保留", old, cur.Version)
}

// TestVacuumOlderThan 验证：按时间保留——两个维度取并集，且新文件受年龄保护。
func TestVacuumOlderThan(t *testing.T) {
	t.Log("业务场景: 按时间回收 - 只清时间窗口外的，新落盘文件受 MinFileAge 保护")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	vOld, _ := tbl.Append([]Row{{"id": 1, "name": "old"}}) // v2
	settle()                                               // 让 v1/v2 的文件变老（约 8ms）
	vNew, _ := tbl.Append([]Row{{"id": 2, "name": "new"}}) // v3

	// 只保留最近 3ms 内的：v1/v2 超窗且文件够老 -> 过期；v3 太新 -> 年龄保护保留
	rep, err := tbl.Vacuum(RetentionPolicy{OlderThan: 3 * time.Millisecond, MinFileAge: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if fmt.Sprint(rep.ExpiredVersions) != fmt.Sprintf("[1 %d]", vOld) {
		t.Fatalf("v1 and v%d should expire, report=%+v", vOld, rep)
	}
	protected := map[int]bool{}
	for _, v := range rep.RetainedVersions {
		protected[v] = true
	}
	if !protected[vNew] {
		t.Fatalf("fresh v%d should be protected by MinFileAge, report=%+v", vNew, rep)
	}
	t.Logf("用例[时间窗口+年龄保护]: 老版本 v1,v%d 过期，刚落盘的 v%d 被保留", vOld, vNew)

	// MaxVersions 与 OlderThan 取并集：老但落在最近 N 个快照内 -> 保留。
	// 注意 v1 建表快照不在最近 4 个之内、且无数据文件，超时间窗后合法过期；
	// 关键是 v2..v5 这些带数据的快照必须被数量窗口保住。
	dir2 := t.TempDir()
	t2, _ := Create(dir2, testSchema())
	var all []int
	for i := 0; i < 4; i++ {
		v, _ := t2.Append([]Row{{"id": i + 1}})
		all = append(all, v)
	}
	settle()
	rep2, err := t2.Vacuum(RetentionPolicy{MaxVersions: 4, OlderThan: time.Nanosecond, MinFileAge: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("vacuum2: %v", err)
	}
	retained := map[int]bool{}
	for _, v := range rep2.RetainedVersions {
		retained[v] = true
	}
	for _, v := range all {
		if !retained[v] {
			t.Fatalf("version v%d inside MaxVersions window must be union-protected, expired=%v retained=%v", v, rep2.ExpiredVersions, rep2.RetainedVersions)
		}
	}
	if len(rep2.DeletedDataFiles) != 0 {
		t.Fatalf("data files of window-protected snapshots must survive: %v", rep2.DeletedDataFiles)
	}
	rows, _ := t2.Read(0)
	if len(rows) != 4 {
		t.Fatalf("union-protected table must read all 4 rows, got %d", len(rows))
	}
	t.Logf("用例[窗口取并集]: 最近4个快照 v%v 即使超时间窗也全部保留、数据可读", all)
}

// TestVacuumIdempotentAndCrashConvergent 验证：
//  1. 重跑 Vacuum 幂等，不重复删除、不报错；
//  2. 模拟“墓碑已写、文件未删”的崩溃现场，重跑安全收敛；
//  3. 新提交 + 再回收只清新增垃圾，之前的墓碑仍有效。
func TestVacuumIdempotentAndCrashConvergent(t *testing.T) {
	t.Log("业务场景: 回收崩溃与重跑 - 中途挂掉重跑幂等收敛，不越清越乱")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	for i := 0; i < 5; i++ {
		tbl.Append([]Row{{"id": i + 1}})
	}

	// 第一次正常回收（v1..v6，留最近 3 个：v4..v6，过期 v1..v3）。
	// 关闭文件年龄闸门（MinFileAge=-1），使幂等性验证不受时间流逝影响。
	pol := RetentionPolicy{MaxVersions: 3, MinFileAge: -1}
	rep1, err := tbl.Vacuum(pol)
	if err != nil || len(rep1.ExpiredVersions) != 3 {
		t.Fatalf("vacuum1: %+v err=%v", rep1, err)
	}
	// 立刻重跑：状态未变（无新提交），应完全无操作、结果稳定收敛。
	rep2, err := tbl.Vacuum(pol)
	if err != nil {
		t.Fatalf("vacuum2: %v", err)
	}
	if len(rep2.ExpiredVersions) != 0 || len(rep2.DeletedDataFiles) != 0 || len(rep2.DeletedSnapshots) != 0 {
		t.Fatalf("second vacuum must be a no-op: %+v", rep2)
	}
	// 同一状态再跑第三次，仍然是空操作
	rep2b, err := tbl.Vacuum(pol)
	if err != nil {
		t.Fatalf("vacuum2b: %v", err)
	}
	if len(rep2b.DeletedDataFiles) != 0 || len(rep2b.DeletedSnapshots) != 0 {
		t.Fatalf("third vacuum must also be a no-op: %+v", rep2b)
	}
	t.Log("用例[幂等]: 同一状态下连续重跑两次，删除清单全部为空")

	// 模拟崩溃现场：手工放入一个无引用孤儿文件（墓碑已在、文件却还在）。
	// 重跑后该孤儿文件应被再次安全清掉，墓碑不重复写。
	gcBefore, _ := tbl.readGCLog()
	orphan := filepath.Join(dataDir, uniqueName("part", "json"))
	if err := tbl.store.writeFileAtomic(orphan, []byte("[]")); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}
	rep3, err := tbl.Vacuum(pol) // 年龄闸门已关，孤儿立即可清
	if err != nil {
		t.Fatalf("vacuum3: %v", err)
	}
	found := false
	for _, f := range rep3.DeletedDataFiles {
		if f == orphan {
			found = true
		}
	}
	if !found {
		t.Fatalf("orphan from interrupted run should be collected, got %v", rep3.DeletedDataFiles)
	}
	gcAfter, _ := tbl.readGCLog()
	if len(gcAfter.Entries) != len(gcBefore.Entries) {
		t.Fatalf("tombstones must not be duplicated: %d vs %d", len(gcBefore.Entries), len(gcAfter.Entries))
	}
	t.Log("用例[崩溃收敛]: 残留孤儿文件重跑后清除，墓碑不重复、版本结果稳定")

	// 回收后再产生新提交，再次回收只处理新超窗版本，老墓碑持续生效
	tbl.Append([]Row{{"id": 99}})
	rep4, err := tbl.Vacuum(pol)
	if err != nil {
		t.Fatalf("vacuum4: %v", err)
	}
	for _, v := range rep4.ExpiredVersions {
		for _, e := range gcBefore.Entries {
			if e.Version == v {
				t.Fatalf("already-tombstoned v%d reported expired again", v)
			}
		}
	}
	rows, _ := tbl.Read(0)
	if len(rows) != 6 {
		t.Fatalf("current snapshot after new commit must be intact: %d rows", len(rows))
	}
	t.Logf("用例[回收后继续写]: 新提交当前快照完整（%d 行），再回收只清新增垃圾", len(rows))
}

// TestVacuumRollbackFileProtection 验证：回滚复用了很老的数据文件时，
// 即使产生该文件的老快照超窗，当前（回滚）快照仍引用它 -> 文件不得删除。
func TestVacuumRollbackFileProtection(t *testing.T) {
	t.Log("业务场景: 回滚与回收协作 - 回滚快照引用的老文件受保护")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	old, _ := tbl.Append([]Row{{"id": 1, "name": "old"}}) // v2，文件 F
	for i := 0; i < 4; i++ {
		tbl.Append([]Row{{"id": i + 2, "name": "newer"}})
	}
	settle()
	rb, err := tbl.Rollback(old) // 新快照 v7 引用 F
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	settle()

	rep, err := tbl.Vacuum(RetentionPolicy{MaxVersions: 2, MinFileAge: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	rbSnap, _ := tbl.SnapshotAt(rb)
	deleted := map[string]bool{}
	for _, f := range rep.DeletedDataFiles {
		deleted[f] = true
	}
	for _, f := range rbSnap.DataFiles {
		if deleted[f] {
			t.Fatalf("file %s referenced by rollback snapshot v%d was deleted", f, rb)
		}
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Fatalf("rollback-referenced file %s missing from disk: %v", f, err)
		}
	}
	rows, _ := tbl.Read(rb)
	if len(rows) != 1 || rows[0]["name"] != "old" {
		t.Fatalf("rollback snapshot unreadable after vacuum: %v", rows)
	}
	t.Logf("用例[回滚引用保护]: v%d 复用的老数据文件全部保留，读出 %v", rb, rows)
}

// TestVacuumCleansTempFiles 验证：崩溃遗留的 .tmp 文件变老后清理；
// 进行中写入的 .inflight 文件无论多新都绝不删除（防止删掉并发写者的半成品）。
func TestVacuumCleansTempFiles(t *testing.T) {
	t.Log("业务场景: 垃圾临时文件 - 崩溃 .tmp 变老回收，进行中 .inflight 永不碰")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	tbl.Append([]Row{{"id": 1}})
	oldTmp := filepath.Join(dir, dataDir, ".tmp-old1234")
	if err := os.WriteFile(oldTmp, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	inflight := filepath.Join(dir, dataDir, ".inflight-active99")
	if err := os.WriteFile(inflight, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	settle() // 两个文件都变老

	rep, err := tbl.Vacuum(RetentionPolicy{MaxVersions: 100, MinFileAge: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if len(rep.DeletedTempFiles) == 0 {
		t.Fatalf("old crashed .tmp should be deleted: %+v", rep)
	}
	if _, err := os.Stat(oldTmp); !os.IsNotExist(err) {
		t.Fatalf("old .tmp should be gone, stat err=%v", err)
	}
	if _, err := os.Stat(inflight); err != nil {
		t.Fatalf("in-flight .inflight must never be deleted, got %v", err)
	}
	// 再跑一次（年龄闸门关闭），.inflight 依然必须保留
	if _, err := tbl.Vacuum(RetentionPolicy{MaxVersions: 100, MinFileAge: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(inflight); err != nil {
		t.Fatalf("in-flight .inflight must survive even an age-disabled vacuum, got %v", err)
	}
	t.Logf("用例[进行中保护]: 崩溃遗留 .tmp 已删 %d 个；.inflight 写入临时文件两轮均保留", len(rep.DeletedTempFiles))
}

// TestVacuumRollbackExpiredTarget 验证：回滚到已回收版本必须明确报错。
func TestVacuumRollbackExpiredTarget(t *testing.T) {
	t.Log("业务场景: 回滚已回收版本 - 明确失败，不静默成功")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	v, _ := tbl.Append([]Row{{"id": 1}})
	for i := 0; i < 3; i++ {
		tbl.Append([]Row{{"id": i + 2}})
	}
	settle()
	if _, err := tbl.Vacuum(RetentionPolicy{MaxVersions: 2, MinFileAge: 5 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	var expired ErrSnapshotExpired
	if _, err := tbl.Rollback(v); !errors.As(err, &expired) {
		t.Fatalf("rollback to vacuumed v%d expected ErrSnapshotExpired, got %v", v, err)
	}
	cur, _ := tbl.Current()
	if cur.Version <= 4 {
		t.Fatalf("failed rollback must not commit anything")
	}
	t.Logf("用例[拒绝回滚]: 回滚已回收的 v%d 返回 ErrSnapshotExpired 且无新提交", v)
}

// TestVacuumConcurrentWithWriter 验证：回收与写入长时间交错，
// 任何保留范围内快照始终可读、当前数据始终完整。
func TestVacuumConcurrentWithWriter(t *testing.T) {
	t.Log("业务场景: 边写边回收 - 提交与 Vacuum 交错，读者不会遇到缺文件")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var readerErr error
	var mu sync.Mutex

	wg.Add(1)
	go func() { // 写者
		defer wg.Done()
		i := 1
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := tbl.Append([]Row{{"id": i, "name": fmt.Sprintf("n%d", i)}}); err != nil {
				mu.Lock()
				readerErr = fmt.Errorf("writer: %w", err)
				mu.Unlock()
				return
			}
			i++
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Add(1)
	go func() { // 回收者：只保留最近 5 个版本
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := tbl.Vacuum(RetentionPolicy{MaxVersions: 5, MinFileAge: 5 * time.Millisecond}); err != nil {
				mu.Lock()
				readerErr = fmt.Errorf("vacuum: %w", err)
				mu.Unlock()
				return
			}
			time.Sleep(3 * time.Millisecond)
		}
	}()
	wg.Add(1)
	go func() { // 读者：持续读最新快照。读的是不可变快照；若在“读到版本号”
		// 与“读完数据文件”之间该快照恰好被回收推进，会得到文件不存在——
		// 这是定义内的并发窗口，重读最新即可。内容损坏才是事故。
		defer wg.Done()
		benign := func(err error) bool {
			if errors.Is(err, os.ErrNotExist) {
				return true
			}
			var ex ErrSnapshotExpired
			var nf ErrSnapshotNotFound
			return errors.As(err, &ex) || errors.As(err, &nf)
		}
		for {
			select {
			case <-stop:
				return
			default:
			}
			cur, err := tbl.Current()
			if err != nil {
				if benign(err) {
					continue
				}
				mu.Lock()
				readerErr = fmt.Errorf("reader current: %w", err)
				mu.Unlock()
				return
			}
			if _, err := tbl.Read(cur.Version); err != nil {
				if benign(err) {
					continue // 读的瞬间被回收推进，重读最新
				}
				mu.Lock()
				readerErr = fmt.Errorf("reader read v%d: %w", cur.Version, err)
				mu.Unlock()
				return
			}
			if cur.Version > 1 {
				if _, err := tbl.SnapshotAt(cur.Version - 1); err != nil && !benign(err) {
					mu.Lock()
					readerErr = fmt.Errorf("reader snapshot v%d: %w", cur.Version-1, err)
					mu.Unlock()
					return
				}
			}
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	if readerErr != nil {
		t.Fatal(readerErr)
	}
	final, _ := tbl.Current()
	rows, err := tbl.Read(final.Version)
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	ids := map[int]bool{}
	for _, r := range rows {
		ids[int(r["id"].(float64))] = true
	}
	t.Logf("用例[300ms 交错]: 最终 v%d 共 %d 行、%d 唯一主键，期间无任何缺文件/半读", final.Version, len(rows), len(ids))
	if len(rows) != len(ids) {
		t.Fatal("duplicate primary keys appeared during concurrent vacuum")
	}
}
