package lakehouse

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// seedHistory 建一张有 n 次 append 的表，返回表对象与各 append 版本。
func seedHistory(t *testing.T, n int) (*Table, []int) {
	t.Helper()
	dir := t.TempDir()
	tbl, err := Create(dir, testSchema())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	vs := make([]int, 0, n)
	for i := 1; i <= n; i++ {
		v, err := tbl.Append([]Row{{"id": i, "name": fmt.Sprintf("n%d", i), "score": float64(i)}})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		vs = append(vs, v)
	}
	return tbl, vs
}

// TestExpireByCount 验证按“最近 N 个版本”回收：窗口外版本被清理，
// 窗口内与当前版本照常可读，已回收版本返回 ErrSnapshotExpired。
func TestExpireByCount(t *testing.T) {
	t.Log("业务场景: 数量窗口回收 - 只留最近 N 个版本，其余被安全清理")
	tbl, vs := seedHistory(t, 6) // v1 create + v2..v7 append
	current := vs[len(vs)-1]

	res, err := tbl.Expire(RetentionPolicy{MinSnapshots: 3})
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	// 保留最近 3 个快照（v5,v6,v7）；过期 v1..v4
	if len(res.KeptVersions) != 3 {
		t.Fatalf("用例[保留窗口]: 应保留 3 个版本，实际 %v", res.KeptVersions)
	}
	if len(res.ExpiredVersions) != 4 {
		t.Fatalf("用例[过期版本]: 应过期 4 个版本，实际 %v", res.ExpiredVersions)
	}
	t.Logf("用例[保留窗口]: 保留 %v，过期 %v", res.KeptVersions, res.ExpiredVersions)

	// 窗口内版本仍可读，数据完整
	for _, v := range res.KeptVersions {
		if _, err := tbl.SnapshotAt(v); err != nil {
			t.Fatalf("用例[保留可读]: v%d 应可读: %v", v, err)
		}
	}
	rows, err := tbl.Read(0)
	if err != nil || len(rows) != 6 {
		t.Fatalf("用例[当前不受影响]: 最新快照应有 6 行，实际 %d err=%v", len(rows), err)
	}
	t.Logf("用例[保留可读]: 窗口内版本 %v 与最新快照(%d 行)全部可读", res.KeptVersions, len(rows))

	// 窗口外版本：元数据已删，返回明确的 expired（区别于从未存在）
	for _, v := range res.ExpiredVersions {
		if _, err := tbl.SnapshotAt(v); !errors.As(err, new(ErrSnapshotExpired)) {
			t.Fatalf("用例[已回收报错]: v%d 应返回 ErrSnapshotExpired，实际 %v", v, err)
		}
		if _, err := tbl.Read(v); !errors.As(err, new(ErrSnapshotExpired)) {
			t.Fatalf("用例[已回收报错]: Read(v%d) 应返回 ErrSnapshotExpired，实际 %v", v, err)
		}
	}
	if _, err := tbl.SnapshotAt(100); !errors.As(err, new(ErrSnapshotNotFound)) {
		t.Fatalf("用例[与不存在区分]: v100 应返回 ErrSnapshotNotFound，实际 %v", err)
	}
	t.Logf("用例[已回收报错]: 过期版本返回 ErrSnapshotExpired，从未存在的 v100 返回 ErrSnapshotNotFound")

	// 旧版本独占的数据文件已被删除；窗口外版本的元数据文件也不在了。
	// 先确认过期版本的元数据文件确实从磁盘移除。
	for _, v := range res.ExpiredVersions {
		if tbl.store.exists(metaPath(v)) {
			t.Fatalf("用例[元数据清理]: v%d 的元数据文件应已删除", v)
		}
	}
	latestSnap, _ := tbl.SnapshotAt(current)
	latestSet := map[string]bool{}
	for _, f := range latestSnap.DataFiles {
		latestSet[f] = true
	}
	dataFiles := listDataFiles(t, tbl)
	for _, f := range dataFiles {
		if latestSet[f] {
			continue
		}
		// 非最新快照引用的文件，只允许被“保留窗口内”的快照引用。
		inKept := false
		for _, kv := range res.KeptVersions {
			s, _ := tbl.SnapshotAt(kv)
			for _, kf := range s.DataFiles {
				if kf == f {
					inKept = true
				}
			}
		}
		if !inKept {
			t.Fatalf("用例[文件清理]: 无任何保留快照引用的文件仍残留: %s", f)
		}
	}
	t.Logf("用例[元数据清理]: %d 个过期版本的 v*.json 已从磁盘删除", len(res.ExpiredVersions))
	t.Logf("用例[文件清理]: data/ 剩余 %d 个文件，全部仍被保留快照引用", len(dataFiles))

	// 历史列表仍记录已回收版本（Expired=true）
	hist, _ := tbl.History()
	expiredInHist := 0
	for _, h := range hist {
		if h.Expired {
			expiredInHist++
		}
	}
	if expiredInHist != 4 || len(hist) != 7 {
		t.Fatalf("用例[历史留痕]: 历史 %d 条其中已回收 %d 条，期望 7/4", len(hist), expiredInHist)
	}
	t.Logf("用例[历史留痕]: 历史仍列出 %d 个版本，%d 个标记 Expired", len(hist), expiredInHist)
}

// TestExpirePinnedByTagAndCurrent 验证：标签指向的版本和当前版本
// 即使落在窗口外也绝不回收，其数据文件一个不删。
func TestExpirePinnedByTagAndCurrent(t *testing.T) {
	t.Log("业务场景: 回收安全 - 标签引用与当前快照强制保留")
	tbl, vs := seedHistory(t, 6)
	old := vs[0] // v2，将落在窗口外
	if err := tbl.Tag("release-1.0", old); err != nil {
		t.Fatalf("tag: %v", err)
	}

	res, err := tbl.Expire(RetentionPolicy{MinSnapshots: 1}) // 窗口极窄
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	for _, v := range res.ExpiredVersions {
		if v == old {
			t.Fatalf("用例[标签保护]: 标签指向的 v%d 绝不能被回收", old)
		}
	}
	t.Logf("用例[标签保护]: release-1.0 指向的 v%d 在窗口外仍被保留（过期集 %v）", old, res.ExpiredVersions)

	// 标签版本连同其数据文件可读
	if _, err := tbl.SnapshotAt(old); err != nil {
		t.Fatalf("用例[标签版本可读]: %v", err)
	}
	rows, err := tbl.Read(old)
	if err != nil || len(rows) != 1 || rows[0]["name"] != "n1" {
		t.Fatalf("用例[标签版本可读]: %v rows=%v", err, rows)
	}
	t.Logf("用例[标签版本可读]: v%d 结构与数据完整: %v", old, rows)

	// 移动标签到新版本后，旧版本在下次回收时才允许过期
	if err := tbl.Tag("release-1.0", vs[len(vs)-1]); err != nil {
		t.Fatalf("retag: %v", err)
	}
	res2, err := tbl.Expire(RetentionPolicy{MinSnapshots: 1})
	if err != nil {
		t.Fatalf("expire2: %v", err)
	}
	found := false
	for _, v := range res2.ExpiredVersions {
		if v == old {
			found = true
		}
	}
	if !found {
		t.Fatalf("用例[标签移走后可回收]: v%d 应在标签移走后过期", old)
	}
	t.Logf("用例[标签移走后可回收]: 标签重指向后 v%d 被回收，新版本被保护", old)
}

// TestExpireByAge 验证按时间窗口回收。
func TestExpireByAge(t *testing.T) {
	t.Log("业务场景: 时间窗口回收 - 只保留某个时间点之后的快照")
	tbl, vs := seedHistory(t, 4)

	// OlderThan 极小：除当前/强制保留外，时间戳早于 cutoff 的都过期
	res, err := tbl.Expire(RetentionPolicy{OlderThan: 1 * time.Nanosecond})
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	// 至少当前版本保留；其余全部落在窗口外
	if len(res.KeptVersions) != 1 || res.KeptVersions[0] != vs[len(vs)-1] {
		t.Fatalf("用例[时间窗口]: 仅当前版本应保留，实际 %v", res.KeptVersions)
	}
	t.Logf("用例[时间窗口]: 1ns 窗口下仅保留当前 %v，过期 %v", res.KeptVersions, res.ExpiredVersions)

	// 极大窗口：一切都在窗口内，什么也不删
	tbl2, vs2 := seedHistory(t, 4)
	res2, err := tbl2.Expire(RetentionPolicy{OlderThan: 24 * time.Hour})
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if len(res2.ExpiredVersions) != 0 || len(res2.KeptVersions) != len(vs2)+1 {
		t.Fatalf("用例[宽窗口不清理]: kept=%v expired=%v", res2.KeptVersions, res2.ExpiredVersions)
	}
	t.Logf("用例[宽窗口不清理]: 24h 窗口下 %d 个版本全部保留", len(res2.KeptVersions))
}

// TestExpireIdempotentAndSafeRerun 验证回收幂等、可安全重跑：
// 再跑一遍不报错、不重复删、回收后写入/读取正常。
func TestExpireIdempotentAndSafeRerun(t *testing.T) {
	t.Log("业务场景: 回收可靠性 - 重复执行收敛、回收后表仍可正常读写")
	tbl, vs := seedHistory(t, 6)

	first, err := tbl.Expire(RetentionPolicy{MinSnapshots: 2})
	if err != nil {
		t.Fatalf("expire1: %v", err)
	}
	second, err := tbl.Expire(RetentionPolicy{MinSnapshots: 2})
	if err != nil {
		t.Fatalf("用例[幂等重跑]: 第二次回收报错: %v", err)
	}
	if len(second.ExpiredVersions) != 0 || len(second.DeletedFiles) != 0 {
		t.Fatalf("用例[幂等重跑]: 第二次不应再清理，实际 %+v", second)
	}
	if len(second.KeptVersions) != len(first.KeptVersions) {
		t.Fatalf("用例[幂等重跑]: 保留集应稳定")
	}
	t.Logf("用例[幂等重跑]: 首次过期 %v，重跑无新增清理、保留集稳定", first.ExpiredVersions)

	// 账本不重复记账
	recs, err := tbl.readExpired()
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	if len(recs) != len(first.ExpiredVersions) {
		t.Fatalf("用例[账本一致]: 账本 %d 条，期望 %d", len(recs), len(first.ExpiredVersions))
	}
	t.Logf("用例[账本一致]: 回收账本 %d 条记录无重复", len(recs))

	// 回收后继续提交：新版本正常生成，数据可叠加读取
	nv, err := tbl.Append([]Row{{"id": 100, "name": "post-gc"}})
	if err != nil {
		t.Fatalf("用例[回收后可写]: %v", err)
	}
	rows, _ := tbl.Read(nv)
	if len(rows) != 7 {
		t.Fatalf("用例[回收后可写]: 期望 7 行，实际 %d", len(rows))
	}
	// 回滚到已回收版本必须明确失败
	if _, err := tbl.Rollback(vs[0]); !errors.As(err, new(ErrSnapshotExpired)) {
		t.Fatalf("用例[回滚拦截]: 回滚已回收版本应返回 ErrSnapshotExpired，实际 %v", err)
	}
	t.Logf("用例[回收后可写]: 新提交 v%d 正常；回滚到已回收的 v%d 被明确拒绝", nv, vs[0])
}

// TestExpireDeletesOrphanDataFiles 验证：合并产生的、仅被窗口外旧快照引用
// 的数据文件会被物理删除；保留快照引用的文件一个不少。
func TestExpireDeletesOrphanDataFiles(t *testing.T) {
	t.Log("业务场景: 数据文件物理回收 - 仅旧快照引用的 part 文件被删除腾空间")
	tbl, _ := seedHistory(t, 6)
	before, _ := tbl.Current()

	// 合并重写：新快照只引用 1 个新文件，旧的 6 个 part 文件仅旧快照引用。
	if _, err := tbl.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	after, _ := tbl.Current()
	if len(after.DataFiles) != 1 {
		t.Fatalf("compact 后应只有 1 个数据文件，实际 %d", len(after.DataFiles))
	}
	t.Logf("用例[合并产生孤儿]: compact 前 %d 个 part，compact 后最新快照引用 1 个新文件",
		len(before.DataFiles))

	// 窄窗口回收：旧快照与它们独占的旧文件全部清理。
	res, err := tbl.Expire(RetentionPolicy{MinSnapshots: 1})
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	// 旧 part 文件（compact 前的）应出现在删除清单里。
	deletedSet := map[string]bool{}
	for _, f := range res.DeletedFiles {
		deletedSet[f] = true
	}
	removedOldParts := 0
	for _, f := range before.DataFiles {
		if deletedSet[f] {
			removedOldParts++
		}
	}
	if removedOldParts != len(before.DataFiles) {
		t.Fatalf("用例[旧文件删除]: %d 个旧 part 中只删除了 %d 个",
			len(before.DataFiles), removedOldParts)
	}
	t.Logf("用例[旧文件删除]: %d 个仅被旧快照引用的 part 文件已物理删除", removedOldParts)

	// 最新快照引用的合并文件仍在，数据完整（主键去重后 6 行）。
	f := after.DataFiles[0]
	if !tbl.store.exists(f) {
		t.Fatalf("用例[新文件保留]: 当前快照引用的 %s 不应被删除", f)
	}
	rows, err := tbl.Read(0)
	if err != nil || len(rows) != 6 {
		t.Fatalf("用例[回收后读取]: 期望 6 行，实际 %d err=%v", len(rows), err)
	}
	t.Logf("用例[新文件保留]: 合并文件保留，最新快照 %d 行数据完整可读", len(rows))

	// 磁盘上 data 目录只剩当前引用的文件。
	remaining := listDataFiles(t, tbl)
	if len(remaining) != 1 {
		t.Fatalf("用例[空间释放]: data/ 应只剩 1 个文件，实际 %v", remaining)
	}
	t.Logf("用例[空间释放]: data/ 现存 %v", remaining)
}

// TestExpireConcurrentReaders 验证回收进行时，并发读者始终读到完整快照，
// 不会因为文件被删而出现缺文件错误。
func TestExpireConcurrentReaders(t *testing.T) {
	t.Log("业务场景: 回收与在线读写并发 - 读者不见缺文件，状态不被撕裂")
	tbl, _ := seedHistory(t, 10)

	stop := make(chan struct{})
	errc := make(chan error, 8)
	done := make(chan struct{})
	// 读者只跟踪“最新快照”：它始终被强制保留，其引用文件绝不删除。
	// 行数应单调不减（只有 append 提交），任何读错误都意味着撕裂。
	for r := 0; r < 4; r++ {
		go func() {
			defer func() { done <- struct{}{} }()
			last := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				rows, err := tbl.Read(0)
				if err != nil {
					errc <- fmt.Errorf("latest read: %w", err)
					return
				}
				if len(rows) < last {
					errc <- fmt.Errorf("rows shrank: %d -> %d", last, len(rows))
					return
				}
				last = len(rows)
			}
		}()
	}

	// 交替写入、合并与回收，制造“边写边读边清理”。
	for round := 0; round < 3; round++ {
		for i := 0; i < 3; i++ {
			if _, err := tbl.Append([]Row{{"id": 200 + round*3 + i, "name": "live"}}); err != nil {
				t.Fatalf("append during gc: %v", err)
			}
		}
		if round == 1 {
			if _, err := tbl.Compact(); err != nil {
				t.Fatalf("compact during gc: %v", err)
			}
		}
		if _, err := tbl.Expire(RetentionPolicy{MinSnapshots: 4}); err != nil {
			t.Fatalf("expire during load: %v", err)
		}
	}
	close(stop)
	for r := 0; r < 4; r++ {
		<-done
	}
	close(errc)
	for e := range errc {
		t.Fatalf("用例[读者不缺文件]: %v", e)
	}
	rows, _ := tbl.Read(0)
	t.Logf("用例[读者不缺文件]: 3 轮写入+回收（含 1 次合并）期间 4 个读者全部成功；最终 %d 行", len(rows))

	// 回收后保留窗口内的版本全部可读。
	res, _ := tbl.Expire(RetentionPolicy{MinSnapshots: 4})
	for _, v := range res.KeptVersions {
		if _, err := tbl.Read(v); err != nil {
			t.Fatalf("用例[保留版本可读]: v%d: %v", v, err)
		}
	}
	t.Logf("用例[保留版本可读]: 最终保留的 %v 全部数据完整", res.KeptVersions)
}

func listDataFiles(t *testing.T, tbl *Table) []string {
	t.Helper()
	files, err := tbl.store.listFilesRecursive(dataDir)
	if err != nil {
		t.Fatalf("list data: %v", err)
	}
	return files
}

// 确保 filepath 被使用（辅助断言相对路径布局）。
var _ = filepath.Join
var _ = os.Getenv

// writeExpiredLedger 直接覆写回收账本，供崩溃恢复测试手工构造中间状态。
func (t *Table) writeExpiredLedger(recs []expiredRecord) error {
	return t.appendExpired(recs)
}

// TestExpireCrashRecovery 验证回收在任意阶段崩溃后重跑都能安全收敛：
//   阶段A：账本已标记但元数据/数据文件尚未删（模拟“落账后即崩溃”）；
//   阶段B：元数据已删但数据文件残留（模拟“删元数据后崩溃”）；
//   以及存在“孤儿数据文件”（崩溃提交残留）时也能被收敛清理。
func TestExpireCrashRecovery(t *testing.T) {
	t.Log("业务场景: 回收中途崩溃 - 模拟各崩溃点后重跑，状态安全收敛")

	// ---- 阶段A：账本先行后崩溃 ----
	tbl, vs := seedHistory(t, 6)
	current := vs[len(vs)-1]
	victims := []int{1, 2, 3} // 假设这三个版本被判过期并已记账
	var recs []expiredRecord
	now := timeNow().UnixNano()
	for _, v := range victims {
		snap, _ := tbl.readSnapshotFile(v)
		recs = append(recs, expiredRecord{
			Version: v, Operation: snap.Operation, Timestamp: snap.Timestamp, ExpiredAt: now,
		})
	}
	if err := tbl.writeExpiredLedger(recs); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	t.Logf("用例[阶段A-已落账未删除]: 手工把 v%v 写入账本后模拟崩溃（元数据/文件仍在）", victims)

	// 崩溃状态下：这些版本已应被视为过期
	for _, v := range victims {
		if _, err := tbl.SnapshotAt(v); !errors.As(err, new(ErrSnapshotExpired)) {
			t.Fatalf("阶段A: v%d 账本已标记，应立即视为过期，实际 %v", v, err)
		}
	}
	t.Log("用例[阶段A-读者语义]: 已落账版本立刻返回 ErrSnapshotExpired，无中间态歧义")

	// 重跑 Expire 收敛
	res, err := tbl.Expire(RetentionPolicy{MinSnapshots: 2})
	if err != nil {
		t.Fatalf("rerun after stage A: %v", err)
	}
	for _, v := range victims {
		if tbl.store.exists(metaPath(v)) {
			t.Fatalf("阶段A 重跑后 v%d 元数据应已删", v)
		}
	}
	// 账本不重复
	recs2, _ := tbl.readExpired()
	if len(recs2) != len(victims) {
		t.Fatalf("阶段A 重跑后账本应为 %d 条，实际 %d", len(victims), len(recs2))
	}
	t.Logf("用例[阶段A-重跑收敛]: 重跑后 v%v 元数据删除、账本仍 %d 条不重复", victims, len(recs2))

	// ---- 阶段B：元数据已删、数据文件残留 ----
	// 先找一个仅被已过期版本引用的数据文件；这里用 compact 制造孤儿。
	tbl2, vs2 := seedHistory(t, 6)
	before, _ := tbl2.Current()
	if _, err := tbl2.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	// 手工把旧版本记账本并删掉其元数据，但保留旧数据文件（模拟崩溃点B）。
	var recsB []expiredRecord
	for _, v := range vs2 {
		snap, _ := tbl2.readSnapshotFile(v)
		recsB = append(recsB, expiredRecord{
			Version: v, Operation: snap.Operation, Timestamp: snap.Timestamp, ExpiredAt: now,
		})
	}
	if err := tbl2.writeExpiredLedger(recsB); err != nil {
		t.Fatalf("seed ledger B: %v", err)
	}
	for _, v := range vs2 {
		if err := tbl2.store.remove(metaPath(v)); err != nil {
			t.Fatalf("remove meta %d: %v", v, err)
		}
	}
	// 此刻旧 part 文件仍残留在磁盘（账本与元数据已处理完、还没轮到删数据）。
	orphanBefore := 0
	for _, f := range before.DataFiles {
		if tbl2.store.exists(f) {
			orphanBefore++
		}
	}
	if orphanBefore == 0 {
		t.Fatal("阶段B 前置构造失败：应存在残留的旧数据文件")
	}
	t.Logf("用例[阶段B-元数据已删文件残留]: 手工删 v%v 元数据后保留 %d 个旧 part 模拟崩溃",
		vs2, orphanBefore)

	resB, err := tbl2.Expire(RetentionPolicy{MinSnapshots: 1})
	if err != nil {
		t.Fatalf("rerun after stage B: %v", err)
	}
	// 残留旧文件应在重跑时被清掉；当前合并快照文件保留。
	for _, f := range before.DataFiles {
		if tbl2.store.exists(f) {
			t.Fatalf("阶段B 重跑后残留文件应已删除: %s", f)
		}
	}
	cur2, _ := tbl2.Current()
	for _, f := range cur2.DataFiles {
		if !tbl2.store.exists(f) {
			t.Fatalf("阶段B 重跑误删了当前快照文件: %s", f)
		}
	}
	rows, _ := tbl2.Read(0)
	if len(rows) != 6 {
		t.Fatalf("阶段B 重跑后最新快照应仍有 6 行，实际 %d", len(rows))
	}
	t.Logf("用例[阶段B-重跑收敛]: 残留 %d 个旧 part 已清，删除清单 %d 项，当前快照 6 行完整",
		orphanBefore, len(resB.DeletedFiles))

	// ---- 连续第三次执行：完全幂等，无任何变化 ----
	resC, err := tbl2.Expire(RetentionPolicy{MinSnapshots: 1})
	if err != nil {
		t.Fatalf("third run: %v", err)
	}
	if len(resC.ExpiredVersions) != 0 || len(resC.DeletedFiles) != 0 {
		t.Fatalf("用例[最终幂等]: 第三次执行应零清理，实际 %+v", resC)
	}
	t.Log("用例[最终幂等]: 第三次 Expire 零过期、零删除，状态完全收敛")

	// 保留窗口内版本在崩溃恢复全程始终可读
	kept := res.KeptVersions
	if len(kept) < 2 {
		t.Fatalf("阶段A 应保留至少 2 个版本，实际 %v", kept)
	}
	for _, v := range kept {
		if _, err := tbl.Read(v); err != nil {
			t.Fatalf("用例[保留版本全程可读]: v%d: %v", v, err)
		}
	}
	if rows, _ := tbl.Read(current); len(rows) != 6 {
		t.Fatalf("当前版本应保持 6 行可读，实际 %d", len(rows))
	}
	t.Logf("用例[保留版本全程可读]: 崩溃恢复过程中保留版本 %v 始终可读", kept)
}
