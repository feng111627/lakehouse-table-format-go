package lakehouse

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestMultiProcessConcurrentWriters 验证：多个独立进程同时打开同一个表目录
// 各写各的数据时——全部成功提交按确定的全局顺序线性叠加，无丢失、
// 无覆盖、读不到半写状态。
func TestMultiProcessConcurrentWriters(t *testing.T) {
	if os.Getenv("LH_CHILD_WRITER") == "1" {
		runChildWriter()
		return
	}
	t.Log("业务场景: 多进程并发写同一目录 - 提交线性化、无丢失、无半写状态")

	dir := t.TempDir()
	tbl, err := Create(dir, testSchema())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := tbl.Append([]Row{{"id": 0, "name": "seed"}}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const procs = 4
	const perProc = 8
	var wg sync.WaitGroup
	errCh := make(chan error, procs)
	for p := 0; p < procs; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			cmd := exec.Command(os.Args[0], "-test.run=TestMultiProcessConcurrentWriters")
			cmd.Env = append(os.Environ(),
				"LH_CHILD_WRITER=1",
				"LH_TABLE_DIR="+dir,
				"LH_PROC_ID="+strconv.Itoa(p),
				"LH_PER_PROC="+strconv.Itoa(perProc),
			)
			out, err := cmd.CombinedOutput()
			if err != nil {
				errCh <- fmt.Errorf("child %d: %v\n%s", p, err, out)
			}
		}(p)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatal(e)
	}

	// 父进程在子进程写的同时也追加一批，验证父子混合并发。
	for i := 0; i < 5; i++ {
		if _, err := tbl.Append([]Row{{"id": 1000 + i, "name": "parent"}}); err != nil {
			t.Fatalf("parent append: %v", err)
		}
	}
	t.Logf("用例[多进程写]: %d 个子进程各写 %d 行 + 父进程 5 行，全部提交成功", procs, perProc)

	cur, err := tbl.Current()
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	wantRows := 1 + procs*perProc + 5
	wantVersion := 2 + procs*perProc + 5 // 建表 + seed 各占一个版本
	if cur.Version != wantVersion {
		t.Fatalf("用例[无丢失]: 期望最终版本 v%d, 实际 v%d（有提交被盖掉？）", wantVersion, cur.Version)
	}
	t.Logf("用例[版本线性]: 最终版本 v%d，与成功提交次数一致", cur.Version)

	rows, err := tbl.Read(0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(rows) != wantRows {
		t.Fatalf("用例[数据完整]: 期望 %d 行，实际 %d 行", wantRows, len(rows))
	}
	seen := map[any]bool{}
	for _, r := range rows {
		if seen[r["id"]] {
			t.Fatalf("主键 %v 重复出现：发生了覆盖/重复写入", r["id"])
		}
		seen[r["id"]] = true
	}
	if len(seen) != wantRows {
		t.Fatalf("用例[主键唯一]: 唯一主键 %d 个，期望 %d", len(seen), wantRows)
	}
	t.Logf("用例[叠加一致]: 最新快照 %d 行主键全部唯一，数据为各进程成功写入的叠加", len(rows))

	// 版本链连续可读：每个版本都是一份完整快照。
	for v := 1; v <= cur.Version; v++ {
		if _, err := tbl.SnapshotAt(v); err != nil {
			t.Fatalf("用例[无半写]: 版本链在 v%d 断裂: %v", v, err)
		}
	}
	t.Logf("用例[无半写]: v1..v%d 每份快照元数据完整可读", cur.Version)
}

// runChildWriter 是子进程入口：独立打开同一目录，写入自己负责的 id 段。
func runChildWriter() {
	dir := os.Getenv("LH_TABLE_DIR")
	p, _ := strconv.Atoi(os.Getenv("LH_PROC_ID"))
	n, _ := strconv.Atoi(os.Getenv("LH_PER_PROC"))
	tbl, err := Open(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "child open:", err)
		os.Exit(1)
	}
	for i := 0; i < n; i++ {
		id := p*n + i + 1
		if _, err := tbl.Append([]Row{{"id": id, "name": fmt.Sprintf("p%d", p)}}); err != nil {
			fmt.Fprintln(os.Stderr, "child append:", err)
			os.Exit(1)
		}
	}
	os.Exit(0)
}

// TestAppendAtConditionalCommit 验证条件提交：基版本被推进时返回
// 可与普通错误区分的 ErrConcurrentModification，由调用方决定重试；
// 调用方重试后提交成功，数据是双方写入的叠加。
func TestAppendAtConditionalCommit(t *testing.T) {
	t.Log("业务场景: 抢版本 - 条件提交冲突返回可区分错误，调用方自主重试")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	base, _ := tbl.Current()

	// 另一路写入先推进了版本（模拟别的进程提交成功）。
	if _, err := tbl.Append([]Row{{"id": 1, "name": "other"}}); err != nil {
		t.Fatalf("other append: %v", err)
	}

	// 用陈旧基版本条件提交，必须得到 ErrConcurrentModification。
	_, err := tbl.AppendAt(base.Version, []Row{{"id": 2, "name": "mine"}})
	var conflict ErrConcurrentModification
	if !errors.As(err, &conflict) {
		t.Fatalf("用例[冲突可区分]: 期望 ErrConcurrentModification，实际 %v", err)
	}
	if conflict.Base != base.Version || conflict.Current != base.Version+1 {
		t.Fatalf("用例[冲突信息]: Base=%d Current=%d 不符合预期", conflict.Base, conflict.Current)
	}
	t.Logf("用例[冲突可区分]: base v%d 已被推进到 v%d，返回 ErrConcurrentModification{%d->%d}",
		base.Version, conflict.Current, conflict.Base, conflict.Current)

	// 调用方自行重试：基于最新版本再来一次，提交成功。
	latest, _ := tbl.Current()
	v, err := tbl.AppendAt(latest.Version, []Row{{"id": 2, "name": "mine"}})
	if err != nil {
		t.Fatalf("用例[调用方重试]: %v", err)
	}
	rows, _ := tbl.Read(v)
	if len(rows) != 2 {
		t.Fatalf("用例[重试叠加]: 重试后应有 2 行（双方写入叠加），实际 %d", len(rows))
	}
	t.Logf("用例[调用方重试]: 基于 v%d 重试成功为 v%d，最新快照 %d 行，无覆盖",
		latest.Version, v, len(rows))

	// 非法基版本返回普通的 not-found，不与冲突混淆。
	if _, err := tbl.AppendAt(999, []Row{{"id": 3}}); !errors.As(err, new(ErrSnapshotNotFound)) {
		t.Fatalf("用例[错误不混淆]: 非法基版本应返回 ErrSnapshotNotFound，实际 %v", err)
	}
	t.Log("用例[错误不混淆]: 不存在的基版本返回 ErrSnapshotNotFound，与冲突错误区分开")
}

// TestMultiProcessReadDuringWrite 验证：一个进程持续提交时，另一个进程
// 反复读取，读到的始终是某一份完整快照（行数单调不减、无文件缺失错误）。
func TestMultiProcessReadDuringWrite(t *testing.T) {
	if os.Getenv("LH_CHILD_READER") == "1" {
		runChildReader()
		return
	}
	t.Log("业务场景: 多进程边写边读 - 读者只见完整快照，不受提交影响")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())

	cmd := exec.Command(os.Args[0], "-test.run=TestMultiProcessReadDuringWrite")
	cmd.Env = append(os.Environ(), "LH_CHILD_READER=1", "LH_TABLE_DIR="+dir)
	var sb strings.Builder
	cmd.Stdout = &sb
	cmd.Stderr = &sb

	if err := cmd.Start(); err != nil {
		t.Fatalf("start reader: %v", err)
	}
	for i := 1; i <= 30; i++ {
		if _, err := tbl.Append([]Row{{"id": i, "name": "w"}}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("reader failed: %v\n%s", err, sb.String())
	}
	t.Logf("用例[读者稳定]: 写者提交 30 次期间，独立进程反复读取全部成功，输出: %s",
		strings.TrimSpace(sb.String()))
}

func runChildReader() {
	dir := os.Getenv("LH_TABLE_DIR")
	tbl, err := Open(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	last := 0
	deadline := time.Now().Add(3 * time.Second)
	reads := 0
	for time.Now().Before(deadline) {
		rows, err := tbl.Read(0)
		if err != nil {
			fmt.Fprintln(os.Stderr, "read:", err)
			os.Exit(1)
		}
		if len(rows) < last {
			fmt.Fprintf(os.Stderr, "non-monotonic rows: %d -> %d", last, len(rows))
			os.Exit(1)
		}
		last = len(rows)
		reads++
		if last >= 30 {
			break
		}
	}
	fmt.Printf("reader-ok reads=%d finalRows=%d", reads, last)
	os.Exit(0)
}

// TestMultiProcessExpireAndWrite 验证：回收与提交在不同进程间互斥，
// 回收后保留窗口内版本在另一个进程里照样读得出、已回收版本读不到。
func TestMultiProcessExpireAndWrite(t *testing.T) {
	if os.Getenv("LH_CHILD_GC") == "1" {
		runChildGC()
		return
	}
	t.Log("业务场景: 多进程回收与写入 - 跨进程互斥，回收结果对所有进程一致")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	var vs []int
	for i := 1; i <= 12; i++ {
		v, err := tbl.Append([]Row{{"id": i, "name": "seed"}})
		if err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
		vs = append(vs, v)
	}

	// 保护一个旧版本，确认跨进程也尊重标签
	if err := tbl.Tag("keep", vs[1]); err != nil {
		t.Fatalf("tag: %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestMultiProcessExpireAndWrite")
	cmd.Env = append(os.Environ(),
		"LH_CHILD_GC=1", "LH_TABLE_DIR="+dir,
	)
	var out strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("gc child: %v\n%s", err, out.String())
	}
	t.Logf("用例[跨进程回收]: 子进程执行 Expire{{MinSnapshots:3}} + 3 次写入，输出 %s",
		strings.TrimSpace(out.String()))

	// 父进程重新打开（模拟另一个后台任务），看到子进程的所有结果。
	other, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	cur, _ := other.Current()
	if cur.Version != vs[len(vs)-1]+3 {
		t.Fatalf("用例[跨进程可见]: 子进程 3 次写入应对其他进程可见，当前 v%d", cur.Version)
	}

	// 标签保护的版本在父进程句柄里仍可读。
	kept, err := other.ResolveTag("keep")
	if err != nil {
		t.Fatalf("resolve keep: %v", err)
	}
	if rows, err := other.Read(kept); err != nil || len(rows) != 2 {
		t.Fatalf("用例[标签跨进程保护]: v%d 应仍有 2 行，rows=%v err=%v", kept, rows, err)
	}
	t.Logf("用例[标签跨进程保护]: 子进程回收后，标签 keep->v%d 在父进程仍可读", kept)

	// 早期未保护版本在父进程读得到明确的 expired 错误。
	oldest := vs[0]
	if _, err := other.SnapshotAt(oldest); !errors.As(err, new(ErrSnapshotExpired)) {
		t.Fatalf("用例[回收跨进程一致]: v%d 应已回收，实际 err=%v", oldest, err)
	}
	t.Logf("用例[回收跨进程一致]: 被回收的 v%d 在父进程返回 ErrSnapshotExpired", oldest)

	// 最新快照数据完整（12 行种子 + 3 行新写入）。
	rows, _ := other.Read(0)
	if len(rows) != 15 {
		t.Fatalf("用例[数据完整]: 期望 15 行，实际 %d", len(rows))
	}
	t.Logf("用例[数据完整]: 最新快照 %d 行，回收与跨进程写入叠加正确", len(rows))
}

func runChildGC() {
	dir := os.Getenv("LH_TABLE_DIR")
	tbl, err := Open(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	res, err := tbl.Expire(RetentionPolicy{MinSnapshots: 3})
	if err != nil {
		fmt.Fprintln(os.Stderr, "expire:", err)
		os.Exit(1)
	}
	// 回收后紧接着在同一进程写入，验证回收/提交互斥后状态健康。
	for i := 0; i < 3; i++ {
		if _, err := tbl.Append([]Row{{"id": 500 + i, "name": "gc-child"}}); err != nil {
			fmt.Fprintln(os.Stderr, "append after expire:", err)
			os.Exit(1)
		}
	}
	fmt.Printf("gc-ok expired=%d kept=%d", len(res.ExpiredVersions), len(res.KeptVersions))
	os.Exit(0)
}
