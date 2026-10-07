package lakehouse

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

// TestMultiProcessConcurrentWriters 验证：多个进程各自打开同一表目录并发写，
// 全部成功提交按确定顺序叠加，无覆盖、无半写状态、无版本号冲突。
func TestMultiProcessConcurrentWriters(t *testing.T) {
	t.Log("业务场景: 跨进程并发写 - 多进程同目录读写，提交全部生效、线性叠加")
	if os.Getenv("LH_CHILD_WRITER") == "1" {
		runChildWriter(t)
		return
	}

	dir := t.TempDir()
	if _, err := Create(dir, Schema{
		Fields:     []Field{{Name: "id", Type: TypeInt, Required: true}, {Name: "who", Type: TypeString}},
		PrimaryKey: []string{"id"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	const procs = 6
	const perProc = 5
	var wg sync.WaitGroup
	errs := make(chan error, procs)
	for p := 0; p < procs; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			cmd := exec.Command(os.Args[0], "-test.run=TestMultiProcessConcurrentWriters")
			cmd.Env = append(os.Environ(),
				"LH_CHILD_WRITER=1",
				fmt.Sprintf("LH_CHILD_ID=%d", p),
				fmt.Sprintf("LH_CHILD_N=%d", perProc),
				fmt.Sprintf("LH_TABLE_DIR=%s", dir),
			)
			out, err := cmd.CombinedOutput()
			if err != nil {
				errs <- fmt.Errorf("child %d: %v\n%s", p, err, out)
			}
		}(p)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	t.Logf("用例[多进程提交]: %d 个独立进程各追加 %d 行，全部退出码为 0", procs, perProc)

	tbl, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	rows, err := tbl.Read(0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := procs * perProc
	if len(rows) != want {
		t.Fatalf("expected %d rows from all processes, got %d", want, len(rows))
	}
	seen := map[int]bool{}
	for _, r := range rows {
		id := int(r["id"].(float64))
		if seen[id] {
			t.Fatalf("id %d appeared twice: a commit was silently overwritten", id)
		}
		seen[id] = true
	}
	for p := 0; p < procs; p++ {
		for i := 0; i < perProc; i++ {
			id := p*1000 + i + 1
			if !seen[id] {
				t.Fatalf("missing row from process %d: id=%d", p, id)
			}
		}
	}
	t.Logf("用例[无覆盖无丢失]: 最新快照 %d 行，跨进程主键全部唯一且齐全", len(rows))

	// 版本链连续：版本号 1..current 无空洞
	cur, err := tbl.Current()
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if cur.Version != 1+want {
		t.Fatalf("expected version %d (create + %d appends), got %d", 1+want, want, cur.Version)
	}
	for v := 1; v <= cur.Version; v++ {
		if _, err := tbl.SnapshotAt(v); err != nil {
			t.Fatalf("version chain broken at v%d after multi-process writes: %v", v, err)
		}
	}
	t.Logf("用例[版本链连续]: v1..v%d 全部可读，无版本号冲突", cur.Version)

	// 任意中间快照读取都必须完整（无半写状态）
	v := cur.Version / 2
	mid, err := tbl.Read(v)
	if err != nil {
		t.Fatalf("read mid snapshot v%d: %v", v, err)
	}
	for _, r := range mid {
		if r["id"] == nil {
			t.Fatalf("half-written row in v%d: %v", v, r)
		}
	}
	t.Logf("用例[无半写状态]: 中间快照 v%d 读出 %d 行均完整", v, len(mid))
}

func runChildWriter(t *testing.T) {
	dir := os.Getenv("LH_TABLE_DIR")
	p := atoiEnv("LH_CHILD_ID")
	n := atoiEnv("LH_CHILD_N")
	tbl, err := Open(dir) // 独立进程、独立句柄
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	for i := 0; i < n; i++ {
		// 加一点随机停顿放大交错
		time.Sleep(time.Duration((p*7+i*3)%11) * time.Millisecond)
		if _, err := tbl.Append([]Row{{"id": p*1000 + i + 1, "who": fmt.Sprintf("p%d", p)}}); err != nil {
			fmt.Fprintln(os.Stderr, "append:", err)
			os.Exit(1)
		}
	}
	os.Exit(0)
}

func atoiEnv(key string) int {
	var n int
	fmt.Sscanf(os.Getenv(key), "%d", &n)
	return n
}

// TestMultiProcessCreateRaces 验证：两个进程同时 Create 同一目录，只有一个成功。
func TestMultiProcessCreateRaces(t *testing.T) {
	t.Log("业务场景: 跨进程建表竞争 - 只有一个 Create 成功，不产生两套元数据")
	if os.Getenv("LH_CHILD_CREATE") == "1" {
		dir := os.Getenv("LH_TABLE_DIR")
		if _, err := Create(dir, Schema{Fields: []Field{{Name: "id", Type: TypeInt}}}); err != nil {
			fmt.Fprintln(os.Stderr, err.Error()) // 预期的一方会失败
			os.Exit(1)
		}
		os.Exit(0)
	}

	dir := t.TempDir()
	var wg sync.WaitGroup
	results := make(chan bool, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := exec.Command(os.Args[0], "-test.run=TestMultiProcessCreateRaces")
			cmd.Env = append(os.Environ(), "LH_CHILD_CREATE=1", "LH_TABLE_DIR="+dir)
			err := cmd.Run()
			results <- err == nil
		}()
	}
	wg.Wait()
	close(results)
	ok := 0
	for success := range results {
		if success {
			ok++
		}
	}
	if ok != 1 {
		t.Fatalf("exactly one Create should win the race, got %d", ok)
	}
	t.Logf("用例[建表互斥]: 两个进程同时 Create，成功 %d 个、失败 %d 个", ok, 2-ok)

	// 表目录完整可用
	tbl, err := Open(dir)
	if err != nil {
		t.Fatalf("open after create race: %v", err)
	}
	cur, _ := tbl.Current()
	if cur.Version != 1 {
		t.Fatalf("new table should be v1, got v%d", cur.Version)
	}
}

// TestConcurrentModificationDistinguishable 验证：基版本被“非协作写者”推进时，
// 调用方拿到的是可与普通错误区分的 *ErrConcurrentModification，而不是稀里糊涂失败。
func TestConcurrentModificationDistinguishable(t *testing.T) {
	t.Log("业务场景: 版本抢占 - 冲突以 ErrConcurrentModification 明确返回，调用方可自行重试")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	tbl.Append([]Row{{"id": 1, "name": "a"}})

	// 把重试预算收紧为 1（仅做一次乐观尝试），并在 CAS 前由“非协作写者”推进版本。
	savedRetries := commitMaxRetries
	savedHook := beforeCASHook
	commitMaxRetries = 1
	defer func() { commitMaxRetries = savedRetries; beforeCASHook = savedHook }()

	beforeCASHook = func() {
		// 模拟绕过锁协议的外部写者：直接原子推进 current 指针（沿用当前数据文件）。
		v, _ := tbl.currentVersion()
		s, _ := tbl.SnapshotAt(v)
		rogue := Snapshot{
			Version:   v + 1,
			Schema:    s.Schema,
			DataFiles: s.DataFiles,
			Operation: OpAppend,
			Timestamp: time.Now().UnixNano(),
		}
		if err := tbl.commitSnapshot(rogue); err != nil {
			t.Errorf("rogue commit: %v", err)
		}
	}

	_, err := tbl.Append([]Row{{"id": 2, "name": "b"}})
	var cm ErrConcurrentModification
	if !errors.As(err, &cm) {
		t.Fatalf("expected *ErrConcurrentModification, got %T: %v", err, err)
	}
	t.Logf("用例[可区分冲突]: 抢占时返回 %v（Base=%d, Current=%d）", cm, cm.Base, cm.Current)

	// 调用方自行决定重试：移除干扰后再提交即成功，最终表数据是两者的叠加
	beforeCASHook = nil
	commitMaxRetries = savedRetries
	v, err := tbl.Append([]Row{{"id": 3, "name": "c"}})
	if err != nil {
		t.Fatalf("caller-driven retry should succeed: %v", err)
	}
	rows, _ := tbl.Read(0)
	if len(rows) != 2 { // id=1 与 id=3（抢占那次调用未生效，符合调用方预期）
		t.Fatalf("after caller retry expect 2 rows, got %d: %v", len(rows), rows)
	}
	t.Logf("用例[调用方自重试]: 冲突结果未生效，调用方重试后提交 v%d 成功，共 %d 行", v, len(rows))
}

// TestReadWhileOthersWrite 验证：一个进程持续读历史/当前快照时，
// 其他进程的提交与合并不会让它读到缺文件或半写状态。
func TestReadWhileOthersWrite(t *testing.T) {
	t.Log("业务场景: 边写边读 - 清理外的并发读写互不破坏快照可复现性")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	v0, _ := tbl.Append([]Row{{"id": 1, "name": "seed"}})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // 写者
		defer wg.Done()
		i := 2
		for {
			select {
			case <-stop:
				return
			default:
			}
			tbl.Append([]Row{{"id": i, "name": fmt.Sprintf("n%d", i)}})
			i++
		}
	}()
	wg.Add(1)
	go func() { // 读者：反复读固定的历史快照与最新快照
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			rows, err := tbl.Read(v0)
			if err != nil || len(rows) != 1 {
				t.Errorf("historical read broken: %v rows=%d", err, len(rows))
				return
			}
			if _, err := tbl.Read(0); err != nil {
				t.Errorf("latest read broken: %v", err)
				return
			}
		}
	}()
	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
	t.Log("用例[读写并发]: 连续 150ms 边写边读，历史快照始终 1 行、最新快照始终可读")
}
