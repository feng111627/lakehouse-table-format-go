package lakehouse

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	metaDir    = "metadata"
	dataDir    = "data"
	currentPtr = "metadata/current"
	maxRetries = 16
)

// commitMaxRetries 可在同包测试中临时调小，用于确定性地制造冲突结果。
var commitMaxRetries = maxRetries

// beforeCASHook 在“锁外准备完成、进入跨进程锁做 CAS”之间调用，
// 仅用于测试注入并发提交；生产路径为空。
var beforeCASHook func()

// errCASAbort 是内部信号：CAS 发现基版本被推进，本次尝试作废重来。
var errCASAbort = errors.New("cas abort: base version advanced")

// Table 是一张本地数据湖表，句柄可在 goroutine 间共享；
// 不同进程可各自 Open 同一目录并发读写。
type Table struct {
	store *localStore
	mu    sync.Mutex // 串行化本进程内的提交
}

// Create 在 dir 下创建一张新表；目录下已存在表时返回错误。
// 多个进程同时 Create 同一目录时，只有一个成功，其余得到明确错误。
func Create(dir string, schema Schema) (*Table, error) {
	if len(schema.Fields) == 0 {
		return nil, fmt.Errorf("schema must have at least one field")
	}
	for _, pk := range schema.PrimaryKey {
		if _, ok := schema.Field(pk); !ok {
			return nil, fmt.Errorf("primary key column %q not in schema", pk)
		}
	}
	store := newLocalStore(dir)
	t := &Table{store: store}

	// 跨进程锁内复查 current，防止两个进程同时建表。
	lock, err := acquireFlock(dir)
	if err != nil {
		return nil, err
	}
	defer lock.release()
	if store.exists(currentPtr) {
		return nil, fmt.Errorf("table already exists at %s", dir)
	}
	snap := Snapshot{
		Version:   1,
		Schema:    schema,
		DataFiles: nil,
		Operation: OpCreate,
		Timestamp: time.Now().UnixNano(),
	}
	if err := t.commitSnapshot(snap); err != nil {
		return nil, err
	}
	return t, nil
}

// Open 打开 dir 下已存在的表。
func Open(dir string) (*Table, error) {
	store := newLocalStore(dir)
	if !store.exists(currentPtr) {
		return nil, fmt.Errorf("no table at %s", dir)
	}
	return &Table{store: store}, nil
}

// Current 返回当前最新快照。
func (t *Table) Current() (Snapshot, error) {
	v, err := t.currentVersion()
	if err != nil {
		return Snapshot{}, err
	}
	return t.SnapshotAt(v)
}

// SnapshotAt 返回指定版本的历史快照。
// 版本曾经存在但已被 Vacuum 回收时返回 *ErrSnapshotExpired；
// 从未存在返回 *ErrSnapshotNotFound。
func (t *Table) SnapshotAt(version int) (Snapshot, error) {
	if err := t.checkExpired(version); err != nil {
		return Snapshot{}, err
	}
	data, err := t.store.readFile(metaPath(version))
	if os.IsNotExist(err) {
		return Snapshot{}, ErrSnapshotNotFound{Version: version}
	}
	if err != nil {
		return Snapshot{}, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return Snapshot{}, fmt.Errorf("corrupt snapshot v%d: %w", version, err)
	}
	return snap, nil
}

// Append 以当前快照为基提交一批新增行，返回新快照版本。
// 若表中定义了主键，批内重复主键会被拒绝而不是静默取舍。
func (t *Table) Append(rows []Row) (int, error) {
	return t.commitData(OpAppend, rows, func(cur Snapshot, files []string) Snapshot {
		return Snapshot{
			Schema:    cur.Schema,
			DataFiles: append(append([]string{}, cur.DataFiles...), files...),
			Operation: OpAppend,
		}
	})
}

// EvolveSchema 校验并提交一次结构变更；数据文件不变，仅推进结构版本。
func (t *Table) EvolveSchema(next Schema) (int, error) {
	if err := checkSchemaChangeReadonly(next); err != nil {
		return 0, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := acquireFlock(t.store.dir)
	if err != nil {
		return 0, err
	}
	defer lock.release()
	cur, err := t.Current()
	if err != nil {
		return 0, err
	}
	if err := checkSchemaChange(cur.Schema, next); err != nil {
		return 0, err
	}
	base, err := t.currentVersion()
	if err != nil {
		return 0, err
	}
	return t.commitAtLocked(base, Snapshot{
		Schema:    next,
		DataFiles: cur.DataFiles,
		Operation: OpSchema,
	})
}

// checkSchemaChangeReadonly 做一次不依赖当前快照的前置结构自洽校验，
// 让明显的非法请求在抢锁前就失败。
func checkSchemaChangeReadonly(next Schema) error {
	seen := map[string]bool{}
	for _, f := range next.Fields {
		if f.Name == "" {
			return fmt.Errorf("schema contains empty field name")
		}
		if seen[f.Name] {
			return fmt.Errorf("schema contains duplicate field %q", f.Name)
		}
		seen[f.Name] = true
	}
	for _, pk := range next.PrimaryKey {
		if !seen[pk] {
			return ErrIncompatibleSchema{
				Reason: ReasonDropPrimaryKey,
				Detail: fmt.Sprintf("primary key column %q not in schema", pk),
			}
		}
	}
	return nil
}

// Read 读取指定快照版本下的全部有效行；version<=0 表示最新快照。
// 同一版本的读取结果稳定且可复现：文件按名排序、行按文件内顺序返回。
// 读取不持任何写锁，只经由不可变快照与不可变数据文件，与并发写入互不阻塞。
func (t *Table) Read(version int) ([]Row, error) {
	if version <= 0 {
		v, err := t.currentVersion()
		if err != nil {
			return nil, err
		}
		version = v
	}
	snap, err := t.SnapshotAt(version)
	if err != nil {
		return nil, err
	}
	var out []Row
	for _, f := range snap.DataFiles {
		rows, err := t.readDataFile(f)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}

// ---- 内部实现 ----

func metaPath(version int) string {
	return fmt.Sprintf("%s/v%d.json", metaDir, version)
}

func (t *Table) currentVersion() (int, error) {
	data, err := t.store.readFile(currentPtr)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(data)))
}

// commitSnapshot 写入快照元数据并原子推进 current 指针。
// 先写 v{N}.json 再原子替换 current：中途失败时 current 仍指向旧版本，
// 未引用的元数据文件成为垃圾但不影响一致性（Vacuum 会回收）。
func (t *Table) commitSnapshot(snap Snapshot) error {
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	if err := t.store.writeFileAtomic(metaPath(snap.Version), data); err != nil {
		return err
	}
	return t.store.writeFileAtomic(currentPtr, []byte(strconv.Itoa(snap.Version)))
}

// commitAtLocked 在已持有跨进程锁的状态下，以 base+1 为版本号提交。
func (t *Table) commitAtLocked(base int, snap Snapshot) (int, error) {
	snap.Version = base + 1
	snap.Timestamp = time.Now().UnixNano()
	if err := t.commitSnapshot(snap); err != nil {
		return 0, err
	}
	return snap.Version, nil
}

// withCommitLock 持有“进程内互斥 + 跨进程文件锁”执行 fn。
// fn 在锁内重新读取当前版本作为 CAS 依据。
func (t *Table) withCommitLock(fn func(base int) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := acquireFlock(t.store.dir)
	if err != nil {
		return err
	}
	defer lock.release()
	base, err := t.currentVersion()
	if err != nil {
		return err
	}
	return fn(base)
}

// optimisticCommit 是所有数据类提交的统一执行器：
//
//	attempt 0（乐观）：锁外读基快照、准备数据文件，再持双锁 CAS 提交。
//	                 锁区极短，同进程/跨进程写者吞吐高；基版本被推进则本次作废。
//	attempt >=1（串行）：持有“进程内互斥 + 跨进程文件锁”重新读最新基快照、
//	                 重新准备并提交——协作写者无法再插入，必然不会饿死；
//	                 若持锁期间仍观察到基版本变化，说明存在不遵守锁协议的
//	                 外部写者，立即以 *ErrConcurrentModification 返回，
//	                 由调用方自行决定是否重试。
//
// prepare 以当前基快照为输入，产出待提交快照（Schema/DataFiles/Operation），
// 允许在其中写新数据文件；校验类错误直接中断、不重试。
func (t *Table) optimisticCommit(prepare func(cur Snapshot) (Snapshot, error)) (int, error) {
	var lastConflict error
	for attempt := 0; attempt < commitMaxRetries; attempt++ {
		if attempt == 0 {
			// 第一段：锁外准备 + 锁内 CAS。
			base, baseSnap, err := t.currentSnapshot()
			if err != nil {
				return 0, err
			}
			next0, perr := prepare(baseSnap)
			if perr != nil {
				return 0, perr
			}
			if beforeCASHook != nil {
				beforeCASHook()
			}
			var committed int
			err = t.withCommitLock(func(curBase int) error {
				if curBase != base {
					lastConflict = ErrConcurrentModification{Base: base, Current: curBase}
					return errCASAbort
				}
				// 锁外准备的数据文件可能在此期间被一轮 Vacuum 当作“尚未引用
				// 的新文件”清掉（rename 完成到拿锁之间存在窗口）。确认文件仍在，
				// 否则作废重算（第二段会在锁内重新写文件，Vacuum 无法再插入）。
				if err := t.dataFilesExist(next0.DataFiles); err != nil {
					lastConflict = err
					return errCASAbort
				}
				v, err := t.commitAtLocked(curBase, next0)
				if err != nil {
					return err
				}
				committed = v
				return nil
			})
			if errors.Is(err, errCASAbort) {
				continue
			}
			if err != nil {
				return 0, err
			}
			return committed, nil
		}

		// 第二段起：全程持锁重算，协作写者无法插队。
		var committed int
		err := t.withCommitLock(func(base int) error {
			baseSnap, err := t.SnapshotAt(base)
			if err != nil {
				return err
			}
			next, perr := prepare(baseSnap)
			if perr != nil {
				return perr
			}
			curBase, err := t.currentVersion()
			if err != nil {
				return err
			}
			if curBase != base {
				lastConflict = ErrConcurrentModification{Base: base, Current: curBase}
				return errCASAbort
			}
			if err := t.dataFilesExist(next.DataFiles); err != nil {
				lastConflict = err
				return errCASAbort
			}
			v, err := t.commitAtLocked(curBase, next)
			if err != nil {
				return err
			}
			committed = v
			return nil
		})
		if errors.Is(err, errCASAbort) {
			// 持锁仍被推进：非协作写者。不再空转，把冲突交给调用方。
			return 0, lastConflict
		}
		if err != nil {
			return 0, err
		}
		return committed, nil
	}
	if lastConflict != nil {
		return 0, lastConflict
	}
	return 0, fmt.Errorf("commit failed after %d retries", commitMaxRetries)
}

// commitData 处理“写数据文件 + 提交”的公共流程，冲突由 optimisticCommit 处理。
func (t *Table) commitData(op Operation, rows []Row, build func(cur Snapshot, files []string) Snapshot) (int, error) {
	return t.optimisticCommit(func(cur Snapshot) (Snapshot, error) {
		if err := validateRows(cur.Schema, rows); err != nil {
			return Snapshot{}, err
		}
		if len(cur.Schema.PrimaryKey) > 0 {
			if err := checkBatchPKUnique(cur.Schema, rows); err != nil {
				return Snapshot{}, err
			}
		}
		var files []string
		if len(rows) > 0 {
			f, err := t.writeDataFile(rows)
			if err != nil {
				return Snapshot{}, err
			}
			files = []string{f}
		}
		snap := build(cur, files)
		snap.Operation = op
		return snap, nil
	})
}

// currentSnapshot 读取当前版本及其快照（不加锁，供锁外准备阶段使用）。
func (t *Table) currentSnapshot() (int, Snapshot, error) {
	base, err := t.currentVersion()
	if err != nil {
		return 0, Snapshot{}, err
	}
	snap, err := t.SnapshotAt(base)
	if err != nil {
		return 0, Snapshot{}, err
	}
	return base, snap, nil
}

// dataFilesExist 确认候选快照引用的每个数据文件都还在磁盘上。
func (t *Table) dataFilesExist(files []string) error {
	for _, f := range files {
		if !t.store.exists(f) {
			return fmt.Errorf("data file %s vanished before commit (collected by concurrent vacuum?)", f)
		}
	}
	return nil
}

// writeDataFile 把一批行写成一个数据文件，返回相对路径。
func (t *Table) writeDataFile(rows []Row) (string, error) {
	rel := filepath.Join(dataDir, uniqueName("part", "json"))
	data, err := json.Marshal(rows)
	if err != nil {
		return "", err
	}
	if err := t.store.writeFileAtomic(rel, data); err != nil {
		return "", err
	}
	return rel, nil
}

func (t *Table) readDataFile(rel string) ([]Row, error) {
	data, err := t.store.readFile(rel)
	if err != nil {
		return nil, err
	}
	var rows []Row
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("corrupt data file %s: %w", rel, err)
	}
	return rows, nil
}

// validateRows 校验每行与结构兼容：必填列存在、已知列类型匹配。
func validateRows(schema Schema, rows []Row) error {
	for i, r := range rows {
		for _, f := range schema.Fields {
			v, ok := r[f.Name]
			if !ok || v == nil {
				if f.Required {
					return fmt.Errorf("row %d: required field %q missing", i, f.Name)
				}
				continue
			}
			if !typeMatches(f.Type, v) {
				return fmt.Errorf("row %d: field %q expects %s, got %T", i, f.Name, f.Type, v)
			}
		}
	}
	return nil
}

func typeMatches(t DataType, v any) bool {
	switch t {
	case TypeString:
		_, ok := v.(string)
		return ok
	case TypeBool:
		_, ok := v.(bool)
		return ok
	case TypeInt:
		// JSON 数字反序列化为 float64；写入侧允许 int 系类型
		switch v.(type) {
		case int, int32, int64, float64:
			return true
		}
		return false
	case TypeFloat:
		switch v.(type) {
		case int, int32, int64, float32, float64:
			return true
		}
		return false
	}
	return false
}

// checkBatchPKUnique 拒绝批内重复主键，不做静默取舍。
func checkBatchPKUnique(schema Schema, rows []Row) error {
	seen := map[string]struct{}{}
	for _, r := range rows {
		key := pkString(schema, r)
		if _, dup := seen[key]; dup {
			return ErrDuplicatePrimaryKey{Key: key}
		}
		seen[key] = struct{}{}
	}
	return nil
}

func pkString(schema Schema, r Row) string {
	parts := make([]string, 0, len(schema.PrimaryKey))
	for _, pk := range schema.PrimaryKey {
		parts = append(parts, fmt.Sprintf("%v", r[pk]))
	}
	return strings.Join(parts, "|")
}
