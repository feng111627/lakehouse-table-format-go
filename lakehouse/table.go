package lakehouse

import (
	"encoding/json"
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
)

// Table 是一张本地数据湖表，句柄可在 goroutine 间共享。
type Table struct {
	store *localStore
	mu    sync.Mutex // 串行化本进程内的提交
}

// Create 在 dir 下创建一张新表；目录下已存在表时返回错误。
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
	err := t.withCommitLock(func() error {
		if store.exists(currentPtr) {
			return fmt.Errorf("table already exists at %s", dir)
		}
		return t.commitSnapshot(Snapshot{
			Version:   1,
			Schema:    schema,
			DataFiles: nil,
			Operation: OpCreate,
			Timestamp: time.Now().UnixNano(),
		})
	})
	if err != nil {
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
// 版本号从未存在返回 ErrSnapshotNotFound；版本曾存在但已被回收
// 返回 ErrSnapshotExpired（依据回收账本 expired.jsonl 区分）。
func (t *Table) SnapshotAt(version int) (Snapshot, error) {
	// 回收账本优先：一旦某版本被标记回收（即使元数据文件因崩溃尚在），
	// 都视为已过期，读者得到明确的 ErrSnapshotExpired 而非读到中间态。
	if exp, _ := t.isExpiredVersion(version); exp {
		return Snapshot{}, ErrSnapshotExpired{Version: version}
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

// Append 提交一批新增行，返回新快照版本。
// 跨进程提交锁保证多个进程同时写入时按确定的全局顺序线性叠加，
// 不会出现一个进程盖掉另一个进程更新的情况。
// 若表中定义了主键，批内重复主键会被拒绝而不是静默取舍。
func (t *Table) Append(rows []Row) (int, error) {
	return t.appendLocked(0, rows)
}

// EvolveSchema 校验并提交一次结构变更；数据文件不变，仅推进结构版本。
func (t *Table) EvolveSchema(next Schema) (int, error) {
	var version int
	err := t.withCommitLock(func() error {
		cur, err := t.Current()
		if err != nil {
			return err
		}
		if err := checkSchemaChange(cur.Schema, next); err != nil {
			return err
		}
		v, err := t.commitLocked(Snapshot{
			Schema:    next,
			DataFiles: cur.DataFiles,
			Operation: OpSchema,
		})
		if err != nil {
			return err
		}
		version = v
		return nil
	})
	return version, err
}

// Read 读取指定快照版本下的全部有效行；version<=0 表示最新快照。
// 同一版本的读取结果稳定且可复现：文件按名排序、行按文件内顺序返回。
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
// 未引用的元数据文件成为垃圾但不影响一致性。
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

// commitLocked 在持锁状态下提交一份新快照（版本 = 当前版本+1）。
func (t *Table) commitLocked(snap Snapshot) (int, error) {
	cur, err := t.currentVersion()
	if err != nil {
		return 0, err
	}
	snap.Version = cur + 1
	snap.Timestamp = time.Now().UnixNano()
	if err := t.commitSnapshot(snap); err != nil {
		return 0, err
	}
	return snap.Version, nil
}

// appendLocked 在提交锁内完成“校验 -> 写数据文件 -> 提交”。
//
// base>0 表示条件提交：仅当当前版本仍等于 base 时才提交，否则返回
// ErrConcurrentModification，由调用方自行决定是否重试；
// base<=0 表示无条件追加到当前最新版本（被提交锁串行，必定成功定版）。
//
// 数据文件在锁内写入：这样回收（Expire）持锁扫描存活集时，绝不会看到
// 一个尚未被任何快照引用、却即将被引用的新文件，避免“边写边删”的竞态。
func (t *Table) appendLocked(base int, rows []Row) (int, error) {
	var version int
	err := t.withCommitLock(func() error {
		curVer, err := t.currentVersion()
		if err != nil {
			return err
		}
		if base > 0 {
			// 基版本本身不存在或已回收时，返回对应的明确错误，
			// 不与“版本被推进”的冲突错误混淆。
			if _, err := t.SnapshotAt(base); err != nil {
				return err
			}
		}
		if base > 0 && curVer != base {
			return ErrConcurrentModification{Base: base, Current: curVer}
		}
		baseSnap, err := t.SnapshotAt(curVer)
		if err != nil {
			return err
		}
		if err := validateRows(baseSnap.Schema, rows); err != nil {
			return err
		}
		if len(baseSnap.Schema.PrimaryKey) > 0 {
			if err := checkBatchPKUnique(baseSnap.Schema, rows); err != nil {
				return err
			}
		}
		var files []string
		if len(rows) > 0 {
			f, err := t.writeDataFile(rows)
			if err != nil {
				return err
			}
			files = []string{f}
		}
		v, err := t.commitLocked(Snapshot{
			Schema:    baseSnap.Schema,
			DataFiles: append(append([]string{}, baseSnap.DataFiles...), files...),
			Operation: OpAppend,
		})
		if err != nil {
			return err
		}
		version = v
		return nil
	})
	return version, err
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
