package lakehouse

import (
	"sort"
	"strconv"
	"strings"
)

// History 返回从新到旧的历史版本列表：版本号、提交时间、操作类型、
// 指向它的标记、以及是否已被 Vacuum 回收（墓碑中仍保留基本信息）。
func (t *Table) History() ([]HistoryEntry, error) {
	// 1) 现存快照元数据
	metaFiles, err := t.store.listDir(metaDir)
	if err != nil {
		return nil, err
	}
	type metaInfo struct {
		version   int
		operation Operation
		timestamp int64
	}
	live := map[int]metaInfo{}
	var versions []int
	for _, n := range metaFiles {
		if !strings.HasPrefix(n, "v") || !strings.HasSuffix(n, ".json") {
			continue
		}
		num, err := strconv.Atoi(n[1 : len(n)-len(".json")])
		if err != nil {
			continue
		}
		snap, err := t.SnapshotAt(num)
		if err != nil {
			return nil, err
		}
		live[num] = metaInfo{version: num, operation: snap.Operation, timestamp: snap.Timestamp}
		versions = append(versions, num)
	}

	// 2) 已回收版本（墓碑）
	gc, err := t.readGCLog()
	if err != nil {
		return nil, err
	}
	tomb := map[int]gcEntry{}
	for _, e := range gc.Entries {
		tomb[e.Version] = e
	}

	// 3) 标记反查
	byVersion, err := t.versionsByTag()
	if err != nil {
		return nil, err
	}

	all := map[int]bool{}
	for v := range live {
		all[v] = true
	}
	for v := range tomb {
		all[v] = true
	}
	vs := make([]int, 0, len(all))
	for v := range all {
		vs = append(vs, v)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(vs)))

	entries := make([]HistoryEntry, 0, len(vs))
	for _, v := range vs {
		e := HistoryEntry{Version: v, Tags: byVersion[v]}
		if info, ok := live[v]; ok {
			e.Operation = info.operation
			e.Timestamp = info.timestamp
			e.Expired = false
		} else if g, ok := tomb[v]; ok {
			e.Operation = Operation(g.Operation)
			e.Timestamp = g.Timestamp
			e.Expired = true
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// Rollback 把整张表回滚到指定历史版本：复用目标版本的结构与数据文件清单，
// 提交为一个新的快照（Operation=rollback）。中间发生过的提交不被抹掉，
// 回滚之后读取使用目标版本的结构与数据语义。
// 目标版本从未存在返回 *ErrSnapshotNotFound；已被 Vacuum 回收返回
// *ErrSnapshotExpired；version<=0 返回普通参数错误。
func (t *Table) Rollback(version int) (int, error) {
	if version <= 0 {
		return 0, &ErrInvalidRollback{Version: version, Detail: "version must be positive"}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := acquireFlock(t.store.dir)
	if err != nil {
		return 0, err
	}
	defer lock.release()

	base, err := t.currentVersion()
	if err != nil {
		return 0, err
	}
	target, err := t.SnapshotAt(version) // 内含过期检查
	if err != nil {
		return 0, err
	}
	if base == version {
		// 已在目标版本：仍是一次幂等的新提交，便于在历史中留下回滚记录。
	}
	files := append([]string{}, target.DataFiles...)
	return t.commitAtLocked(base, Snapshot{
		Schema:              target.Schema,
		DataFiles:           files,
		Operation:           OpRollback,
		RollbackFromVersion: target.Version,
	})
}

// listSnapshotsVersions 列出磁盘上现存的全部快照版本号（升序）。
func (t *Table) listSnapshotsVersions() ([]int, error) {
	metaFiles, err := t.store.listDir(metaDir)
	if err != nil {
		return nil, err
	}
	var out []int
	for _, n := range metaFiles {
		if !strings.HasPrefix(n, "v") || !strings.HasSuffix(n, ".json") {
			continue
		}
		num, err := strconv.Atoi(n[1 : len(n)-len(".json")])
		if err != nil {
			continue
		}
		out = append(out, num)
	}
	sort.Ints(out)
	return out, nil
}
