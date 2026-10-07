package lakehouse

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	tagsDir        = "metadata/tags"
	gcLogName      = "metadata/gc-log.json"
	lockFileName   = "metadata/lock"
	defaultMinAge  = time.Hour
	tempFilePrefix = ".tmp-"      // 崩溃遗留临时文件前缀：Vacuum 会清理
	inflightPrefix = ".inflight-" // 进行中写入的临时文件前缀：Vacuum 永不清理
)

// gcEntry 记录一次 Vacuum 对某个版本的回收（墓碑），让“已过期”可与
// “从未存在”区分，并保证重跑 Vacuum 幂等收敛。
type gcEntry struct {
	Version   int    `json:"version"`
	Timestamp int64  `json:"timestamp"` // 原始快照的提交时间
	GcedAt    int64  `json:"gcedAt"`    // 回收发生时间
	Operation string `json:"operation"`
}

// gcLog 是追加式墓碑日志，先于任何文件删除落盘（崩溃安全）。
type gcLog struct {
	entriesSet map[int]struct{}
	Entries    []gcEntry `json:"entries"`
}

func (t *Table) readGCLog() (*gcLog, error) {
	l := &gcLog{entriesSet: map[int]struct{}{}}
	data, err := t.store.readFile(gcLogName)
	if os.IsNotExist(err) {
		return l, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, l); err != nil {
		return nil, err
	}
	for _, e := range l.Entries {
		l.entriesSet[e.Version] = struct{}{}
	}
	return l, nil
}

func (l *gcLog) add(e gcEntry) bool {
	if _, ok := l.entriesSet[e.Version]; ok {
		return false
	}
	l.entriesSet[e.Version] = struct{}{}
	l.Entries = append(l.Entries, e)
	sort.Slice(l.Entries, func(i, j int) bool { return l.Entries[i].Version < l.Entries[j].Version })
	return true
}

// versions 返回墓碑中记录的版本号。
func (l *gcLog) versions() []int {
	out := make([]int, 0, len(l.Entries))
	for _, e := range l.Entries {
		out = append(out, e.Version)
	}
	return out
}

func dedupeInts(xs []int) []int {
	if len(xs) < 2 {
		return xs
	}
	out := xs[:1]
	for _, x := range xs[1:] {
		if x != out[len(out)-1] {
			out = append(out, x)
		}
	}
	return out
}

func (t *Table) writeGCLog(l *gcLog) error {
	data, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return err
	}
	return t.store.writeFileAtomic(gcLogName, data)
}

// checkExpired 若版本已在 GC 墓碑日志中，返回 *ErrSnapshotExpired。
func (t *Table) checkExpired(version int) error {
	l, err := t.readGCLog()
	if err != nil {
		return err
	}
	if _, ok := l.entriesSet[version]; ok {
		return ErrSnapshotExpired{Version: version}
	}
	return nil
}

// Vacuum 按保留窗口清理过期快照与不再被任何保留快照引用的数据文件。
//
// 安全性：
//   - 当前版本、任何标记指向的版本永远保留；
//   - MaxVersions 与 OlderThan 两个维度取并集（落在任一维度内即保留），
//     只有同时落在两个窗口之外的快照才过期；
//   - 数据文件只要还被任一保留快照引用就不删（回滚复用旧文件同样受保护）；
//   - MinFileAge 保护并发中的文件：落盘不足该时长的文件一律不删，
//     默认 1 小时，调用方可按需调小（例如测试）；
//   - 先把墓碑原子写入 gc-log 再删文件：崩溃后重跑依据墓碑与现存文件
//     重新收敛，不会越清理越乱；
//   - 全程持有跨进程排他锁，与其他进程的提交互斥；读取不持锁、只走
//     不可变文件，保留快照的读取不会遇到缺文件。
func (t *Table) Vacuum(policy RetentionPolicy) (VacuumReport, error) {
	minAge := policy.MinFileAge
	switch {
	case minAge < 0:
		minAge = 0 // 负值：显式禁用文件年龄闸门（主要用于确定性测试）
	case minAge == 0:
		minAge = defaultMinAge
	}
	now := time.Now()
	cutoff := now.Add(-minAge)

	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := acquireFlock(t.store.dir)
	if err != nil {
		return VacuumReport{}, err
	}
	defer lock.release()

	current, err := t.currentVersion()
	if err != nil {
		return VacuumReport{}, err
	}
	// 先读墓碑日志：保留窗口必须基于“完整逻辑版本链”（现存版本 + 已回收
	// 墓碑）计算。否则删完一轮后在仅剩的版本上重算窗口，会让每次重跑都把
	// 保留窗口再吃掉一截（不幂等）。
	gc, err := t.readGCLog()
	if err != nil {
		return VacuumReport{}, err
	}
	versions, err := t.listSnapshotsVersions()
	if err != nil {
		return VacuumReport{}, err
	}
	snaps := make(map[int]Snapshot, len(versions))
	for _, v := range versions {
		s, err := t.SnapshotAt(v)
		if err != nil {
			return VacuumReport{}, err
		}
		snaps[v] = s
	}
	tags, err := t.listTags()
	if err != nil {
		return VacuumReport{}, err
	}

	// fullChain 是含墓碑的完整版本号链（升序），用于计算数量窗口。
	fullChain := append(append([]int{}, versions...), gc.versions()...)
	sort.Ints(fullChain)
	fullChain = dedupeInts(fullChain)

	// 诊断悬空标记：指向的版本既不在磁盘也不在墓碑（通常是外部破坏）。
	var danglingTags []string
	for name, tf := range tags {
		if _, onDisk := snaps[tf.Version]; !onDisk {
			if _, tombstoned := gc.entriesSet[tf.Version]; !tombstoned {
				danglingTags = append(danglingTags, name)
			}
		}
	}
	sort.Strings(danglingTags)

	// ---- 计算保留集合 ----
	protected := map[int]bool{current: true}
	for _, tf := range tags {
		protected[tf.Version] = true
	}
	// 数量窗口基于完整逻辑版本链：最近 MaxVersions 个版本（含已回收的位置）。
	if policy.MaxVersions > 0 && len(fullChain) > policy.MaxVersions {
		keepFrom := len(fullChain) - policy.MaxVersions
		for _, v := range fullChain[keepFrom:] {
			protected[v] = true
		}
	}
	if policy.OlderThan > 0 {
		tsCutoff := now.Add(-policy.OlderThan).UnixNano()
		for _, v := range versions {
			if snaps[v].Timestamp >= tsCutoff {
				protected[v] = true
			}
		}
	}

	// ---- 判定过期快照 ----
	// 快照去留只由保留窗口（MaxVersions/OlderThan，取并集）与
	// 当前版本/标记保护决定；MinFileAge 不参与快照判定，它只用于保护
	// 数据文件与临时文件（可能正被并发提交使用）。
	var expired []int
	var retained []int
	for _, v := range versions {
		if protected[v] {
			retained = append(retained, v)
			continue
		}
		expired = append(expired, v)
	}

	// ---- 第一步：墓碑先落盘（任何物理删除之前）----
	added := false
	for _, v := range expired {
		s := snaps[v]
		if gc.add(gcEntry{
			Version:   v,
			Timestamp: s.Timestamp,
			GcedAt:    now.UnixNano(),
			Operation: string(s.Operation),
		}) {
			added = true
		}
	}
	if added {
		if err := t.writeGCLog(gc); err != nil {
			return VacuumReport{}, err
		}
	}

	report := VacuumReport{
		RetainedVersions: retained,
		ExpiredVersions:  expired,
		DanglingTags:     danglingTags,
	}

	// ---- 第二步：收集保留快照引用的全部数据文件 ----
	referenced := map[string]bool{}
	for _, v := range retained {
		for _, f := range snaps[v].DataFiles {
			referenced[f] = true
		}
	}

	// ---- 第三步：删除无引用且足够老的数据文件；顺手清理陈旧临时文件 ----
	dataFiles, err := t.store.listDir(dataDir)
	if err != nil {
		return report, err
	}
	for _, n := range dataFiles {
		// 进行中的写入临时文件：可能属于某个尚未进入锁、尚未提交的并发写者，
		// 无论多老都绝不能删除（rename 由写者自己完成或由其失败路径清理）。
		if hasInflightPrefix(n) {
			continue
		}
		rel := filepath.Join(dataDir, n) // 快照 DataFiles 中记录的相对路径形态
		info, statErr := os.Stat(filepath.Join(t.store.dir, rel))
		ageOK := statErr == nil && !info.ModTime().After(cutoff)
		isTemp := hasTempPrefix(n)
		if !ageOK {
			continue // 太新：可能正被并发提交使用
		}
		switch {
		case isTemp:
			if t.removeQuiet(rel) {
				report.DeletedTempFiles = append(report.DeletedTempFiles, rel)
			}
		case !referenced[rel]:
			if t.removeQuiet(rel) {
				report.DeletedDataFiles = append(report.DeletedDataFiles, rel)
			}
		}
	}

	// ---- 第四步：删除过期快照元数据及 metadata 下陈旧临时文件 ----
	for _, v := range expired {
		rel := metaPath(v)
		if t.removeQuiet(rel) {
			report.DeletedSnapshots = append(report.DeletedSnapshots, rel)
		}
	}
	metaFiles, err := t.store.listDir(metaDir)
	if err != nil {
		return report, err
	}
	for _, n := range metaFiles {
		if !hasTempPrefix(n) {
			continue
		}
		rel := filepath.Join(metaDir, n)
		info, statErr := os.Stat(filepath.Join(t.store.dir, rel))
		if statErr != nil || info.ModTime().After(cutoff) {
			continue
		}
		if t.removeQuiet(rel) {
			report.DeletedTempFiles = append(report.DeletedTempFiles, rel)
		}
	}
	return report, nil
}

// removeQuiet 删除文件；文件已不存在视为成功（幂等重跑）。
func (t *Table) removeQuiet(rel string) bool {
	err := os.Remove(filepath.Join(t.store.dir, rel))
	if os.IsNotExist(err) {
		return false
	}
	return err == nil
}

func hasTempPrefix(name string) bool {
	return len(name) >= len(tempFilePrefix) && name[:len(tempFilePrefix)] == tempFilePrefix
}

func hasInflightPrefix(name string) bool {
	return len(name) >= len(inflightPrefix) && name[:len(inflightPrefix)] == inflightPrefix
}
