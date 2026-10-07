package lakehouse

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// timeNow 返回当前时间；单独包一层便于测试中按快照时间戳推导窗口。
func timeNow() time.Time { return time.Now() }

// expiredLog 是回收账本：每个被回收的版本一行 JSON（整体原子重写）。
// 账本让“版本不存在”与“版本曾经存在但已回收”可以明确区分——
// 后者在账本里留痕，读取/回滚时返回 ErrSnapshotExpired。
const expiredLog = metaDir + "/expired.jsonl"

// expiredRecord 是回收账本中的一条记录。
type expiredRecord struct {
	Version   int       `json:"version"`
	Operation Operation `json:"operation"`
	Timestamp int64     `json:"timestamp"`
	ExpiredAt int64     `json:"expiredAt"`
}

// readExpired 读取回收账本；账本不存在时返回空列表。
func (t *Table) readExpired() ([]expiredRecord, error) {
	data, err := t.store.readFile(expiredLog)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []expiredRecord
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec expiredRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			continue // 跳过损坏行；账本整体重写，历史正常行不会损坏
		}
		out = append(out, rec)
	}
	return out, sc.Err()
}

// isExpiredVersion 通过账本判断版本是否已被回收。
func (t *Table) isExpiredVersion(version int) (bool, error) {
	recs, err := t.readExpired()
	if err != nil {
		return false, err
	}
	for _, r := range recs {
		if r.Version == version {
			return true, nil
		}
	}
	return false, nil
}

// appendExpired 把一批回收记录合并进账本后原子重写：天然去重、幂等。
// 账本体积受保留窗口控制（只记录被回收的版本），规模可接受。
func (t *Table) appendExpired(recs []expiredRecord) error {
	existing, err := t.readExpired()
	if err != nil {
		return err
	}
	seen := map[int]bool{}
	all := existing
	for _, r := range all {
		seen[r.Version] = true
	}
	for _, r := range recs {
		if !seen[r.Version] {
			seen[r.Version] = true
			all = append(all, r)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Version < all[j].Version })
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range all {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	return t.store.writeFileAtomic(expiredLog, buf.Bytes())
}

// readSnapshotFile 直接解析磁盘上的 v{N}.json，不经过账本判定。
func (t *Table) readSnapshotFile(version int) (Snapshot, error) {
	data, err := t.store.readFile(metaPath(version))
	if err != nil {
		return Snapshot{}, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return Snapshot{}, fmt.Errorf("corrupt snapshot v%d: %w", version, err)
	}
	return snap, nil
}

// Expire 按保留窗口回收超出窗口、且不再被任何保留快照或标签引用的
// 快照元数据与数据文件。
//
// 安全性：
//   - 全程持有跨进程提交锁，期间没有其他进程能提交或写入新文件，
//     存活集一旦算出即稳定，不会与在线读写发生“边写边删”；
//   - 读某个快照不持锁，但只要快照仍保留，其 DataFiles 引用的每个
//     文件都在存活集内，绝不删除，读者不会读到缺文件；
//   - 当前快照、被标签指向的快照强制保留，与时间/数量窗口无关；
//   - 删除顺序为「回收账本原子落账 -> 删快照元数据 -> 删孤立数据文件」。
//     账本一旦写入，该版本在任何进程看来都已过期（SnapshotAt 优先查
//     账本），读者拿到 ErrSnapshotExpired，而不会读到“元数据还在但
//     数据文件已删”的中间态；任一步崩溃后重跑：账本已有的不重复记、
//     文件删除天然幂等，最终安全收敛，不会越清理越乱。
func (t *Table) Expire(policy RetentionPolicy) (ExpireResult, error) {
	var result ExpireResult
	err := t.withCommitLock(func() error {
		current, err := t.currentVersion()
		if err != nil {
			return err
		}

		// 1. 枚举磁盘上现存的快照（直接读元数据文件；不经过 SnapshotAt，
		//    后者会把“已记账本但元数据尚未删”的版本判为过期）。
		metaFiles, err := t.store.listDir(metaDir)
		if err != nil {
			return err
		}
		var versions []int
		snaps := map[int]Snapshot{}
		for _, f := range metaFiles {
			if !strings.HasPrefix(f, "v") || !strings.HasSuffix(f, ".json") {
				continue
			}
			num := strings.TrimSuffix(strings.TrimPrefix(f, "v"), ".json")
			v, perr := strconv.Atoi(num)
			if perr != nil {
				continue
			}
			snap, serr := t.readSnapshotFile(v)
			if serr != nil {
				return serr
			}
			versions = append(versions, v)
			snaps[v] = snap
		}
		sort.Ints(versions)

		// 已有账本：崩溃前已标记的版本必须继续清理，不得重新保留。
		ledger, err := t.readExpired()
		if err != nil {
			return err
		}
		marked := map[int]bool{}
		for _, r := range ledger {
			marked[r.Version] = true
		}

		// 2. 强制保留集：当前版本 + 所有标签指向的版本。
		tags, err := t.Tags()
		if err != nil {
			return err
		}
		pinned := map[int]bool{current: true}
		for _, tv := range tags {
			pinned[tv] = true
		}

		// 3. 应用保留窗口（数量/时间两个条件取并集；已标记过期者不保留）。
		now := timeNow()
		cutoff := now.Add(-policy.OlderThan).UnixNano()
		keep := map[int]bool{}
		for i, v := range versions {
			snap := snaps[v]
			recent := policy.MinSnapshots > 0 && i >= len(versions)-policy.MinSnapshots
			fresh := policy.OlderThan > 0 && snap.Timestamp >= cutoff
			if !marked[v] && (pinned[v] || recent || fresh) {
				keep[v] = true
			}
		}

		// 4. 汇总存活快照引用的数据文件。
		liveFiles := map[string]bool{}
		for v := range keep {
			for _, f := range snaps[v].DataFiles {
				liveFiles[f] = true
			}
		}

		// 5. 账本先行：本次新判定过期的版本原子写入账本。
		var newRecords []expiredRecord
		var expiredVers []int
		for _, v := range versions {
			if keep[v] {
				continue
			}
			expiredVers = append(expiredVers, v)
			if !marked[v] {
				snap := snaps[v]
				newRecords = append(newRecords, expiredRecord{
					Version:   v,
					Operation: snap.Operation,
					Timestamp: snap.Timestamp,
					ExpiredAt: now.UnixNano(),
				})
			}
		}
		sort.Ints(expiredVers)
		if len(newRecords) > 0 {
			if err := t.appendExpired(newRecords); err != nil {
				return err
			}
		}

		// 6. 删除过期快照元数据。
		for _, v := range expiredVers {
			if err := t.store.remove(metaPath(v)); err != nil {
				return err
			}
		}

		// 7. 最后删除孤立数据文件：data/ 下未被任何存活快照引用者。
		dataFiles, err := t.store.listFilesRecursive(dataDir)
		if err != nil {
			return err
		}
		var deletedData []string
		for _, f := range dataFiles {
			if !liveFiles[f] {
				if err := t.store.remove(f); err != nil {
					return err
				}
				deletedData = append(deletedData, f)
			}
		}

		kept := make([]int, 0, len(versions))
		for _, v := range versions {
			if keep[v] {
				kept = append(kept, v)
			}
		}
		result = ExpireResult{
			ExpiredVersions: expiredVers,
			DeletedFiles:    deletedData,
			KeptVersions:    kept,
		}
		return nil
	})
	return result, err
}
