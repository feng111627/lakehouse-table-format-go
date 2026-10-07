package lakehouse

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// tagsDir 是标签文件目录，每个标签一个小文件，内容为指向的版本号。
const tagsDir = metaDir + "/tags"

// Tag 把名字 name 指向版本 version。
//   - 同名再次指向新版本：原子替换标签文件，旧指向立即作废，
//     任何时刻一个名字最多对应一个版本，不存在一名字两版本的歧义；
//   - 标签移动与数据提交共用同一把跨进程提交锁，与写入/回收互斥；
//   - version 必须真实存在（未被回收），否则返回对应错误。
func (t *Table) Tag(name string, version int) error {
	if err := validateTagName(name); err != nil {
		return err
	}
	return t.withCommitLock(func() error {
		if _, err := t.SnapshotAt(version); err != nil {
			return err
		}
		return t.store.writeFileAtomic(tagPath(name), []byte(strconv.Itoa(version)))
	})
}

// Untag 删除一个标签；名字不存在返回 ErrTagNotFound。
func (t *Table) Untag(name string) error {
	if err := validateTagName(name); err != nil {
		return err
	}
	return t.withCommitLock(func() error {
		p := filepath.Join(t.store.dir, tagPath(name))
		if err := os.Remove(p); err != nil {
			if os.IsNotExist(err) {
				return ErrTagNotFound{Name: name}
			}
			return err
		}
		return nil
	})
}

// ResolveTag 解析标签当前指向的版本；名字不存在返回 ErrTagNotFound。
// 标签文件原子写入，读不到半写状态；若指向的版本事后被回收，
// 仍返回版本号（标签本身保证该版本不会被回收，见 Expire）。
func (t *Table) ResolveTag(name string) (int, error) {
	if err := validateTagName(name); err != nil {
		return 0, err
	}
	data, err := t.store.readFile(tagPath(name))
	if os.IsNotExist(err) {
		return 0, ErrTagNotFound{Name: name}
	}
	if err != nil {
		return 0, err
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("corrupt tag %q: %w", name, err)
	}
	return v, nil
}

// Tags 返回全部标签名到版本的映射。
func (t *Table) Tags() (map[string]int, error) {
	names, err := t.store.listDir(tagsDir)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int, len(names))
	for _, n := range names {
		// 跳过原子写可能残留的临时文件（崩溃窗口）。
		if strings.HasPrefix(n, ".tmp-") {
			continue
		}
		data, err := t.store.readFile(filepath.Join(tagsDir, n))
		if err != nil {
			return nil, err
		}
		v, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			return nil, fmt.Errorf("corrupt tag %q: %w", n, err)
		}
		out[n] = v
	}
	return out, nil
}

// History 返回版本历史列表，按版本号升序。
// 每条包含：版本号、产生操作、提交时间、当时指向它的标签、是否已回收。
// 已回收版本来自回收账本（见 expire.go），仍会出现在列表中但 Expired=true。
func (t *Table) History() ([]HistoryEntry, error) {
	snapFiles, err := t.store.listDir(metaDir)
	if err != nil {
		return nil, err
	}
	var entries []HistoryEntry
	for _, f := range snapFiles {
		if !strings.HasPrefix(f, "v") || !strings.HasSuffix(f, ".json") {
			continue
		}
		num := strings.TrimSuffix(strings.TrimPrefix(f, "v"), ".json")
		v, err := strconv.Atoi(num)
		if err != nil {
			continue
		}
		snap, err := t.SnapshotAt(v)
		if err != nil {
			return nil, err
		}
		entries = append(entries, HistoryEntry{
			Version:   v,
			Operation: snap.Operation,
			Timestamp: snap.Timestamp,
		})
	}

	// 合并已回收版本账本。
	expired, err := t.readExpired()
	if err != nil {
		return nil, err
	}
	expiredSet := map[int]bool{}
	for _, e := range expired {
		expiredSet[e.Version] = true
	}
	have := map[int]bool{}
	for _, en := range entries {
		have[en.Version] = true
	}
	for _, e := range expired {
		if !have[e.Version] {
			entries = append(entries, HistoryEntry{
				Version:   e.Version,
				Operation: e.Operation,
				Timestamp: e.Timestamp,
				Expired:   true,
			})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Version < entries[j].Version })

	// 附上标签归属。
	tags, err := t.Tags()
	if err != nil {
		return nil, err
	}
	tagOf := map[int][]string{}
	for name, v := range tags {
		tagOf[v] = append(tagOf[v], name)
	}
	for v := range tagOf {
		sort.Strings(tagOf[v])
	}
	for i := range entries {
		if names := tagOf[entries[i].Version]; len(names) > 0 {
			entries[i].Tags = names
		}
	}
	return entries, nil
}

// ---- 内部 ----

func tagPath(name string) string {
	return filepath.Join(tagsDir, name)
}

// validateTagName 拒绝会破坏目录布局或造成路径穿越的名字。
func validateTagName(name string) error {
	if name == "" {
		return ErrInvalidTagName{Name: name}
	}
	if name == "." || name == ".." ||
		strings.ContainsAny(name, `/\`) ||
		strings.HasPrefix(name, ".") {
		return ErrInvalidTagName{Name: name}
	}
	if filepath.Separator == '/' && strings.Contains(name, "\x00") {
		return ErrInvalidTagName{Name: name}
	}
	return nil
}
