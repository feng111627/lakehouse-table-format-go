package lakehouse

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// tagFile 是单个标记的持久化内容：一个名字只对应一个版本。
// 重新指向即原子覆盖整个文件（临时文件 + rename），旧指向立即作废，
// 任何读者都不可能看到“一个名字对应两个版本”。
type tagFile struct {
	Name      string `json:"name"`
	Version   int    `json:"version"`
	UpdatedAt int64  `json:"updatedAt"`
}

// tagPath 返回标记文件的相对路径。标记名经校验只含安全字符，可直接入路径。
func tagPath(name string) string {
	return filepath.Join(tagsDir, name+".json")
}

// validateTagName 校验标记名：1..128 字符，字母/数字/点/下划线/连字符。
func validateTagName(name string) error {
	if name == "" {
		return ErrInvalidTag{Name: name, Detail: "empty name"}
	}
	if len(name) > 128 {
		return ErrInvalidTag{Name: name, Detail: "too long (max 128 bytes)"}
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '.', r == '_', r == '-':
		default:
			return ErrInvalidTag{Name: name, Detail: "allowed chars: [A-Za-z0-9._-]"}
		}
	}
	if name == "." || name == ".." {
		return ErrInvalidTag{Name: name, Detail: "reserved name"}
	}
	return nil
}

// Tag 给某次提交命名。version<=0 表示当前最新版本；
// 同名再次调用即把该名原子地重新指到新版本，旧指向立即作废。
// 目标版本不存在返回 *ErrSnapshotNotFound；已回收返回 *ErrSnapshotExpired。
func (t *Table) Tag(name string, version int) (int, error) {
	if err := validateTagName(name); err != nil {
		return 0, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := acquireFlock(t.store.dir)
	if err != nil {
		return 0, err
	}
	defer lock.release()

	if version <= 0 {
		v, err := t.currentVersion()
		if err != nil {
			return 0, err
		}
		version = v
	}
	// 确认目标快照存在且未被回收（不允许把名字钉在一个读不到的版本上）。
	if _, err := t.SnapshotAt(version); err != nil {
		return 0, err
	}
	tf := tagFile{Name: name, Version: version, UpdatedAt: time.Now().UnixNano()}
	data, err := json.MarshalIndent(tf, "", "  ")
	if err != nil {
		return 0, err
	}
	if err := t.store.writeFileAtomic(tagPath(name), data); err != nil {
		return 0, err
	}
	return version, nil
}

// ResolveTag 返回名字当前指向的版本号；名字不存在返回 *ErrTagNotFound。
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
	var tf tagFile
	if err := json.Unmarshal(data, &tf); err != nil {
		return 0, err
	}
	return tf.Version, nil
}

// DeleteTag 删除一个标记；不存在返回 *ErrTagNotFound。
func (t *Table) DeleteTag(name string) error {
	if err := validateTagName(name); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := acquireFlock(t.store.dir)
	if err != nil {
		return err
	}
	defer lock.release()

	abs := filepath.Join(t.store.dir, tagPath(name))
	if err := os.Remove(abs); err != nil {
		if os.IsNotExist(err) {
			return ErrTagNotFound{Name: name}
		}
		return err
	}
	return nil
}

// listTags 读取全部标记，返回 name -> tagFile。
func (t *Table) listTags() (map[string]tagFile, error) {
	names, err := t.store.listDir(tagsDir)
	if err != nil {
		return nil, err
	}
	out := make(map[string]tagFile, len(names))
	for _, n := range names {
		if filepath.Ext(n) != ".json" {
			continue
		}
		data, err := t.store.readFile(filepath.Join(tagsDir, n))
		if err != nil {
			return nil, err
		}
		var tf tagFile
		if err := json.Unmarshal(data, &tf); err != nil {
			return nil, err
		}
		if tf.Name == "" {
			tf.Name = n[:len(n)-len(".json")]
		}
		out[tf.Name] = tf
	}
	return out, nil
}

// versionsByTag 反查 version -> 排序后的标记名列表。
func (t *Table) versionsByTag() (map[int][]string, error) {
	tags, err := t.listTags()
	if err != nil {
		return nil, err
	}
	out := map[int][]string{}
	for name, tf := range tags {
		out[tf.Version] = append(out[tf.Version], name)
	}
	for v := range out {
		sort.Strings(out[v])
	}
	return out, nil
}
