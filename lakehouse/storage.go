package lakehouse

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// localStore 封装表目录下的文件读写，提供原子写能力。
type localStore struct {
	dir string
}

func newLocalStore(dir string) *localStore { return &localStore{dir: dir} }

// writeFileAtomic 通过 临时文件+rename 保证写入要么整体生效、要么不出现：
// 提交中途失败只会留下无名临时文件，不会留下半可见的目标文件。
func (s *localStore) writeFileAtomic(relPath string, data []byte) error {
	abs := filepath.Join(s.dir, relPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, abs); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

func (s *localStore) readFile(relPath string) ([]byte, error) {
	return os.ReadFile(filepath.Join(s.dir, relPath))
}

// listDir 返回子目录下排序后的文件名列表；目录不存在时返回空列表。
func (s *localStore) listDir(relPath string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, relPath))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

func (s *localStore) exists(relPath string) bool {
	_, err := os.Stat(filepath.Join(s.dir, relPath))
	return err == nil
}

// uniqueName 生成目录内不冲突的数据文件名。
func uniqueName(prefix, ext string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%s.%s", prefix, hex.EncodeToString(b[:]), ext)
}
