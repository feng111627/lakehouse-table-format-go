//go:build !windows

package lakehouse

import (
	"os"
	"path/filepath"
	"syscall"
)

// flockFile 封装一个跨进程排他锁（Unix 下基于 flock(2)）。
// flock 与打开的文件描述符关联：进程退出/崩溃时内核自动释放，
// 因此不会出现写者崩溃后死锁的情况。
type flockFile struct {
	f *os.File
}

// acquireFlock 在 dir 下创建（或打开）锁文件并加排他锁；阻塞到拿到为止。
// 同一进程内多个 goroutine 争用同一文件描述符时 flock 会互相阻塞，
// 但 Table 自身另有 sync.Mutex 串行化进程内提交，避免该问题。
func acquireFlock(dir string) (*flockFile, error) {
	if err := os.MkdirAll(filepath.Join(dir, metaDir), 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	// LOCK_EX 排他锁；同进程不同 fd 也互斥，配合进程内互斥量使用。
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return &flockFile{f: f}, nil
}

func (l *flockFile) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	err1 := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	err2 := l.f.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
