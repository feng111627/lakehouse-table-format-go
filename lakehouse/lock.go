package lakehouse

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// commitLockFile 是跨进程提交锁在表目录中的位置。
const commitLockFile = metaDir + "/commit.lock"

// fileLock 是跨进程提交锁的句柄。
type fileLock struct {
	file *os.File
}

// acquireCommitLock 获取表目录上的跨进程排他提交锁。
// flock 为建议锁（advisory lock）：同一台机器上，所有经由本协议
// 打开同一目录的进程都会在此串行；阻塞直到锁可用，避免抢版本。
func acquireCommitLock(dir string) (*fileLock, error) {
	// 锁文件所在目录可能尚未创建（建表场景），先确保存在。
	if err := os.MkdirAll(filepath.Join(dir, metaDir), 0o755); err != nil {
		return nil, fmt.Errorf("create metadata dir: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, commitLockFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open commit lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, fmt.Errorf("acquire commit lock: %w", err)
	}
	return &fileLock{file: f}, nil
}

// release 释放跨进程提交锁并关闭文件。
func (l *fileLock) release() error {
	if l == nil || l.file == nil {
		return nil
	}
	_ = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	return l.file.Close()
}

// withCommitLock 在“进程内互斥 + 跨进程排他锁”双重保护下执行 fn。
// 所有推进 current 指针、改写标签或回收文件的操作都必须经由该入口，
// 从而在单进程多 goroutine 与多进程两种部署下都被全局串行化。
func (t *Table) withCommitLock(fn func() error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := acquireCommitLock(t.store.dir)
	if err != nil {
		return err
	}
	defer lock.release()
	return fn()
}
