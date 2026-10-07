//go:build windows

package lakehouse

import "errors"

// flockFile 是 Windows 下的占位实现（本实现只在类 Unix 本地文件系统验证）。
type flockFile struct{}

func acquireFlock(dir string) (*flockFile, error) {
	return nil, errors.New("cross-process locking is not supported on windows")
}

func (l *flockFile) release() error { return nil }
