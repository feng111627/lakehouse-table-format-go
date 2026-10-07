package lakehouse

// AppendAt 以指定基版本 base 做一次条件追加：
//   - 提交时当前版本仍为 base：正常提交，返回新版本号；
//   - 当前版本已被其他进程/写者推进：返回 ErrConcurrentModification
//     （携带 Base 与 Current），调用方可据此自行决定重试或放弃，
//     该错误与参数校验等普通错误用类型即可区分，绝不静默失败。
//
// 典型用法是“读出 base -> 计算增量 -> AppendAt(base, rows)”的
// 读改写循环；冲突后重新读最新版本再重算。
func (t *Table) AppendAt(base int, rows []Row) (int, error) {
	if base <= 0 {
		return 0, ErrSnapshotNotFound{Version: base}
	}
	return t.appendLocked(base, rows)
}
