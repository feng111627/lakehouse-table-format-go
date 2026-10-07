package lakehouse

// Rollback 把整张表回滚到指定历史版本，并以一次新的提交呈现结果：
//   - 新快照复制目标版本当时的 Schema 与 DataFiles，Operation=rollback，
//     并记录 RollbackOf；版本号继续向前递增，回滚点之后发生过的提交
//     仍然保留在历史里，不会被抹掉；
//   - 回滚后读取最新版本即得到目标版本的结构与数据；
//   - 目标版本不存在返回 ErrSnapshotNotFound，已被回收返回
//     ErrSnapshotExpired，绝不默默成功。
func (t *Table) Rollback(version int) (int, error) {
	var v int
	err := t.withCommitLock(func() error {
		target, err := t.SnapshotAt(version) // 已区分 not-found / expired
		if err != nil {
			return err
		}
		// 复制文件清单（去重防御性拷贝），目标快照的文件必然仍在：
		// 只有不再被任何存活快照引用时文件才会被回收。
		files := append([]string{}, target.DataFiles...)
		nv, err := t.commitLocked(Snapshot{
			Schema:     target.Schema,
			DataFiles:  files,
			Operation:  OpRollback,
			RollbackOf: target.Version,
		})
		if err != nil {
			return err
		}
		v = nv
		return nil
	})
	return v, err
}
