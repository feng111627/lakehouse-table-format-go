package lakehouse

import "fmt"

// compactFileRows 合并重写时每个数据文件的行数上限。
const compactFileRows = 500

// Compact 将当前快照的全部数据文件合并重写为少量文件，提交新快照。
// 若表定义了主键，合并时按“后提交者覆盖先提交者”的确定规则去重；
// 历史快照仍引用旧数据文件，保持可读。
func (t *Table) Compact() (int, error) {
	return t.commitRewrite(OpCompact, func(cur Snapshot, curRows []Row) ([]Row, error) {
		if len(cur.Schema.PrimaryKey) == 0 {
			return curRows, nil
		}
		return dedupeByPK(cur.Schema, curRows), nil
	})
}

// Upsert 按主键合并一批行：主键已存在则覆盖整行，否则追加；
// 批内重复主键返回 ErrDuplicatePrimaryKey，不做静默取舍。
// 要求表定义了主键。
func (t *Table) Upsert(rows []Row) (int, error) {
	return t.commitRewrite(OpUpsert, func(cur Snapshot, curRows []Row) ([]Row, error) {
		if len(cur.Schema.PrimaryKey) == 0 {
			return nil, fmt.Errorf("upsert requires a primary key")
		}
		if err := validateRows(cur.Schema, rows); err != nil {
			return nil, err
		}
		if err := checkBatchPKUnique(cur.Schema, rows); err != nil {
			return nil, err
		}
		return mergeUpsert(cur.Schema, curRows, rows), nil
	})
}

// commitRewrite 在跨进程提交锁内完成“读当前快照 -> 计算新数据 ->
// 整体重写提交”。锁的排他性保证计算期间没有其他提交者能推进版本，
// 因此不需要乐观重试；多进程交错时各自在锁上排队，最终结果仍是
// 各次成功写入按确定顺序的叠加。
func (t *Table) commitRewrite(op Operation, merge func(cur Snapshot, curRows []Row) ([]Row, error)) (int, error) {
	var version int
	err := t.withCommitLock(func() error {
		base, err := t.Current()
		if err != nil {
			return err
		}
		curRows, err := t.readSnapshotRows(base)
		if err != nil {
			return err
		}
		merged, err := merge(base, curRows)
		if err != nil {
			return err // 校验类错误（如重复主键）直接返回，不重试
		}
		files, err := t.writeChunked(merged)
		if err != nil {
			return err
		}
		v, err := t.commitLocked(Snapshot{
			Schema:    base.Schema,
			DataFiles: files,
			Operation: op,
		})
		if err != nil {
			return err
		}
		version = v
		return nil
	})
	return version, err
}

func (t *Table) readSnapshotRows(snap Snapshot) ([]Row, error) {
	var out []Row
	for _, f := range snap.DataFiles {
		rows, err := t.readDataFile(f)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}

// writeChunked 把行按 compactFileRows 分片写成多个数据文件。
func (t *Table) writeChunked(rows []Row) ([]string, error) {
	var files []string
	for start := 0; start < len(rows); start += compactFileRows {
		end := start + compactFileRows
		if end > len(rows) {
			end = len(rows)
		}
		f, err := t.writeDataFile(rows[start:end])
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, nil
}

// dedupeByPK 按主键去重，同一主键保留最后出现的行（后提交者生效）。
func dedupeByPK(schema Schema, rows []Row) []Row {
	var order []string
	byKey := map[string]Row{}
	for _, r := range rows {
		k := pkString(schema, r)
		if _, ok := byKey[k]; !ok {
			order = append(order, k)
		}
		byKey[k] = r
	}
	out := make([]Row, 0, len(order))
	for _, k := range order {
		out = append(out, byKey[k])
	}
	return out
}

// mergeUpsert 将 batch 按主键合并进 curRows：
// 已存在的主键原位覆盖，新主键按批内顺序追加到末尾，结果确定可复现。
func mergeUpsert(schema Schema, curRows, batch []Row) []Row {
	index := map[string]int{}
	out := make([]Row, len(curRows))
	for i, r := range curRows {
		out[i] = r
		index[pkString(schema, r)] = i
	}
	for _, r := range batch {
		k := pkString(schema, r)
		if i, ok := index[k]; ok {
			out[i] = r
		} else {
			index[k] = len(out)
			out = append(out, r)
		}
	}
	return out
}
