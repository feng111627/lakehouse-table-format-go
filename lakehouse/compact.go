package lakehouse

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// CompactResult summarizes a compaction commit.
type CompactResult struct {
	SnapshotID   int64
	FilesRemoved int
	FilesAdded   int
	Records      int
	NoOp         bool
}

// chainLocked returns snapshots from the root to id by following ParentID.
// Caller must hold t.mu (or otherwise guarantee the maps are stable).
func (t *Table) chainLocked(id int64) ([]*Snapshot, error) {
	var rev []*Snapshot
	cur := id
	seen := map[int64]bool{}
	for cur != 0 {
		s := t.snaps[cur]
		if s == nil {
			return nil, fmt.Errorf("%w: %d", ErrSnapshotNotFound, cur)
		}
		if seen[cur] {
			return nil, fmt.Errorf("lakehouse: snapshot cycle detected at %d", cur)
		}
		seen[cur] = true
		rev = append(rev, s)
		cur = s.ParentID
	}
	out := make([]*Snapshot, len(rev))
	for i, s := range rev {
		out[len(rev)-1-i] = s
	}
	return out, nil
}

// liveFilesFromChain replays Added/Removed deltas root -> id and returns the
// logically-live files in deterministic path order.
func liveFilesFromChain(chain []*Snapshot) []DataFile {
	live := map[string]DataFile{}
	order := map[string]int{} // path -> index in paths
	var paths []string
	removePath := func(p string) {
		idx, ok := order[p]
		if !ok {
			return
		}
		delete(order, p)
		paths[idx] = ""
	}
	for _, s := range chain {
		for _, f := range s.RemovedFiles {
			removePath(f.Path)
		}
		for _, f := range s.AddedFiles {
			if _, ok := live[f.Path]; ok {
				continue
			}
			order[f.Path] = len(paths)
			paths = append(paths, f.Path)
			live[f.Path] = f
		}
	}
	out := make([]DataFile, 0, len(live))
	for _, p := range paths {
		if p == "" {
			continue
		}
		out = append(out, live[p])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// liveFiles resolves live files of one snapshot (takes the read lock itself).
func (t *Table) liveFiles(id int64) ([]DataFile, error) {
	t.mu.Lock()
	chain, err := t.chainLocked(id)
	t.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return liveFilesFromChain(chain), nil
}

// projectRecord converts a record stored under fileSch into a record shaped
// by targetSch: dropped columns disappear, later-added columns read as null,
// and widened columns are deterministically coerced.
func projectRecord(fileSch, targetSch *Schema, rec record) (record, error) {
	out := record{}
	for i := range targetSch.Fields {
		f := &targetSch.Fields[i]
		v, present := rec[f.ID]
		if !present || v == nil {
			out[f.ID] = nil
			continue
		}
		if ff, ok := fileSch.FieldByID(f.ID); ok && ff.Type != f.Type {
			cv, err := coerceValue(v, ff.Type, f.Type)
			if err != nil {
				return nil, err
			}
			out[f.ID] = cv
		} else {
			out[f.ID] = v
		}
	}
	return out, nil
}

func recordToRow(sch *Schema, rec record) Row {
	row := Row{}
	for i := range sch.Fields {
		f := &sch.Fields[i]
		row[f.Name] = rec[f.ID]
	}
	return row
}

// readAtID implements snapshot-isolated time travel. Only immutable files and
// metadata are touched, so concurrent commits cannot change the result. The
// mutex is held only long enough to snapshot the immutable metadata pointers;
// all file I/O happens lock-free.
func (t *Table) readAtID(ctx context.Context, id int64) (*ResultSet, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, ErrTableClosed
	}
	chain, err := t.chainLocked(id)
	if err != nil {
		t.mu.Unlock()
		return nil, err
	}
	snap := t.snaps[id]
	target := t.schemas[snap.SchemaVersion]
	// Collect the immutable schema pointers this read needs. reload() only
	// ever replaces whole maps, so the grabbed pointers remain valid.
	fileSchemas := map[int]*Schema{}
	for _, s := range chain {
		for _, f := range s.AddedFiles {
			if sch := t.schemas[f.SchemaVersion]; sch != nil {
				fileSchemas[f.SchemaVersion] = sch
			}
		}
	}
	st := t.st
	t.mu.Unlock()

	files := liveFilesFromChain(chain)
	rows := make([]Row, 0)
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		payload, err := st.readData(f)
		if err != nil {
			return nil, err
		}
		fileSch := fileSchemas[payload.SchemaVersion]
		if fileSch == nil {
			// Schema was written by a process this handle hasn't replayed yet.
			fileSch, err = st.readSchemaVersion(payload.SchemaVersion)
			if err != nil {
				return nil, err
			}
		}
		for _, rec := range payload.Records {
			pr, err := projectRecord(fileSch, target, rec)
			if err != nil {
				return nil, err
			}
			rows = append(rows, recordToRow(target, pr))
		}
	}
	sortRows(rows, target)
	return &ResultSet{SnapshotID: id, Schema: target, Rows: rows}, nil
}

// pkKey builds a type-tagged composite primary-key encoding.
func pkKey(sch *Schema, rec record) string {
	var b strings.Builder
	for _, id := range sch.PrimaryKeyIDs() {
		v := rec[id]
		switch x := v.(type) {
		case nil:
			b.WriteString("n:")
		case bool:
			b.WriteString("b:")
			if x {
				b.WriteByte('1')
			} else {
				b.WriteByte('0')
			}
		case int64:
			b.WriteString("i:")
			b.WriteString(fmt.Sprintf("%d", x))
		case float64:
			b.WriteString("f:")
			b.WriteString(fmt.Sprintf("%v", x))
		case string:
			b.WriteString("s:")
			b.WriteString(x)
		default:
			b.WriteString("?:")
			b.WriteString(fmt.Sprintf("%v", x))
		}
		b.WriteByte(0)
	}
	return b.String()
}

// checkDuplicatePK rejects repeated primary keys inside one batch so data is
// never silently chosen between duplicates.
func checkDuplicatePK(sch *Schema, recs []record) error {
	seen := make(map[string]int, len(recs))
	for i, rec := range recs {
		k := pkKey(sch, rec)
		if first, ok := seen[k]; ok {
			return fmt.Errorf("%w: rows %d and %d share primary key %s",
				ErrDuplicatePK, first, i, pkDisplay(sch, rec))
		}
		seen[k] = i
	}
	return nil
}

func pkDisplay(sch *Schema, rec record) string {
	var parts []string
	for _, id := range sch.PrimaryKeyIDs() {
		f, _ := sch.FieldByID(id)
		parts = append(parts, fmt.Sprintf("%s=%v", f.Name, rec[id]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// liveStateLocked loads the effective records and files of HEAD projected to
// the HEAD schema. Caller holds the commit lock.
func (t *Table) liveStateLocked() (*Schema, []record, []DataFile, error) {
	headSnap := t.snaps[t.head]
	sch := t.schemas[headSnap.SchemaVersion]
	chain, err := t.chainLocked(t.head)
	if err != nil {
		return nil, nil, nil, err
	}
	files := liveFilesFromChain(chain)
	recs := make([]record, 0)
	for _, f := range files {
		payload, err := t.st.readData(f)
		if err != nil {
			return nil, nil, nil, err
		}
		fileSch := t.schemas[payload.SchemaVersion]
		if fileSch == nil {
			fileSch, err = t.st.readSchemaVersion(payload.SchemaVersion)
			if err != nil {
				return nil, nil, nil, err
			}
		}
		for _, rec := range payload.Records {
			pr, err := projectRecord(fileSch, sch, rec)
			if err != nil {
				return nil, nil, nil, err
			}
			recs = append(recs, pr)
		}
	}
	return sch, recs, files, nil
}

// checkLiveDuplicatePK refuses upsert when existing data already contains
// duplicate primary keys; silently picking a winner would be wrong.
func (t *Table) checkLiveDuplicatePK(sch *Schema, recs []record) error {
	seen := map[string]bool{}
	for _, rec := range recs {
		k := pkKey(sch, rec)
		if seen[k] {
			return fmt.Errorf("%w: existing table rows share primary key %s; dedupe explicitly before upsert",
				ErrDuplicatePK, pkDisplay(sch, rec))
		}
		seen[k] = true
	}
	return nil
}

// mergeUpsert replaces live records sharing a key with the incoming record,
// appending new keys otherwise. Output order is deterministic (PK-sorted).
func mergeUpsert(sch *Schema, live []record, incoming []record) (merged []record, inserted, updated int) {
	byKey := make(map[string]record, len(live)+len(incoming))
	for _, rec := range live {
		byKey[pkKey(sch, rec)] = rec
	}
	for _, rec := range incoming {
		k := pkKey(sch, rec)
		if _, ok := byKey[k]; ok {
			updated++
		} else {
			inserted++
		}
		byKey[k] = rec
	}
	merged = make([]record, 0, len(byKey))
	for _, rec := range byKey {
		merged = append(merged, rec)
	}
	sortRecords(merged, sch)
	return merged, inserted, updated
}

func sortRecords(recs []record, sch *Schema) {
	rows := make([]Row, len(recs))
	for i, rec := range recs {
		rows[i] = recordToRow(sch, rec)
	}
	sortRows(rows, sch)
	for i, row := range rows {
		rec := record{}
		for _, f := range sch.Fields {
			rec[f.ID] = row[f.Name]
		}
		recs[i] = rec
	}
}

// rowsToRecords validates every row against sch and rejects intra-batch
// duplicate primary keys.
func rowsToRecords(sch *Schema, rows []Row) ([]record, error) {
	recs := make([]record, 0, len(rows))
	for i, r := range rows {
		rec, err := validateRow(sch, r)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		recs = append(recs, rec)
	}
	if err := checkDuplicatePK(sch, recs); err != nil {
		return nil, err
	}
	return recs, nil
}

// Upsert merges rows by immutable primary key and commits one atomic update
// snapshot. It fails loudly on duplicate keys in the input or pre-existing
// duplicate keys in the table; the whole commit is rejected.
func (t *Table) Upsert(ctx context.Context, rows []Row) (int64, error) {
	unlock, err := t.lockForCommit(ctx)
	if err != nil {
		return 0, err
	}
	defer unlock()

	if len(rows) == 0 {
		return t.head, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	sch, live, liveFiles, err := t.liveStateLocked()
	if err != nil {
		return 0, err
	}
	incoming, err := rowsToRecords(sch, rows)
	if err != nil {
		return 0, err
	}
	if err := t.checkLiveDuplicatePK(sch, live); err != nil {
		return 0, err
	}
	merged, inserted, updated := mergeUpsert(sch, live, incoming)
	payload := &dataFilePayload{SchemaVersion: sch.Version, Records: merged}
	df, err := t.st.stageData(t.head+1, 1, payload)
	if err != nil {
		return 0, err
	}
	id, err := t.commitLocked(OpUpdate, []DataFile{df}, liveFiles, sch.Version,
		map[string]string{
			"rows":        fmt.Sprintf("%d", len(merged)),
			"inserted":    fmt.Sprintf("%d", inserted),
			"updated":     fmt.Sprintf("%d", updated),
			"files_added": "1",
		})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// Compact rewrites all live small files of HEAD into one file containing the
// exact effective records (projected to the current schema). Row content is
// preserved; historical snapshots stay untouched.
func (t *Table) Compact(ctx context.Context, smallFileThreshold int) (CompactResult, error) {
	unlock, err := t.lockForCommit(ctx)
	if err != nil {
		return CompactResult{}, err
	}
	defer unlock()

	threshold := smallFileThreshold
	if threshold <= 0 {
		threshold = t.cfg.CompactFileThreshold
	}
	if threshold <= 0 {
		threshold = 2
	}
	if err := ctx.Err(); err != nil {
		return CompactResult{}, err
	}
	sch, recs, files, err := t.liveStateLocked()
	if err != nil {
		return CompactResult{}, err
	}
	if len(files) < threshold {
		return CompactResult{SnapshotID: t.head, NoOp: true,
			FilesRemoved: 0, FilesAdded: 0, Records: len(recs)}, nil
	}
	sortRecords(recs, sch)
	payload := &dataFilePayload{SchemaVersion: sch.Version, Records: recs}
	df, err := t.st.stageData(t.head+1, 1, payload)
	if err != nil {
		return CompactResult{}, err
	}
	id, err := t.commitLocked(OpCompact, []DataFile{df}, files, sch.Version,
		map[string]string{
			"rows":          fmt.Sprintf("%d", len(recs)),
			"files_added":   "1",
			"files_removed": fmt.Sprintf("%d", len(files)),
		})
	if err != nil {
		return CompactResult{}, err
	}
	return CompactResult{
		SnapshotID:   id,
		FilesRemoved: len(files),
		FilesAdded:   1,
		Records:      len(recs),
	}, nil
}
