package lakehouse

import (
	"context"
	"fmt"
	"os"
	"sync"
)

// Config controls table creation.
type Config struct {
	// CompactFileThreshold is the advisory default small-file count threshold.
	CompactFileThreshold int
}

// Table is a handle onto a local lakehouse table directory. A single handle
// is safe for concurrent use; multiple processes may share one directory.
type Table struct {
	mu      sync.Mutex // serializes mutating operations inside this process
	closed  bool
	dir     string
	st      *storage
	cfg     Config
	head    int64
	schemas map[int]*Schema     // version -> compiled schema
	snaps   map[int64]*Snapshot // id -> snapshot
	ids     []int64             // snapshot ids in commit order
}

// CreateTable bootstraps a new empty table. Snapshot 1 is an empty snapshot
// carrying the initial schema.
func CreateTable(ctx context.Context, dir string, sch *Schema, cfg *Config) (*Table, error) {
	if cfg == nil {
		cfg = &Config{}
	}
	st := newStorage(dir)
	if err := st.initLayout(); err != nil {
		return nil, err
	}
	fl, err := acquireFileLock(ctx, dir)
	if err != nil {
		return nil, err
	}
	defer fl.release()
	if err := st.cleanupOrphans(); err != nil {
		return nil, err
	}
	if st.exists("meta/HEAD") {
		return nil, fmt.Errorf("%w at %s", ErrTableExists, dir)
	}
	if err := validateCreateSchema(sch); err != nil {
		return nil, err
	}
	persisted := sch.WithVersion(1)
	if err := st.writeSchemaVersion(persisted); err != nil {
		return nil, err
	}
	// Snapshot 1 is the empty root snapshot.
	root := &Snapshot{
		ID:            1,
		ParentID:      0,
		SchemaVersion: 1,
		Operation:     OpAppend,
		TimestampMs:   nowMillis(),
		Summary:       map[string]string{"rows": "0", "note": "table creation"},
	}
	if err := st.writeSnapshot(root); err != nil {
		return nil, err
	}
	if err := st.writeHEAD(root.ID); err != nil {
		return nil, err
	}
	t := &Table{
		dir:     dir,
		st:      st,
		cfg:     *cfg,
		head:    root.ID,
		schemas: map[int]*Schema{1: persisted},
		snaps:   map[int64]*Snapshot{root.ID: root},
		ids:     []int64{root.ID},
	}
	return t, nil
}

// OpenTable opens an existing table and replays its metadata.
func OpenTable(ctx context.Context, dir string) (*Table, error) {
	st := newStorage(dir)
	fl, err := acquireFileLock(ctx, dir)
	if err != nil {
		return nil, err
	}
	if err := st.initLayout(); err != nil {
		_ = fl.release()
		return nil, err
	}
	if err := st.cleanupOrphans(); err != nil {
		_ = fl.release()
		return nil, err
	}
	head, err := st.readHEAD()
	if err != nil {
		if os.IsNotExist(err) {
			_ = fl.release()
			return nil, fmt.Errorf("%w at %s", ErrTableNotFound, dir)
		}
		_ = fl.release()
		return nil, err
	}
	fl.release()

	t := &Table{
		dir:     dir,
		st:      st,
		schemas: map[int]*Schema{},
		snaps:   map[int64]*Snapshot{},
	}
	if err := t.reload(); err != nil {
		return nil, err
	}
	t.head = head
	return t, nil
}

// reload re-reads all snapshot metadata and referenced schema versions from
// disk. Called on open and after acquiring the cross-process commit lock, so
// a handle always commits on top of the newest durable state.
func (t *Table) reload() error {
	ids, err := t.st.listSnapshotIDs()
	if err != nil {
		return err
	}
	snaps := make(map[int64]*Snapshot, len(ids))
	needed := map[int]struct{}{}
	for _, id := range ids {
		s, err := t.st.readSnapshot(id)
		if err != nil {
			return err
		}
		snaps[id] = s
		needed[s.SchemaVersion] = struct{}{}
	}
	schemas := make(map[int]*Schema, len(needed))
	for v := range needed {
		sch, err := t.st.readSchemaVersion(v)
		if err != nil {
			return err
		}
		schemas[v] = sch
	}
	t.snaps = snaps
	t.ids = ids
	t.schemas = schemas
	return nil
}

// lockForCommit serializes in-process (t.mu) and cross-process (flock)
// writers, then refreshes the in-memory view. The returned unlocker is nil
// only on error.
func (t *Table) lockForCommit(ctx context.Context) (func(), error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, ErrTableClosed
	}
	fl, err := acquireFileLock(ctx, t.dir)
	if err != nil {
		t.mu.Unlock()
		return nil, err
	}
	if err := t.st.cleanupOrphans(); err != nil {
		_ = fl.release()
		t.mu.Unlock()
		return nil, err
	}
	if err := t.reload(); err != nil {
		_ = fl.release()
		t.mu.Unlock()
		return nil, err
	}
	head, err := t.st.readHEAD()
	if err != nil {
		_ = fl.release()
		t.mu.Unlock()
		return nil, err
	}
	t.head = head
	return func() {
		_ = fl.release()
		t.mu.Unlock()
	}, nil
}

func (t *Table) checkOpen() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrTableClosed
	}
	return nil
}

// Close releases the handle.
func (t *Table) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

// Dir returns the table root directory.
func (t *Table) Dir() string { return t.dir }

// CurrentSnapshotID returns the committed HEAD snapshot id.
func (t *Table) CurrentSnapshotID() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.head
}

// CurrentSchema returns the schema of HEAD.
func (t *Table) CurrentSchema() *Schema {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.snaps[t.head]
	if s == nil {
		return nil
	}
	return t.schemas[s.SchemaVersion]
}

// SnapshotInfo is the read-only view of snapshot metadata.
type SnapshotInfo = Snapshot

// Snapshots returns metadata of all snapshots in commit order.
func (t *Table) Snapshots() []SnapshotInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]SnapshotInfo, 0, len(t.ids))
	for _, id := range t.ids {
		out = append(out, *t.snaps[id])
	}
	return out
}

// commitLocked appends one snapshot, updates in-memory state and advances HEAD.
// The caller must hold the commit lock. Durability order: data files were
// already renamed into place before snapshot write; then snapshot metadata;
// HEAD is updated last, so no snapshot can ever reference missing files, and a
// crash before the HEAD rename simply hides the staged commit.
func (t *Table) commitLocked(op Operation, adds []DataFile, removes []DataFile,
	schemaVersion int, summary map[string]string) (int64, error) {
	id := t.head + 1
	snap := &Snapshot{
		ID:            id,
		ParentID:      t.head,
		SchemaVersion: schemaVersion,
		Operation:     op,
		TimestampMs:   nowMillis(),
		AddedFiles:    adds,
		RemovedFiles:  removes,
		Summary:       summary,
	}
	if err := t.st.writeSnapshot(snap); err != nil {
		return 0, err
	}
	if err := t.st.writeHEAD(id); err != nil {
		return 0, err
	}
	t.snaps[id] = snap
	t.ids = append(t.ids, id)
	t.head = id
	return id, nil
}

// Append validates and appends rows as one new atomic snapshot.
func (t *Table) Append(ctx context.Context, rows []Row) (int64, error) {
	unlock, err := t.lockForCommit(ctx)
	if err != nil {
		return 0, err
	}
	defer unlock()

	snap := t.snaps[t.head]
	sch := t.schemas[snap.SchemaVersion]

	if len(rows) == 0 {
		return t.head, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	recs := make([]record, 0, len(rows))
	for i, r := range rows {
		rec, err := validateRow(sch, r)
		if err != nil {
			return 0, fmt.Errorf("append row %d: %w", i, err)
		}
		recs = append(recs, rec)
	}
	if err := checkDuplicatePK(sch, recs); err != nil {
		return 0, err
	}
	payload := &dataFilePayload{SchemaVersion: sch.Version, Records: recs}
	df, err := t.st.stageData(t.head+1, 1, payload)
	if err != nil {
		return 0, err
	}
	id, err := t.commitLocked(OpAppend, []DataFile{df}, nil, sch.Version,
		map[string]string{"rows": fmt.Sprintf("%d", len(rows)), "files_added": "1"})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// Evolve applies schema changes atomically and commits a schema snapshot.
// Returns (new schema version, snapshot id).
func (t *Table) Evolve(ctx context.Context, changes []SchemaChange) (int, int64, error) {
	unlock, err := t.lockForCommit(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer unlock()

	snap := t.snaps[t.head]
	old := t.schemas[snap.SchemaVersion]
	next, err := applyChanges(old, old.Version+1, changes)
	if err != nil {
		return 0, 0, err
	}
	if err := t.st.writeSchemaVersion(next); err != nil {
		return 0, 0, err
	}
	t.schemas[next.Version] = next
	id, err := t.commitLocked(OpSchema, nil, nil, next.Version,
		map[string]string{"schema_version": fmt.Sprintf("%d", next.Version),
			"changes": fmt.Sprintf("%d", len(changes))})
	if err != nil {
		return 0, 0, err
	}
	return next.Version, id, nil
}

// Read returns the live rows of HEAD with snapshot-isolation semantics.
func (t *Table) Read(ctx context.Context) (*ResultSet, error) {
	if err := t.checkOpen(); err != nil {
		return nil, err
	}
	t.mu.Lock()
	head := t.head
	t.mu.Unlock()
	return t.readAtID(ctx, head)
}

// ReadAt returns rows as of snapshot id (time travel).
func (t *Table) ReadAt(ctx context.Context, snapshotID int64) (*ResultSet, error) {
	if err := t.checkOpen(); err != nil {
		return nil, err
	}
	return t.readAtID(ctx, snapshotID)
}
