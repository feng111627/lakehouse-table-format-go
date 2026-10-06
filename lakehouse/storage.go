package lakehouse

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Directory layout of a table rooted at <dir>:
//
//	<dir>/LOCK                 exclusive commit lock (flock)
//	<dir>/meta/HEAD            current snapshot id (atomic replace)
//	<dir>/meta/schema_v<N>.json schema versions (immutable)
//	<dir>/meta/snap-<id>.json  snapshot metadata (immutable)
//	<dir>/data/<id>-<n>.json   data files (immutable once referenced)
//	<dir>/txn/*.tmp            staging area; leftovers cleaned on open
type storage struct {
	dir string
}

func newStorage(dir string) *storage { return &storage{dir: dir} }

func (st *storage) initLayout() error {
	for _, d := range []string{st.metaDir(), st.dataDir(), st.txnDir()} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("lakehouse: create layout %s: %w", d, err)
		}
	}
	return nil
}

func (st *storage) metaDir() string  { return filepath.Join(st.dir, "meta") }
func (st *storage) dataDir() string  { return filepath.Join(st.dir, "data") }
func (st *storage) txnDir() string   { return filepath.Join(st.dir, "txn") }
func (st *storage) lockPath() string { return filepath.Join(st.dir, "LOCK") }
func (st *storage) headPath() string { return filepath.Join(st.metaDir(), "HEAD") }

func (st *storage) exists(rel string) bool {
	_, err := os.Stat(filepath.Join(st.dir, rel))
	return err == nil
}

func tmpName() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf(".tmp-%d-%s", os.Getpid(), hex.EncodeToString(b[:]))
}

// syncDir fsyncs a directory so that a preceding rename is durable.
func syncDir(dir string) {
	if f, err := os.Open(dir); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
}

// writeJSONAtomic marshals v, stages it in txn/, fsyncs, and renames into
// place. A crash at any point leaves only the old version or an unlinked
// temp file in txn/ (cleaned on next open); no torn target file is visible.
func (st *storage) writeJSONAtomic(rel string, v any) error {
	buf, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("lakehouse: marshal %s: %w", rel, err)
	}
	buf = append(buf, '\n')
	final := filepath.Join(st.dir, rel)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(st.txnDir(), tmpName())
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("lakehouse: stage %s: %w", rel, err)
	}
	if _, err := io.Copy(f, bytes.NewReader(buf)); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("lakehouse: rename %s: %w", rel, err)
	}
	syncDir(filepath.Dir(final))
	return nil
}

func (st *storage) readJSON(rel string, v any) error {
	f, err := os.Open(filepath.Join(st.dir, rel))
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: %s", os.ErrNotExist, rel)
		}
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("lakehouse: decode %s: %w", rel, err)
	}
	return nil
}

func (st *storage) snapshotPath(id int64) string {
	return filepath.Join("meta", fmt.Sprintf("snap-%d.json", id))
}

func (st *storage) schemaPath(version int) string {
	return filepath.Join("meta", fmt.Sprintf("schema_v%d.json", version))
}

func (st *storage) writeHEAD(id int64) error {
	return st.writeJSONAtomic("meta"+string(filepath.Separator)+"HEAD",
		map[string]int64{"snapshot_id": id})
}

func (st *storage) readHEAD() (int64, error) {
	var head struct {
		SnapshotID int64 `json:"snapshot_id"`
	}
	if err := st.readJSON(filepath.Join("meta", "HEAD"), &head); err != nil {
		return 0, err
	}
	return head.SnapshotID, nil
}

func (st *storage) writeSchemaVersion(sch *Schema) error {
	return st.writeJSONAtomic(st.schemaPath(sch.Version), sch)
}

func (st *storage) readSchemaVersion(version int) (*Schema, error) {
	sch := &Schema{}
	if err := st.readJSON(st.schemaPath(version), sch); err != nil {
		return nil, err
	}
	if err := sch.Compile(); err != nil {
		return nil, err
	}
	return sch, nil
}

func (st *storage) writeSnapshot(s *Snapshot) error {
	return st.writeJSONAtomic(st.snapshotPath(s.ID), s)
}

func (st *storage) readSnapshot(id int64) (*Snapshot, error) {
	s := &Snapshot{}
	if err := st.readJSON(st.snapshotPath(id), s); err != nil {
		return nil, err
	}
	return s, nil
}

func (st *storage) listSnapshotIDs() ([]int64, error) {
	entries, err := os.ReadDir(st.metaDir())
	if err != nil {
		return nil, err
	}
	var ids []int64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "snap-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(name, "snap-"), ".json"), 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// stageData serializes records into a new immutable data file via rename.
func (st *storage) stageData(snapshotID int64, seq int, payload *dataFilePayload) (DataFile, error) {
	rel := filepath.Join("data", fmt.Sprintf("%d-%d.json", snapshotID, seq))
	if err := st.writeJSONAtomic(rel, payload); err != nil {
		return DataFile{}, err
	}
	return DataFile{
		Path:          rel,
		RecordCount:   len(payload.Records),
		SchemaVersion: payload.SchemaVersion,
	}, nil
}

func (st *storage) readData(f DataFile) (*dataFilePayload, error) {
	p := &dataFilePayload{}
	fh, err := os.Open(filepath.Join(st.dir, f.Path))
	if err != nil {
		return nil, fmt.Errorf("lakehouse: open data file %s: %w", f.Path, err)
	}
	defer fh.Close()
	dec := json.NewDecoder(fh)
	dec.UseNumber()
	if err := dec.Decode(p); err != nil {
		return nil, fmt.Errorf("lakehouse: decode data file %s: %w", f.Path, err)
	}
	for i := range p.Records {
		for k, v := range p.Records[i] {
			if n, ok := v.(json.Number); ok {
				cv, err := normalizeNumber(n)
				if err != nil {
					return nil, fmt.Errorf("lakehouse: bad number in %s: %w", f.Path, err)
				}
				p.Records[i][k] = cv
			}
		}
	}
	return p, nil
}

// normalizeNumber keeps JSON integers as int64 (int/long columns) and parses
// non-integer literals as float64 (double columns).
func normalizeNumber(n json.Number) (any, error) {
	s := n.String()
	if strings.ContainsAny(s, ".eE") {
		f, err := n.Float64()
		if err != nil {
			return nil, err
		}
		return f, nil
	}
	iv, err := n.Int64()
	if err != nil {
		// Oversized integer literal: fall back to double.
		f, ferr := n.Float64()
		if ferr != nil {
			return nil, err
		}
		return f, nil
	}
	return iv, nil
}

// cleanupOrphans removes staged temp files left by crashed processes. It must
// run under the commit lock so a live process's temp files are never removed.
func (st *storage) cleanupOrphans() error {
	entries, err := os.ReadDir(st.txnDir())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		_ = os.Remove(filepath.Join(st.txnDir(), e.Name()))
	}
	return nil
}

// fileLock is an advisory exclusive lock backed by flock(2), so concurrent
// writers inside one process and across processes serialize commits.
type fileLock struct {
	f *os.File
}

func acquireFileLock(ctx context.Context, dir string) (*fileLock, error) {
	path := filepath.Join(dir, "LOCK")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("lakehouse: open lock: %w", err)
	}
	// Block while honoring ctx cancellation.
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return &fileLock{f: f}, nil
		}
		if !isEWouldBlock(err) {
			_ = f.Close()
			return nil, fmt.Errorf("lakehouse: flock: %w", err)
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func isEWouldBlock(err error) bool {
	return err == syscall.EWOULDBLOCK || err == syscall.EAGAIN
}

func (l *fileLock) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	return l.f.Close()
}
