package lakehouse

import "time"

// Operation labels what a snapshot represents.
type Operation string

const (
	OpAppend    Operation = "append"    // new rows appended
	OpOverwrite Operation = "overwrite" // table replaced (reserved)
	OpUpdate    Operation = "update"    // primary-key upsert produced new files
	OpCompact   Operation = "compact"   // small-file compaction
	OpSchema    Operation = "schema"    // schema evolution only, data unchanged
)

// DataFile is one physical file carrying records written at SnapshotID.
type DataFile struct {
	Path        string `json:"path"`
	RecordCount int    `json:"record_count"`
	// SchemaVersion is the schema version used when the file was written;
	// historical reads coerce it to the requested snapshot schema.
	SchemaVersion int `json:"schema_version"`
}

// Snapshot is immutable metadata describing one committed table version.
type Snapshot struct {
	ID            int64     `json:"id"`
	ParentID      int64     `json:"parent_id"` // 0 for the initial snapshot
	SchemaVersion int       `json:"schema_version"`
	Operation     Operation `json:"operation"`
	TimestampMs   int64     `json:"timestamp_ms"`
	// AddedFiles / RemovedFiles are the delta against ParentID. Data files are
	// never physically deleted, so RemovedFiles only means logically obsolete;
	// this keeps historical snapshots readable for ever.
	AddedFiles   []DataFile `json:"added_files"`
	RemovedFiles []DataFile `json:"removed_files"`
	// Summary carries explainable human-readable facts about the commit.
	Summary map[string]string `json:"summary,omitempty"`
}

// record is the on-disk wire format of one row, keyed by stable column id.
type record map[int]any

// dataFilePayload is the JSON envelope of one data file.
type dataFilePayload struct {
	SchemaVersion int      `json:"schema_version"`
	Records       []record `json:"records"`
}

func nowMillis() int64 { return time.Now().UnixMilli() }
