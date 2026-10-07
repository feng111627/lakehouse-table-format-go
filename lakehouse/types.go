// Package lakehouse 实现一套基于本地文件系统的数据湖表格式：
// 快照式提交、结构演进、小文件合并、按主键增量更新、
// 跨进程并发、命名标记与历史回滚、以及保留窗口式空间回收。
package lakehouse

import (
	"fmt"
	"time"
)

// DataType 是列的物理类型。
type DataType string

const (
	TypeString DataType = "string"
	TypeInt    DataType = "int"
	TypeFloat  DataType = "float"
	TypeBool   DataType = "bool"
)

// Field 描述表中的一列。
type Field struct {
	Name     string   `json:"name"`
	Type     DataType `json:"type"`
	Required bool     `json:"required,omitempty"`
}

// Schema 描述某个快照对应的表结构。
type Schema struct {
	Fields     []Field  `json:"fields"`
	PrimaryKey []string `json:"primaryKey,omitempty"`
}

// Field 按名字查找列，未找到返回 false。
func (s Schema) Field(name string) (Field, bool) {
	for _, f := range s.Fields {
		if f.Name == name {
			return f, true
		}
	}
	return Field{}, false
}

// Row 是一行数据，按列名取值。
type Row map[string]any

// Operation 是产生某个快照的操作类型。
type Operation string

const (
	OpCreate   Operation = "create"
	OpAppend   Operation = "append"
	OpUpsert   Operation = "upsert"
	OpCompact  Operation = "compact"
	OpSchema   Operation = "schema"
	OpRollback Operation = "rollback"
)

// Snapshot 是一份不可变的表快照元数据。
type Snapshot struct {
	Version   int       `json:"version"`
	Schema    Schema    `json:"schema"`
	DataFiles []string  `json:"dataFiles"`
	Operation Operation `json:"operation"`
	Timestamp int64     `json:"timestamp"`
	// RollbackFromVersion 仅在 Operation=rollback 时非零，
	// 记录这次回滚恢复到的目标历史版本，便于审计。
	RollbackFromVersion int `json:"rollbackFromVersion,omitempty"`
}

// ---- 可区分的错误类型 ----

// ErrConcurrentModification 表示提交期间基版本被其他提交者推进。
type ErrConcurrentModification struct {
	Base, Current int
}

func (e ErrConcurrentModification) Error() string {
	return fmt.Sprintf("concurrent modification: base version %d, current version %d", e.Base, e.Current)
}

// IncompatReason 区分不兼容结构变更的具体原因。
type IncompatReason string

const (
	ReasonTypeNarrowing   IncompatReason = "type-narrowing"
	ReasonTypeMismatch    IncompatReason = "type-mismatch"
	ReasonDropPrimaryKey  IncompatReason = "drop-primary-key"
	ReasonRequiredNewCol  IncompatReason = "required-new-column"
	ReasonRenameAmbiguous IncompatReason = "rename-ambiguous"
)

// ErrIncompatibleSchema 表示一次被拒绝的结构变更，Reason 可区分原因。
type ErrIncompatibleSchema struct {
	Reason IncompatReason
	Detail string
}

func (e ErrIncompatibleSchema) Error() string {
	return fmt.Sprintf("incompatible schema change (%s): %s", e.Reason, e.Detail)
}

// ErrDuplicatePrimaryKey 表示单批数据中出现重复主键，拒绝静默取舍。
type ErrDuplicatePrimaryKey struct {
	Key string
}

func (e ErrDuplicatePrimaryKey) Error() string {
	return fmt.Sprintf("duplicate primary key in batch: %s", e.Key)
}

// ErrSnapshotNotFound 表示请求的历史快照不存在。
type ErrSnapshotNotFound struct {
	Version int
}

func (e ErrSnapshotNotFound) Error() string {
	return fmt.Sprintf("snapshot not found: version %d", e.Version)
}

// ErrSnapshotExpired 表示请求的快照曾经存在，但已经被 Vacuum 回收。
// 与 ErrSnapshotNotFound 区分：调用方可以据此判断“曾经存在、已被清理”。
type ErrSnapshotExpired struct {
	Version int
}

func (e ErrSnapshotExpired) Error() string {
	return fmt.Sprintf("snapshot expired (vacuumed): version %d", e.Version)
}

// ErrTagNotFound 表示指定名字的标记不存在。
type ErrTagNotFound struct {
	Name string
}

func (e ErrTagNotFound) Error() string {
	return fmt.Sprintf("tag not found: %q", e.Name)
}

// ErrInvalidTag 表示标记名非法。
type ErrInvalidTag struct {
	Name   string
	Detail string
}

func (e ErrInvalidTag) Error() string {
	return fmt.Sprintf("invalid tag name %q: %s", e.Name, e.Detail)
}

// ErrInvalidRollback 表示回滚参数非法（如版本号非正数）。
type ErrInvalidRollback struct {
	Version int
	Detail  string
}

func (e *ErrInvalidRollback) Error() string {
	return fmt.Sprintf("invalid rollback to version %d: %s", e.Version, e.Detail)
}

// HistoryEntry 是历史列表中的一条记录。
type HistoryEntry struct {
	Version   int       `json:"version"`
	Operation Operation `json:"operation"`
	Timestamp int64     `json:"timestamp"`
	Tags      []string  `json:"tags,omitempty"` // 指向该版本的标记名（排序）
	Expired   bool      `json:"expired,omitempty"`
}

// RetentionPolicy 描述 Vacuum 的保留窗口，两个条件取并集：
// 同时满足“超出最近 MaxVersions 个快照”且“早于 OlderThan”才会过期。
// 零值字段表示该维度不限制。
type RetentionPolicy struct {
	MaxVersions int           // 保留最近多少个快照（0 = 不按数量限制）
	OlderThan   time.Duration // 只清理提交时间早于 now-OlderThan 的快照（0 = 不按时间限制）
	// MinFileAge 保护正在进行中的读写：只回收落盘超过该时长的文件。
	// 0 时使用默认值 1 小时，避免删掉刚由并发提交写入、尚未进入任何快照的文件。
	MinFileAge time.Duration
}

// VacuumReport 汇报一次 Vacuum 的清理结果。
type VacuumReport struct {
	RetainedVersions []int // 窗口内、或仍被标记/当前版本引用而保留的版本
	ExpiredVersions  []int // 本次判定为过期（已写墓碑）的版本
	DeletedDataFiles []string
	DeletedSnapshots []string // 删除的元数据文件相对路径
	DeletedTempFiles []string
	// DanglingTags 指向“既不在磁盘、也不在墓碑”版本的标记名。
	// 正常通过本库打标/回收不会产生悬空标记；出现通常意味着目录被外部破坏。
	DanglingTags []string
}
