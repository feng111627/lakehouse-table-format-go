// Package lakehouse 实现一套基于本地文件系统的数据湖表格式：
// 快照式提交、结构演进、小文件合并与按主键增量更新。
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
	// RollbackOf 非零时表示该快照是一次回滚提交，内容复制自该版本。
	RollbackOf int `json:"rollbackOf,omitempty"`
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

// ---- 标签与历史 ----

// ErrTagNotFound 表示按名字解析标签时，该名字当前没有指向任何版本。
type ErrTagNotFound struct {
	Name string
}

func (e ErrTagNotFound) Error() string {
	return fmt.Sprintf("tag not found: %q", e.Name)
}

// ErrInvalidTagName 表示标签名不合法（空串、含路径分隔符或点开头等）。
type ErrInvalidTagName struct {
	Name string
}

func (e ErrInvalidTagName) Error() string {
	return fmt.Sprintf("invalid tag name: %q", e.Name)
}

// ErrSnapshotExpired 表示快照曾经存在，但已被回收策略清理，无法再读取或回滚。
// 与 ErrSnapshotNotFound（该版本号从未存在）区分开。
type ErrSnapshotExpired struct {
	Version int
}

func (e ErrSnapshotExpired) Error() string {
	return fmt.Sprintf("snapshot expired: version %d", e.Version)
}

// HistoryEntry 描述历史列表中的一个版本。
type HistoryEntry struct {
	Version   int       `json:"version"`
	Operation Operation `json:"operation"`
	Timestamp int64     `json:"timestamp"`
	// Tags 是当时指向该版本的全部标签名（已排序）。
	Tags []string `json:"tags,omitempty"`
	// Expired 表示该版本已被回收：元数据与数据文件均已删除。
	Expired bool `json:"expired,omitempty"`
}

// ---- 回收 ----

// RetentionPolicy 描述快照保留窗口，两个条件取并集（满足任一即保留）：
//   - MinSnapshots>0：至少保留最近的 N 个快照；
//   - OlderThan>0：保留时间戳晚于 now-OlderThan 的快照。
//
// 此外当前快照、被标签指向的快照始终保留，与窗口无关。
// 零值策略表示除“强制保留”外不额外保留任何历史。
type RetentionPolicy struct {
	MinSnapshots int
	OlderThan     time.Duration
}

// ExpireResult 汇报一次回收实际清理的内容。
type ExpireResult struct {
	ExpiredVersions []int    `json:"expiredVersions"`
	DeletedFiles    []string `json:"deletedFiles"`
	KeptVersions    []int    `json:"keptVersions"`
}
