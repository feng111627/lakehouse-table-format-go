// Package lakehouse 实现一套基于本地文件系统的数据湖表格式：
// 快照式提交、结构演进、小文件合并与按主键增量更新。
package lakehouse

import "fmt"

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
	OpCreate  Operation = "create"
	OpAppend  Operation = "append"
	OpUpsert  Operation = "upsert"
	OpCompact Operation = "compact"
	OpSchema  Operation = "schema"
)

// Snapshot 是一份不可变的表快照元数据。
type Snapshot struct {
	Version   int       `json:"version"`
	Schema    Schema    `json:"schema"`
	DataFiles []string  `json:"dataFiles"`
	Operation Operation `json:"operation"`
	Timestamp int64     `json:"timestamp"`
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
