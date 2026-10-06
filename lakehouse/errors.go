// Package lakehouse implements a small local data-lake table format: snapshot-based
// writes, schema evolution, compaction and primary-key upsert, all persisted on
// a local directory.
package lakehouse

import "errors"

// Sentinel errors. Callers can use errors.Is to distinguish failure reasons.
var (
	// ErrTableNotFound is returned when opening a table directory without metadata.
	ErrTableNotFound = errors.New("lakehouse: table not found")
	// ErrTableExists is returned when creating a table over an existing table.
	ErrTableExists = errors.New("lakehouse: table already exists")
	// ErrSnapshotNotFound is returned when reading a non-existent snapshot id.
	ErrSnapshotNotFound = errors.New("lakehouse: snapshot not found")
	// ErrTableClosed is returned when calling a method on a closed table handle.
	ErrTableClosed = errors.New("lakehouse: table is closed")

	// ErrIncompatibleSchema groups all schema-compatibility failures.
	ErrIncompatibleSchema = errors.New("lakehouse: incompatible schema change")
	// ErrColumnNotFound indicates a referenced column does not exist.
	ErrColumnNotFound = errors.New("lakehouse: column not found")
	// ErrColumnExists indicates an added column id/name already exists.
	ErrColumnExists = errors.New("lakehouse: column already exists")
	// ErrCannotChangePK indicates a change touching the primary-key columns
	// was rejected.
	ErrCannotChangePK = errors.New("lakehouse: primary-key columns cannot be dropped, added to, or type-changed")
	// ErrTypeMismatch indicates a value/type is not compatible.
	ErrTypeMismatch = errors.New("lakehouse: type mismatch")
	// ErrNullNotNull indicates a required column received a null value.
	ErrNullNotNull = errors.New("lakehouse: null value for non-nullable column")
	// ErrUnknownColumn indicates a row carrying an undeclared column.
	ErrUnknownColumn = errors.New("lakehouse: row contains unknown column")
	// ErrEmptyPrimaryKey indicates creating a table without primary key.
	ErrEmptyPrimaryKey = errors.New("lakehouse: table must declare a non-empty primary key")

	// ErrDuplicatePK indicates duplicate primary keys where none are allowed.
	ErrDuplicatePK = errors.New("lakehouse: duplicate primary key")
)

// SchemaError attaches context to a schema-compatibility failure.
// Its Unwrap target is always ErrIncompatibleSchema (or a more specific
// sentinel such as ErrTypeMismatch), so callers may distinguish reasons via
// errors.Is while still seeing what column/type caused the rejection.
type SchemaError struct {
	Op     string // "add-column", "drop-column", "alter-type", "validate-row", ...
	Column string
	Reason error
	Detail string
}

func (e *SchemaError) Error() string {
	msg := "lakehouse schema error [" + e.Op + "]"
	if e.Column != "" {
		msg += " column=" + e.Column
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	if e.Reason != nil {
		msg += " (" + e.Reason.Error() + ")"
	}
	return msg
}

func (e *SchemaError) Unwrap() error { return e.Reason }

func schemaErr(op, column string, reason error, detail string) *SchemaError {
	return &SchemaError{Op: op, Column: column, Reason: reason, Detail: detail}
}
