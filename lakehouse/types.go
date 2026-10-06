package lakehouse

import (
	"fmt"
	"sort"
)

// DataType is the set of supported physical column types.
type DataType string

const (
	TypeBool   DataType = "bool"
	TypeInt    DataType = "int"    // 64-bit signed integer
	TypeLong   DataType = "long"   // 64-bit signed integer (wider evolution target)
	TypeDouble DataType = "double" // 64-bit IEEE 754 float
	TypeString DataType = "string"
)

// Field describes one table column. IDs are stable across renames and are the
// identity used inside data files, so drop-then-reuse-a-name never aliases
// historical data.
type Field struct {
	ID       int      `json:"id"`
	Name     string   `json:"name"`
	Type     DataType `json:"type"`
	Nullable bool     `json:"nullable"`
	// IsPK marks primary-key columns. PK structure is immutable after creation.
	IsPK bool `json:"is_pk"`
}

// Schema is an ordered, id-indexed collection of fields.
type Schema struct {
	Version int     `json:"version"`
	Fields  []Field `json:"fields"`
	// MaxColumnID is the high-water mark of every column id ever allocated,
	// including dropped ones, so dropped ids are never reused and a column
	// re-added under the same name cannot alias historical data.
	MaxColumnID int `json:"max_column_id,omitempty"`

	byID   map[int]*Field
	byName map[string]*Field
}

// Compile builds lookup indexes for the schema. A compiled schema is required
// before Compile/FieldByID/FieldByName lookups work; storage always compiles
// every schema it loads or writes.
func (s *Schema) Compile() error {
	if s == nil {
		return fmt.Errorf("lakehouse: nil schema")
	}
	s.byID = make(map[int]*Field, len(s.Fields))
	s.byName = make(map[string]*Field, len(s.Fields))
	names := map[string]bool{}
	ids := map[int]bool{}
	pkCount := 0
	for i := range s.Fields {
		f := &s.Fields[i]
		switch f.Type {
		case TypeBool, TypeInt, TypeLong, TypeDouble, TypeString:
		default:
			return schemaErr("compile", f.Name, ErrTypeMismatch,
				"unknown type "+string(f.Type))
		}
		if f.Name == "" {
			return schemaErr("compile", f.Name, ErrIncompatibleSchema,
				"column name is empty")
		}
		if f.ID <= 0 {
			return schemaErr("compile", f.Name, ErrIncompatibleSchema,
				fmt.Sprintf("column id must be positive, got %d", f.ID))
		}
		if ids[f.ID] {
			return schemaErr("compile", f.Name, ErrColumnExists,
				fmt.Sprintf("duplicate column id %d", f.ID))
		}
		if names[f.Name] {
			return schemaErr("compile", f.Name, ErrColumnExists,
				"duplicate column name")
		}
		ids[f.ID] = true
		names[f.Name] = true
		if f.ID > s.MaxColumnID {
			s.MaxColumnID = f.ID
		}
		if f.IsPK {
			pkCount++
			if f.Nullable {
				return schemaErr("compile", f.Name, ErrNullNotNull,
					"primary-key column must be non-nullable")
			}
		}
		s.byID[f.ID] = f
		s.byName[f.Name] = f
	}
	if pkCount == 0 {
		return ErrEmptyPrimaryKey
	}
	return nil
}

func (s *Schema) FieldByID(id int) (*Field, bool) {
	if s == nil || s.byID == nil {
		return nil, false
	}
	f, ok := s.byID[id]
	return f, ok
}

func (s *Schema) FieldByName(name string) (*Field, bool) {
	if s == nil || s.byName == nil {
		return nil, false
	}
	f, ok := s.byName[name]
	return f, ok
}

// ColumnIDs returns all column ids in declaration order.
func (s *Schema) ColumnIDs() []int {
	ids := make([]int, 0, len(s.Fields))
	for i := range s.Fields {
		ids = append(ids, s.Fields[i].ID)
	}
	return ids
}

// PrimaryKeyIDs returns PK column ids in declaration order.
func (s *Schema) PrimaryKeyIDs() []int {
	ids := make([]int, 0, len(s.Fields))
	for i := range s.Fields {
		if s.Fields[i].IsPK {
			ids = append(ids, s.Fields[i].ID)
		}
	}
	return ids
}

// WithVersion returns a compiled deep copy with the given version.
func (s *Schema) WithVersion(v int) *Schema {
	cp := &Schema{
		Version:     v,
		Fields:      append([]Field(nil), s.Fields...),
		MaxColumnID: s.MaxColumnID,
	}
	_ = cp.Compile()
	return cp
}

// Row is a user-facing record keyed by column name.
type Row map[string]any

// ResultSet is a stable, deterministic read result for one snapshot.
type ResultSet struct {
	SnapshotID int64
	Schema     *Schema
	Rows       []Row
}

func (r *ResultSet) Len() int { return len(r.Rows) }

// Less orders rows by primary key columns, giving stable output even for
// identical primary keys (which can only occur before an explicit upsert).
func (r *ResultSet) Less(i, j int) bool {
	a, b := r.Rows[i], r.Rows[j]
	pkIDs := r.Schema.PrimaryKeyIDs()
	for _, id := range pkIDs {
		f := r.Schema.FieldMustByID(id)
		if c := compareScalar(a[f.Name], b[f.Name]); c != 0 {
			return c < 0
		}
	}
	// Tie-breaker on remaining columns keeps ordering fully deterministic.
	for _, f := range r.Schema.Fields {
		if f.IsPK {
			continue
		}
		if c := compareScalar(a[f.Name], b[f.Name]); c != 0 {
			return c < 0
		}
	}
	return false
}

func (r *ResultSet) Swap(i, j int) { r.Rows[i], r.Rows[j] = r.Rows[j], r.Rows[i] }

// FieldMustByID panics-free lookup used in hot sort loops; callers pass ids
// coming from the same schema.
func (s *Schema) FieldMustByID(id int) Field {
	if f, ok := s.byID[id]; ok {
		return *f
	}
	return Field{ID: id}
}

func sortRows(rows []Row, sch *Schema) {
	rs := &ResultSet{Schema: sch, Rows: rows}
	sort.Sort(rs)
}

// compareScalar compares values of the same logical type. nil sorts first.
func compareScalar(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	switch x := a.(type) {
	case bool:
		y := b.(bool)
		if x == y {
			return 0
		}
		if !x { // false < true
			return -1
		}
		return 1
	case int64:
		y := b.(int64)
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		default:
			return 0
		}
	case float64:
		y := b.(float64)
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		default:
			return 0
		}
	case string:
		y := b.(string)
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		default:
			return 0
		}
	}
	return 0
}

// canWiden reports whether a column may evolve from -> to. Only the following
// non-breaking widenings are accepted; every other type change is rejected
// with ErrTypeMismatch so data can never be silently reinterpreted:
//
//	int    -> long   (same physical 64-bit integer domain)
//	int    -> double (widening, exact for values below 2^53)
//	long   -> double
func canWiden(from, to DataType) bool {
	if from == to {
		return true
	}
	switch from {
	case TypeInt:
		return to == TypeLong || to == TypeDouble
	case TypeLong:
		return to == TypeDouble
	}
	return false
}

// coerceValue converts a stored value into the requested (widened) type.
// It only handles conversions allowed by canWiden.
func coerceValue(v any, from, to DataType) (any, error) {
	if v == nil || from == to {
		return v, nil
	}
	switch from {
	case TypeInt, TypeLong:
		iv, ok := v.(int64)
		if !ok {
			return nil, schemaErr("read", "", ErrTypeMismatch,
				fmt.Sprintf("expected stored integer, got %s", typeName(v)))
		}
		switch to {
		case TypeLong:
			return iv, nil
		case TypeDouble:
			return float64(iv), nil
		}
	}
	return nil, schemaErr("read", "", ErrTypeMismatch,
		fmt.Sprintf("cannot coerce %s -> %s", from, to))
}

func typeName(v any) string { return fmt.Sprintf("%T", v) }
