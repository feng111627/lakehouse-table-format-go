package lakehouse

import (
	"fmt"
	"reflect"
)

// SchemaChange is one schema-evolution instruction.
type SchemaChange struct {
	// Kind is "add-column", "drop-column", "alter-type", "rename-column".
	Kind string
	// ColumnName targets an existing column (drop/alter/rename).
	ColumnName string
	// Field describes the new column (add).
	Field Field
	// NewType / RenameTo carry alter-type / rename payloads.
	NewType  DataType
	RenameTo string
}

// Evolution change kinds.
const (
	ChangeAddColumn    = "add-column"
	ChangeDropColumn   = "drop-column"
	ChangeAlterType    = "alter-type"
	ChangeRenameColumn = "rename-column"
)

// AddColumn adds a column (it must be nullable; historical rows read it as
// null). Column id is taken from Field.ID when > 0, otherwise auto-assigned.
func AddColumn(f Field) SchemaChange { return SchemaChange{Kind: ChangeAddColumn, Field: f} }

// DropColumn removes a non-PK column from the current schema. Historical
// snapshots remain readable because data files are keyed by stable column id.
func DropColumn(name string) SchemaChange {
	return SchemaChange{Kind: ChangeDropColumn, ColumnName: name}
}

// AlterColumnType widens (or exactly changes) a non-PK column type.
func AlterColumnType(name string, to DataType) SchemaChange {
	return SchemaChange{Kind: ChangeAlterType, ColumnName: name, NewType: to}
}

// RenameColumn renames a column; the stable column id keeps history intact.
func RenameColumn(name, to string) SchemaChange {
	return SchemaChange{Kind: ChangeRenameColumn, ColumnName: name, RenameTo: to}
}

// validateCreateSchema checks the initial table schema and compiles it.
func validateCreateSchema(sch *Schema) error {
	if sch == nil {
		return schemaErr("create", "", ErrIncompatibleSchema, "nil schema")
	}
	if len(sch.Fields) == 0 {
		return schemaErr("create", "", ErrIncompatibleSchema, "schema has no columns")
	}
	sch.Version = 1
	return sch.Compile()
}

// nextColumnID returns the first unused column id for add-column, skipping
// ids that belonged to since-dropped columns.
func nextColumnID(sch *Schema) int {
	return sch.MaxColumnID + 1
}

// applyChanges validates and applies all changes to a deep-copied schema,
// returning a new schema with version+1. All-or-nothing: any incompatible
// change rejects the whole evolution and the input schema is left untouched.
func applyChanges(sch *Schema, version int, changes []SchemaChange) (*Schema, error) {
	if len(changes) == 0 {
		return nil, schemaErr("evolve", "", ErrIncompatibleSchema, "no schema changes supplied")
	}
	out := &Schema{
		Version:     version,
		Fields:      append([]Field(nil), sch.Fields...),
		MaxColumnID: sch.MaxColumnID,
	}
	if err := out.Compile(); err != nil {
		return nil, err
	}

	for _, ch := range changes {
		switch ch.Kind {
		case ChangeAddColumn:
			f := ch.Field
			if f.Name == "" {
				return nil, schemaErr(ch.Kind, "", ErrIncompatibleSchema, "new column needs a name")
			}
			if _, ok := out.FieldByName(f.Name); ok {
				return nil, schemaErr(ch.Kind, f.Name, ErrColumnExists,
					"column name already in use")
			}
			if f.ID > 0 {
				if _, ok := out.FieldByID(f.ID); ok {
					return nil, schemaErr(ch.Kind, f.Name, ErrColumnExists,
						fmt.Sprintf("column id %d already in use", f.ID))
				}
				if f.ID <= out.MaxColumnID {
					return nil, schemaErr(ch.Kind, f.Name, ErrColumnExists,
						fmt.Sprintf("column id %d belonged to a dropped column and cannot be reused", f.ID))
				}
			} else {
				f.ID = nextColumnID(out)
			}
			if f.IsPK {
				return nil, schemaErr(ch.Kind, f.Name, ErrCannotChangePK,
					"adding primary-key columns after creation is not supported")
			}
			if !f.Nullable {
				return nil, schemaErr(ch.Kind, f.Name, ErrIncompatibleSchema,
					"new column must be nullable because existing rows would read it as null")
			}
			switch f.Type {
			case TypeBool, TypeInt, TypeLong, TypeDouble, TypeString:
			default:
				return nil, schemaErr(ch.Kind, f.Name, ErrTypeMismatch,
					"unknown type "+string(f.Type))
			}
			out.Fields = append(out.Fields, f)

		case ChangeDropColumn:
			f, ok := out.FieldByName(ch.ColumnName)
			if !ok {
				return nil, schemaErr(ch.Kind, ch.ColumnName, ErrColumnNotFound,
					"cannot drop an absent column")
			}
			if f.IsPK {
				return nil, schemaErr(ch.Kind, ch.ColumnName, ErrCannotChangePK,
					"primary-key columns cannot be dropped")
			}
			out.removeColumn(f.ID)

		case ChangeAlterType:
			f, ok := out.FieldByName(ch.ColumnName)
			if !ok {
				return nil, schemaErr(ch.Kind, ch.ColumnName, ErrColumnNotFound,
					"cannot alter an absent column")
			}
			switch ch.NewType {
			case TypeBool, TypeInt, TypeLong, TypeDouble, TypeString:
			default:
				return nil, schemaErr(ch.Kind, ch.ColumnName, ErrTypeMismatch,
					"unknown target type "+string(ch.NewType))
			}
			if f.IsPK {
				return nil, schemaErr(ch.Kind, ch.ColumnName, ErrCannotChangePK,
					"primary-key column type cannot change (join/upsert identity must stay stable)")
			}
			if f.Type == ch.NewType {
				return nil, schemaErr(ch.Kind, ch.ColumnName, ErrTypeMismatch,
					fmt.Sprintf("column is already %s", ch.NewType))
			}
			if !canWiden(f.Type, ch.NewType) {
				return nil, schemaErr(ch.Kind, ch.ColumnName, ErrTypeMismatch,
					fmt.Sprintf("incompatible type change %s -> %s: only int->long, int->double and long->double widenings are allowed",
						f.Type, ch.NewType))
			}
			f.Type = ch.NewType
			out.replaceField(f.ID, *f)

		case ChangeRenameColumn:
			f, ok := out.FieldByName(ch.ColumnName)
			if !ok {
				return nil, schemaErr(ch.Kind, ch.ColumnName, ErrColumnNotFound,
					"cannot rename an absent column")
			}
			if ch.RenameTo == "" {
				return nil, schemaErr(ch.Kind, ch.ColumnName, ErrIncompatibleSchema,
					"new name is empty")
			}
			if _, ok := out.FieldByName(ch.RenameTo); ok {
				return nil, schemaErr(ch.Kind, ch.RenameTo, ErrColumnExists,
					"target column name already in use")
			}
			f.Name = ch.RenameTo
			out.replaceField(f.ID, *f)

		default:
			return nil, schemaErr(ch.Kind, ch.ColumnName, ErrIncompatibleSchema,
				"unknown schema change kind")
		}

		// Rebuild indexes so later changes in the same batch see the effect.
		if err := out.Compile(); err != nil {
			return nil, err
		}
	}

	if err := out.Compile(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Schema) removeColumn(id int) {
	// Allocate a fresh backing array: Fields[:0] aliasing would mutate the
	// caller's previous schema when copies share capacity.
	kept := make([]Field, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.ID != id {
			kept = append(kept, f)
		}
	}
	s.Fields = kept
}

func (s *Schema) replaceField(id int, nf Field) {
	for i := range s.Fields {
		if s.Fields[i].ID == id {
			s.Fields[i] = nf
			return
		}
	}
}

// validateRow checks a user row against the schema and converts it to an
// id-keyed record. Rules:
//
//   - every declared non-nullable column must be present and non-nil;
//   - undeclared columns are rejected (no silent column misalignment);
//   - each value's Go type must exactly match the column physical type
//     (int64 for int/long, float64 for double, bool, string);
//   - missing nullable columns become null.
func validateRow(sch *Schema, row Row) (record, error) {
	rec := record{}
	for name, v := range row {
		f, ok := sch.FieldByName(name)
		if !ok {
			return nil, schemaErr("validate-row", name, ErrUnknownColumn,
				"row carries a column absent from the current schema")
		}
		if v == nil {
			if !f.Nullable {
				return nil, schemaErr("validate-row", name, ErrNullNotNull,
					"null value for non-nullable column")
			}
			rec[f.ID] = nil
			continue
		}
		cv, err := coerceInput(f, v)
		if err != nil {
			return nil, schemaErr("validate-row", name, ErrTypeMismatch, err.Error())
		}
		rec[f.ID] = cv
	}
	for i := range sch.Fields {
		f := &sch.Fields[i]
		if _, present := rec[f.ID]; !present {
			if !f.Nullable {
				return nil, schemaErr("validate-row", f.Name, ErrNullNotNull,
					"missing value for non-nullable column")
			}
			rec[f.ID] = nil
		}
	}
	return rec, nil
}

// coerceInput accepts only exact physical matches. Convenience: Go `int` is
// accepted for int/long columns; json.Number is accepted too.
func coerceInput(f *Field, v any) (any, error) {
	switch x := v.(type) {
	case bool:
		if f.Type == TypeBool {
			return x, nil
		}
	case string:
		if f.Type == TypeString {
			return x, nil
		}
	case int64:
		if f.Type == TypeInt || f.Type == TypeLong {
			return x, nil
		}
	case int:
		if f.Type == TypeInt || f.Type == TypeLong {
			return int64(x), nil
		}
	case int32:
		if f.Type == TypeInt || f.Type == TypeLong {
			return int64(x), nil
		}
	case float64:
		if f.Type == TypeDouble {
			return x, nil
		}
	case float32:
		if f.Type == TypeDouble {
			return float64(x), nil
		}
	}
	return nil, fmt.Errorf("column %s expects %s, got %s (%s)",
		f.Name, f.Type, typeName(v), reflect.ValueOf(v).Kind())
}
