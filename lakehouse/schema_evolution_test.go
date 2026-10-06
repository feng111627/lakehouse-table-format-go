package lakehouse

import (
	"context"
	"errors"
	"testing"
)

func TestEvolveAddColumn(t *testing.T) {
	logCase(t, "B 表结构演进", "新增可空列：新快照有值，历史快照读取确定（缺列为 null）")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	s1, _ := tbl.Append(ctx, []Row{{"id": 1, "name": "a", "age": 20}})

	ver, s2, err := tbl.Evolve(ctx, []SchemaChange{
		AddColumn(Field{ID: 4, Name: "city", Type: TypeString, Nullable: true}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if ver != 2 || s2 != s1+1 {
		t.Fatalf("evolve ids ver=%d snap=%d", ver, s2)
	}
	// Historical read: the new column does not exist and old values stay.
	old, err := tbl.ReadAt(ctx, s1)
	if err != nil {
		t.Fatal(err)
	}
	if _, hasCity := old.Schema.FieldByName("city"); hasCity {
		t.Fatal("historical schema should not contain new column")
	}
	if old.Rows[0]["age"] != int64(20) {
		t.Fatalf("historical age = %v", old.Rows[0]["age"])
	}
	// New read: old rows project city as null.
	head, _ := tbl.Read(ctx)
	if _, ok := head.Schema.FieldByName("city"); !ok {
		t.Fatal("new schema missing city")
	}
	if head.Rows[0]["city"] != nil {
		t.Fatalf("historical row city = %v, want null", head.Rows[0]["city"])
	}
	// A non-null add-column must be rejected (old rows cannot backfill).
	_, _, err = tbl.Evolve(ctx, []SchemaChange{
		AddColumn(Field{ID: 5, Name: "must", Type: TypeString, Nullable: false}),
	})
	if !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("non-null add should be rejected, got %v", err)
	}
	t.Logf("非空新增列被拒绝: %v", err)
}

func TestEvolveDropColumn(t *testing.T) {
	logCase(t, "B 表结构演进", "删列：新快照不再暴露该列，历史快照仍可读出")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	s1, _ := tbl.Append(ctx, []Row{{"id": 1, "name": "a", "age": 20}})
	_, s2, err := tbl.Evolve(ctx, []SchemaChange{DropColumn("age")})
	if err != nil {
		t.Fatal(err)
	}
	head, _ := tbl.Read(ctx)
	if _, ok := head.Schema.FieldByName("age"); ok {
		t.Fatal("dropped column still visible")
	}
	if len(head.Rows[0]) != 2 { // id + name only
		t.Fatalf("row after drop = %v", head.Rows[0])
	}
	// Writes must not name the dropped column anymore.
	if _, err := tbl.Append(ctx, []Row{{"id": 2, "name": "b", "age": 1}}); err == nil {
		t.Fatal("write naming dropped column should fail")
	}
	// History still exposes age.
	old, err := tbl.ReadAt(ctx, s1)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := old.Schema.FieldByName("age")
	if !ok || old.Rows[0][f.Name] != int64(20) {
		t.Fatalf("history after drop broken: %+v", old.Rows)
	}
	_ = s2
}

func TestEvolveRenameKeepsColumnID(t *testing.T) {
	logCase(t, "B 表结构演进", "列改名：按稳定列 id 保持数据连续")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	s1, _ := tbl.Append(ctx, []Row{{"id": 1, "name": "a", "age": 20}})
	_, _, err := tbl.Evolve(ctx, []SchemaChange{RenameColumn("name", "label")})
	if err != nil {
		t.Fatal(err)
	}
	head, _ := tbl.Read(ctx)
	f, ok := head.Schema.FieldByName("label")
	if !ok || f.ID != 2 {
		t.Fatalf("rename lost stable id: %+v", f)
	}
	if _, ok := head.Schema.FieldByName("name"); ok {
		t.Fatal("old name still present")
	}
	if head.Rows[0]["label"] != "a" {
		t.Fatalf("data after rename = %v", head.Rows[0])
	}
	// Historical snapshot keeps the old name.
	old, _ := tbl.ReadAt(ctx, s1)
	if old.Rows[0]["name"] != "a" {
		t.Fatalf("history after rename broken: %v", old.Rows[0])
	}
}

func TestEvolveWideningTypes(t *testing.T) {
	logCase(t, "B 表结构演进", "类型拓宽 int->long/int->double 成功，历史读取按新类型确定投影")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	s1, _ := tbl.Append(ctx, []Row{{"id": 1, "name": "a", "age": 20}})
	_, _, err := tbl.Evolve(ctx, []SchemaChange{AlterColumnType("age", TypeDouble)})
	if err != nil {
		t.Fatal(err)
	}
	head, _ := tbl.Read(ctx)
	if head.Rows[0]["age"] != float64(20) {
		t.Fatalf("widened read = %v (%T), want float64(20)", head.Rows[0]["age"], head.Rows[0]["age"])
	}
	// Historical snapshot reads age through its own int schema.
	old, _ := tbl.ReadAt(ctx, s1)
	if old.Rows[0]["age"] != int64(20) {
		t.Fatalf("history type = %T", old.Rows[0]["age"])
	}
	// New rows may write fractional ages.
	if _, err := tbl.Append(ctx, []Row{{"id": 2, "name": "b", "age": 21.5}}); err != nil {
		t.Fatal(err)
	}
	// No-op widening rejected with explicit reason.
	_, _, err = tbl.Evolve(ctx, []SchemaChange{AlterColumnType("age", TypeDouble)})
	if !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("same-type alter should be rejected, got %v", err)
	}
}

func TestEvolveIncompatibleRejected(t *testing.T) {
	logCase(t, "B 表结构演进", "不兼容类型变更被拒绝并给出可区分原因")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	cases := []struct {
		name   string
		change SchemaChange
		want   error
	}{
		{"string->int", AlterColumnType("name", TypeInt), ErrTypeMismatch},
		{"double->int", AlterColumnType("age", TypeString), ErrTypeMismatch},
		{"drop absent", DropColumn("ghost"), ErrColumnNotFound},
		{"alter absent", AlterColumnType("ghost", TypeLong), ErrColumnNotFound},
		{"rename clash", RenameColumn("name", "age"), ErrColumnExists},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := tbl.Evolve(ctx, []SchemaChange{tc.change})
			if !errors.Is(err, tc.want) {
				t.Fatalf("%s: want %v, got %v", tc.name, tc.want, err)
			}
			t.Logf("%s 拒绝原因: %v", tc.name, err)
		})
	}
}

func TestEvolvePrimaryKeyProtected(t *testing.T) {
	logCase(t, "B 表结构演进", "主键列禁删/禁改类型，新增主键列被拒绝")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	for _, ch := range []SchemaChange{
		DropColumn("id"),
		AlterColumnType("id", TypeDouble),
		AddColumn(Field{ID: 9, Name: "k2", Type: TypeLong, Nullable: false, IsPK: true}),
	} {
		_, _, err := tbl.Evolve(ctx, []SchemaChange{ch})
		if !errors.Is(err, ErrCannotChangePK) {
			t.Fatalf("change %+v: want ErrCannotChangePK, got %v", ch, err)
		}
		t.Logf("主键保护拒绝: %v", err)
	}
}

func TestEvolveAtomicityAllOrNothing(t *testing.T) {
	logCase(t, "B 表结构演进", "一批变更中有非法项时整批回滚，schema 版本不变")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	verBefore := tbl.CurrentSchema().Version
	_, _, err := tbl.Evolve(ctx, []SchemaChange{
		AddColumn(Field{ID: 4, Name: "city", Type: TypeString, Nullable: true}),
		AlterColumnType("name", TypeInt), // incompatible
	})
	if !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("batch should be rejected, got %v", err)
	}
	if tbl.CurrentSchema().Version != verBefore {
		t.Fatalf("schema version changed after rejected batch: %d", tbl.CurrentSchema().Version)
	}
	if _, ok := tbl.CurrentSchema().FieldByName("city"); ok {
		t.Fatal("part of rejected batch became visible")
	}
}

func TestDropThenAddSameNameNoAliasing(t *testing.T) {
	logCase(t, "B 表结构演进", "同名删后再加列不复用旧 id，历史数据不错列")
	tbl, _ := openTestTable(t, testSchema())
	ctx := context.Background()
	s1, _ := tbl.Append(ctx, []Row{{"id": 1, "name": "a", "age": 20}})
	_, _, err := tbl.Evolve(ctx, []SchemaChange{DropColumn("age")})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = tbl.Evolve(ctx, []SchemaChange{
		AddColumn(Field{Name: "age", Type: TypeString, Nullable: true}), // auto id 4
	})
	if err != nil {
		t.Fatal(err)
	}
	head, _ := tbl.Read(ctx)
	f, _ := head.Schema.FieldByName("age")
	if f.ID != 4 || f.Type != TypeString {
		t.Fatalf("new age column = %+v, want id 4 string", f)
	}
	// The old integer age must never leak into the new string column.
	if head.Rows[0]["age"] != nil {
		t.Fatalf("new column picked up old id-keyed data: %v", head.Rows[0]["age"])
	}
	old, _ := tbl.ReadAt(ctx, s1)
	if old.Rows[0]["age"] != int64(20) {
		t.Fatalf("historical age lost: %v", old.Rows[0]["age"])
	}
}
