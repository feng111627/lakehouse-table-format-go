package lakehouse

import (
	"errors"
	"testing"
)

// TestSchemaEvolution 验证：兼容变更被接受，历史快照按各自结构读取，语义确定。
func TestSchemaEvolution(t *testing.T) {
	t.Log("业务场景: 结构演进 - 加列/删列/类型加宽，历史快照读取语义确定")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	v1, _ := tbl.Append([]Row{{"id": 1, "name": "a", "score": 1.5}})

	// 加列（可选）：允许
	schema2 := testSchema()
	schema2.Fields = append(schema2.Fields, Field{Name: "city", Type: TypeString})
	v2, err := tbl.EvolveSchema(schema2)
	if err != nil {
		t.Fatalf("add optional column should be allowed: %v", err)
	}
	t.Logf("用例[加列]: 新增可选列 city 提交为 v%d", v2)

	// 新结构下写入带新列的行
	v3, _ := tbl.Append([]Row{{"id": 2, "name": "b", "score": 2.5, "city": "hz"}})

	// 历史快照 v1 仍按旧结构读，不出现错列/丢字段
	rows1, _ := tbl.Read(v1)
	if len(rows1) != 1 || rows1[0]["name"] != "a" {
		t.Fatalf("historical snapshot v%d read broken: %v", v1, rows1)
	}
	if _, hasCity := rows1[0]["city"]; hasCity {
		t.Fatalf("v%d was written before city existed, must not gain it: %v", v1, rows1[0])
	}
	t.Logf("用例[历史可读]: v%d 读取结果 %v，无错列无新增字段", v1, rows1)

	// 最新快照：旧行新列为空，新行新列有值
	rows3, _ := tbl.Read(v3)
	if len(rows3) != 2 {
		t.Fatalf("latest should have 2 rows, got %d", len(rows3))
	}
	if rows3[1]["city"] != "hz" {
		t.Fatalf("new row should carry city: %v", rows3[1])
	}
	t.Logf("用例[新旧共存]: 最新快照 %v", rows3)

	// 删列（非主键）：允许；历史快照仍能读出该列
	schema3 := Schema{
		Fields:     []Field{{Name: "id", Type: TypeInt, Required: true}, {Name: "name", Type: TypeString}},
		PrimaryKey: []string{"id"},
	}
	if _, err := tbl.EvolveSchema(schema3); err != nil {
		t.Fatalf("drop non-pk column should be allowed: %v", err)
	}
	rows1Again, _ := tbl.Read(v1)
	if rows1Again[0]["score"] != 1.5 {
		t.Fatalf("historical snapshot must keep dropped column readable: %v", rows1Again[0])
	}
	t.Logf("用例[删列]: 删除 score/city 后，v%d 历史数据 score=%v 仍可读", v1, rows1Again[0]["score"])

	// 类型加宽 int -> float：允许
	schema4 := Schema{
		Fields:     []Field{{Name: "id", Type: TypeInt, Required: true}, {Name: "name", Type: TypeString}, {Name: "ratio", Type: TypeInt}},
		PrimaryKey: []string{"id"},
	}
	if _, err := tbl.EvolveSchema(schema4); err != nil {
		t.Fatalf("re-add column should be allowed: %v", err)
	}
	schema5 := Schema{
		Fields:     []Field{{Name: "id", Type: TypeInt, Required: true}, {Name: "name", Type: TypeString}, {Name: "ratio", Type: TypeFloat}},
		PrimaryKey: []string{"id"},
	}
	if _, err := tbl.EvolveSchema(schema5); err != nil {
		t.Fatalf("int->float widening should be allowed: %v", err)
	}
	t.Log("用例[类型加宽]: ratio int -> float 被接受")
}

// TestIncompatibleSchemaRejected 验证：不兼容变更被明确拒绝，原因可区分。
func TestIncompatibleSchemaRejected(t *testing.T) {
	t.Log("业务场景: 结构演进 - 不兼容变更拒绝且原因可区分")
	dir := t.TempDir()
	tbl, _ := Create(dir, testSchema())
	before, _ := tbl.Current()

	cases := []struct {
		name   string
		next   Schema
		reason IncompatReason
	}{
		{
			name: "类型收窄 float->int",
			next: Schema{Fields: []Field{
				{Name: "id", Type: TypeInt, Required: true},
				{Name: "name", Type: TypeString},
				{Name: "score", Type: TypeInt},
			}, PrimaryKey: []string{"id"}},
			reason: ReasonTypeNarrowing,
		},
		{
			name: "跨类变更 string->int",
			next: Schema{Fields: []Field{
				{Name: "id", Type: TypeInt, Required: true},
				{Name: "name", Type: TypeInt},
				{Name: "score", Type: TypeFloat},
			}, PrimaryKey: []string{"id"}},
			reason: ReasonTypeMismatch,
		},
		{
			name: "删除主键列",
			next: Schema{Fields: []Field{
				{Name: "name", Type: TypeString},
				{Name: "score", Type: TypeFloat},
			}},
			reason: ReasonDropPrimaryKey,
		},
		{
			name: "新增必填列",
			next: Schema{Fields: []Field{
				{Name: "id", Type: TypeInt, Required: true},
				{Name: "name", Type: TypeString},
				{Name: "score", Type: TypeFloat},
				{Name: "tag", Type: TypeString, Required: true},
			}, PrimaryKey: []string{"id"}},
			reason: ReasonRequiredNewCol,
		},
	}
	for _, c := range cases {
		_, err := tbl.EvolveSchema(c.next)
		var ie ErrIncompatibleSchema
		if !errors.As(err, &ie) {
			t.Fatalf("用例[%s]: expected ErrIncompatibleSchema, got %v", c.name, err)
		}
		if ie.Reason != c.reason {
			t.Fatalf("用例[%s]: expected reason %s, got %s", c.name, c.reason, ie.Reason)
		}
		t.Logf("用例[%s]: 拒绝原因 %s", c.name, ie.Reason)
	}

	after, _ := tbl.Current()
	if after.Version != before.Version {
		t.Fatalf("rejected changes must not advance version: %d -> %d", before.Version, after.Version)
	}
	t.Logf("用例[拒绝无副作用]: 版本保持 v%d", after.Version)
}
