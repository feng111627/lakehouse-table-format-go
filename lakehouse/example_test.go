package lakehouse_test

import (
	"fmt"
	"os"

	"github.com/feng111627/lakehouse-table-format-go/lakehouse"
)

// Example 与 README 中的最小示例保持一致，确保文档代码可运行。
func Example() {
	dir, _ := os.MkdirTemp("", "lakehouse-example")
	defer os.RemoveAll(dir)

	tbl, _ := lakehouse.Create(dir, lakehouse.Schema{
		Fields: []lakehouse.Field{
			{Name: "id", Type: lakehouse.TypeInt, Required: true},
			{Name: "name", Type: lakehouse.TypeString},
		},
		PrimaryKey: []string{"id"},
	})
	v1, _ := tbl.Append([]lakehouse.Row{{"id": 1, "name": "a"}})
	tbl.Upsert([]lakehouse.Row{{"id": 1, "name": "a2"}})

	hist, _ := tbl.Read(v1)
	latest, _ := tbl.Read(0)
	fmt.Println("v1:", hist[0]["name"])
	fmt.Println("latest:", latest[0]["name"])
	// Output:
	// v1: a
	// latest: a2
}
