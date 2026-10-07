package lakehouse_test

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/feng111627/lakehouse-table-format-go/lakehouse"
)

// ExampleTag 演示：给提交命名、改指向、按名解析。
func Example_tagging() {
	dir, _ := os.MkdirTemp("", "lakehouse-tag")
	defer os.RemoveAll(dir)
	tbl, _ := lakehouse.Create(dir, lakehouse.Schema{
		Fields:     []lakehouse.Field{{Name: "id", Type: lakehouse.TypeInt, Required: true}},
		PrimaryKey: []string{"id"},
	})
	v1, _ := tbl.Append([]lakehouse.Row{{"id": 1}})
	tbl.Tag("batch-1", v1)

	// 同一个名字重新指向新版本：旧指向原子作废，永不一名两版本。
	v2, _ := tbl.Append([]lakehouse.Row{{"id": 2}})
	tbl.Tag("batch-1", v2)
	got, _ := tbl.ResolveTag("batch-1")
	fmt.Println("batch-1 ->", got)
	// Output:
	// batch-1 -> 3
}

// ExampleRollback 演示：回滚是一次新提交，中间历史保留。
func Example_rollback() {
	dir, _ := os.MkdirTemp("", "lakehouse-rb")
	defer os.RemoveAll(dir)
	tbl, _ := lakehouse.Create(dir, lakehouse.Schema{
		Fields:     []lakehouse.Field{{Name: "id", Type: lakehouse.TypeInt, Required: true}},
		PrimaryKey: []string{"id"},
	})
	vGood, _ := tbl.Append([]lakehouse.Row{{"id": 1}}) // v2
	tbl.Append([]lakehouse.Row{{"id": 2}})             // v3：错误数据

	rb, _ := tbl.Rollback(vGood) // 回到 v2，产生新提交 v4
	rows, _ := tbl.Read(0)
	cur, _ := tbl.Current()
	fmt.Println("rollback version:", rb, "op:", cur.Operation, "rows:", len(rows))
	// 中间的 v3 仍然在历史里、仍可读（未被抹掉）。
	old, _ := tbl.Read(3)
	fmt.Println("intermediate v3 rows:", len(old))
	// Output:
	// rollback version: 4 op: rollback rows: 1
	// intermediate v3 rows: 2
}

// ExampleVacuum 演示：按保留窗口回收，被回收版本与从未存在区分开。
func Example_vacuum() {
	dir, _ := os.MkdirTemp("", "lakehouse-vac")
	defer os.RemoveAll(dir)
	tbl, _ := lakehouse.Create(dir, lakehouse.Schema{
		Fields:     []lakehouse.Field{{Name: "id", Type: lakehouse.TypeInt, Required: true}},
		PrimaryKey: []string{"id"},
	})
	for i := 0; i < 5; i++ {
		tbl.Append([]lakehouse.Row{{"id": i + 1}})
	}
	// 只留最近 2 个快照；示例中关闭文件年龄闸门（MinFileAge=-1），生产用默认 1h。
	rep, err := tbl.Vacuum(lakehouse.RetentionPolicy{MaxVersions: 2, MinFileAge: -1})
	if err != nil {
		panic(err)
	}
	fmt.Println("retained:", rep.RetainedVersions)
	fmt.Println("expired:", rep.ExpiredVersions)

	_, err = tbl.Read(rep.ExpiredVersions[0])
	var expired lakehouse.ErrSnapshotExpired
	fmt.Println("read expired -> ErrSnapshotExpired:", errors.As(err, &expired))
	// Output:
	// retained: [5 6]
	// expired: [1 2 3 4]
	// read expired -> ErrSnapshotExpired: true
}

// ExampleMultiProcess 演示：多个后台任务各自打开同一目录并发写，
// 抢版本时收到可区分的冲突结果，由调用方决定是否重试。
func Example_multiProcess() {
	dir, _ := os.MkdirTemp("", "lakehouse-mp")
	defer os.RemoveAll(dir)
	tbl, _ := lakehouse.Create(dir, lakehouse.Schema{
		Fields:     []lakehouse.Field{{Name: "id", Type: lakehouse.TypeInt, Required: true}},
		PrimaryKey: []string{"id"},
	})

	appendRetry := func(id int) error {
		for {
			_, err := tbl.Append([]lakehouse.Row{{"id": id}})
			if err == nil {
				return nil
			}
			var cm lakehouse.ErrConcurrentModification
			if errors.As(err, &cm) {
				// 版本被其他进程抢先：由调用方自行决定重试。
				time.Sleep(time.Millisecond)
				continue
			}
			return err
		}
	}
	if err := appendRetry(1); err != nil {
		panic(err)
	}
	rows, _ := tbl.Read(0)
	fmt.Println("rows:", len(rows))
	// Output:
	// rows: 1
}
