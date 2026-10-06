package lakehouse

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestMain prints a business-coverage map so `go test -v` logs visibly show
// which business areas and cases are covered and how each case fares.
func TestMain(m *testing.M) {
	fmt.Println("=== Lakehouse 本地数据湖表 单测业务覆盖清单 ===")
	for _, b := range businessCatalog() {
		fmt.Printf("业务域 [%s]\n", b.name)
		for _, cName := range b.cases {
			fmt.Printf("  - 用例: %s\n", cName)
		}
	}
	fmt.Println("=== 开始执行用例 ===")
	os.Exit(m.Run())
}

type bizCase struct {
	name  string
	cases []string
}

func businessCatalog() []bizCase {
	return []bizCase{
		{name: "A 快照提交与并发写入", cases: []string{
			"单写者多次 append 各自产生原子快照，快照链父指针连续",
			"并发 append 提交串行化且全部成功，HEAD 单调递增",
			"读快照隔离：读取期间其他提交不影响已拿到的 ResultSet",
			"失败的提交不留下半可见状态：校验失败行数与 HEAD 不变",
			"时间旅行：任意历史快照可重复读出相同结果",
			"读者/写入者/合并并发：无数据竞争，固定快照读取始终稳定",
			"跨进程（第二句柄/重开）读取与写入一致",
		}},
		{name: "B 表结构演进", cases: []string{
			"新增可空列：新快照有值，历史快照读取确定（缺列为 null）",
			"删列：新快照不再暴露该列，历史快照仍可读出",
			"列改名：按稳定列 id 保持数据连续",
			"类型拓宽 int->long/int->double/long->double 成功",
			"不兼容类型变更（string->int 等）被拒绝并给出可区分原因",
			"主键列禁删/禁改类型，新增主键列被拒绝",
			"未知列、类型不匹配、非空列缺值的行写入被拒绝",
			"同名删后再加列不复用旧 id，历史数据不错列",
		}},
		{name: "C 小文件合并与主键增量更新", cases: []string{
			"多次 append 产生多小文件，compact 后行数与内容一致",
			"compact 不改有效数据（合并前后投影逐行相等）",
			"小文件数低于阈值时 compact 为 NoOp",
			"合并后历史快照仍可读",
			"upsert 按主键更新已有行并插入新行",
			"upsert 输入内重复主键被显式拒绝",
			"表内已存在重复主键时 upsert 被拒绝，不静默取舍",
			"append 与 upsert 交替后再 compact，最终数据确定一致",
		}},
	}
}

func testSchema() *Schema {
	return &Schema{Fields: []Field{
		{ID: 1, Name: "id", Type: TypeLong, Nullable: false, IsPK: true},
		{ID: 2, Name: "name", Type: TypeString, Nullable: false},
		{ID: 3, Name: "age", Type: TypeInt, Nullable: true},
	}}
}

func openTestTable(t *testing.T, sch *Schema) (*Table, string) {
	t.Helper()
	dir := t.TempDir()
	ctx := context.Background()
	tbl, err := CreateTable(ctx, filepath.Join(dir, "table"), sch, &Config{CompactFileThreshold: 2})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	return tbl, dir
}

func mustRowIDs(t *testing.T, rs *ResultSet) []int64 {
	t.Helper()
	ids := make([]int64, len(rs.Rows))
	for i, r := range rs.Rows {
		ids[i] = r["id"].(int64)
	}
	return ids
}
