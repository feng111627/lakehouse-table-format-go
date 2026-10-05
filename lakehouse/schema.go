package lakehouse

import "fmt"

// checkSchemaChange 校验从 old 到 next 的结构变更是否兼容，
// 不兼容时返回 *ErrIncompatibleSchema 并带可区分原因。
//
// 兼容规则：
//   - 新增列：允许，但新列不能是 Required（历史数据文件没有该列）；
//   - 删除列：允许，但不能删除主键列；
//   - 类型变更：仅允许 int -> float 的加宽；收窄或跨类变更拒绝；
//   - 主键集合：不允许变更。
func checkSchemaChange(old, next Schema) error {
	if !sameStringSet(old.PrimaryKey, next.PrimaryKey) {
		return ErrIncompatibleSchema{
			Reason: ReasonDropPrimaryKey,
			Detail: fmt.Sprintf("primary key changed from %v to %v", old.PrimaryKey, next.PrimaryKey),
		}
	}
	for _, of := range old.Fields {
		nf, ok := next.Field(of.Name)
		if !ok {
			// 删列
			if contains(old.PrimaryKey, of.Name) {
				return ErrIncompatibleSchema{
					Reason: ReasonDropPrimaryKey,
					Detail: fmt.Sprintf("cannot drop primary key column %q", of.Name),
				}
			}
			continue
		}
		if of.Type == nf.Type {
			continue
		}
		if of.Type == TypeInt && nf.Type == TypeFloat {
			continue // 加宽，允许
		}
		if of.Type == TypeFloat && nf.Type == TypeInt {
			return ErrIncompatibleSchema{
				Reason: ReasonTypeNarrowing,
				Detail: fmt.Sprintf("column %q: float -> int loses precision", of.Name),
			}
		}
		return ErrIncompatibleSchema{
			Reason: ReasonTypeMismatch,
			Detail: fmt.Sprintf("column %q: cannot change %s -> %s", of.Name, of.Type, nf.Type),
		}
	}
	for _, nf := range next.Fields {
		if _, ok := old.Field(nf.Name); !ok && nf.Required {
			return ErrIncompatibleSchema{
				Reason: ReasonRequiredNewCol,
				Detail: fmt.Sprintf("new column %q must be optional: existing data files lack it", nf.Name),
			}
		}
	}
	return nil
}

func contains(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		if !contains(b, x) {
			return false
		}
	}
	return true
}
