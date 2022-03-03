package diff

import "github.com/pingcap/parser/ast"

type DiffIgnoreType int

// DiffIgnore types.
const (
	TableOptionNone DiffIgnoreType = iota
	DiffIgnoreTableDiff
	DiffIgnoreTableAppend
	DiffIgnoreTableRemove
	DiffIgnoreTableOptionEngine
	DiffIgnoreTableOptionCharset
	DiffIgnoreTableOptionRowFormat
	DiffIgnoreTableOptionAutoIncrement
	DiffIgnoreColumnDiff
	DiffIgnoreColumnRemove
	DiffIgnoreColumnAppend
	DiffIgnoreColumnOptionNull
	DiffIgnoreColumnOptionComment
	DiffIgnoreIndexOption
	DiffIgnoreIndexDiff
	DiffIgnoreIndexRemove
	DiffIgnoreIndexAppend
)

func (m DiffIgnoreType) GetTableOption() ast.TableOptionType {
	switch m {
	case DiffIgnoreTableOptionEngine:
		return ast.TableOptionEngine
	case DiffIgnoreTableOptionCharset:
		return ast.TableOptionCharset
	case DiffIgnoreTableOptionRowFormat:
		return ast.TableOptionRowFormat
	case DiffIgnoreTableOptionAutoIncrement:
		return ast.TableOptionAutoIncrement
	}

	return ast.TableOptionNone
}

func (m DiffOption) HasIgnoreTableOption(tp ast.TableOptionType) bool {
	for _, opt := range m.IgnoreOpts {
		if tp == opt.GetTableOption() {
			return true
		}
	}

	return false
}

func (m DiffOption) HasIgnoreColumnName(name string) bool {
	for _, ignoreName := range m.IgnoreColumns {
		if name == ignoreName {
			return true
		}
	}

	return false
}

func (m DiffOption) HasIgnoreColumnOption(tp ast.ColumnOptionType) bool {
	for _, opt := range m.IgnoreOpts {
		if tp == opt.GetColumnOption() {
			return true
		}
	}

	return false
}

func (m DiffIgnoreType) GetColumnOption() ast.ColumnOptionType {
	switch m {
	case DiffIgnoreColumnOptionNull:
		return ast.ColumnOptionNull
	case DiffIgnoreColumnOptionComment:
		return ast.ColumnOptionComment
	}

	return ast.ColumnOptionNoOption
}

// TableOption is used for parsing table option from SQL.
type DiffOption struct {
	IgnoreOpts    []DiffIgnoreType
	IgnoreColumns []string
}

func (m DiffOption) Has(ty DiffIgnoreType) bool {
	hit := false
	for _, ignoreOpt := range m.IgnoreOpts {
		if ty == ignoreOpt {
			hit = true
			break
		}
	}

	return hit
}

var DefaultDiffIgnoreTypes = []DiffIgnoreType{
	DiffIgnoreTableOptionEngine,
	DiffIgnoreTableOptionCharset,
	DiffIgnoreTableOptionRowFormat,
	DiffIgnoreTableOptionAutoIncrement,
	DiffIgnoreIndexOption,
	DiffIgnoreColumnOptionNull,
}
