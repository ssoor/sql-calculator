package diff

import (
	"fmt"

	"github.com/ssoor/sql-calculator/utils"
	"github.com/ssoor/sql-calculator/virtualdb"

	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

type DiffIgnoreType int

// DiffIgnore types.
const (
	TableOptionNone DiffIgnoreType = iota
	DiffIgnoreTableRemove
	DiffIgnoreTableAppend
	DiffIgnoreTableOptionEngine
	DiffIgnoreTableOptionCharset
	DiffIgnoreTableOptionRowFormat
	DiffIgnoreTableOptionAutoIncrement
	DiffIgnoreColumnRemove
	DiffIgnoreColumnAppend
	DiffIgnoreColumnOptionNull
	DiffIgnoreColumnOptionComment
	DiffIgnoreIndexOption
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

func GetDiffFromSqlFile(dbName, sourceSqlFile, targetSqlFile string, ignores ...DiffIgnoreType) ([]ast.StmtNode, error) {
	ignores = append(ignores, DefaultDiffIgnoreTypes...)

	return GetDiffSQLWithOpt(dbName, sourceSqlFile, targetSqlFile, DiffOption{IgnoreOpts: ignores})
}

func GetDiffSQLWithOpt(dbName, sourceSqlFile, targetSqlFile string, opt DiffOption) ([]ast.StmtNode, error) {
	sourceDb := virtualdb.NewVirtualDB(dbName)
	if err := sourceDb.ExecSQL(sourceSqlFile); err != nil {
		return nil, err
	}

	targetDb := virtualdb.NewVirtualDB(dbName)
	if err := targetDb.ExecSQL(targetSqlFile); err != nil {
		return nil, err
	}

	allDDL := []ast.StmtNode{}
	sourceTables, _ := sourceDb.GetTableStmts(dbName)
	targetTables, _ := targetDb.GetTableStmts(dbName)

	for name, sourceTable := range sourceTables {
		var alter ast.StmtNode

		targetTable, exist := targetTables[name]
		if !exist {
			if !opt.Has(DiffIgnoreTableRemove) { // 目标中不存在，需要删除
				alter = &ast.DropTableStmt{Tables: []*ast.TableName{sourceTable.Table.Table}}
			}
		} else {
			delete(targetTables, name) // 存在，从目标中删除并处理差异
			alter = GetDiffTable(sourceTable.Table, targetTable.Table, opt)
		}

		if alter == nil {
			continue
		}

		allDDL = append(allDDL, alter)
	}

	if opt.Has(DiffIgnoreTableAppend) {
		return allDDL, nil
	}

	// 创建剩余的表
	for _, table := range targetTables {
		allDDL = append(allDDL, table.Table)
	}

	return allDDL, nil
}

func GetDiffTable(sourceTable, targetTable *ast.CreateTableStmt, opt DiffOption) ast.StmtNode {
	columnMap := make(map[string]*ast.ColumnDef)
	for _, col := range targetTable.Cols {
		if opt.HasIgnoreColumnName(col.Name.Name.String()) {
			continue
		}

		columnMap[col.Name.Name.String()] = col
	}

	alterSpecs := []*ast.AlterTableSpec{}
	removeColumns := []*ast.ColumnDef{}
	for _, sourceCol := range sourceTable.Cols {
		if opt.HasIgnoreColumnName(sourceCol.Name.Name.String()) {
			continue
		}

		col, exist := columnMap[sourceCol.Name.Name.String()]
		if !exist {
			removeColumns = append(removeColumns, sourceCol)
			continue
		}
		delete(columnMap, sourceCol.Name.Name.String()) // 存在，从目标中删除并处理差异

		if compareColumn(col, sourceCol, opt) {
			continue
		}
		alterSpecs = append(alterSpecs, &ast.AlterTableSpec{
			Tp:         ast.AlterTableModifyColumn,
			NewColumns: []*ast.ColumnDef{col},
			Position: &ast.ColumnPosition{
				Tp: ast.ColumnPositionNone,
			},
		})
	}

	if !opt.Has(DiffIgnoreColumnRemove) {
		// 目标中不存在，需要删除
		for _, col := range removeColumns {
			fmt.Printf("%s\t%s\n", sourceTable.Table.Name.String(), col.Name.Name.String())

			alterSpecs = append(alterSpecs, &ast.AlterTableSpec{
				Tp:            ast.AlterTableDropColumn,
				OldColumnName: col.Name,
			})
		}
	}

	if !opt.Has(DiffIgnoreColumnAppend) {
		// 创建剩余的字段
		cols := []*ast.ColumnDef{}
		for _, col := range columnMap {
			cols = append(cols, col)
		}

		if len(cols) > 0 {
			alterSpecs = append(alterSpecs, &ast.AlterTableSpec{
				Tp:         ast.AlterTableAddColumns,
				NewColumns: cols,
			})
		}
	}

	constraintMap := make(map[string]*ast.Constraint)
	for _, con := range targetTable.Constraints {
		constraintMap[con.Name] = con
	}

	for _, sourceCon := range sourceTable.Constraints {
		con, exist := constraintMap[sourceCon.Name]
		if !exist { // 目标中不存在，需要删除
			alterSpecs = append(alterSpecs, &ast.AlterTableSpec{
				Tp:   ast.AlterTableDropIndex,
				Name: sourceCon.Name,
			})
			continue
		}
		delete(constraintMap, sourceCon.Name) // 存在，从目标中删除并处理差异

		if compareConstraint(sourceCon, con, opt) {
			continue
		}

		addDDL := &ast.AlterTableSpec{
			Tp:         ast.AlterTableAddConstraint,
			Constraint: con,
		}
		delDDL := &ast.AlterTableSpec{
			Tp:   ast.AlterTableDropIndex,
			Name: con.Name,
		}

		switch sourceCon.Tp {
		case ast.ConstraintPrimaryKey:
			delDDL = &ast.AlterTableSpec{
				Tp: ast.AlterTableDropPrimaryKey,
			}
		}
		alterSpecs = append(alterSpecs, delDDL)
		alterSpecs = append(alterSpecs, addDDL)
	}

	// 创建剩余的约束
	for _, con := range constraintMap {
		alterSpecs = append(alterSpecs, &ast.AlterTableSpec{
			Tp:         ast.AlterTableAddConstraint,
			Constraint: con,
		})
	}

	if !compareTableOptions(targetTable, sourceTable, opt) {
		alterSpecs = append(alterSpecs, &ast.AlterTableSpec{
			Tp:      ast.AlterTableOption,
			Options: targetTable.Options,
		})
	}

	if len(alterSpecs) == 0 {
		return nil
	}

	return &ast.AlterTableStmt{
		Table: targetTable.Table,
		Specs: alterSpecs,
	}
}

func compareTableOptions(source, target *ast.CreateTableStmt, opt DiffOption) bool {
	rawOpts := [][]*ast.TableOption{
		source.Options,
		target.Options,
	}

	restoreOpts := make([][]*ast.TableOption, len(rawOpts))
	for i, opts := range rawOpts {
		for _, tableOpt := range opts {
			if opt.HasIgnoreTableOption(tableOpt.Tp) {
				continue
			}

			restoreOpts[i] = append(restoreOpts[i], tableOpt)
		}
	}

	s, _ := utils.RestoreToSql(&ast.CreateTableStmt{Table: source.Table, Options: restoreOpts[0]})
	t, _ := utils.RestoreToSql(&ast.CreateTableStmt{Table: target.Table, Options: restoreOpts[1]})

	return s == t
}

func compareColumn(source, target *ast.ColumnDef, opt DiffOption) bool {
	rawOpts := [][]*ast.ColumnOption{
		source.Options,
		target.Options,
	}

	restoreOpts := make([][]*ast.ColumnOption, len(rawOpts))
	for i, opts := range rawOpts {
		for _, columnOpt := range opts {
			if opt.HasIgnoreColumnOption(columnOpt.Tp) {
				continue
			}

			restoreOpts[i] = append(restoreOpts[i], columnOpt)
		}
	}

	source.Options = restoreOpts[0]
	target.Options = restoreOpts[1]

	s, _ := utils.RestoreToSql(source)
	t, _ := utils.RestoreToSql(target)

	source.Options = rawOpts[0]
	target.Options = rawOpts[1]

	return s == t
}

func compareConstraint(source, target *ast.Constraint, opt DiffOption) bool {
	rawOpts := []*ast.IndexOption{
		source.Option,
		target.Option,
	}

	if opt.Has(DiffIgnoreIndexOption) {
		source.Option = nil
		target.Option = nil
	}

	s, _ := utils.RestoreToSql(source)
	t, _ := utils.RestoreToSql(target)

	source.Option = rawOpts[0]
	target.Option = rawOpts[1]

	return s == t
}
