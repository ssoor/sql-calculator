package diff

import (
	"github.com/ssoor/sql-calculator/utils"
	"github.com/ssoor/sql-calculator/virtualdb"

	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

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
			if !opt.Has(DiffIgnoreTableDiff) {
				alter = GetDiffTable(sourceTable.Table, targetTable.Table, opt)
			}
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

		if !opt.Has(DiffIgnoreColumnDiff) {
			// fmt.Printf("DIFF: %s.%s\n", sourceTable.Table.Name.String(), col.Name.Name.String())

			alterSpecs = append(alterSpecs, &ast.AlterTableSpec{
				Tp:         ast.AlterTableModifyColumn,
				NewColumns: []*ast.ColumnDef{col},
				Position: &ast.ColumnPosition{
					Tp: ast.ColumnPositionNone,
				},
			})
		}
	}

	if !opt.Has(DiffIgnoreColumnRemove) {
		// 目标中不存在，需要删除
		for _, col := range removeColumns {
			// fmt.Printf("DEL: %s.%s\n", sourceTable.Table.Name.String(), col.Name.Name.String())

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
			// fmt.Printf("ADD: %s.%s\n", sourceTable.Table.Name.String(), col.Name.Name.String())

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
		if !exist {
			// 目标中不存在，需要删除
			if !opt.Has(DiffIgnoreIndexRemove) {
				alterSpecs = append(alterSpecs, &ast.AlterTableSpec{
					Tp:   ast.AlterTableDropIndex,
					Name: sourceCon.Name,
				})
			}
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
		if !opt.Has(DiffIgnoreIndexDiff) {
			alterSpecs = append(alterSpecs, delDDL)
			alterSpecs = append(alterSpecs, addDDL)
		}
	}

	// 创建剩余的约束
	if !opt.Has(DiffIgnoreIndexAppend) {
		for _, con := range constraintMap {
			alterSpecs = append(alterSpecs, &ast.AlterTableSpec{
				Tp:         ast.AlterTableAddConstraint,
				Constraint: con,
			})
		}
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
