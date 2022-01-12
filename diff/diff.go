package diff

import (
	"sql-calculator/utils"
	"sql-calculator/virtualdb"

	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func GetDiffFromSqlFile(dbName, sourceSqlFile, targetSqlFile string) ([]ast.StmtNode, error) {
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
			// 目标中不存在，需要删除
			alter = &ast.DropTableStmt{Tables: []*ast.TableName{sourceTable.Table.Table}}
		} else {
			delete(targetTables, name) // 存在，从目标中删除并处理差异
			alter = GetDiffFromTable(sourceTable.Table, targetTable.Table)
		}

		if alter == nil {
			continue
		}

		allDDL = append(allDDL, alter)
	}

	// 创建剩余的表
	for _, table := range targetTables {
		allDDL = append(allDDL, table.Table)
	}

	return allDDL, nil
}

func GetDiffFromTable(sourceTable, targetTable *ast.CreateTableStmt) ast.StmtNode {
	columnMap := make(map[string]*ast.ColumnDef)
	for _, col := range targetTable.Cols {
		columnMap[col.Name.Name.String()] = col
	}

	alterSpecs := []*ast.AlterTableSpec{}
	for _, sourceCol := range sourceTable.Cols {
		col, exist := columnMap[sourceCol.Name.Name.String()]
		if !exist { // 目标中不存在，需要删除
			alterSpecs = append(alterSpecs, &ast.AlterTableSpec{
				Tp:            ast.AlterTableDropColumn,
				OldColumnName: sourceCol.Name,
			})
			continue
		}
		delete(columnMap, sourceCol.Name.Name.String()) // 存在，从目标中删除并处理差异

		if compareColumn(col, sourceCol) {
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

	// 创建剩余的字段
	for _, col := range columnMap {
		alterSpecs = append(alterSpecs, &ast.AlterTableSpec{
			Tp:         ast.AlterTableAddColumns,
			NewColumns: []*ast.ColumnDef{col},
		})
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

		if compareConstraint(con, sourceCon) {
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

	if !compareTableOptions(targetTable, sourceTable) {
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

func compareTableOptions(source, target *ast.CreateTableStmt) bool {
	sourceOpts := []*ast.TableOption{}
	for _, opt := range source.Options {
		switch opt.Tp {
		case ast.TableOptionEngine:
		case ast.TableOptionCharset:
		case ast.TableOptionRowFormat:
		case ast.TableOptionAutoIncrement:
		default:
			sourceOpts = append(sourceOpts, opt)
		}
	}

	targetOpts := []*ast.TableOption{}
	for _, opt := range target.Options {
		switch opt.Tp {
		case ast.TableOptionEngine:
		case ast.TableOptionCharset:
		case ast.TableOptionRowFormat:
		case ast.TableOptionAutoIncrement:
		default:
			targetOpts = append(targetOpts, opt)
		}
	}
	s, _ := utils.RestoreToSql(&ast.CreateTableStmt{Table: source.Table, Options: sourceOpts})
	t, _ := utils.RestoreToSql(&ast.CreateTableStmt{Table: target.Table, Options: targetOpts})

	return s == t
}

func compareColumn(source, target *ast.ColumnDef) bool {
	opts := []*ast.ColumnOption{}
	for _, opt := range source.Options {
		if opt.Tp == ast.ColumnOptionNull {
			continue
		}

		opts = append(opts, opt)
	}
	source.Options = opts

	opts = []*ast.ColumnOption{}
	for _, opt := range target.Options {
		if opt.Tp == ast.ColumnOptionNull {
			continue
		}

		opts = append(opts, opt)
	}
	target.Options = opts

	s, _ := utils.RestoreToSql(source)
	t, _ := utils.RestoreToSql(target)

	return s == t
}

func compareConstraint(source, target *ast.Constraint) bool {
	s, _ := utils.RestoreToSql(source)
	t, _ := utils.RestoreToSql(target)

	return s == t
}
