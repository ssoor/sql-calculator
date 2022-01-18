package dump

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

func GetTables(dsn string) ([]string, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.WithMessage(err, "open db error")
	}
	defer db.Close()
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, errors.WithMessage(err, "query show create table error")
	}
	defer rows.Close()

	tables := make([]string, 0)
	for {
		if !rows.Next() {
			break
		}

		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}

	return tables, nil
}

func GetTableCreateSQL(dsn string, totalTables []string, batchCount int) ([]string, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.WithMessage(err, "open db error")
	}
	defer db.Close()

	sqls := make([]string, 0)
	for {
		if len(totalTables) == 0 {
			break
		}

		count := batchCount
		if count > len(totalTables) {
			count = len(totalTables)
		}

		tables := totalTables[:count]
		totalTables = totalTables[count:]

		sql := ""
		for _, tableName := range tables {
			sql += "SHOW CREATE TABLE " + tableName + ";\n"
		}

		rows, err := db.Query(sql)
		if err != nil {
			return nil, errors.WithMessage(err, "query show create table error")
		}
		defer rows.Close()

		for {
			if !rows.Next() {
				if !rows.NextResultSet() {
					break
				}
				if !rows.Next() {
					break
				}
			}

			var table string
			var createSql string
			err = rows.Scan(&table, &createSql)
			if err != nil {
				return nil, err
			}

			sqls = append(sqls, createSql)
		}
	}

	return sqls, nil
}
