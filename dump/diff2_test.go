package dump

import (
	"fmt"
	"testing"

	"github.com/ssoor/sql-calculator/diff"
	"github.com/ssoor/sql-calculator/utils"

	_ "github.com/go-sql-driver/mysql"

	_ "github.com/pingcap/tidb/types/parser_driver"
)

func TestGetDiffFromSqlFile2(t *testing.T) {

	devDsn := "yk_sz_dev:SL2Gd78df8gs4SA@tcp(rm-wz91aumnig032y0p2io.mysql.rds.aliyuncs.com:3306)/potential-customer_fangzhiadmin_test"
	testDsn := "nieml01@233:87f410f5b932688a87bf73a7ad587a7e@tcp(rdsproxy.myscrm.cn:3366)/potential-customer_fangzhiadmin_test"

	sourceTables, err := GetTables(testDsn)
	if err != nil {
		return
	}
	targetTables, err := GetTables(devDsn)
	if err != nil {
		return
	}

	// sourceTables = []string{"qk_operate"}
	// targetTables = []string{"qk_operate"}

	sourceSqls, _ := GetTableCreateSQL(testDsn, sourceTables, 1)
	var sourceTable string
	for _, sql := range sourceSqls {
		sourceTable += sql + ";\n"
	}

	targetSqls, _ := GetTableCreateSQL(devDsn, targetTables, 1)
	var targetTable string
	for _, sql := range targetSqls {
		targetTable += sql + ";\n"
	}

	// 	targetTable := `
	// CREATE TABLE t1(
	// id int,
	// name varchar(30),
	// age int
	// );
	// CREATE TABLE t3(
	// id int
	// );
	// `

	// binSql, _ := ioutil.ReadFile("/mnt/source_code/golang/src/github.com/sjjian/sql-calculator/target.sql")
	// targetTable = string(binSql)
	// binSql, _ = ioutil.ReadFile("/mnt/source_code/golang/src/github.com/sjjian/sql-calculator/source.sql")
	// sourceTable = string(binSql)

	alters, err := diff.GetDiffFromSqlFile("", sourceTable, targetTable)
	// alters, err = GetDiffFromSqlFile("db1", sourceTable, targetTable)
	if err != nil {
		t.Error(err)
		return
	}

	modifySql := ""
	for _, alter := range alters {
		sql, _ := utils.RestoreToSql(alter)
		modifySql += sql + ";\n"
	}

	fmt.Println(modifySql)
	t.Error(err)
}
