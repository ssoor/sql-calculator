package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/ssoor/sql-calculator/dump"

	"github.com/spf13/cobra"
)

var DumpCmd = &cobra.Command{
	Use:   "dump [dsn](string)",
	Args:  cobra.MinimumNArgs(1),
	Short: "SQL Dump",
	Example: "   ./sql-calculator dump root:root@tcp(localhost:3306)/testdb\n" +
		"Output:\n   t1\n   t2\n   t3\n   t4\n   t5",
	Long: "SQL Dump - Compare the differences between the two SQL content and output the synchronization script",
	Run: func(cmd *cobra.Command, args []string) {
		tableList := []string{}
		if tables, exist := os.LookupEnv("TABLE_FILTER_LIST"); exist {
			tableList = strings.Split(tables, ",")
		}

		if len(tableList) == 0 {
			sourceTables, err := dump.GetTables(args[0])
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			tableList = sourceTables
		}

		sourceSqls, err := dump.GetTableCreateSQL(args[0], tableList, 1)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		var sourceTable string
		for _, sql := range sourceSqls {
			sourceTable += sql + ";\n"
		}

		fmt.Println(sourceTable)
	},
}
