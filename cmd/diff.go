package cmd

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/ssoor/sql-calculator/diff"
	"github.com/ssoor/sql-calculator/utils"

	"github.com/spf13/cobra"
)

var DiffCmd = &cobra.Command{
	Use:   "diff [Source SQL filename](string) [Target SQL filename](string)",
	Args:  cobra.MinimumNArgs(2),
	Short: "SQL Diff",
	Example: "   ./sql-calculator diff ./source.sql ./target.sql\n" +
		"Output:\n   ALTER TABLE `qk_t2` COMMENT = '注释被修改'",
	Long: "SQL Diff - Compare the differences between the two SQL content and output the synchronization script",
	Run: func(cmd *cobra.Command, args []string) {
		sourceSql, err := ioutil.ReadFile(args[0])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		targetSql, err := ioutil.ReadFile(args[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		alters, err := diff.GetDiffFromSqlFile("", string(sourceSql), string(targetSql))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		modifySql := ""
		for _, alter := range alters {
			sql, _ := utils.RestoreToSql(alter)
			modifySql += sql + ";\n"
		}
		fmt.Println(modifySql)
	},
}
