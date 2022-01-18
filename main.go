package main

import (
	"github.com/ssoor/sql-calculator/cmd"

	"github.com/spf13/cobra"
)

func main() {
	var rootCmd = &cobra.Command{}
	rootCmd.AddCommand(cmd.DiffCmd)
	rootCmd.AddCommand(cmd.DumpCmd)
	rootCmd.AddCommand(cmd.FingerprintCmd)
	rootCmd.Execute()
}
