package cmd

/*
	This file is the entry point for creating datasets;

	currently, can only call the jsondata sub command
	in the future, will add csv, txt, maybe parquet

*/

import (
	"fmt"

	"github.com/spf13/cobra"
)

func CreateCmd() *cobra.Command {
	var create = &cobra.Command{
		Use:   "create",
		Short: "creates a fake dataset",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("executing the create part")
		},
	}

	create.AddCommand(gen_data_json)
	create.AddCommand(gen_data_csv)

	return create
}

func init() {
	rootCmd.AddCommand(CreateCmd())
}
