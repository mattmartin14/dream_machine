package cmd

//standard root file for cobra cli; nothing special here

import (
	"fmt"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "fake_data",
	Short: "generates fake data",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("This is the main process.")
	},
}

func Execute() error {
	return rootCmd.Execute()
}
