package cmd

//standard root file for cobra cli; nothing special here

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "fake_data",
	Short: "generates fake data",
	Long:  "Allows users to generate a dataset of fake data using the faker API",
}

func Execute() error {
	return rootCmd.Execute()
}
