package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	rows, file_cnt        int
	file_type, output_dir string
)

func init() {
	rootCmd.AddCommand(createCmd)
	createCmd.Flags().IntVarP(&rows, "rows", "r", 0, "Number of rows to generate")
	createCmd.Flags().IntVarP(&file_cnt, "files", "f", 0, "Total number of files to generate")
	createCmd.Flags().StringVarP(&file_type, "filetype", "t", "", "Type of file; can be either csv or json")
	createCmd.Flags().StringVarP(&output_dir, "outputdir", "o", "", "Directory to Output the file")
}

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "creates a fake dataset",
	Run:   generate_dataset,
}

func generate_dataset(cmd *cobra.Command, args []string) {

	output_dir, err := resolveOutputDir(output_dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to resolve the output directory: %v\n", err)
		os.Exit(1)
	}

	if err := write_data_parallel(rows, file_cnt, file_type, output_dir); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

}
