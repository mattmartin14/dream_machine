package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var Jrows int
var JfileName, JoutPutDir string

func init() {
	createCmd.AddCommand(genJsonDataCmd)
	genJsonDataCmd.Flags().IntVarP(&Jrows, "rows", "r", 0, "Number of rows to generate")
	genJsonDataCmd.Flags().StringVarP(&JfileName, "filename", "f", "", "Name of the file")
	genJsonDataCmd.Flags().StringVarP(&JoutPutDir, "outputdir", "o", "", "Directory to Output the file")
}

var genJsonDataCmd = &cobra.Command{
	Use:   "json",
	Short: "creates a fake dataset formatted in json arrays",
	Run:   genJsonData,
}

func genJsonData(cmd *cobra.Command, args []string) {
	//fmt.Printf("Generating JSON data with %d rows\n", rows)
	//err := create_js_file(Jrows, JfileName)

	if err := create_js_file(Jrows, JfileName, JoutPutDir); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

}

func create_js_file(rows int, fileName string, outputdir string) error {

	outputdir, err := resolveOutputDir(outputdir)
	if err != nil {
		return fmt.Errorf("failed to resolve the output directory: %v", err)
	}

	f_path := outputdir + fileName

	file, err := os.Create(f_path)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}

	defer file.Close()

	_, err = file.WriteString("[\n")
	if err != nil {
		return err
	}

	for i := 1; i <= rows; i++ {

		dsJSON := GenFakeDataJson()

		_, err = file.Write(dsJSON)
		if err != nil {
			return err
		}

		// Add a comma after each dataset entry, except for the last one
		if i < rows {
			_, err = file.WriteString(",\n")
			if err != nil {
				return err
			}
		}
	}

	_, err = file.WriteString("\n]")
	if err != nil {
		return err
	}

	return nil
}
