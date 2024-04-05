package cmd

// import (
// 	"fmt"
// 	"os"

// 	"github.com/spf13/cobra"
// )

// var Crows int
// var CfileName, CoutPutDir string

// func init() {
// 	createCmd.AddCommand(genCsvDataCmd)
// 	genCsvDataCmd.Flags().IntVarP(&Crows, "rows", "r", 0, "Number of rows to generate")
// 	genCsvDataCmd.Flags().StringVarP(&CfileName, "filename", "f", "", "Name of the file")
// 	genCsvDataCmd.Flags().StringVarP(&CoutPutDir, "outputdir", "o", "", "Directory to Output the file")
// }

// var genCsvDataCmd = &cobra.Command{
// 	Use:   "csv",
// 	Short: "creates a fake dataset formatted in csv",
// 	Run:   GenCsvData,
// }

// func GenCsvData(cmd *cobra.Command, args []string) {

// 	if err := create_csv_file(Crows, CfileName, CoutPutDir); err != nil {
// 		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
// 		os.Exit(1)
// 	}

// }

// func create_csv_file(rows int, fileName string, outputdir string) error {

// 	outputdir, err := resolveOutputDir(outputdir)
// 	if err != nil {
// 		return fmt.Errorf("failed to resolve the output directory: %v", err)
// 	}

// 	f_path := outputdir + fileName

// 	file, err := os.Create(f_path)
// 	if err != nil {
// 		return fmt.Errorf("failed to create file: %v", err)
// 	}

// 	defer file.Close()

// 	// add headers
// 	if _, err := file.WriteString(GetCSVHeaders() + "\n"); err != nil {
// 		return err
// 	}

// 	// add data
// 	for i := 1; i <= rows; i++ {

// 		dsCSV := GenFakeDataCSV()

// 		_, err = file.Write(dsCSV)
// 		if err != nil {
// 			return err
// 		}

// 	}

// 	return nil
// }
