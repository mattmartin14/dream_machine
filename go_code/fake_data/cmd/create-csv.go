package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var Crows int
var CfileName string

var gen_data_csv = &cobra.Command{
	Use:   "csv",
	Short: "creates a fake dataset formatted in csv",
	Run:   GenCsvData,
}

func GenCsvData(cmd *cobra.Command, args []string) {
	create_csv_file(Crows, CfileName)
}

func init() {
	CreateCmd().AddCommand(gen_data_csv)
	gen_data_csv.Flags().IntVarP(&Crows, "rows", "r", 0, "Number of rows to generate")
	gen_data_csv.Flags().StringVarP(&CfileName, "filename", "f", "", "Name of the file")
}

func create_csv_file(rows int, fileName string) error {
	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/fake_data/" + fileName

	file, err := os.Create(f_path)
	if err != nil {
		return err
	}
	defer file.Close()

	// add headers
	if _, err := file.WriteString(GetCSVHeaders() + "\n"); err != nil {
		return err
	}

	// add data
	for i := 1; i <= rows; i++ {

		dsCSV := GenFakeDataCSV()

		_, err = file.Write(dsCSV)
		if err != nil {
			return err
		}

	}

	return nil
}
