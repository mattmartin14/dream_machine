package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var Jrows int
var JfileName string

var gen_data_json = &cobra.Command{
	Use:   "json",
	Short: "creates a fake dataset formatted in json arrays",
	Run:   GenJsonData,
}

func GenJsonData(cmd *cobra.Command, args []string) {
	//fmt.Printf("Generating JSON data with %d rows\n", rows)
	create_js_file(Jrows, JfileName)
}

func init() {
	CreateCmd().AddCommand(gen_data_json)
	gen_data_json.Flags().IntVarP(&Jrows, "rows", "r", 0, "Number of rows to generate")
	gen_data_json.Flags().StringVarP(&JfileName, "filename", "f", "", "Name of the file")
}

func create_js_file(rows int, fileName string) error {
	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/fake_data/" + fileName

	file, err := os.Create(f_path)
	if err != nil {
		return err
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
