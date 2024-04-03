package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/bxcodec/faker/v3"
	"github.com/spf13/cobra"
)

var rows int

var gen_data_json = &cobra.Command{
	Use:   "jsondata",
	Short: "creates a fake dataset formatted in json arrays",
	Run:   GenJsonData,
}

func GenJsonData(cmd *cobra.Command, args []string) {
	fmt.Printf("Generating JSON data with %d rows\n", rows)
	create_file(rows)
}

func init() {
	CreateCmd().AddCommand(gen_data_json)
	gen_data_json.Flags().IntVarP(&rows, "rows", "r", 0, "Number of rows to generate")
}

// build out the json dataset
type dataset struct {
	Latitude         float32 `faker:"lat"`
	Longitude        float32 `faker:"long"`
	CreditCardNumber string  `faker:"cc_number"`
	Email            string  `faker:"email"`
	PhoneNumber      string  `faker:"phone_number"`
	FirstName        string  `faker:"first_name"`
	LastName         string  `faker:"last_name"`
	Date             string  `faker:"date"`
	NetWorth         float64 `faker:"amount"`
	TxnKey           string  `faker:"uuid_hyphenated"`
}

func create_file(rows int) error {
	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/fake_data/dataset.json"

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

		ds := gen_data()
		dsJSON, err := json.Marshal(ds)
		if err != nil {
			return err
		}

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

func gen_data() dataset {
	ds := dataset{}
	err := faker.FakeData(&ds)
	if err != nil {
		fmt.Println(err)
	}
	return ds
}
