package cmd

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bxcodec/faker/v3"
)

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

func GenFakeDataJson() []byte {
	ds := dataset{}
	err := faker.FakeData(&ds)
	if err != nil {
		fmt.Println(err)
	}

	dsJSON, err := json.Marshal(ds)
	if err != nil {
		fmt.Println(err)
	}

	return dsJSON
}

func GetCSVHeaders() string {
	var headers []string
	headerRow := []string{
		"Latitude",
		"Longitude",
		"CreditCardNumber",
		"Email",
		"PhoneNumber",
		"FirstName",
		"LastName",
		"Date",
		"NetWorth",
		"TxnKey",
	}
	headers = append(headers, strings.Join(headerRow, ","))

	return strings.Join(headers, "\n")

}

func GenFakeDataCSV() []byte {
	ds := dataset{}
	err := faker.FakeData(&ds)
	if err != nil {
		fmt.Println(err)
	}

	// Convert struct to CSV
	var csvData [][]string
	csvData = append(csvData, []string{
		fmt.Sprintf("%f", ds.Latitude),
		fmt.Sprintf("%f", ds.Longitude),
		ds.CreditCardNumber,
		ds.Email,
		ds.PhoneNumber,
		ds.FirstName,
		ds.LastName,
		ds.Date,
		fmt.Sprintf("%f", ds.NetWorth),
		ds.TxnKey,
	})

	// Write CSV data to a buffer
	buffer := &bytes.Buffer{}
	writer := csv.NewWriter(buffer)
	err = writer.WriteAll(csvData)
	if err != nil {
		fmt.Println(err)
	}

	return buffer.Bytes()
}
