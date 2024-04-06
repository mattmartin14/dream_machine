package cmd

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"reflect"

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

func GenFakeDataJson() ([]byte, error) {
	ds := dataset{}
	err := faker.FakeData(&ds)
	if err != nil {
		return nil, err
	}

	dsJSON, err := json.Marshal(ds)
	if err != nil {
		return nil, err
	}

	return dsJSON, nil
}

// grabs the headers from the struct dynamically so if we add more columns, they will get picked up
func GetCSVHeaders() ([]byte, error) {

	var ds dataset
	dataType := reflect.TypeOf(ds)

	var headerRow []string

	// Iterate over the fields of the struct type and extract their names
	for i := 0; i < dataType.NumField(); i++ {
		field := dataType.Field(i)
		headerRow = append(headerRow, field.Name)
	}

	// Write the header row to the buffer
	var buf bytes.Buffer
	csvWriter := csv.NewWriter(&buf)
	err := csvWriter.Write(headerRow)
	if err != nil {
		return nil, err
	}

	csvWriter.Flush()

	return buf.Bytes(), nil
}

func GenFakeDataCSV() ([]byte, error) {
	ds := dataset{}
	err := faker.FakeData(&ds)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	return buffer.Bytes(), nil
}
