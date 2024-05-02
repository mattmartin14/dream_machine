package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"os/user"
	"simple_etl/app"
	"strconv"
	"time"
)

func main() {

	start := time.Now()

	hasHeader := true

	groupingColName := "FirstName" // Specify the grouping column name
	summingColName := "NetWorth"

	usr, _ := user.Current()
	f_path := usr.HomeDir + "/test_dummy_data/fd/fin_data_1.csv"

	file, err := os.Open(f_path)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// -------------------------------------------------
	// Grab Header Info

	headers, err := reader.Read()
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	groupingColIndex, summingColIndex, err := app.FindColumnPositions(headers, groupingColName, summingColName)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// read the data into a 2d array aka a table aka a dataframeish thingy
	data, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// grab row count
	row_cnt := len(data)

	// build our map where we will store our key (the grouping column) and aggregate up the summing column
	tsfmData := make(map[string]float64)

	// Iterate over each record in the CSV file
	for i, record := range data {

		// Skip the header row if it exists
		if hasHeader && i == 0 {
			continue
		}

		groupingValue := record[groupingColIndex]
		summingValueStr := record[summingColIndex]

		summingValue, err := strconv.ParseFloat(summingValueStr, 64)
		if err != nil {
			fmt.Println("Error parsing summing value:", err)
			continue
		}

		tsfmData[groupingValue] += summingValue
	}

	// write aggregate to csv
	csv_fpath := "data.csv"
	err = app.WriteToCSV(tsfmData, groupingColName, summingColName, csv_fpath)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// write aggregate to parquet
	p_fpath := "output.parquet"
	err = app.WriteToParquet(tsfmData, p_fpath)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	elapsed := time.Since(start) // Calculate the elapsed time

	fmt.Printf("Total Rows Processed: %s. Total time to process: %s\n", app.Format_nbr_with_commas(row_cnt), elapsed)

}
