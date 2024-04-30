package main

// used this to generate the fake dataset
// ./fd create --filetype csv --maxworkers 6 --prefix fin_data_ --outputdir ~/test_dummy_data/fd --files 1 --rows 10000

/*
	to do:
		move the functions to a separate package
		add ability to write to parquet
		add ability to read multiple files based on wild card
		add ability to do averages and counts
		-- maybe add ability to do a distinct count?

*/

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

	data, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	row_cnt := len(data)

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

	// Write the aggregated net worth to a new CSV file
	outFile, err := os.Create("output.csv")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer outFile.Close()

	// Create a CSV writer
	writer := csv.NewWriter(outFile)
	defer writer.Flush()

	// add current timestamp as output
	loc, _ := time.LoadLocation("America/New_York")
	currentTime := time.Now().In(loc)
	process_ts := currentTime.Format("2006-01-02 15:04:05")

	// Write header
	header := []string{groupingColName, "Summed_" + summingColName, "process_ts"}
	writer.Write(header)

	// Write aggregated net worth for each first name to the CSV file
	for groupingValue, summingValue := range tsfmData {
		row := []string{groupingValue, fmt.Sprintf("%.2f", summingValue), process_ts}
		writer.Write(row)
	}

	fmt.Printf("Total Rows Processed: %s. Output written to output.csv\n", app.Format_nbr_with_commas(row_cnt))

}
