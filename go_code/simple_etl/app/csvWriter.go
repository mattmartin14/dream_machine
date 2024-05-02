package app

import (
	"encoding/csv"
	"fmt"
	"os"
)

func WriteToCSV(data map[string]float64, groupingColName string, summingColName string, fpath string) error {
	// Write the aggregated net worth to a new CSV file
	outFile, err := os.Create(fpath)
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}
	defer outFile.Close()

	// Create a CSV writer
	writer := csv.NewWriter(outFile)
	defer writer.Flush()

	process_ts, err := GetProcessTS()

	// Write header
	header := []string{groupingColName, "Summed_" + summingColName, "process_ts"}
	writer.Write(header)

	// Write aggregated net worth for each first name to the CSV file
	for groupingValue, summingValue := range data {
		row := []string{groupingValue, fmt.Sprintf("%.2f", summingValue), process_ts}
		writer.Write(row)
	}

	return nil
}
