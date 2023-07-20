package main

/*
	Author: Matt Martin
	Date: 2023-07-20
	Desc: Writes a bunch of dummy data to a CSV File
*/

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
)

func main() {

	file, err := os.Create("dummy_data.csv")
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}

	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	//append headers
	headers := []string{"dummy_header1", "dummy_header2", "dummy_header3"}
	writer.Write(headers)

	max_iterations := 1000

	for i := 1; i <= max_iterations; i++ {
		rec := []string{"test", strconv.Itoa(i), "test2"}
		writer.Write(rec)
		fmt.Println(i)
	}

}
