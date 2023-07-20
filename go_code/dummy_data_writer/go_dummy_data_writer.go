package main

/*
	Author: Matt Martin
	Date: 2023-07-20
	Desc: Writes a bunch of dummy data to a CSV File

	TO DO:
		1) Redirect the output to a non-git folder
		2) create a random value generator as a column
		3) have it pick from random date range
		4) have it pick from random list of strings (from slice)

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

	max_iterations := 100000000

	for i := 1; i <= max_iterations; i++ {
		rec := []string{"test", strconv.Itoa(i), "test2"}
		writer.Write(rec)
		//fmt.Println(i)
	}

}
