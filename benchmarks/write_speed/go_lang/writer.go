package main

/*
	Author: Matt Martin
	Date: 2023-08-28
	Desc: Benchmark testing writing 1B rows in go lang

*/

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {

	tot_rows := 1000000000
	batch_size := 10000

	start_ts := time.Now()

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/write_benchmark/go_generated.csv"

	file, _ := os.Create(f_path)
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	var buffer bytes.Buffer

	newline := "\n"
	for i := 1; i <= tot_rows; i += batch_size {

		start_row := i
		end_row := start_row + batch_size - 1
		if end_row > tot_rows {
			end_row = tot_rows
		}

		//fmt.Printf("start row is %d, end row is %d\n", start_row, end_row)

		for j := start_row; j <= end_row; j++ {
			buffer.WriteString(strconv.Itoa(j))
			buffer.WriteString(newline)
		}

		_, err := buffer.WriteTo(writer)
		if err != nil {
			fmt.Println("Error writing data buffer to file: ", err)
			return
		}

	}

	elapsed_time := time.Since(start_ts).Seconds()
	msg := fmt.Sprintf("Elapsed Time using Go Lang to process %s rows: %.2f seconds", format_nbr_with_commas(tot_rows), elapsed_time)
	fmt.Println(msg)

}

func format_nbr_with_commas(rows int) string {
	rows_str := strconv.Itoa(rows)

	for i := len(rows_str) - 3; i > 0; i -= 3 {
		rows_str = rows_str[:i] + "," + rows_str[i:]
	}
	return rows_str
}
