package app

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"time"
)

/*
	Author: Matt Martin
	Date: 2023-08-28
	Last Mod: 2023-09-06
		-- updated to write byte slices and time was cut in half. now runs under 13 seconds

		-- this is a single thread writer; for the parallel version, see writer_parallel.go

*/

// go build -o go_writer
// time ./go_writer && tail -10 ~/test_dummy_data/write_benchmark/go_generated.csv

func Writer_V2() {

	tot_rows := 1000000000
	//tot_rows = 10005

	buffer_size := 1 * 1024 * 1024

	start_ts := time.Now()

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/write_benchmark/go_generated.csv"

	file, _ := os.Create(f_path)
	defer file.Close()

	writer := bufio.NewWriterSize(file, buffer_size)
	defer writer.Flush()

	var buffer []byte

	// the nice thing about bufio writer is we dont need to calc batch sizes and flushes. it will flush when buffer gets full
	for i := 1; i <= tot_rows; i++ {

		// this first part appends the integer to the beginning of the byte buffer
		buffer = strconv.AppendInt(buffer[:0], int64(i), 10)

		//appends a new line
		buffer = append(buffer, '\n')

		//write the buffer to the bufio stream writer
		_, err := writer.Write(buffer)
		if err != nil {
			fmt.Println("Error writing data buffer to file: ", err)
			return
		}

		// u only need this if the first opp was not to do the buffer[:0] which effectively clears it
		//buffer = buffer[:0]
	}

	elapsed_time := time.Since(start_ts).Seconds()
	msg := fmt.Sprintf("Single Thread Benchmark: Elapsed Time (v2) using Go Lang to process %s rows: %.2f seconds", format_nbr_with_commas(tot_rows), elapsed_time)
	fmt.Println(msg)
}
