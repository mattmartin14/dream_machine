/*
Author: Matt Martin
Date: 2023-08-01
Desc: Demonstrates how doing multiple system calls (e.g. write to file) slows down perf
*/
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

func main() {

	row_cnt := flag.Int("rows", 0, "How many rows you want to generate")

	flag.Parse()

	start_ts := time.Now()

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/scb_every_iter.txt"

	file, err := os.Create(f_path)
	if err != nil {
		fmt.Println("Error creating file: ", err)
		return
	}

	defer file.Close()

	var buffer bytes.Buffer

	for i := 1; i <= *row_cnt; i++ {
		buffer.WriteString(strconv.Itoa(i) + "\n")
		//flush to file
		_, err2 := io.Copy(file, &buffer)
		buffer.Reset()
		if err2 != nil {
			fmt.Println("Error writing data buffer to file: ", err)
			return
		}
	}

	end_ts := time.Now()
	elapsed_time := end_ts.Sub(start_ts).Seconds()

	fmt.Printf("Total processing with %d system calls: %.2f seconds\n", *row_cnt, elapsed_time)

	//accumulate and flush the buffer test

	start_ts = time.Now()

	f_path2 := work_dir + "/test_dummy_data/scb_batch_iter.txt"

	file2, err := os.Create(f_path2)
	if err != nil {
		fmt.Println("Error creating file: ", err)
		return
	}

	defer file2.Close()

	var buffer2 bytes.Buffer
	var accumulator int = 0
	var max_rows_per_buffer int = 1000000

	flush_cnt := 0
	for i := 1; i <= *row_cnt; i++ {
		buffer2.WriteString(strconv.Itoa(i) + "\n")
		accumulator += 1

		if accumulator >= max_rows_per_buffer || i == *row_cnt {
			_, err2 := io.Copy(file2, &buffer2)
			if err2 != nil {
				fmt.Println("Error writing data buffer to file: ", err)
				return
			}
			flush_cnt++
			buffer2.Reset()
			accumulator = 0
		}

	}

	end_ts = time.Now()
	elapsed_time = end_ts.Sub(start_ts).Seconds()

	fmt.Printf("Total processing with %d system calls: %.2f seconds\n", flush_cnt, elapsed_time)

}
