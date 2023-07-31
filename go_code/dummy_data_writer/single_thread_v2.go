package main

/*
	Author: Matt Martin
	Date: 2023-07-30
	Desc: writes dummy data to csv in single loop
		this version uses a byte buffer
		-- wrote 1B rows in 183 seconds...so way faster than the encoding/csv package
*/

import (
	"bytes"
	"dummy_data_writer/helpers"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func main() {

	max_rows := flag.Int("rows", 0, "How many rows you want to generate")

	flag.Parse()

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/dummy_data_single_thread.csv"

	rand_src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(rand_src)

	file, err := os.Create(f_path)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	var buffer bytes.Buffer

	People := helpers.Get_people()

	start_ts := time.Now()

	// apply headers
	headers := "index,first_name,last_name,last_mod_dt\n"
	buffer.WriteString(headers)

	// did 1B in 183 seconds on buffer size of 100000
	var max_buffer_size int64 = 50000
	var row_cnt int64 = 0
	for i := 1; i <= *max_rows; i++ {

		rec := strconv.Itoa(i) + "," + helpers.Get_random_name(*r, People, "first_name") + "," +
			helpers.Get_random_name(*r, People, "first_name") + "," +
			helpers.Get_random_date(*r) + "\n"

		buffer.WriteString(rec)
		row_cnt += 1

		//flusht the buffer to disk if we hit the max size
		if row_cnt >= max_buffer_size || i == *max_rows {
			if _, err := io.Copy(file, &buffer); err != nil {
				return
			}
			buffer.Reset()
			row_cnt = 0
		}
	}

	end_ts := time.Now()
	elapsed_time := end_ts.Sub(start_ts).Seconds()

	fileInfo, err := os.Stat(f_path)
	if err != nil {
		fmt.Println("Error getting stats on file: ", err)
	}

	fsize := fileInfo.Size()
	fSizeFriendly := helpers.Format_file_size(fsize)

	rows_friendly := helpers.Format_nbr_with_commas(*max_rows)

	fmt.Printf("File '%s' written with %s rows. File size is %s. Total time to process: %.2f seconds\n", f_path, rows_friendly, fSizeFriendly, elapsed_time)

}
