package main

/*
	Author: Matt Martin
	Create Date: 2023-07-20
	Last Mod: 2023-07-30
	Desc: Creates dummy data csv files in parallel using go routines and channels
		current benchmark is 100M rows in 5 seconds using 10 files for parallel writing and a buffer
		-- version 1 with csv encoder does 1B rows in 52 seconds
		-- version 2 with a byte buffer does 1b rows 20 files wide in 25 seconds

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
	"sync"
	"time"
)

// keeps memory pressure low; i'm sure there is a way to optimize this calc and look at the load on the machine
// 100k seems reasonable when going 15 files wide for 1B rows
var max_rows_per_buffer int = 200000

type written_file struct {
	file_name    string
	batch_nbr    int
	total_rows   int
	start_time   time.Time
	end_time     time.Time
	elapsed_time float64
	err          error
}

var People []helpers.Person

func init() {
	People = helpers.Get_people()
}

func write_recs(wg *sync.WaitGroup, start_row int, end_row int, batch_nbr int, results chan<- written_file) {

	defer wg.Done()

	rand_src := rand.NewSource(time.Now().UnixNano() + int64(batch_nbr))
	r := rand.New(rand_src)

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/multi_files/dummy_data_parallel_batch_" + strconv.Itoa(batch_nbr) + ".csv"

	item := written_file{
		file_name:  f_path,
		batch_nbr:  batch_nbr,
		total_rows: (end_row - start_row),
		start_time: time.Now(),
		err:        nil,
	}

	file, err := os.Create(f_path)
	if err != nil {
		item.err = err
		fmt.Println("Error creating file: ", err)
		return
	}

	defer file.Close()

	var buffer bytes.Buffer

	//apply headers
	headers := "index,first_name,last_name,last_mod_dt\n"
	buffer.WriteString(headers)

	row_cnt := 0
	for i := start_row; i <= end_row; i++ {

		rec := strconv.Itoa(i) + "," + helpers.Get_random_name(*r, People, "first_name") + "," +
			helpers.Get_random_name(*r, People, "last_name") + "," +
			helpers.Get_random_date(*r) + "\n"

		buffer.WriteString(rec)
		row_cnt += 1

		//flush the buffer to disk if we hit the max size
		if row_cnt >= max_rows_per_buffer || i == end_row {
			_, err := io.Copy(file, &buffer)
			if err != nil {
				fmt.Println("Error writing data buffer to file: ", err)
				return
			}
			buffer.Reset()
			row_cnt = 0
		}

	}

	item.end_time = time.Now()
	item.elapsed_time = item.end_time.Sub(item.start_time).Seconds()

	//load the item to the channel
	results <- item

}

func main() {

	row_cnt := flag.Int("rows", 0, "How many rows you want to generate")
	total_files := flag.Int("files", 0, "How many files you want to generate")

	flag.Parse()

	var wg sync.WaitGroup

	results := make(chan written_file)

	start_ts := time.Now()

	// calculate total batches
	rows_per_batch := *row_cnt / *total_files

	fmt.Printf("Total batches to process: %d\n", *total_files)

	wg.Add(*total_files)

	var start_row int = 1
	var end_row int

	for i := 1; i <= *total_files; i++ {
		//fmt.Println("Entered loop for batch loading")
		// calc the start and end row
		if i > 1 {
			start_row = (i * rows_per_batch) - (rows_per_batch - 1)
		}

		//if we are on the last batch, add the tail (if any)
		if i == *total_files {
			end_row = *row_cnt
		} else {
			end_row = i * rows_per_batch
		}

		go write_recs(&wg, start_row, end_row, i, results)
	}
	// by not buffering the channel and wrapping the wait/close here, as the routines complete, they will read out
	go func() {
		wg.Wait()
		close(results)
	}()

	// process channel results
	for item := range results {
		if item.err != nil {
			fmt.Printf("Error processing batch %d: %v\n", item.batch_nbr, item.err)
		} else {
			fmt.Printf("Successfully processed batch %d with %d rows. Batch Processing Time: %.2f seconds\n", item.batch_nbr, item.total_rows, item.elapsed_time)
		}
	}

	end_ts := time.Now()
	elapsed_time := end_ts.Sub(start_ts).Seconds()

	rows_friendly := helpers.Format_nbr_with_commas(*row_cnt)

	fmt.Printf("Total files writen: %d. Total rows: %s. Total time to process: %.2f seconds\n", *total_files, rows_friendly, elapsed_time)

}
