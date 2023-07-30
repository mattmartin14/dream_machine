package main

/*
	Author: Matt Martin
	Create Date: 2023-07-20
	Last Mod: 2023-07-30
	Desc: Creates dummy data csv files in parallel using go routines and channels
		current benchmark is 100M rows in 5 seconds using 10 files for parallel writing and a buffer
		-- 1B rows in 52 seconds
		-- single thread takes 209 seconds
		-- this version runs faster than single thread now
*/

import (
	"dummy_data_writer/helpers"
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

type written_file struct {
	file_name string
	batch_nbr int
	err       error
}

var People []helpers.Person

func init() {
	People = helpers.Get_people()
}

func write_recs(wg *sync.WaitGroup, start_row int, end_row int, batch_nbr int, results chan<- written_file) {

	defer wg.Done()

	rand_src := rand.NewSource(time.Now().UnixNano() + int64(batch_nbr))
	r := rand.New(rand_src)

	var headers []string = []string{"index", "first_name", "last_name", "last_mod_dt"}

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/multi_files/dummy_data_parallel_batch_" + strconv.Itoa(batch_nbr) + ".csv"

	item := written_file{
		file_name: f_path,
		batch_nbr: batch_nbr,
		err:       nil,
	}

	file, err := os.Create(f_path)
	if err != nil {
		item.err = err
		fmt.Println("Error creating file: ", err)
		return
	}

	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// apply headers
	writer.Write(headers)

	var buffer [][]string = nil

	//keeps memory pressure low; i'm sure there is a way to optimize this calc and look at the load on the machine
	var max_rows_per_buffer int = 50000

	for i := start_row; i <= end_row; i++ {

		rec := []string{
			strconv.Itoa(i),
			helpers.Get_random_name(*r, People, "first_name"),
			helpers.Get_random_name(*r, People, "last_name"),
			helpers.Get_random_date(*r),
		}

		buffer = append(buffer, rec)

		if len(buffer) >= max_rows_per_buffer || i == end_row {
			writer.WriteAll(buffer)
			buffer = nil
		}
		if err := writer.Error(); err != nil {
			item.err = err
			fmt.Println("Error writing row to csv: ", err)
		}

	}

	//load the item to the channel
	results <- item

}

func main() {

	row_cnt := flag.Int("rows", 0, "How many rows you want to generate")
	total_files := flag.Int("files", 0, "How many rows you want to generate")

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
			fmt.Printf("Successfully processed batch %d\n", item.batch_nbr)
		}
	}

	end_ts := time.Now()
	elapsed_time := end_ts.Sub(start_ts).Seconds()

	rows_friendly := helpers.Format_nbr_with_commas(*row_cnt)

	fmt.Printf("Total files writen: %d. Total rows: %s. Total time to process: %.2f seconds\n", *total_files, rows_friendly, elapsed_time)

}
