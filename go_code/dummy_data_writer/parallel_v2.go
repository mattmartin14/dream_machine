package main

/*
	Author: Matt Martin
	Date: 2023-07-20
	Desc: does parallel writing to multiple files via go routines and channels
		-- seems crazy slow vs the other parallel writer that pre-processes the data before going into a go routine thread
		-- to write 100M rows in this version to 100 files took 80 seconds
		-- when i lowered it to 6 files, it ran in 60 seconds
		-- 5 files, it runs in 45 seconds; still double the other
		-- not sure if there is some magic file number/batch size i need to be targeting
		--- checked chat gpt and it appears you can control the number of concurrent go routines via a channel/semiphone
		-- might want to look into that

		-- i think there is memory pressure because as they are all building their arrays up

		-- with the reg parallel version, it takes 23 seconds
			-- now it runs in 18 seconds...i optimized the helper date function to initialize some stuff at the beginning
*/

import (
	"dummy_data_writer/helpers"
	"encoding/csv"
	"flag"
	"fmt"
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

func write_recs_v2(wg *sync.WaitGroup, start_row int, end_row int, batch_nbr int, results chan<- written_file) {

	defer wg.Done()

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

	var batch_recs [][]string = nil

	// need to clock this build time for the rand stuff

	for i := start_row; i <= end_row; i++ {

		rec := []string{
			strconv.Itoa(i),
			helpers.Get_random_name(People, "first_name"),
			helpers.Get_random_name(People, "last_name"),
			helpers.Get_random_date(),
		}

		batch_recs = append(batch_recs, rec)
	}
	start_ts := time.Now()
	fmt.Printf("Starting to write batch %d to file, rows %d to %d\n", batch_nbr, start_row, end_row)
	writer.WriteAll(batch_recs)
	if err := writer.Error(); err != nil {
		item.err = err
		fmt.Println("Error writing row to csv: ", err)
	}
	end_ts := time.Now()
	elapsed_time := end_ts.Sub(start_ts).Seconds()

	// based on these outputs, time to write a huge chunk of data to csv is very fast
	// so its really just memory pressure we are dealing with here
	fmt.Printf("Batch %d written. Time to write to file: %.2f seconds\n", batch_nbr, elapsed_time)

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

	total_batches := *total_files

	//total_batches := *row_cnt / rows_per_batch

	fmt.Printf("Total batches to process: %d\n", total_batches)

	wg.Add(total_batches)

	var start_row int = 1
	var end_row int

	for i := 1; i <= total_batches; i++ {
		//fmt.Println("Entered loop for batch loading")
		// calc the start and end row
		if i > 1 {
			start_row = (i * rows_per_batch) - (rows_per_batch - 1)
		}

		end_row = i * rows_per_batch

		go write_recs_v2(&wg, start_row, end_row, i, results)
	}

	// last batch (if any)
	if *row_cnt%rows_per_batch != 0 || *row_cnt < rows_per_batch {
		fmt.Print("Entered last batch to load\n")

		if total_batches >= 1 {
			start_row = (total_batches*rows_per_batch + 1)
		}

		end_row = *row_cnt

		wg.Add(1)
		go write_recs_v2(&wg, start_row, end_row, total_batches+1, results)
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

	fmt.Printf("Total files writen: %d. Total rows: %s. Total time to process: %.2f seconds\n", total_batches, rows_friendly, elapsed_time)

}
