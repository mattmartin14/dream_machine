package app

/*
	Author: Matt Martin
	Create Date: 2023-09-14
	Last Mod: 2023-09-14
	Uses go routines to write 1B rows to 20 files in parallel. Can run in about 2.96 seconds

*/

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

// keeps memory pressure low; i'm sure there is a way to optimize this calc and look at the load on the machine
// 100k seems reasonable when going 15 files wide for 1B rows
//var max_rows_per_buffer int = 200000

var buffer_size int = 10 * 1024 * 1024
var max_workers int = 30
var row_cnt int = 1000000000
var total_files int = 30

// with 20 workers and 20 files, it did it in 2.96 seconds

type written_file struct {
	file_name    string
	batch_nbr    int
	total_rows   int
	start_time   time.Time
	end_time     time.Time
	elapsed_time float64
	err          error
}

func write_recs(wg *sync.WaitGroup, start_row int, end_row int, batch_nbr int,
	worker_pool chan struct{}, results chan<- written_file) {

	defer wg.Done()

	//ad a slot to the worker pool (bounded parallelism)
	worker_pool <- struct{}{}

	//fire the defer function at the end to release a worker
	defer func() { <-worker_pool }()

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/write_benchmark/go_parallel_batch_" + strconv.Itoa(batch_nbr) + ".csv"

	item := written_file{
		file_name:  f_path,
		batch_nbr:  batch_nbr,
		total_rows: (end_row - start_row + 1),
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

	writer := bufio.NewWriterSize(file, buffer_size)
	defer writer.Flush()

	var buffer []byte

	for i := start_row; i <= end_row; i++ {

		// this first part appends the integer to the beginning of the byte buffer
		buffer = strconv.AppendInt(buffer[:0], int64(i), 10)

		//appends a new line
		buffer = append(buffer, '\n')

		//write the buffer to the bufio stream writer
		_, err := writer.Write(buffer)
		if err != nil {
			item.err = err
			fmt.Println("Error writing data buffer to file: ", err)
			return
		}

	}

	item.end_time = time.Now()
	item.elapsed_time = item.end_time.Sub(item.start_time).Seconds()

	//load the item to the channel
	results <- item

}

func Parallel_Writer() {

	//set how many processes we want max in parallel
	worker_pool := make(chan struct{}, max_workers)

	var wg sync.WaitGroup

	results := make(chan written_file)

	start_ts := time.Now()

	// calculate total batches
	rows_per_batch := row_cnt / total_files

	fmt.Printf("Total batches to process: %d\n", total_files)

	wg.Add(total_files)

	var start_row int = 1
	var end_row int

	for i := 1; i <= total_files; i++ {
		//fmt.Println("Entered loop for batch loading")
		// calc the start and end row
		if i > 1 {
			start_row = (i * rows_per_batch) - (rows_per_batch - 1)
		}

		//if we are on the last batch, add the tail (if any)
		if i == total_files {
			end_row = row_cnt
		} else {
			end_row = i * rows_per_batch
		}

		go write_recs(&wg, start_row, end_row, i, worker_pool, results)
	}
	// by not buffering the channel and wrapping the wait/close here, as the routines complete, they will read out
	go func() {
		wg.Wait()
		close(results)
		close(worker_pool)
	}()

	// process channel results
	for item := range results {
		if item.err != nil {
			fmt.Printf("Error processing batch %d: %v\n", item.batch_nbr, item.err)
		} else {
			fmt.Printf("Successfully processed batch %d with %s rows. Batch Processing Time: %.2f seconds\n", item.batch_nbr, format_nbr_with_commas(item.total_rows), item.elapsed_time)
		}
	}

	end_ts := time.Now()
	elapsed_time := end_ts.Sub(start_ts).Seconds()

	rows_friendly := format_nbr_with_commas(row_cnt)

	fmt.Printf("Parallel Writer Benchmark: Total files writen: %d. Total rows: %s. Total time to process: %.2f seconds\n", total_files, rows_friendly, elapsed_time)

}
