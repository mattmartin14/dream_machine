package main

/*
	Author: Matt Martin
	Date: 2023-07-20
	Desc: writes dummy data to csv using batches and go routines

	next steps: make another version of this parallel processing that moves the row building process inside the go routine
	-- we got a 18% improvement using go routines, but curious when we move the compute of data to a go routine to see how much of a lift we can get
		-- it will need to track the row index start/end
			-- loop will be for i = start, i <= end, i ++
		-- need a single rec writer
		-- need a multi rec writer for last batch?

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

// roughly 500 mb for 1m rows
/*
	batch size of 2000000:  229 seconds
	batch size of 1000:     234 seconds
	batch size of 10000000: 238 seconds


*/
const batch_size int = 2000000

func write_recs(wg *sync.WaitGroup, f_path string, recs [][]string) {
	defer wg.Done()

	file, err := os.OpenFile(f_path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file for write: ", err)
	}

	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	writer.WriteAll(recs)
	if err := writer.Error(); err != nil {
		fmt.Println("Error writing row to csv: ", err)
	}

}

func main() {

	max_rows := flag.Int("rows", 0, "How many rows you want to generate")

	flag.Parse()

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/dummy_data_parallel.csv"

	// kill file if exists
	err := os.Remove(f_path)

	var wg sync.WaitGroup

	People := helpers.Get_people()

	start_ts := time.Now()

	// apply headers
	var headers [][]string
	headers = append(headers, []string{"index", "first_name", "last_name", "last_mod_dt"})
	wg.Add(1)
	write_recs(&wg, f_path, headers)

	var batch_recs [][]string
	for i := 1; i <= *max_rows; i++ {

		rec := []string{
			strconv.Itoa(i),
			helpers.Get_random_name(People, "first_name"),
			helpers.Get_random_name(People, "last_name"),
			helpers.Get_random_date(),
		}

		batch_recs = append(batch_recs, rec)

		if len(batch_recs) > batch_size {
			wg.Add(1)
			go write_recs(&wg, f_path, batch_recs)
			batch_recs = nil
		}
	}

	// last batch
	if len(batch_recs) > 0 {
		//fmt.Printf("writing last batch with size %d\n", len(batch_recs))
		wg.Add(1)
		go write_recs(&wg, f_path, batch_recs)
		batch_recs = nil
	}

	wg.Wait()

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
