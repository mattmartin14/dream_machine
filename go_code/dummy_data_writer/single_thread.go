package main

/*
	Author: Matt Martin
	Date: 2023-07-20
	Desc: writes dummy data to csv in single loop
*/

import (
	"dummy_data_writer/helpers"
	"encoding/csv"
	"flag"
	"fmt"
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
	writer := csv.NewWriter(file)
	defer writer.Flush()

	People := helpers.Get_people()

	start_ts := time.Now()

	// apply headers
	headers := []string{"index", "first_name", "last_name", "last_mod_dt"}
	writer.Write(headers)

	// apply data
	for i := 1; i <= *max_rows; i++ {
		rec := []string{
			strconv.Itoa(i),
			helpers.Get_random_name(*r, People, "first_name"),
			helpers.Get_random_name(*r, People, "last_name"),
			helpers.Get_random_date(*r),
		}
		writer.Write(rec)
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
