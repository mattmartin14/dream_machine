package utils

/*
	Author: Matt Martin
	Date: 2023-07-20
	Desc: Writes a bunch of dummy data to a CSV File
*/

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func get_random_string(arr []string) string {
	rand_index := rand.Intn(len(arr))
	return arr[rand_index]
}

func get_random_date() string {
	lower_bound := time.Date(2020, 1, 1, 0, 0, 0, 0, time.Local)
	upper_bound := time.Date(2023, 1, 1, 0, 0, 0, 0, time.Local)

	time_span := upper_bound.Sub(lower_bound)

	rand_span := time.Duration(rand.Int63n(int64(time_span)))

	rand_dt := lower_bound.Add(rand_span)

	//note: the 2006-01-02 thing is significant for go lang; its when the time package was written....
	return rand_dt.Format("2006-01-02")
}

func gen_data() {

	start_ts := time.Now()

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/dummy_data2.csv"

	file, err := os.Create(f_path)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}

	first_names_list := []string{
		"Bob", "Bill", "William", "Matt", "Matthew", "Jake", "Betsy", "George", "Phil", "Alex", "Lindsey", "Erin",
		"Robert", "Mackensy", "Blake", "Elvis", "Leon", "Randy", "Amy", "Courtney", "Lisa", "Debbie", "Daniel",
		"Ian", "Brad", "Bart", "Mariano", "Tom", "Greg", "John", "Manny", "Steph", "Clay", "Draymond", "Kevin", "Russell",
	}

	last_names_list := []string{
		"Jones", "Clinton", "Smith", "Martin", "James", "Stein", "Carter", "Pitt", "Jolie", "Clooney", "Roberts", "Damon",
		"Hanks", "Decaprio", "Parsons", "Ramirez", "Smoltz", "Glavine", "Maddux", "Ramiro", "Rivera", "Curry", "Thompson",
		"Johnson", "Greene", "Durant", "Wilson",
	}

	defer file.Close()

	one_mb := 1048576
	buffer_multiplier := 1
	buffer_size := buffer_multiplier * one_mb // default is 4096
	buffer_size = 4096
	fmt.Printf("Running on buffer size: %d\n", buffer_size)
	bufWriter := bufio.NewWriterSize(file, buffer_size)
	defer bufWriter.Flush()

	//writer := csv.NewWriter(file)

	writer := csv.NewWriter(bufWriter)
	// takes 26.439320 seconds to write a 100M row file with buf io
	// takes same amount of time to write 100M rows without the bufio writer
	/*
		need to maybe update the bufio writer to have a larger buffer than 4k
		-- currently at 100M rows and file size of 3.3GB
			its roughly 35 bytes per row
			so one buffer is roughly 115 rows
			and that equates to a total of 865k write calls
	*/
	defer writer.Flush()

	//append headers
	headers := []string{"index", "first_name", "last_name", "last_mod_dt"}
	writer.Write(headers)

	size_choices := make(map[string]int)
	size_choices["1 billion"] = 1000000000
	size_choices["1 million"] = 1000000
	size_choices["10 million"] = 10000000
	size_choices["10k"] = 1000

	max_iterations := size_choices["10 million"]

	for i := 1; i <= max_iterations; i++ {
		rec := []string{
			strconv.Itoa(i),
			get_random_string(first_names_list),
			get_random_string(last_names_list),
			get_random_date(),
		}
		writer.Write(rec)
	}

	end_ts := time.Now()

	elapsed_time := end_ts.Sub(start_ts).Seconds()
	fmt.Printf("Total time to process %d rows: %2f", max_iterations, elapsed_time)

}
