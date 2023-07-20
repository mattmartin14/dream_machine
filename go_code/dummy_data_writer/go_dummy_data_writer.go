package main

/*
	Author: Matt Martin
	Date: 2023-07-20
	Desc: Writes a bunch of dummy data to a CSV File
*/

import (
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

func main() {

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

	writer := csv.NewWriter(file)
	defer writer.Flush()

	//append headers
	headers := []string{"index", "first_name", "last_name", "last_mod_dt"}
	writer.Write(headers)

	max_iterations := 100000000

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
