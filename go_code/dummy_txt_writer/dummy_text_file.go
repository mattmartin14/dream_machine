package main

/*
	Notes:
		Explore using a goroutine here to write in chunks in parallel to see if it goes any faster
			try with 4 workers first

		Also, since testing with CSV as well, might want to move the random strings picker and date picker to seperate module
*/

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
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
	f_path := work_dir + "/test_dummy_data/dummy_tab_data.txt"

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

	file, err := os.Create(f_path)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	one_mb := 1048576
	buffer_multiplier := 100
	buffer_size := buffer_multiplier * one_mb // default is 4096
	buffer_size = 4096
	fmt.Printf("Running on buffer size: %d bytes\n", buffer_size)
	//writer := bufio.NewWriterSize(file, buffer_size)
	writer := bufio.NewWriter(file)
	defer writer.Flush()

	//append headers
	//headers := []string{"index", "first_name", "last_name", "last_mod_dt"}
	fmt.Fprintf(writer, "index\tfirst_name\tlast_name\tlast_mod_dt\n")
	//writer.Write(headers)

	size_choices := make(map[string]int)
	size_choices["1 billion"] = 1000000000
	size_choices["1 million"] = 1000000
	size_choices["10 million"] = 10000000
	size_choices["100 million"] = 100000000
	size_choices["10k"] = 1000

	size_choice := "1 million"

	max_iterations := size_choices[size_choice]

	for i := 1; i <= max_iterations; i++ {
		fmt.Fprintf(writer, "%d\t%s\t%s\t%s\n", i,
			get_random_string(first_names_list),
			get_random_string(last_names_list),
			get_random_date(),
		)
	}

	end_ts := time.Now()

	elapsed_time := end_ts.Sub(start_ts).Seconds()
	fmt.Printf("Total time to process %s rows: %2f", size_choice, elapsed_time)
}
