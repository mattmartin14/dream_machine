package main

/*
	Author: Matt Martin
	Date: 2023-07-20
	Desc: Writes a bunch of dummy data to a CSV File

	-- next steps:
		move the data fetch functions into separate package and reorg the code
*/

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const people_data string = "./data/people.json"

var people_data_size int

type Person struct {
	First_name string `json:"first_name"`
	Last_name  string `json:"last_name"`
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func get_random_name(list []Person, nm_type string) string {
	rand_index := rand.Intn(people_data_size)
	if nm_type == "first_name" {
		return list[rand_index].First_name
	} else {
		return list[rand_index].Last_name
	}

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

	max_rows := flag.Int("rows", 0, "How many rows you want to generate")

	flag.Parse()

	start_ts := time.Now()

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/dummy_data2.csv"

	file, err := os.Create(f_path)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	js_file, err := os.Open(people_data)
	if err != nil {
		fmt.Println("Error open json data file:", err)
		return
	}

	byteData, err := ioutil.ReadAll(js_file)

	var People []Person
	err = json.Unmarshal(byteData, &People)
	if err != nil {
		fmt.Println("Error mapping json data to struct:", err)
		return
	}

	people_data_size = len(People)

	//append headers
	headers := []string{"index", "first_name", "last_name", "last_mod_dt"}
	writer.Write(headers)

	// size_choices := make(map[string]int)
	// size_choices["1 billion"] = 1000000000
	// size_choices["1 million"] = 1000000
	// size_choices["10 million"] = 10000000
	// size_choices["10k"] = 1000

	//max_iterations := size_choices["10 million"]

	for i := 1; i <= *max_rows; i++ {
		rec := []string{
			strconv.Itoa(i),
			get_random_name(People, "first_name"),
			get_random_name(People, "last_name"),
			get_random_date(),
		}
		writer.Write(rec)
	}

	end_ts := time.Now()

	fileInfo, err := os.Stat(f_path)
	if err != nil {
		fmt.Println("Error getting stats on file: ", err)
	}

	fsize := fileInfo.Size()

	// inline function to format the file size
	fSizeFriendly := func(fsize int64) string {
		const (
			//shortcut to 2 raised to power of ###; e.g. first one KB is 2^10 which is 1024
			// could have also written as KB = 1024, MB = (1024 * 1024) etc.
			KB = 1 << 10
			MB = 1 << 20
			GB = 1 << 30
		)

		switch {
		case fsize >= GB:
			return fmt.Sprintf("%.2f GB", float64(fsize)/GB)
		case fsize >= MB:
			return fmt.Sprintf("%.2f MB", float64(fsize)/MB)
		case fsize >= KB:
			return fmt.Sprintf("%.2f KB", float64(fsize)/KB)
		default:
			return fmt.Sprintf("%d bytes", fsize)
		}
	}

	//inline function to convert the int to a comma separated string for readability := Syntax Sugar :-)
	commaSepNbr := func(rows int) string {
		rows_str := strconv.Itoa(rows)

		for i := len(rows_str) - 3; i > 0; i -= 3 {
			rows_str = rows_str[:i] + "," + rows_str[i:]
		}
		return rows_str
	}

	elapsed_time := end_ts.Sub(start_ts).Seconds()
	fmt.Printf("File '%s' written with %s rows. File size is %s. Total time to process: %.2f", f_path, commaSepNbr(*max_rows), fSizeFriendly(fsize), elapsed_time)

}
