package helpers

/*
	Author: Matt Martin
	Date: 2023-07-28
	Desc: helper functions for dummy data generator

*/

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Person struct {
	First_name string `json:"first_name"`
	Last_name  string `json:"last_name"`
}

func Get_random_date() string {
	lower_bound := time.Date(2020, 1, 1, 0, 0, 0, 0, time.Local)
	upper_bound := time.Date(2023, 1, 1, 0, 0, 0, 0, time.Local)

	time_span := upper_bound.Sub(lower_bound)

	rand_span := time.Duration(rand.Int63n(int64(time_span)))

	rand_dt := lower_bound.Add(rand_span)

	//note: the 2006-01-02 thing is significant for go lang; its when the time package was written....
	return rand_dt.Format("2006-01-02")
}

func Get_random_name(list []Person, nm_type string) string {
	rand_index := rand.Intn(len(list))
	if nm_type == "first_name" {
		return list[rand_index].First_name
	} else {
		return list[rand_index].Last_name
	}

}

func Get_people() []Person {

	var people_fpath string = "./data/people.json"

	js_file, err := os.Open(people_fpath)
	if err != nil {
		fmt.Println("Error open json data file:", err)
		return nil
	}

	byteData, err := ioutil.ReadAll(js_file)

	var People []Person
	err = json.Unmarshal(byteData, &People)
	if err != nil {
		fmt.Println("Error mapping json data to struct:", err)
		return nil
	}

	return People

}

func Format_file_size(fsize int64) string {
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

func Format_nbr_with_commas(rows int) string {
	rows_str := strconv.Itoa(rows)

	for i := len(rows_str) - 3; i > 0; i -= 3 {
		rows_str = rows_str[:i] + "," + rows_str[i:]
	}
	return rows_str
}
