package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"

	_ "github.com/marcboeker/go-duckdb"
)

/*
	work on a wrapper/error check function to reduce lines of if nil blah

*/

type peep struct {
	firstName string
	lastName  string
	age       uint8
}

func check_err(err_step_prefix string, err error) {
	if err != nil {
		log.Fatalln("Error", err_step_prefix, "\n\t>>> ", err)
	}
}

func main() {

	// simple for loop
	for i := 0; i <= 5; i++ {
		fmt.Printf("%d\n", i)
	}

	//maps are like dicts or hashmaps in other languages
	s1 := make(map[int]int)

	s1[2] = 3
	s1[1] = 4
	s1[3] = 42

	for key, val := range s1 {
		fmt.Printf("key is %d, val is %d\n", key, val)
	}

	// example using go routines to kick off stuff in parallel; uses an anonymous function
	numWorkers := 7
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 1; i <= numWorkers; i++ {
		go func(i_in int) {
			defer wg.Done()
			fmt.Println(i_in)
		}(i)
	}

	// this waits for all go routines to finish
	wg.Wait()

	p1 := peep{}

	p1.firstName = "bob"
	p1.lastName = "sanders"
	p1.age = 5

	fmt.Printf("first name is %s, last name is %s, age is %d\n", p1.firstName, p1.lastName, p1.age)

	fmt.Println("test", "test2")

	phrase := "hello world"

	sl1 := phrase[0:5]

	sl2 := strings.TrimSpace(phrase[5:])

	fmt.Println(sl1)
	fmt.Println(sl2)

	var err error
	var db *sql.DB

	db, err = sql.Open("duckdb", "?access_mode=READ_WRITE")
	check_err("Creating Duckdb Instance", err)

	defer db.Close()

	sql := "Create view v_csv_stuff as Select * from read_csv_auto('~/test_dummy_data/fd/*.csv')"
	_, err = db.Exec(sql)
	check_err("Creating View", err)

	parquet_path := "~/test_dummy_data/fd/data1.parquet"

	sql = "COPY (SELECT * FROM v_csv_stuff) TO '" + parquet_path + "' (FORMAT PARQUET);"
	_, err = db.Exec(sql)
	check_err("Exporting data to parquet", err)

}
