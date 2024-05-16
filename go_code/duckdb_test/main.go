package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/marcboeker/go-duckdb"
)

// go get github.com/marcboeker/go-duckdb

func main() {

	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	sql := "Create view v_csv_stuff as Select * from read_csv_auto('~/test_dummy_data/fd/*.csv')"

	_, err = db.Exec(sql)
	if err != nil {
		log.Fatal(err)
	}

	parquet_path := "~/test_dummy_data/fd/data.parquet"

	sql = "COPY (SELECT * FROM v_csv_stuff) TO '" + parquet_path + "' (FORMAT PARQUET);"

	_, err = db.Exec(sql)
	if err != nil {
		log.Fatal(err)
	}

	// test sample query

	sql = "SELECT TxnKey, NetWorth FROM read_parquet('" + parquet_path + "') LIMIT 5"

	rows, err := db.Query(sql)
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()
	for rows.Next() {
		var (
			TxnKey   string
			NetWorth float64
		)

		err := rows.Scan(&TxnKey, &NetWorth)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Current Transaction Key is %s and net worth is %f\n", TxnKey, NetWorth)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Export complete")

}
