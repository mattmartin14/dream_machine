package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/marcboeker/go-duckdb"
)

// go get github.com/marcboeker/go-duckdb

func check_err(err_step_prefix string, err error) {
	if err != nil {
		log.Fatalln("Error", err_step_prefix, "\n\t>>> ", err)
	}
}

func main() {

	db, err := sql.Open("duckdb", "")
	check_err("Opening Duckdb database", err)

	defer db.Close()

	sql := "Create view v_csv_stuff as Select * from read_csv_auto('~/test_dummy_data/fd/*.csv')"

	_, err = db.Exec(sql)
	check_err("Creating View", err)

	parquet_path := "~/test_dummy_data/fd/data.parquet"

	sql = "COPY (SELECT * FROM v_csv_stuff) TO '" + parquet_path + "' (FORMAT PARQUET);"

	_, err = db.Exec(sql)
	check_err("Exporting Results to parquet", err)

	// test sample query

	sql = "SELECT TxnKey, NetWorth FROM read_parquet('" + parquet_path + "') LIMIT 5"

	rows, err := db.Query(sql)
	check_err("Querying parquet results", err)

	defer rows.Close()
	for rows.Next() {
		var (
			TxnKey   string
			NetWorth float64
		)

		err := rows.Scan(&TxnKey, &NetWorth)
		check_err("Binding Query Result Values", err)

		fmt.Printf("Current Transaction Key is %s and net worth is %f\n", TxnKey, NetWorth)
	}

	check_err("Error fetching rows", err)

	fmt.Println("Export complete")

}
