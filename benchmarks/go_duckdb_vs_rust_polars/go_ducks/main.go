package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {

	start := time.Now()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	sql := `
		COPY 
		(
			SELECT FIRSTNAME AS FIRST_NAME
				, CURRENT_TIMESTAMP AS PROCESS_TS
				, COUNT(DISTINCT TXNKEY) AS KEY_CNT
				, SUM(NETWORTH) AS NET_WORTH_AMT
			FROM read_csv_auto('~/test_dummy_data/fd/test_data*.csv')
			GROUP BY 1
		) TO '~/test_dummy_data/fd/ducks.parquet'
		(FORMAT PARQUET)
	`

	_, err = db.Exec(sql)
	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start)

	fmt.Println("Export complete")
	fmt.Printf("Elapsed time: %v ms\n", elapsed.Milliseconds())

}
