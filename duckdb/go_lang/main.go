package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/duckdb/duckdb-go/v2"
)

func main() {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		COPY 
		(
			FROM (VALUES (1, 'foo'), (2, 'bar')) AS t(id, val)
		) TO 'data.parquet' ;
	`)
	if err != nil {
		log.Fatalf("create table: %v", err)
	}

	rows, err := db.Query("SELECT id, val FROM 'data.parquet' ORDER BY id")
	if err != nil {
		log.Fatalf("query rows: %v", err)
	}
	defer rows.Close()

	fmt.Println("data:")
	for rows.Next() {
		var id int
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			log.Fatalf("scan row: %v", err)
		}
		fmt.Printf("%d: %s\n", id, val)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("rows iteration: %v", err)
	}
}
