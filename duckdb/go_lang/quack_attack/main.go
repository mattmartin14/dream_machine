package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/duckdb/duckdb-go/v2"
)

func exec_sql(db *sql.DB, query string) {
	_, err := db.Exec(query)
	if err != nil {
		log.Fatalf("exec query: %v", err)
	}
}

func main() {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	exec_sql(db, `
		install quack from core_nightly; load quack;
		create secret (type quack, token 'yolo');
		attach 'quack:localhost' as db1
	`)

	for i := 0; i < 10; i++ {
		exec_sql(db, fmt.Sprintf("insert into db1.data values (%d)", i))
		fmt.Printf("inserted value %d to table data\n", i)
	}

	rows, err := db.Query("SELECT * from db1.data ORDER BY id")
	if err != nil {
		log.Fatalf("query rows: %v", err)
	}
	defer rows.Close()

	fmt.Println("data:")
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			log.Fatalf("scan row: %v", err)
		}
		fmt.Printf("%d\n", id)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("rows iteration: %v", err)
	}
}
