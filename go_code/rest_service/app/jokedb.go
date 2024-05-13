package app

import (
	"database/sql"

	_ "github.com/lib/pq"
)

//go get github.com/lib/pq

func GetCnJokeDb() (string, error) {
	db, err := sql.Open("postgres", "postgresql://localhost/testdb1?sslmode=disable")
	if err != nil {
		return "", err
	}
	defer db.Close()

	// q := "create table test_sch1.cn_jokes (joke_txt varchar(1000), jk_ts timestamp)"
	// db.Query(q)

	rows, err := db.Query("SELECT joke_txt FROM test_sch1.cn_jokes ORDER BY RANDOM() limit 1")
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var joke_txt string

	// Iterate over the rows
	for rows.Next() {

		err := rows.Scan(&joke_txt)
		if err != nil {
			return "", err
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	return joke_txt, nil
}
