package app

import (
	"database/sql"

	_ "github.com/lib/pq"
)

//go get github.com/lib/pq

func GetCnJokeDb() (joke string, err error) {
	db, err := sql.Open("postgres", "postgresql://localhost/testdb1?sslmode=disable")
	if err != nil {
		return
	}

	defer db.Close()

	err = db.QueryRow("SELECT joke_txt FROM test_sch1.cn_jokes ORDER BY RANDOM() LIMIT 1").Scan(&joke)
	if err != nil {
		return
	}

	return
}
