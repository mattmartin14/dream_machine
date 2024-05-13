package app

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	_ "github.com/lib/pq"
)

//go get github.com/lib/pq

type chuck_joke struct {
	Joke       string `json:"value"`
	Created_at string `json:"created_at"`
}

func GetCNJokeCache() (string, error) {
	db, err := sql.Open("postgres", "postgresql://localhost/testdb1?sslmode=disable")
	if err != nil {
		return "", err
	}
	defer db.Close()

	// q := "create table test_sch1.cn_jokes (joke_txt varchar(1000), jk_ts timestamp)"
	// db.Query(q)

	rows, err := db.Query("SELECT joke_txt FROM test_sch1.cn_jokes")
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

func GetCNJokeLive() (string, error) {
	url := "https://api.chucknorris.io/jokes/random"

	response, err := http.Get(url)
	if err != nil {
		fmt.Println("Error making GET request: ", err)
		return "", err
	}
	defer response.Body.Close()

	// Read the response body
	body, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Error reading response from CN Joke API: ", err)
		return "", err
	}

	joke := chuck_joke{}
	err = json.Unmarshal(body, &joke)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		return "", err
	}

	return joke.Joke, nil

}
