package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	_ "github.com/lib/pq"
)

type chuck_joke struct {
	Joke       string `json:"value"`
	Created_at string `json:"created_at"`
}

func queryPg() error {
	db, err := sql.Open("postgres", "postgresql://localhost/testdb1?sslmode=disable")
	if err != nil {
		return err
	}
	defer db.Close()

	// q := "create table test_sch1.cn_jokes (joke_txt varchar(1000), jk_ts timestamp)"
	// db.Query(q)

	rows, err := db.Query("SELECT * FROM test_sch1.cn_jokes")
	if err != nil {
		return err
	}
	defer rows.Close()

	// Iterate over the rows
	for rows.Next() {
		var (
			joke_txt string
			jk_ts    string
		)
		err := rows.Scan(&joke_txt, &jk_ts)
		if err != nil {
			return err
		}
		fmt.Printf("joke: %s, joke created on: %s\n", joke_txt, jk_ts)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func loadJokeToPg(joke_txt string, created_at string) error {
	db, err := sql.Open("postgres", "postgresql://localhost/testdb1?sslmode=disable")
	if err != nil {
		return err
	}
	defer db.Close()

	q, err := db.Prepare("insert into test_sch1.cn_jokes (joke_txt, jk_ts) values ($1, $2)")
	if err != nil {
		return err
	}

	_, err = q.Exec(joke_txt, created_at)
	if err != nil {
		return err
	}

	return nil
}

func main() {

	err := queryPg()
	if err != nil {
		fmt.Println("Error running PG query: ", err)
		os.Exit(1)
	}

	url := "https://api.chucknorris.io/jokes/random"

	response, err := http.Get(url)
	if err != nil {
		fmt.Println("Error making GET request: ", err)
		os.Exit(1)
	}
	defer response.Body.Close()

	// Read the response body
	body, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Error reading response:", err)
		os.Exit(1)
	}

	joke := chuck_joke{}
	err = json.Unmarshal(body, &joke)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		os.Exit(1)
	}

	err = loadJokeToPg(joke.Joke, joke.Created_at)
	if err != nil {
		fmt.Println("Error loading joke: ", err)
		os.Exit(1)
	}

	// fmt.Println(joke.Joke)
	// fmt.Println(joke.Created_at)

}
