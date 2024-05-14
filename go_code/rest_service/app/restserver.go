package app

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type Message struct {
	Text string `json:"text"`
}

func LaunchRestServer() {

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleHome(w, r)
	})

	http.HandleFunc("/getjokeapi", func(w http.ResponseWriter, r *http.Request) {
		getJokeApi(w, r)
	})

	http.HandleFunc("/getjokedb", func(w http.ResponseWriter, r *http.Request) {
		getJokeDb(w, r)
	})

	port := ":8080"

	fmt.Printf("Starting server on %s\n", port)
	http.ListenAndServe(port, nil)
}

// update to do joke api direct, joke db

func getJokeApi(w http.ResponseWriter, r *http.Request) {

	j, err := GetCnJokeApi()
	if err != nil {
		err_msg := fmt.Sprintf("Failed to fetch joke from API: %v", err)
		http.Error(w, err_msg, http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "(Via Live API Call), Here is your daily dose of Chuck Norris jokes:\n\n%s", j)
}

func getJokeDb(w http.ResponseWriter, r *http.Request) {

	j, err := GetCnJokeDb()
	if err != nil {
		err_msg := fmt.Sprintf("Failed to fetch joke from Database: %v", err)
		http.Error(w, err_msg, http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "(Via Database Fetch), Here is your daily dose of Chuck Norris jokes:\n\n%s", j)
}

// default
func handleHome(w http.ResponseWriter, r *http.Request) {
	message := Message{
		Text: "Demo Go Lang Web Request Server",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(message)
}
