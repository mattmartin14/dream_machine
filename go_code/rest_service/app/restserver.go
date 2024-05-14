package app

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type Message struct {
	Joke   string `json:"joke"`
	Source string `json:"source"`
}

func LaunchRestServer() {

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleHome(w)
	})

	http.HandleFunc("/getjokeapi", func(w http.ResponseWriter, r *http.Request) {
		handleJokeApi(w)
	})

	http.HandleFunc("/getjokedb", func(w http.ResponseWriter, r *http.Request) {
		handleJokeDb(w)
	})

	port := ":8080"

	fmt.Printf("Starting server on %s\n", port)
	http.ListenAndServe(port, nil)
}

func handleJokeApi(w http.ResponseWriter) {

	j, err := GetCnJokeApi()
	if err != nil {
		err_msg := fmt.Sprintf("Failed to fetch joke from API: %v", err)
		http.Error(w, err_msg, http.StatusInternalServerError)
		return
	}

	message := Message{
		Joke:   j,
		Source: "Live API",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(message)

}

func handleJokeDb(w http.ResponseWriter) {

	j, err := GetCnJokeDb()
	if err != nil {
		err_msg := fmt.Sprintf("Failed to fetch joke from Database: %v", err)
		http.Error(w, err_msg, http.StatusInternalServerError)
		return
	}

	message := Message{
		Joke:   j,
		Source: "Database Fetch",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(message)

}

// default
func handleHome(w http.ResponseWriter) {
	message := Message{
		Joke: "Not a joke here; Demo Go Lang Web Request Server",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(message)
}
