package main

/*

	Author: Matt Martin
	Date: 9/17/2023
	Desc: tests serving up web requests
	Note: had to redirect port to 8081 because airflow is on 8080

	to terminate the program listener in vs code, press control+c

*/

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type Message struct {
	Text string `json:"text"`
}

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGet(w, r)
		case http.MethodPost:
			handlePost(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	http.HandleFunc("/test123", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handle123(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	//using 8081 because 8080 has airflow on it
	port := ":8081"

	fmt.Printf("Starting server on %s\n", port)
	http.ListenAndServe(port, nil)
}

func handle123(w http.ResponseWriter, r *http.Request) {
	message := Message{
		Text: "subpath test 1",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(message)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	message := Message{
		Text: "Hello, World! 123",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(message)
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	var message Message

	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Assuming successful processing
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Received message: %s", message.Text)
}
