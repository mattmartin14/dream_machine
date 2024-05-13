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
		switch r.Method {
		case http.MethodGet:
			handleGet(w, r)
		// case http.MethodPost:
		// 	handlePost(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	http.HandleFunc("/getjoke", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getjoke(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	port := ":8080"

	fmt.Printf("Starting server on %s\n", port)
	http.ListenAndServe(port, nil)
}

// update to do joke cached, joke live

func getjoke(w http.ResponseWriter, r *http.Request) {

	j, err := GetCNJokeLive()
	if err != nil {
		http.Error(w, "Failed to fetch joke", http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "%s", j)

	// if we wanted to serve as a json
	// message := Message{
	// 	Text: j,
	// }

	// w.Header().Set("Content-Type", "application/json")
	// json.NewEncoder(w).Encode(message)
}

// default
func handleGet(w http.ResponseWriter, r *http.Request) {
	message := Message{
		Text: "Hello, World! 123",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(message)
}

// func handlePost(w http.ResponseWriter, r *http.Request) {
// 	var message Message

// 	err := json.NewDecoder(r.Body).Decode(&message)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}

// 	// Assuming successful processing
// 	w.WriteHeader(http.StatusCreated)
// 	fmt.Fprintf(w, "Received message: %s", message.Text)
// }
