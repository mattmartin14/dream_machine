package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type chuck_joke struct {
	Joke       string `json:"value"`
	Created_at string `json:"created_at"`
}

func main() {

	url := "https://api.chucknorris.io/jokes/random"

	response, err := http.Get(url)
	if err != nil {
		fmt.Println("Error making GET request:", err)
		return
	}
	defer response.Body.Close()

	// Read the response body
	body, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Error reading response:", err)
		return
	}

	//var data map[string]interface{}

	joke := chuck_joke{}
	err = json.Unmarshal(body, &joke)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		return
	}

	// grab json elements

	// joke := data["value"]
	// crt_dt := data["created_at"]

	fmt.Println(joke.Joke)
	fmt.Println(joke.Created_at)

	//fmt.Println(string(body))

}
