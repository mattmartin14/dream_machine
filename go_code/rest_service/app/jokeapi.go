package app

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func GetCnJokeApi() (string, error) {
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
