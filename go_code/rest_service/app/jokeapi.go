package app

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func GetCnJokeApi() (joke string, err error) {
	url := "https://api.chucknorris.io/jokes/random"
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("error making GET request: %v", err)
	}
	defer resp.Body.Close()

	var j struct {
		Joke string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&j); err != nil {
		return "", fmt.Errorf("error parsing JSON: %v", err)
	}
	return j.Joke, nil
}
