package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"
)

func fetchURL(wg *sync.WaitGroup, url string) {
	defer wg.Done() // Decrement the counter when the goroutine completes
	response, err := http.Get(url)
	if err != nil {
		fmt.Printf("Failed to fetch URL %s: %v\n", url, err)
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Printf("Failed to read response body from URL %s: %v\n", url, err)
		return
	}
	fmt.Printf("Response from URL %s: %s\n", url, body[:250])
}

func main() {
	var wg sync.WaitGroup
	urls := []string{
		"http://www.google.com",
		"http://www.yahoo.com",
		"http://www.espn.com",
		"http://www.engadget.com",
		"http://www.space.com",
	}

	for _, url := range urls {
		wg.Add(1)
		go fetchURL(&wg, url)
	}

	wg.Wait()
	fmt.Println("All web requests completed.")
}
