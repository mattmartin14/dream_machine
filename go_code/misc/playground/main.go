package main

import (
	"fmt"
	"sync"
)

func main() {
	for i := 0; i <= 5; i++ {
		fmt.Printf("%d\n", i)
	}

	s1 := make(map[int]int)

	s1[2] = 3
	s1[1] = 4

	for key, val := range s1 {
		fmt.Printf("key is %d, val is %d\n", key, val)
	}

	numWorkers := 2
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 1; i <= numWorkers; i++ {
		go func(i int) {
			defer wg.Done()
			fmt.Println(i)
		}(i)
	}

	wg.Wait()

}
