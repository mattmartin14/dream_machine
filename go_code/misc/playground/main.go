package main

import (
	"fmt"
	"sync"
)

type peep struct {
	firstName string
	lastName  string
	age       uint8
}

func main() {

	// simple for loop
	for i := 0; i <= 5; i++ {
		fmt.Printf("%d\n", i)
	}

	//maps are like dicts or hashmaps in other languages
	s1 := make(map[int]int)

	s1[2] = 3
	s1[1] = 4

	for key, val := range s1 {
		fmt.Printf("key is %d, val is %d\n", key, val)
	}

	// example using go routines to kick off stuff in parallel; uses an anonymous function
	numWorkers := 12
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 1; i <= numWorkers; i++ {
		go func(i_in int) {
			defer wg.Done()
			fmt.Println(i_in)
		}(i)
	}

	// this waits for all go routines to finish
	wg.Wait()

	p1 := peep{}

	p1.firstName = "bob"
	p1.lastName = "sanders"
	p1.age = 5

	fmt.Printf("first name is %s, last name is %s, age is %d\n", p1.firstName, p1.lastName, p1.age)

}
