package main

import (
	"fmt"
	"sync"
)

type peep struct {
	first_name string
	last_name  string
	age        int32
	age2       uint8
}

func main() {
	fmt.Println("Yolo!")

	for i := 0; i <= 10; i++ {
		fmt.Printf("test %d\n", i)
	}

	z := make(map[string]float32)

	z["a"] = 3.14
	z["b"] = 5.41

	for key, val := range z {
		fmt.Println(key, val)
	}

	var wg sync.WaitGroup

	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fmt.Println("xx")
		}()
	}

	wg.Wait()

	p1 := peep{}
	p1.first_name = "Ted"
	p1.last_name = "Smith"
	p1.age = 42
	p1.age2 = 255

	fmt.Printf("First name is %s, last name is %s, age is %d, age2 is %d\n", p1.first_name, p1.last_name, p1.age, p1.age2)
}
