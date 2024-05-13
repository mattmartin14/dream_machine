package main

import (
	"fmt"
	"pg1/app"
)

// to build: go build -o whatever_u_want_to_call_it e.g. go build -o runner

func main() {
	fmt.Println("test")
	x := app.Run_stuff(1, 4, 6)
	fmt.Println(x)
}
