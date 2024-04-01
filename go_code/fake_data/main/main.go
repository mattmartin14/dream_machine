package main

// docs here: https://pkg.go.dev/github.com/bxcodec/faker/v3

/*
	to do:
		update this to be a cobra package that accepts the rows_to_write as a flag

*/

import (
	fake_data "fake_data/app"
	"fmt"
)

func main() {

	fake_data.Write_fake_data(500000)
	fmt.Println("Process complete")

}
