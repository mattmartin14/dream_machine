package main

// docs here: https://pkg.go.dev/github.com/bxcodec/faker/v3

import (
	fake_data "fake_data/app"
	"fmt"
)

func main() {

	fake_data.Write_fake_data(500000)
	fmt.Println("Process complete")

}
