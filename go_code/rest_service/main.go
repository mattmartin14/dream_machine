package main

import (
	"fmt"

	"rest_service/app"
)

func main() {

	app.LaunchRestServer()
	fmt.Println("success!")
}
