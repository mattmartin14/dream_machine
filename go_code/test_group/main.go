package main

/*
	main folder always requires a package "main"
	to make the module access in lower folders easy, always in the main folder initiate the module the same as the project folder name
	only the parent folder requires a go.mod file

	keep package names lowercase
	if you need to access a method in a sub package, make sure it starts with an upper case
	Go will automatically remove unused module imports. you cannot stop it

*/

import (
	"fmt"
	"test_group/writer"
)

func main() {
	fmt.Printf("test %s\n", "blah")
	writer.Test()
}
