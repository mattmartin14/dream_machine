package main

/*
	This program follows the cobra best practices of:
		CLI name > Verb > Noun > Adjective
			found here: https://pkg.go.dev/github.com/spf13/cobra#section-readme


	go build -o fd

	example execution:
		fd create json --rows 50 --filename test.json
		fd create csv -- rows 10 --filename test1.csv

*/

import (
	cmd "fake_data/cmd"
	"fmt"
	"os"
)

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
