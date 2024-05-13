package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"slicers/app"
)

/*
	Both test1 - using byte slicers
		and test2 - using byte buffer seem to run just as fast

		the byte buffer seems to have a slight edge

*/

func main() {

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	app.Test1()
	//app.Test2()
}
