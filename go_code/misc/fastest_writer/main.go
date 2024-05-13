package main

import (
	"fastest_writer/testers"
	"flag"
)

func main() {

	tot_rows := flag.Int("rows", 0, "How many rows you want to generate")
	buffer_size := flag.Int("buffer_size_mb", 0, "How many megabytes to allocate to the buffer")

	flag.Parse()

	testers.Csv_encoder(*tot_rows, *buffer_size)
	testers.Byte_buffer(*tot_rows, *buffer_size)
	testers.Alloc_byte_buffer(*tot_rows, *buffer_size)
	testers.Bufio_test(*tot_rows, *buffer_size)

}
