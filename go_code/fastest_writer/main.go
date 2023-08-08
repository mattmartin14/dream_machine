package main

import (
	"fastest_writer/testers"
	"flag"
	"fmt"
	"time"
)

func elapsed_time_msg(harness_type string, start_ts time.Time, end_ts time.Time) string {

	elapse_time := end_ts.Sub(start_ts).Seconds()

	msg := fmt.Sprintf("Elapsed Time for [%s] harness: %.2f seconds\n", harness_type, elapse_time)
	return msg
}

func main() {

	tot_rows := flag.Int("rows", 0, "How many rows you want to generate")
	buffer_size := flag.Int("buffer_size_mb", 0, "How many megabytes to allocate to the buffer")

	flag.Parse()

	var start_ts time.Time
	var end_ts time.Time

	start_ts = time.Now()
	testers.Csv_encoder(*tot_rows, *buffer_size)
	end_ts = time.Now()
	fmt.Print(elapsed_time_msg("CSV Encoder", start_ts, end_ts))

	start_ts = time.Now()
	testers.Byte_buffer(*tot_rows, *buffer_size)
	end_ts = time.Now()
	fmt.Print(elapsed_time_msg("Byte Buffer", start_ts, end_ts))

	start_ts = time.Now()
	testers.Alloc_byte_buffer(*tot_rows, *buffer_size)
	end_ts = time.Now()
	fmt.Print(elapsed_time_msg("Pre-Allocated Byte Buffer", start_ts, end_ts))

	start_ts = time.Now()
	testers.Bufio_test(*tot_rows, *buffer_size)
	end_ts = time.Now()
	fmt.Print(elapsed_time_msg("Byte Buffer with BufIO", start_ts, end_ts))

	fmt.Println("Done")

}
