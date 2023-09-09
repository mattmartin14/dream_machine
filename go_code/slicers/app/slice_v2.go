package app

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"time"
)

func Test2() {

	tot_rows := 1000000000
	batch_size := 10000
	init_buffer_size := 10 * 1024 * 1024

	start_ts := time.Now()

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/write_benchmark/go_generated.csv"

	file, _ := os.Create(f_path)
	defer file.Close()

	writer := bufio.NewWriterSize(file, init_buffer_size)
	defer writer.Flush()

	var buffer bytes.Buffer

	for i := 1; i <= tot_rows; i += batch_size {
		start_row := i
		end_row := min(start_row+batch_size-1, tot_rows)

		for j := start_row; j <= end_row; j++ {
			buffer.WriteString(strconv.FormatInt(int64(j), 10))
			buffer.WriteByte(',')
			buffer.WriteString("Test")
			buffer.WriteByte('\n')
		}

		_, err := writer.Write(buffer.Bytes())
		if err != nil {
			fmt.Println("Error writing data buffer to file: ", err)
			return
		}

		buffer.Reset() // Reset the buffer
	}

	elapsed_time := time.Since(start_ts).Seconds()
	msg := fmt.Sprintf("Elapsed Time using Go Lang to process %s rows: %.2f seconds", format_nbr_with_commas(tot_rows), elapsed_time)
	fmt.Println(msg)
}
