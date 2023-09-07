package others

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"time"
)

func Harness_v2() {
	tot_rows := 1000000000
	batch_size := 100000
	init_buffer_size := 10 * 1024 * 1024

	start_ts := time.Now()

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/write_benchmark/go_generated.csv"

	file, _ := os.Create(f_path)
	defer file.Close()

	writer := bufio.NewWriterSize(file, init_buffer_size)
	defer writer.Flush()

	var buffer []byte
	//newline := []byte{'\n'}

	for i := 1; i <= tot_rows; i += batch_size {
		start_row := i
		end_row := start_row + batch_size - 1
		if end_row > tot_rows {
			end_row = tot_rows
		}

		for j := start_row; j <= end_row; j++ {
			buffer = strconv.AppendInt(buffer[:0], int64(j), 10)
			buffer = append(buffer, '\n')

			_, err := writer.Write(buffer)
			if err != nil {
				fmt.Println("Error writing data buffer to file: ", err)
				return
			}

			buffer = buffer[:0] // Clear the buffer
		}
	}

	elapsed_time := time.Since(start_ts).Seconds()
	msg := fmt.Sprintf("Elapsed Time using Go Lang to process %s rows: %.2f seconds", format_nbr_with_commas(tot_rows), elapsed_time)
	fmt.Println(msg)
}

func format_nbr_with_commas(rows int) string {
	return strconv.FormatInt(int64(rows), 10)
}
