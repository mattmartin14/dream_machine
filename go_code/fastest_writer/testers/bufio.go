package testers

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"time"
)

func Bufio_test(tot_rows int, buffer_size_mb int) {

	start_ts := time.Now()

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/perf_testing/bufio_test.csv"

	file, _ := os.Create(f_path)
	defer file.Close()

	alloc_amt := buffer_size_mb * 1024 * 1024
	writer := bufio.NewWriterSize(file, alloc_amt)
	defer writer.Flush()

	var buffer bytes.Buffer

	buffer_rows := (buffer_size_mb * 1024 * 1024) / 4

	row_cnt := 0
	newline := "\n"
	for i := 1; i <= tot_rows; i++ {

		// more efficient to run to write string operations vs. a + concatenation
		buffer.WriteString(strconv.Itoa(i))
		buffer.WriteString(newline)

		row_cnt += 1

		//flush the buffer to disk if we hit the max size
		if row_cnt >= buffer_rows || i == tot_rows {
			_, err := buffer.WriteTo(writer)
			if err != nil {
				fmt.Println("Error writing data buffer to file: ", err)
				return
			}
			//buffer.Reset()
			row_cnt = 0
		}

	}

	Elapsed_time(start_ts, tot_rows, "Buf IO")

}
