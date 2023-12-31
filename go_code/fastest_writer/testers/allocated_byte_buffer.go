package testers

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

func Alloc_byte_buffer(tot_rows int, buffer_size_mb int) {

	start_ts := time.Now()

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/perf_testing/pre_alloc_byte_buffer_test.csv"

	file, _ := os.Create(f_path)
	defer file.Close()

	// ints are 4 bytes
	alloc_amt := buffer_size_mb * 1024 * 1024
	var buffer bytes.Buffer
	buffer.Grow(alloc_amt)

	buffer_rows := (buffer_size_mb * 1024 * 1024) / 4

	row_cnt := 0
	for i := 1; i <= tot_rows; i++ {

		// more efficient to run to write string operations vs. a + concatenation
		buffer.WriteString(strconv.Itoa(i))
		buffer.WriteString("\n")

		row_cnt += 1

		//flush the buffer to disk if we hit the max size
		if row_cnt >= buffer_rows || i == tot_rows {
			_, err := io.Copy(file, &buffer)
			if err != nil {
				fmt.Println("Error writing data buffer to file: ", err)
				return
			}
			buffer.Reset()
			buffer.Grow(alloc_amt)
			row_cnt = 0
		}

	}

	Elapsed_time(start_ts, tot_rows, "Pre-Allocted Byte Buffer")

}
