package testers

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
)

func Bufio_test(tot_rows int, buffer_size int) {

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/perf_testing/bufio_test.csv"

	file, _ := os.Create(f_path)
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	var buffer bytes.Buffer

	row_cnt := 0
	for i := 1; i <= tot_rows; i++ {

		// more efficient to run to write string operations vs. a + concatenation
		buffer.WriteString(strconv.Itoa(i))
		buffer.WriteString("\n")

		row_cnt += 1

		//flush the buffer to disk if we hit the max size
		if row_cnt >= buffer_size || i == tot_rows {
			_, err := buffer.WriteTo(writer)
			if err != nil {
				fmt.Println("Error writing data buffer to file: ", err)
				return
			}
			buffer.Reset()
			row_cnt = 0
		}

	}

}
