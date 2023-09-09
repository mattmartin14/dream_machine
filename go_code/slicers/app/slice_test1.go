package app

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"time"
)

func Test1() {

	tot_rows := 1000000000
	//tot_rows := 10005
	batch_size := 10000
	init_buffer_size := 10 * 1024 * 1024

	start_ts := time.Now()

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/write_benchmark/go_generated.csv"

	file, _ := os.Create(f_path)
	defer file.Close()

	writer := bufio.NewWriterSize(file, init_buffer_size)
	defer writer.Flush()

	var buffer []byte

	for i := 1; i <= tot_rows; i += batch_size {
		start_row := i
		end_row := min(start_row+batch_size-1, tot_rows)

		for j := start_row; j <= end_row; j++ {
			temp := strconv.AppendInt(nil, int64(i), 10)
			temp = append(temp, ',')
			temp = append(temp, []byte("Test")...)
			temp = append(temp, '\n')

			buffer = append(buffer, temp...)

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

	// for i := 1; i <= 5; i++ {
	// 	temp := strconv.AppendInt(nil, int64(i), 10)
	// 	temp = append(temp, ',')
	// 	temp = append(temp, []byte("Test")...)
	// 	temp = append(temp, '\n')

	// 	buffer = append(buffer, temp...)
	// }

	// fmt.Printf("%s\n", buffer)
}

func format_nbr_with_commas(rows int) string {
	str := strconv.FormatInt(int64(rows), 10)
	result := ""

	// Add commas
	for i, char := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result += ","
		}
		result += string(char)
	}

	return result
}
