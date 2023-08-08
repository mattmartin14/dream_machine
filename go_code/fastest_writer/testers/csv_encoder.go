package testers

import (
	"encoding/csv"
	"os"
	"strconv"
)

func Csv_encoder(tot_rows int, buffer_size int) {

	work_dir, _ := os.UserHomeDir()
	f_path := work_dir + "/test_dummy_data/perf_testing/csv_encoder_test.csv"

	file, _ := os.Create(f_path)
	defer file.Close()

	writer := csv.NewWriter(file)

	var buffer [][]string = nil

	for i := 1; i <= tot_rows; i++ {

		rec := []string{strconv.Itoa(i)}

		buffer = append(buffer, rec)

		if len(buffer) >= buffer_size || i == tot_rows {
			writer.WriteAll(buffer)
			writer.Flush()
			buffer = nil
		}

	}

}
