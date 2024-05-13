package testers

import (
	"fmt"
	"strconv"
	"time"
)

func Elapsed_time(start_ts time.Time, row_cnt int, harness_type string) {

	elapsed_time := time.Since(start_ts).Seconds()

	msg := fmt.Sprintf("Elapsed Time to process %s rows for [%s] harness: %.2f seconds", format_nbr_with_commas(row_cnt), harness_type, elapsed_time)
	fmt.Println(msg)
}

func format_nbr_with_commas(rows int) string {
	rows_str := strconv.Itoa(rows)

	for i := len(rows_str) - 3; i > 0; i -= 3 {
		rows_str = rows_str[:i] + "," + rows_str[i:]
	}
	return rows_str
}
