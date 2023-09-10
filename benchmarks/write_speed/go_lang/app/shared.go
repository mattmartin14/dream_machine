package app

import (
	"strconv"
)

func format_nbr_with_commas(rows int) string {
	str := strconv.Itoa(rows)
	var result string
	for i, v := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result += ","
		}
		result += string(v)
	}
	return result
}
