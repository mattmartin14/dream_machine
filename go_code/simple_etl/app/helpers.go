package app

import (
	"fmt"
	"strconv"
	"time"
)

func GetProcessTS() (string, error) {
	// add current timestamp as output
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		return "", fmt.Errorf("Error getting timestamp tz info")
	}
	currentTime := time.Now().In(loc)
	process_ts := currentTime.Format("2006-01-02 15:04:05")
	return process_ts, nil
}

// figures out the column positions in the file
func FindColumnPositions(headers []string, groupColName string, sumColName string) (int32, int32, error) {

	var groupColIndex int32 = -1
	var sumColIndex int32 = -1

	for i, columnName := range headers {
		if columnName == groupColName {
			groupColIndex = int32(i)
		}
		if columnName == sumColName {
			sumColIndex = int32(i)
		}
	}

	if groupColIndex == -1 {
		return -1, -1, fmt.Errorf("error locating grouping column %s in file", groupColName)
	}

	if sumColIndex == -1 {
		return -1, -1, fmt.Errorf("error locating summing column %s in file", sumColName)

	}

	return groupColIndex, sumColIndex, nil
}

func Format_nbr_with_commas(rows int) string {
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
