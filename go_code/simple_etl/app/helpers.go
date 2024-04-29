package app

import "fmt"

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
