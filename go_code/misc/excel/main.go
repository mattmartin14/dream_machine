package main

import (
	"fmt"
	// ran go get github.com/xuri/excelize/v2
	"github.com/xuri/excelize/v2"
)

// to build the binary: go build -o excel main.go
func main() {
	// Create a new Excel file
	file := excelize.NewFile()

	// Create a new sheet in the Excel file
	sheet := "Sheet1"
	//index := file.NewSheet(sheet)

	// Set some values in specific cells
	file.SetCellValue(sheet, "A1", "Hello")
	file.SetCellValue(sheet, "B1", "World!")
	file.SetCellValue(sheet, "A2", 3.14)
	file.SetCellValue(sheet, "B2", 42.0)
	file.SetCellValue(sheet, "E1", "abcdefg")
	

	// Save the Excel file
	err := file.SaveAs("output.xlsx")
	if err != nil {
		fmt.Println("Error saving Excel file:", err)
		return
	}

	fmt.Println("Excel file created successfully.")
}
