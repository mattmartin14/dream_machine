package app

import (
	"fmt"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

// set the schema for the parquet file
type FinData struct {
	FirstName string  `parquet:"name=FirstName, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	NetWorth  float64 `parquet:"name=NetWorth, type=DOUBLE"`
	ProcessTs string  `parquet:"name=ProcessTs, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func WriteToParquet(data map[string]float64, file_path string) error {

	parquetFile, err := local.NewLocalFileWriter(file_path)
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}

	defer parquetFile.Close()

	// Create a new Parquet file writer
	pw, err := writer.NewParquetWriter(parquetFile, new(FinData), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return err
	}

	defer pw.WriteStop()

	loc, _ := time.LoadLocation("America/New_York")
	currentTime := time.Now().In(loc)
	process_ts := currentTime.Format("2006-01-02 15:04:05")

	// Write aggregated net worth to Parquet file
	for groupingValue, summingValue := range data {
		fd := FinData{
			FirstName: groupingValue,
			NetWorth:  summingValue,
			ProcessTs: process_ts,
		}
		if err = pw.Write(fd); err != nil {
			fmt.Println("Error writing to Parquet file:", err)
			return err
		}
	}

	return nil
}
