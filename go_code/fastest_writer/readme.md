### Test Harness to Determine Fastest Way in Go Lang to write 1B integers to a CSV

Author: Matt Martin<br>
Date: 2023-08-08<br>

This project is looking at several methods in Go Lang to write 1B rows of integers to a CSV file to see which one is the most performant. This repo tests teh following methods:

1. Encoding/csv; go package that makes writing CSV files easy due to it auto-encoding stuff properly
2. bufio - a more low level pacakge that manages the buffer stream
3. Byte Buffer - Very low level package that allows you to copy the buffer to a file with a simple IO command
4. Pre-Memory allocated byte buffer - Identical to Byte Buffer except in this test harness, we pre-allocate memory to the buffer so it does not have to grow for every integer written to it

#### Some interesting gotchas found during this project:
1. Byte Buffers can handle direct string writes individually very well. In fact, I found there is a performance hit when you try to do one byte buffer string write with concatenating strings vs. multiple byte buffer string write calls with individual strings. Additonally, formatting a string into a buffer is VERY slow:<br>
e.g.</br>
This is fastest:
```go
buffer.WriteString(strconv.Itoa(i))
buffer.WriteString("\n")
```
This is slow even though it's one WriteString call. You take a perf hit on the string concat:
```go
buffer.WriteString(strconv.Itoa(i)+"\n")
```
And this is really slow:
```go
buffer.WriteString(fmt.Sprintf("%d\n", i)
```

#### Benchmark Results for 1B row integer CSV file and 100MB Buffer

Below is the benchmark average results for each method. I ran each test harness 5 times. I found that there is not really any measurable difference between byte buffers with an IO.Copy to the file vs. a bufIO. For now, it will come down to personal preference. Additonally, I also found that when I experimented with the buffer size, from 1 MB to 10 to 100MB, I did not see any significant performance boost.
. 

| Method | Time (Seconds) |
| ------ | -------------- |
| CSV Encorder | 93.6 |
| Byte Buffer | 29.15 |
| Byte Buffer Pre-Allocated Memory | 29.25 |
| BufIO | 29 |

#### Running the program
The main.go file takes 2 params:
1. rows (how many rows you want to write) 
2. buffer_size_mb (the size you want your buffer to be before it flushes to disk)

Below is an example of running the main program:
```bash
go run main.go -rows 100000000 -buffer_size_mb 10
```