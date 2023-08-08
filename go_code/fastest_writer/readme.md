### Test Harness to Determine Fastest Way in Go Lang to write 1B integers to a CSV

This project is looking at several methods in Go Lang to write 1B rows of integers to a CSV file to see which one is the most performant. I've tested the following methods and below are their benchmarks:

1. Encoding/csv - this one is by far the slowest
2. bufio - Using a BufIO writer is very performant
3. Byte Buffer - This one appears to be the most performant
    - Side note: Discovered that concatenating a string for the Buffer.WriteString method is slower vs. doing 2 Buffer.WriteString calls (1 for the integer value and another for the new line)</br>
e.g.</br>
This is faster:
```go
buffer.WriteString(strconv.Itoa(i))
buffer.WriteString("\n")
```
This is slower:
```go
buffer.WriteString(strconv.Itoa(i)+"\n")
```
And this is really slow:
```go
buffer.WriteString(fmt.Sprintf("%d\n", i)
```

4. Pre-Memory allocated byte buffer - this doesn't appear to be any faster or slower than a non-pre allocated buffer; i experimented with buffers as small as 1MB and as high as 100MB. No substantial speed improvement

#### Benchmark Results for 1B row integer CSV file and 100MB Buffer

| Method | Time (Seconds) |
| ------ | -------------- |
| CSV Encorder | 93.6 |
| Byte Buffer | 29.15 |
| Byte Buffer Pre-Allocated Memory | 29.25 |
| BufIO | 29 |