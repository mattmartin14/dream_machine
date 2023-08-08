### Test Harness to Determine Fastest way to write 1B integers to a CSV

This project is looking at several methods in Go Lang to write 1B rows of integers to a CSV file to see which one is the most performant. I've tested the following methods and below are their benchmarks:

1. Encoding/csv - this one is by far the slowest
2. bufio - Using a BufIO writer is very performant
3. Byte Buffer - This one appears to be the most performant
    - Side note: Discovered that concatenating a string for the Buffer.WriteString method is slower vs. doing 2 Buffer.WriteString calls (1 for the integer value and another for the new line)
4. Pre-Memory allocated byte buffer - this doesn't appear to be any faster or slower than a non-pre allocated buffer