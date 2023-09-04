### Benchmark testing Write Speeds of Various Programming Languages

This repo is designed to test the write speed of several programming languages. The criteria for this test harness is it has to write 1 billion integer rows to a csv file and must be single threaded (no parallel processing allowed). At the moment, I'm currently testing these languages:

1. C
2. Rust
3. Go
4. Python

At the end of this project, I'm intending to score several factors:

1. Overall speed to write the 1 billion rows
2. Size of the binary (or in python's case py file)
3. Ease of use to program (subjective by user but going to include my impression)
4. Maybe will measure power used (e.g. CPU time)

<h4>/<h4>

Results:
<h4>/<h4>

| Language | Write Speed (Seconds) | Binary Size | Ease of Use |
| -------- | --------------------  | ----------- | ----------- |
| C        | 22.5                  |  34kb       | very hard, inflexible at times |
| Rust     | 16                    |  470kb      | hard, took a little while to get used to syntax |
| Go       | 24.7                  |  1.5mb      | Very easy to learn and use |
| Python   |                       |             | Easiest to use; clearly as an interpreted language, not as fast |