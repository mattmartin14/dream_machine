### Benchmark testing Write Speeds of Various Programming Languages
#### Author: Matt Martin
#### Last Updated: 9/4/2023


This repo is designed to test the write speed of several programming languages. The criteria for this test harness is it has to write 1 billion integer rows to a csv file and must be single threaded (no parallel processing allowed). At the moment, I'm currently testing these languages:

1. [C](https://github.com/mattmartin14/dream_machine/blob/main/benchmarks/write_speed/c_lang/c_writer_v3.c)
2. [Rust](https://github.com/mattmartin14/dream_machine/blob/main/benchmarks/write_speed/rust/rust/src/main.rs)
3. Go
4. Python

At the end of this project, I'm intending to score several factors:

1. Overall speed to write the 1 billion rows
2. Size of the binary (or in python's case py file)
3. Ease of use to program (subjective by user but going to include my impression)
4. Maybe will measure power used (e.g. CPU time)

<h4></h4>

<h4>Results</h4>

| Language | Write Speed (Seconds) | Binary Size | Ease of Use |
| -------- | --------------------  | ----------- | ----------- |
| C        | 22.5                  |  34kb       | very hard, inflexible at times |
| Rust     | 16                    |  470kb      | hard, took a little while to get used to syntax |
| Go       | 24.7                  |  1.5mb      | Very easy to learn and use |
| Python   | 179                   |  732b       | Easiest to use; clearly as an interpreted language, not as fast |

<hr></hr>

<h4>Conclusion</h4>
Overall, this project had some interesting gotchas for me. I am shocked that rust is faster than C, given Rust is written in C. Obviously, I'm not the most seasoned expert in C and I'm pretty sure sommeone could optimize the C code further. In the C project, I experimented with multiple ways to write the data via fputs, fwrite. I found that when I load the snprintf function to do blocks of 100, it went orders of magnitude faster than doing a single row with snprintf. 
</h5>
I enjoyed programming in Go the most, even though Python is the easiest. Go was very fast and not that far behind C; however, the ease of the syntax in Go far outweighs me wanting to use C for future projects. If speed was the upmost important factor, I'd muscle through the Rust code. But if Go performs good enough, I'll stick with that language for now.
