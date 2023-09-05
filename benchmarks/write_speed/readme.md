### Benchmark testing Write Speeds of Various Programming Languages
#### Author: Matt Martin
#### Last Updated: 9/4/2023


This repo is designed to test the write speed of several programming languages. The criteria for this test harness is it has to write 1 billion integer rows to a csv file and must be single threaded (no parallel processing allowed). Below are the following languages I tested with source code links:

1. [C](https://github.com/mattmartin14/dream_machine/blob/main/benchmarks/write_speed/c_lang/c_writer_v3.c)
2. [Rust](https://github.com/mattmartin14/dream_machine/blob/main/benchmarks/write_speed/rust/rust/src/main.rs)
3. [Go](https://github.com/mattmartin14/dream_machine/blob/main/benchmarks/write_speed/go_lang/writer.go)
4. [Python](https://github.com/mattmartin14/dream_machine/blob/main/benchmarks/write_speed/python_lang/py_writer.py)
5. [C++](https://github.com/mattmartin14/dream_machine/blob/main/benchmarks/write_speed/cpp/main_v2.cpp)

At the end of this project, I'm going to score these factors:

1. Overall speed to write the 1 billion rows
2. Size of the binary (or in python's case py file)
3. Ease of use to program (subjective by user but going to include my impression)

<h4></h4>

<h4>Results</h4>
Note: The hardware I ran my tests on is a base Apple Mackbook M2 pro with 16GB of ram. I ran each test 5 times and took the average for the speed.
<hr></hr>

| Language | Write Speed (Seconds) | Binary Size | Ease of Use |
| -------- | --------------------  | ----------- | ----------- |
| C        | 22.5                  |  34kb       | very hard, inflexible at times e.g. snprintf not allowing dynamic arguments as an array pased in|
| Rust     | 13                    |  470kb      | hard, took a little while to get used to syntax |
| Go       | 24.7                  |  1.5mb      | Very easy to learn and use |
| Python   | 179                   |  732b       | Easiest to use; clearly as an interpreted language, not as fast |
| C++      | 29                    |  48kb       | Easier than C; still hard at points to work with |

<hr></hr>

<h4>Conclusion</h4>
Overall, this project had some interesting gotchas for me. I am shocked that Rust is faster than C, given Rust is written in C. Obviously, I'm not the most seasoned expert in C and I'm pretty sure sommeone could optimize the C code further. In the C project, I experimented with multiple ways to write the data via fputs, fwrite. I found that when I load up the snprintf function to do blocks of 100, it went orders of magnitude faster than doing a single row with snprintf. 
<h5></h5>
With Rust, there was a bit of a learning curve for me as I'm still relatively new to the language. However, I do like some of the syntax sugar rust uses such as a question mark to propagagte errors up instead of you having to write extra code. Also, once I got the rust code running, there was very little performance tweaking I had to do. It was fast right out of the gate. Also, I found out to not use the standard crate::csv package. Performance on that is terrible.
<h5></h5>
I enjoyed programming in Go the most. This language is very modern and has a well built out ecosystem of libraries and developers. Given I do a lot of data engineering work, I see Go as my daily driver for professional ETL pipelines. I like how go routines work and how easy they are to program to enable flexible parallel processing.
<h5></h5>
Python, which I've used for over 7 years now, was very easy to write the script in. I view python as a general "GSD" language of sorts. You can prototype things very fast in python, the ecosystem is huge and well supported. And pretty much every type of data engineering service you can think of has a python API today. If I need to get something written quick, I'll grativate to python. However, if speed is a concern, I'll then flip to go if python is just not quick enough.

