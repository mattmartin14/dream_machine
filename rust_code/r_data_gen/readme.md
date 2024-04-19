### Program: Generate Fake Data using Rust
#### Author: Matt Martin
#### Date: 4/18/24

<hr>
<h3>Overview</h3>
The purpose of this program is to use Rust to generate fake data to several CSV's in parallel. I've done similar exercises in Python and Go (links below), but I wanted to give Rust a try for reasons of improving my Rust programming skills as well to see if it's any faster than the other languages.

- [Python Data Generator](https://github.com/mattmartin14/dream_machine/blob/main/polars/volume_testing/gen_volume_data.py)
- [Go Data Generator](https://github.com/mattmartin14/dream_machine/blob/main/go_code/fake_data/readme.md)

<hr>
<h3>Overall Architecture</h3>
For this project, I tried 3 different approaches to see which would go the fastest:

1. Rayon for parallel processing
2. Tokio for async processing
3. Single Threaded

What I found was the primary package I used for this project (fakeit) puts a heavy CPU load on my system while running. Rayon ended up performing the best since this is a CPU bound task and Rayon was able to fan out to all cores available. Also, I really liked how Rayon's implementation of parallel processing is very clean. For instance, to kick stuff off in parallel, it's as simple as:

```rust
(0..files).into_par_iter().for_each(|i| {
    // do stuff here

    });
```

This is much cleaner vs. Tokio async where I have to tag every single function as asynchronous. This reminds me a lot of Python's nifty ProcessPoolExecutor, which makes fanning out a task to multiple cores very clean and easy.

<hr>
<h3>Code Files</h3>
I ended up organizing my code into 2 separate files:

1. [main.rs](./src/main.rs) - this is required by rust; this is where Rayon kicks off the parallel writing
2. [common.rs](./src/common.rs) - this has all the common functions like generating a fake row of data and formatting stuff

<hr>
<h3>Results</h3>
Below are the results of running various row counts with this project across Rayon, Tokio, and Single Threaded:

| Strategy | 1M Rows | 10M Rows | 100M Rows |
| -------- | ------- | -------- | --------- |
| Rayon    | 2 seconds | 22 seconds | 244 seconds |
| Tokio    | 4 seconds | 48 seconds | 493 seconds |
| Single Thread | 8 seconds | 81 seconds | 992 seconds |

<hr>
<h3>Final Thoughts</h3>
I also pulled in the Clap cargo module to enable me to pass in the row count as a parameter. To launch the program, you can simply do this once its built:

```bash
./target/release/r_data_gen --rows 100000
```

Using Clap was very straight forward. I'm pretty sure I could spend more time to make it clean, but it works well and you can name your parameters like you would with standard posix flags.

Overall, I think this was a good exercise. I'm still trying to figure out in my mind where Rust makes sense, and if I should go all-in on it and start to do more DE projects with it instead of Python. At this point, I'm undecided. I see pros to both Python and Rust as a DE programming language.