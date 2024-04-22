### Rust Vs. C for Performance  
#### Author: Matt Martin
#### Date: 4/20/24

<hr>
<h3>Overview</h3>
Rust has been getting a lot of traction in the programming world as a potential replacement one day for C and C++ due to its memory saftey and borrowing guarantees. I've been working on some small data engineering projects over the last few months with Rust, but I was curious the other day if Rust has gotten to a point where it can be faster than C? The examples below are nothing to get too bent out of shape on, as they are just simple loops to test raw power, but low-and-behold...Rust beats C in both of the test harnesses I did.

<hr>
<h3>The Setup</h3>
I'm running all benchmarks below on a Macbook Pro m2 with 16GB of ram and 12 cores. I used the following tools to compile both the c and rust files:

```bash
clang looper.c -o looper;
rust build --release
```

I have not done much research on if the GCC compiler or some other C compiler would do better, but hey...clang was pre-installed on my machine and from some light googling, it gets decent praise so I figured, why not?

<hr>
<h3>The Tests</h3>
I ended up performing 2 different tests to compare C and Rust:

1. Iterate 1 billion times in a loop and accumulate a number - deterministic
2. Iterate 1 billion times in a loop and add a random value between 1 and 100 to the accumulated number - non-deterministic

I chatted with some coworkers the other day and one pointed out an interesting math formula which essentially is a shortcut on summing numbers 1-N. Instead of having to do a loop, you can simply do ```n x (n+1)/2``` and you will get the same result. I'm not sure if under the hood, either C or rust is doing this to short-circuit a loop, but this is why I also included a non-deterministic version of the loop, in case that is happening.

<hr>
<h3>The Code</h3>
Below are the links to the 2 code sets. Both have a function called "det_looper" and another called "non_det_looper". I tried to keep the data types and structures as close as possible to make sure I'm getting an apples-to-apples on performance:

1. [Rust Code](./rust_looper/src/main.rs)
2. [C Code](./c_code/looper.c)

<hr>
<h3>The results</h3>
I ran each code set 5 times and below are the results in milliseconds:

<h4>Deterministic Looper</h4>

| Run # | Rust | C |
| ----- | ---- | -- |
| 1     |  637    | 892  |
| 2     |  647    | 884  |
| 3     |  651    | 886 |
| 4     |  655   | 887 |
| 5     |  648    | 887 |
| **Avg**    |   **648**   |  **887**  |

<h4>Non-Deterministic Looper</h4>

| Run # | Rust | C |
| ----- | ---- | -- |
| 1     |  6101    | 6542 |
| 2     |  6086    | 6544  |
| 3     |  6101    | 6544  |
| 4     |  6089    | 6552  |
| 5     |  6097    | 6531  |
| **Avg**    |   **6095**   |  **6543**  |

<hr>
<h3>Conclusion</h3>
As you can see in this simple test, Rust outperformed C in both the deterministic and non-deterministic loops. This I think implicates a few things:

1. I might not know what I'm doing on C with compiler flags and could probably further optimize the compilation so the C code performs better.
2. The rust compiler these days is getting a lot more attention and contributions for optimizing code, where as C compilers are getting less attention because C is becoming an outdated language and less people are more wanting to learn.