### Comparing Parallel Write Speeds of Popular Programming Languages
#### Author: Matt Martin
#### Date: 5/6/24

---
#### Overview
I've been on a crusade lately to figure out the absolute fastest way to write 1B integer rows across multiple text files in parallel. I think I finally have a well rounded set of test code to share. I settled on building the program in C, Rust, and Go. Below are my explanations for each:

- Why C? Because it's the OG of programming languages. I've been told by many seasoned and wise dev's that if you know how to "actually" program in C, you will do well. As of this writing, I'd classify my C skills as a softmore in High School. I might get to senior level one day.

- Why Rust? Because Rust is quickly positioning itself as the Data Engineering (DE) programming language of the future. It's fast, it's "safe", and it hurts your head to learn how to code in it. It really brings the best...or sometimes worst out of your skills.

- Why Go? Because Go is awesome and fun to program in. Because Go makes the concurrency model a first class citizen with go routines. Because with Go, it's quick to learn, and I don't fight with the compiler like I do with Rust. Because the gopher mascot is just awesome.

---
#### C Code Deep Dive
The C code can be found [here](./c_par/c_parallel_int.c). This code is very VERY involved. When I first started experimenting with C code for this exercise, I used the nifty sprintf function from the C standard lib, which can convert integers to strings (which is what we need to pop into a human readable text file). However, sprintf can do a whole lot more than just convert integers. It was built to convert a multitude of things such as bytes, structs, arrays, floats, and other things. Because of this, once you scale this process to try and crunch 100M+ rows, you will start to see a degredation on performance. Thus, at the suggestion of Thomas Kejser on LinkedIn, I instead decided to boot strap an algorithm that was invented by Terje Mathisen back in 1991. It's purpose is to convert integers to strings (in ascii format) as fast as possible. It does not do anything else besides that. More on that function can be found on this thorough [Stack Overflow article](https://stackoverflow.com/questions/7890194/optimized-itoa-function).In laymen's terms, what the Terje algorithm does is split the integer into 2 halves, computes both halve's ascii/hex representation, then sandwiches the values back together and Voila!. The functions in the C code that execute this magic are "itoa_terje_nopad" and "itoa_terje_impl". The latter creates a padded version of the integer with leading zeroes when needed. One might ask why? It's because when you are dealing with integers, you can have a max of 10 full digits. For a signed integer (32 bits), you can go as high as 2,147,483,647. For an unsigned integer, that number is doubled, but still you are at a total of 10 digits. The "itoa_terje_nopad" strips off the leading zeroes for our exercise. Could these be combined? Probably, but I'm not going to try and solve that Rubik's cube today.

---
#### Go Code Deep Dive
lorum ipsum

---
#### Rust Code Deep Dive
lorum ipsum

---
#### Results
lorum ipsum

---
#### Conclusion
lorum ipsum

---
#### Other Thoughts
lorum ipsum
