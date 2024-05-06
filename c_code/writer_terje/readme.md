### Testing Various Algorithms to Get C to Write Ints Fast
##### Author: Matt Martin
##### Date: 5/4/24

---

#### Overview

C code can be incredibly fast as long as you know what makes it tick. For the use-case of writing a lot of integers to a human readable text file, I tested numerous approaches. What I found is the fastest is the following method:

1. Building a char buffer
2. Looping and using an int to ascii conversion function to convert the int to is ascii/byte representation
3. Loading the buffer to a degree then calling fwrite

This approach is much faster than using this method:

```c
for (int i = 1; i <= row_cnt; i++) {
        fprintf(file, "%d\n", i);
}
```

The fprintf method, from what I've read, is very inefficient on how it converts integers to strings. When you leverage a function to convert the int to ascii, you can then leverage the fwrite command in C, which is significantly faster than fprintf. The fwrite command implementation looks like this:

```c
fwrite(buffer, pos, 1, file);
```

---
#### Algorithms Tested
WHen I worked through getting an ascii converter function up and running, I was suggested the Terje Mathisen algorithm by Thomas Kejser on LinkedIn; he provided the function in a post reply, but more detail can be found [here](https://stackoverflow.com/questions/7890194/optimized-itoa-function). The algorithm uses a clever trick of splitting the int into 2 parts and processing them both, then sandwhiching them back together for the final ascii output. I also tested my own int to ascii function. Both methods were able to write 1B row across 30 files in under 4 seconds. I also tested using C's snprintf to convert the int's to strings. All three version are linked below:

1. [Terje Algorithm](./terje_nopad_v2.c) - runs in 3.3 seconds
2. [My Own Int to Ascii](./parallel_v2.c) - runs in 3.8 seconds
3. [Stadard C sprintf](./parallel_writer.c) - runs in 8 seconds

---
#### Some Other Thoughts

Working with buffers and pointers in C is always tricky. But once you get it working, it's very rewarding. One thing to remember though is in C, you have to roll your own functions a lot of the time to get the performance boost. In more modern languages like Go, they have implemented a very fast Int to Ascii function, which can be found [here](https://github.com/golang/go/blob/master/src/strconv/itoa.go). When I use Go's Int to Ascii (Iota) function, I can get the same performance I'm getting with C, where I can write 1B rows in under 4 seconds. Here's the link to that one: [Go Parallel Writer](https://github.com/mattmartin14/dream_machine/blob/main/benchmarks/write_speed/go_lang/app/writer_v2.go).