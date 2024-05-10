### C Tutorial For Data Engineers
#### Author: Matt Martin
#### Last Updated: 5/8/24

---
### Overview

C is the foundational programming language that runs pretty much everything in the world from databases to airline reservation systems to applications like Microsoft Excel and operating systems. It is a very low level programming language that allows developers to directly manipulate memory/RAM on the machine...it doesn't get much faster than that (unless you are a gluten for punishment and want to write in assembler). Many higher level languages depend on C to compile and run. Go Lang uses C libraries for garbage collection. Rust uses C libraries for compilation. Python uses C for its runtime. With all that said, in my personal opinion, I think it's important for data engineers to have a basic understanding of the C programming language. I'm not saying you should start writing pipelines in it, but by having a general understanding of how it works, it will help you appreciate how far we have come with our more modern langauges.

For this tutorial, I will cover the following topics in C:

1. Creating a simple hello world program
2. Working with strings
3. Working with integers
4. Writing to text files

At that point, that is pretty much all you really need to know about C for data engineering (just my opinion). I would not reach for C for any DE task in today's landscape. And why you might ask?

- Because C does not have a built-in dataframe library
- Because C does not have a native parquet reader or writer
- Because C requires a lot of extra code to things like query a SQL Server.
- Because C is considered an "unsafe" language
- Because...just don't :-)

Below is my current decision logic on the programming language to use for data pipelines.

- Default to Python unless speed becomes a problem
- If Python is not cutting, go with Rust

That's pretty much it. So let's dive in.

### Hello World in C

To get your fist C program up and running, it's really quite simple. Crack open notepad or VS code and create a file called "hello_world.c". In that file, add the following contents:

```C
#include <stdio.h>

int main() {
    printf("Hello, World!\n");
    return 0;
}
```

What this code is doing is it is importing the standard IO library from C <stdio.h> and using that to print to the console "Hello World!". The ```\n``` at the end signifies a new line after the print statement.

Next, we need to compile the code in order for it to run. To do this, you have some options. You can download the GCC compiler or you can download the Clang compiler. I'm on a mac and used homebrew to install clang, which is part of the Apple xcode suite. Once Clang was installed, to compile the file, we run this in terminal:

```bash
clang hello_world.c -o hw
```

What this does is creates a binary executable file called "hw". If you were in windows, it would have a ".exe" on the end. Next, to run this file in terminal, we run this command:

```bash
./hw
```

What you should see in terminal is "Hello World!". Here's a screenshot of what it should look like:

![hw](./photos/hw.jpg)

Congrats, you have now written a C program and can tack that on your resume.

---

### Strings in C
Now that we have written a basic C program, let's look at how we can manipulate strings in C. To do this, there are numerous methods. I will be covering 3 of the most common ones I know. But before we get to the code, there is some important info you need to know about strings in C. The string data type does not exist! Instead, you have a single character ```char``` data type. So then...how do we actually create a string? A string is just an array of chars! And that is literally how it is treated by all other programming languages. When you create a "string" in C#, python, Go, or Rust, under the hood, it's really just an array of chars. 


#### String Method 1: Individual Characters
Here's the first method to create a "string" in C. We will create a char array and add the characters individually:

```C
#include <stdio.h>

int main() {
    char buf[4];

    buf[0] = 'a';
    buf[1] = 'b';
    buf[2] = 'c';
    buf[3] = '\0';

    printf("contents of the buffer are [%s]\n",buf);
}
```
You might be asking yourself...what the heck is that ```\0``` thing at the end?? In C code, when you create an array of characters, you have to explicitely terminate it at the end with what is called the null terminator a.k.a. ```\0```. If you do not add that null terminator at the end, the print statement will add some weird looking characters at the end.

Additionally, when you create a character array in C that you want to fill (in our case "buf"), you need to give it enough space to add all the characters. If you do not and try to add an addtional character in, you will get what is called a "segmentation fault". This is both a blessing and a curse of C. It's tedius to program in, but since you are being very explicit with how much space to allocate, your program will be very efficient. Also, when you are creating that variable ```buf[4]```, you are explicitely telling C to allocate 4 bytes of RAM for that variable to use.

#### String Method 2: Adding a series of chars in 1 shot

In this next method, we will add the characters "abc" in 1 shot using the c function ```strcpy```. This method effectively allows us to add a "string" into a character array in 1 shot. Under the hood though, that C function is actually going 1x1 on adding the letters to the array. If you don't believe me, here's a link to the source code (Apple's version) [strcpy Apple](https://opensource.apple.com/source/Libc/Libc-262/i386/gen/strcpy.c.auto.html) and GNU's version [strcpy GNU](https://codebrowser.dev/glibc/glibc/string/strcpy.c.html). One nice additonal feature of the ```strcpy``` command is that it adds the null terminator at the end so you don't have to. Below is an example using ```strcpy```. Notice that we have to add an addtional import with our code for the ```<string.h>``` library:

```C
#include <stdio.h>
#include <string.h>

int main() {

    char buf[4];
    strcpy(buf, "abc");

    printf("The contents of the buffer are [%s]\n",buf);
}
```

Pay close attention. The length of string "abc" is only 3; however, our buffer needs that 1 addtional space for the null terminator that ```strcpy``` will add in. If our buffer was only ```char buf[3]```, we would get another segmentation fault.

#### String Method 3: Adding a series of chars with a pointer
Ahhhhhh...the dreaded "pointer"! Pointers are not bad, believe it or not. In fact, they are one of the biggest ingrediants in C's secret sauce to make the code fast and efficient. A pointer is simply an object that tracks the position in memory for a variable. To illustrate this, in our next example, we will add strings "abc" and "def", but will leverage a pointer so that it makes it easy to track the position in the char array and advance it as we add more contents:

```C
#include <stdio.h>
#include <string.h>

int main() {

    char buf[7];

    // create the pointer
    // note that pointers will default their position
    // to the beginning of the buffer (position 0)
    char *p = buf;

    strcpy(p, "abc");

    // now that we are about to add more characters
    // we need to advance the pointer to the newest blank space
    // we previously added characters "abc" which has a length of 3
    // so...just add 3 to the pointer
    p+=3;

    strcpy(p,"def");

    printf("The contents of the buffer are [%s]\n",buf);
}

```

That wasn't so bad now was it? But how would we take this a step further to allow the pointer to calculate how much it needs to advance dynamically? There's a function for that called ```strlen``` in C. Below is the updated code:

```C
#include <stdio.h>
#include <string.h>

int main() {

    char buf[7];

    // create the pointer
    // note that pointers will default their position
    // to the beginning of the buffer (position 0)
    char *p = buf;

    strcpy(p, "abc");

    p+=strlen("abc");

    strcpy(p,"def");

    printf("The contents of the buffer are [%s]\n",buf);
}
```

Alright, so at this point, we have demonstrated 3 ways to work with strings in C. Now it's time to move onto integers.

---
### Integers in C

Integers in C behave a little differently from strings. For one, integers take up 4 bytes each, whereas a single char in C takes up just 1 byte. But, if you wanted to declare an array of Ints, you simply just provide the number of ints you want in the array. You do not need to do a bunch of math. Below is an example declaring a single int as well as an array of ints:

```C
#include <stdio.h>

int main() {
    int i = 1;
    int j[2]; 
    j[0] = 3;
    j[1] = 4;

    printf("Integer I is %d\n", i);
    printf("Integer Array j has contents [%d, %d]\n", j[0], j[1]);  // Correctly prints the 2nd and 3rd elements

    return 0;
}
```

Pretty simple right? Remember how I said that ints take up 4 bytes each? Well, is there a function that can tell us that? Yes there is. It's called ```sizeof```, and it is very powerful when you need to work dynamically with buffers as you add content. Below is an updated example that shows us the size of variable j, which is 8 bytes since we have 2 ints in the variable, each of which are 4 bytes each:

```C
#include <stdio.h>

int main() {
    int i = 1;
    int j[2]; 
    j[0] = 3;
    j[1] = 4;

    printf("Integer I is %d\n", i);
    printf("Integer Array j has contents [%d, %d]\n", j[0], j[1]);
    printf("Size of array j in bytes: %zu bytes\n", sizeof(j));

    return 0;
}
```

Alright, we have strings and integers covered, let's get to writing some data to text files.

---
### Writing to Text Files in C

Writing to files in C is not overly complicated. To do this, we need to create a file object pointer. Then we can add items to the file using a C function called ```fprintf```. Below is an example adding 2 rows of text to a file:

```C
#include <stdio.h>

int main() {
    
    //create and open the file
    FILE *fout;  
    fout = fopen("output.txt", "w");

    // we are making sure a file was actually created
    if (fout == NULL) {
        printf("Error opening file.\n");
        return 1; 
    }

    // Write two lines of text 
    fprintf(fout, "Hello, this is the first line.\n");
    fprintf(fout, "And this is the second line.\n");

    // Close the file
    fclose(fout);

    printf("File has been written successfully.\n");

    return 0; // Successful execution
}

```

Ok, so this should have created a text file called "output.txt" and added the items to it; but let's kick things up a notch and use the buffer pointer trick we had previously learned in the tutorial to add the info. Additionally, we will use a C function called ```sprintf``` which does some pretty cool stuff. It allows us to add a string to the buffer, and it will return how many bytes were added; this in turn will help us advance the buffer as needed to the next available empty spot, making the code dynamic. Below is the updated code using a buffer pointer:

```C
#include <stdio.h>

int main() {
    
    //create and open the file
    FILE *fout;  
    fout = fopen("output.txt", "w");

    // we are making sure a file was actually created
    if (fout == NULL) {
        printf("Error opening file.\n");
        return 1; 
    }

    char buf[250];
    char *p = buf;

    p += sprintf(p, "Hello, this is the first line.\n");
    p += sprintf(p, "And this is the second line.\n");
    
    fputs(buf, fout);

    // Close the file
    fclose(fout);

    printf("File has been written successfully.\n");

    return 0; // Successful execution
}
```

Note: The C function ```fputs``` allows us to copy the buffer contents to the file.

Alright, but what if we want to add both strings and integers to this file? The ```sprintf``` function allows us to do just that. We can pass it a format string and add integers. Below is the updated code that does this:

```C
#include <stdio.h>

int main() {
    
    //create and open the file
    FILE *fout;  
    fout = fopen("output.txt", "w");

    // we are making sure a file was actually created
    if (fout == NULL) {
        printf("Error opening file.\n");
        return 1; 
    }

    char buf[250];
    char *p = buf;

    p += sprintf(p, "Integer: %d, String: %s\n", 123, "Hello");
    p += sprintf(p, "Integer: %d, String: %s\n", 456, "World");

    
    fputs(buf, fout);

    // Close the file
    fclose(fout);

    printf("File has been written successfully.\n");

    return 0; // Successful execution
}

```

Alright, we have now demonstrated how to work with strings, integers, and how to add them to text files in C code. Wahoo.

---

### Conclusion
This tutorial was aimed to give data engineers a basic overview of C and how it works at it's most fundamental level. At this point, I hope you are comfortable understanding how C works with strings and integers as well as simple file writes. This tutorial was not intended to encourage you to go write a bunch of data pipelines in C. Like I mentioned earlier, you can achieve great performance and productivity with either Python or Rust. Those programming languages can accomplish reading/writing and dataframe aggregations, which C either does not have native support for, or would require orders of magnitude more lines of code. Plus, coding examples in Python and Rust for data engineering tasks are a lot eaisier to find than something in C.

If you do want to see the sheer power of C showcased, I recommend you check out this test harness I built that compares C to Go and Rust on writing 1 billion integers to text files in parallel:

- [Writing 1 Billion Ints Under 3 Seconds](https://github.com/mattmartin14/dream_machine/blob/main/benchmarks/parallel_write_speed/readme.md)

Hope you enjoyed this overview and good luck!
