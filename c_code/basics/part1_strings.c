#include <stdio.h>
#include <string.h>

int main() {

    /*
        Step 1: Create a static buffer and a position tracker;
        reminder that buffers start at position 0
    */

    char buf[10];

    int buf_pos = 0;

    // add a string into the buffer using strcpy

    strcpy(buf+buf_pos, "abc");

    // update the buffer position (we have added 3 characters)
    buf_pos +=3;

    strcpy(buf+buf_pos,"def");

   
    //buf_pos +=3;

    // not needed since we are using strcpy which adds the null terminator make sure to terminate the string with a null terminator '\0'
    ///buf[buf_pos] = '\0';


    // print out the results
    printf("the items in the buffer are [%s]\n",buf);

    // Same exercise but with a pointer; pointers make it a litle cleaner in that they can maintain a position
    // so instead of you having to write buf + current position, you can advance the pointer and then just refernce it again

    char buf2[10];

    char *p = buf2;

    strcpy(p, "abc");

    p+=3;
    strcpy(p, "def");

    printf("the items in buffer 2 are [%s]\n",buf2);

    // same exercise with individual characters

    char buf3[10];

    buf3[0] = 'a';
    buf3[1] = 'b';
    buf3[2] = 'c';
    buf3[3] = 'd';
    buf3[4] = 'e';
    buf3[5] = 'f';

    // since we are doing individual characters and not strcpy, need to add whats called a null terminator at the end
    // otherwise the print statement will show a bunch of weird stuff
    buf3[6] = '\0';

    printf("the items in buffer 3 are [%s]\n",buf3);

}