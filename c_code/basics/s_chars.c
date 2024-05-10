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
    p+=strlen("abc");

    strcpy(p,"def");

    printf("The contents of the buffer are [%s]\n",buf);
}