#include <stdio.h>
#include <string.h>

int main() {

    char buf[10];

    // declare how many items you want in the int array
    int x[3];

    // starts at base zero
    x[0] = 1;
    x[1] = 3;
    x[2] = 90000;

    int pos = 0;

    // when using the strcpy, it will add the null string terminator at the end
    //strcpy(buf, "abc");

    size_t x_sz = sizeof(x);
    

    buf[0] = 'a';
    buf[1] = 'b';
    buf[2] = 'f';
    buf[3] = '\0';
    //pos+=3;
    //buf[pos] = '\0';

    size_t buf_sz = strlen(buf);

    printf("x is of size %zu bytes\n",x_sz);

    printf("buf is of size %zu bytes\n",buf_sz);

    printf("%s and %d and %d\n",buf, x[0],x[1]);
}