#include <stdio.h>
#include <string.h>
#include <sys/types.h>

/*
    To know what makes C tick, you need to get comfortable with buffers;

    buffers allow you write data in memory first before flushing to disk.

    where it gets tricky is you need to constantly track the position of the buffer 
    as you write data to it so that you don't ovewrite data nor do you hit the end of the buffer


*/

//#define MIN(x, y) (((x) < (y)) ? (x) : (y))


typedef uint32_t fix4_28;

void itoa_padded(char *buf, uint32_t val)
{
    fix4_28 const f1_10000 = (1 << 28) / 10000;
    fix4_28 tmplo, tmphi;

    uint32_t lo = val % 100000;
    uint32_t hi = val / 100000;

    tmplo = lo * (f1_10000 + 1) - (lo / 4);
    tmphi = hi * (f1_10000 + 1) - (hi / 4);

    for(size_t i = 0; i < 5; i++)
    {
        buf[i + 0] = '0' + (char)(tmphi >> 28);
        buf[i + 5] = '0' + (char)(tmplo >> 28);
        tmphi = (tmphi & 0x0fffffff) * 10;
        tmplo = (tmplo & 0x0fffffff) * 10;
    }
   // return buf+10;
}


// void itoa_unpadded(char *buf, uint32_t val) {
//     char *p;
//     itoa_padded(buf, val);

//     p = buf;

//     // Note: will break on GCC, but you can work around it by using memcpy() to dereference p.
//     if (*((uint64_t *) p) == 0x3030303030303030)
//         p += 8;

//     if (*((uint32_t *) p) == 0x30303030)
//         p += 4;

//     if (*((uint16_t *) p) == 0x3030)
//         p += 2;

//     if (*((uint8_t *) p) == 0x30)
//         p += 1;

//     return min(p, &buf[15]);
// }





int main() {

    


    char buf[20];

    int buf_pos = 0;

    char firstword[] = "abcde";

    // string copy signature: char* strcpy(char* destination, const char* source);
    strcpy(buf+buf_pos, firstword);

    // update the buffer position
    buf_pos += strlen(firstword);
    printf("buffer is at position %d after the first word\n",buf_pos);

    // add a space
    buf[buf_pos++] = ' ';
    printf("buffer is at position %d after adding a space\n",buf_pos);

    char secondword[] = "fgh";

    strcpy(buf+buf_pos, secondword);

    buf_pos += strlen(secondword);
    printf("buffer is at position %d after adding the second word\n",buf_pos);

    // add a space
    buf[buf_pos++] = ' ';

    // add in an int
    u_int32_t n1 = 123;

    // note if you don't update the buffer position after this, the null terminator overwrites
    // or you can use the snprintf which will advance the buffer position for you
    //sprintf(buf + buf_pos, "%u", n1);
    //buf_pos += strlen(buf + buf_pos);

    buf_pos += snprintf(buf + buf_pos, sizeof(buf) - buf_pos, "%u", n1);

    // this is causing a weird representation of the data in binary
    // snprintf seems good enough
    // memcpy(buf + buf_pos, &n1, sizeof(n1));
    // buf_pos += sizeof(n1);


    // add string null terminator to properly close off a string (2 characters)
    buf[buf_pos] = '\0';

    printf("buffer is at position %d after adding the null string terminator\n",buf_pos);

    printf("buffer contents: %s\n", buf);

    FILE *file = fopen("output.txt", "wb"); // Use "wb" mode for binary writing
    if (file == NULL) {
        printf("Error opening file.\n");
        return 1;
    }

    // char buf2[11]; // Buffer for holding converted numbers
    // buf_pos = 0;
    // for (uint32_t i = 1; i<=50;i++) {
    //     itoa_padded(buf2, i);
    //     buf2[buf_pos+10] = '\n';
    //     fwrite(buf2, sizeof(char), 11, file);
    // }

    char buf3[11]; // Buffer for holding converted numbers
    buf_pos = 0;
    for (uint32_t i = 1; i<=50;i++) {
        itoa_unpadded(buf3, i);
        buf3[buf_pos+10] = '\n';
        fwrite(buf3, sizeof(char), 11, file);
    }

    // char b1[10];

    // for (int i=0;i<=1000000000;i++){
    //     itoa(i, b1);
    // }


    //printf("converted asci value is %s\n",b1);

    //fwrite(buf, sizeof(char), strlen(buf), file);

    // Close the file
    fclose(file);
}
