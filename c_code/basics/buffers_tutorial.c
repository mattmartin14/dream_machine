#include <stdio.h>
#include <string.h>


/*
    To know what makes C tick, you need to get comfortable with buffers;

    buffers allow you write data in memory first before flushing to disk.

    where it gets tricky is you need to constantly track the position of the buffer 
    as you write data to it so that you don't ovewrite data nor do you hit the end of the buffer


*/


// void reverse(char s[])
//  {
//      int i, j;
//      char c;
 
//      for (i = 0, j = strlen(s)-1; i<j; i++, j--) {
//          c = s[i];
//          s[i] = s[j];
//          s[j] = c;
//      }
//  }

//  void itoa(int n, char s[])
//  {
//      int i, sign;
 
//      if ((sign = n) < 0)  /* record sign */
//          n = -n;          /* make n positive */
//      i = 0;
//      do {       /* generate digits in reverse order */
//          s[i++] = n % 10 + '0';   /* get next digit */
//      } while ((n /= 10) > 0);     /* delete it */
//      if (sign < 0)
//          s[i++] = '-';
//      s[i] = '\0';
//      reverse(s);
//  }

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


    // char b1[10];

    // for (int i=0;i<=1000000000;i++){
    //     itoa(i, b1);
    // }


    //printf("converted asci value is %s\n",b1);

    fwrite(buf, sizeof(char), strlen(buf), file);

    // Close the file
    fclose(file);
}
