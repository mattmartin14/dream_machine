#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <time.h>

#define NUM_FILES 20
#define TOTAL_ROWS 1000000000

/*
    Uses fork to create sub processes to write in parallel.
    does 10 files @ 1B rows in under 8 seconds

    -- updated using buffers
        -- now writes in under 5 seconds spread accross 20 files

*/

// -- note: single write requires more sys calls; moved to buffers to accumulate and lessen sys calls

// void writeToFiles(int start, int end, int fileNum) {
//     char filePath[100];
//     snprintf(filePath, sizeof(filePath), "%s/test_dummy_data/c/data_%d.txt", getenv("HOME"), fileNum);
//     FILE *outFile = fopen(filePath, "w");
//     if (outFile == NULL) {
//         perror("Error opening file");
//         exit(1);
//     }

//     // for (int i = start; i <= end; ++i) {
//     //     fprintf(outFile, "%d\n", i);
//     // }

//     for (int i = start; i <= end; i+=4) {
//         fprintf(outFile, "%d\n%d\n%d\n%d\n", i, i+1,i+2,i+3);
//     }

//     fclose(outFile);
// }

typedef uint32_t fix4_28;

void itoa(char *buf, uint32_t val)
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
 
}
// char* itoa_unpadded(char *buf, uint32_t val) {
//     char *p;
//     itoa(buf, val);

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

void writeToFiles(int start, int end, int fileNum) {
    char filePath[100];
    snprintf(filePath, sizeof(filePath), "%s/test_dummy_data/c/data_%d.txt", getenv("HOME"), fileNum);
    FILE *outFile = fopen(filePath, "w");
    if (outFile == NULL) {
        perror("Error opening file");
        exit(1);
    }

    const int buffer_size = 1024*1024; // 100MB
    char buffer[buffer_size];
    //int buffer_pos = 0;

    for (int i = start; i <= end; ++i) {

        itoa(buffer, i);

        //int *buffer_pos = itoa_unpadded(buffer, i);
        fwrite(buffer, sizeof(char), 10, outFile);
        //*buffer_pos = '\0';
        //fprintf(outFile, "%s\n", buffer);

        // Format the number into the buffer
        //buffer_pos += sprintf(buffer + buffer_pos, "%d\n", i);
        
        // // Flush the buffer if it's full
        // if (buffer_pos >= buffer_size - 10) { // Safety margin for ensuring we don't overrun the buffer
        //     fwrite(buffer, sizeof(char), buffer_pos, outFile);
        //     buffer_pos = 0;
        // }
    }

    // Write remaining data in the buffer
    // if (buffer_pos > 0) {
    //     fwrite(buffer, sizeof(char), buffer_pos, outFile);
    // }

    fclose(outFile);
}

void fmt_num(int num) {
    if (num < 1000) {
        printf("%d", num);
        return;
    }

    fmt_num(num / 1000);
    printf(",%03d", num % 1000);
}

double getElapsedTime(struct timespec start, struct timespec end) {
    return (double)(end.tv_sec - start.tv_sec) + (double)(end.tv_nsec - start.tv_nsec) / 1e9;
}

int main() {
    const int rows_per_file = TOTAL_ROWS / NUM_FILES;
    pid_t pid;

    struct timespec start_time, stop_time;
    clock_gettime(CLOCK_REALTIME, &start_time);

    for (int i = 0; i < NUM_FILES; ++i) {
        pid = fork();
        if (pid < 0) {
            perror("Fork failed!");
            return 1;
        } else if (pid == 0) {
            // Child process
            int start = i * rows_per_file + 1;
            int end = start + rows_per_file - 1;
            writeToFiles(start, end, i + 1);
            return 0;
        }
    }

    // Parent process waits for all child processes to finish
    for (int i = 0; i < NUM_FILES; ++i) {
        wait(NULL);
    }

    clock_gettime(CLOCK_REALTIME, &stop_time);
    double duration = getElapsedTime(start_time, stop_time);

    printf("Using C, a total of %d files have been written successfully with ", NUM_FILES);
    fmt_num(TOTAL_ROWS);
    printf(" rows in %f seconds.\n", duration);

    return 0;
}
