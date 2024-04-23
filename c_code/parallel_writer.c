#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <time.h>

#define NUM_FILES 10
#define TOTAL_ROWS 1000000000

void writeToFiles(int start, int end, int fileNum) {
    char filePath[100];
    snprintf(filePath, sizeof(filePath), "%s/test_dummy_data/c/data_%d.txt", getenv("HOME"), fileNum);
    FILE *outFile = fopen(filePath, "w");
    if (outFile == NULL) {
        perror("Error opening file");
        exit(1);
    }

    for (int i = start; i <= end; ++i) {
        fprintf(outFile, "%d\n", i);
    }

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

    printf("A total of %d files have been written successfully with ", NUM_FILES);
    fmt_num(TOTAL_ROWS);
    printf(" in %f seconds.\n", duration);

    return 0;
}
