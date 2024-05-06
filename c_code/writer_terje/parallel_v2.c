#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <time.h>

#define NUM_FILES 30
#define TOTAL_ROWS 1000000000

/*
    Uses fork to create sub processes to write in parallel.
    does 10 files @ 1B rows in under 4 seconds

    -- updated using buffers
        -- now writes in under 5 seconds spread accross 20 files

*/

// Function to convert an integer to ASCII
int int_to_ascii(int value, char* buffer, size_t buffer_size) {
    if (buffer_size == 0) {
        return -1;  // No space in the buffer
    }
    char temp[12];  // Temporary buffer for the integer
    int temp_pos = 0;

    do {
        int digit = value % 10;
        temp[temp_pos++] = '0' + digit;  // Convert to ASCII
        value /= 10;
    } while (value > 0);

    // Reverse the characters and copy to the output buffer
    if (temp_pos > buffer_size) {
        return -1;  // Buffer overflow
    }

    for (int i = 0; i < temp_pos; i++) {
        buffer[i] = temp[temp_pos - i - 1];  // Copy in the correct order
    }

    return temp_pos;  // Number of characters written
}

int writeToFilesV2(int start, int end, int fileNum) {

    char filePath[100];
    snprintf(filePath, sizeof(filePath), "%s/test_dummy_data/c/data_%d.txt", getenv("HOME"), fileNum);
    FILE *file = fopen(filePath, "w");
    if (file == NULL) {
        perror("Error opening file");
        exit(1);
    }

    int buffer_size = 100*1024;

    char buffer[buffer_size];  // Large enough to hold multiple numbers
    memset(buffer, 0, sizeof(buffer));  // Clear the buffer

    int pos = 0;  // Position in the buffer

    for (int i = start; i <= end; ++i) {
        // Convert the integer to ASCII
        char temp[20];  // Temporary buffer for each number
        memset(temp, 0, sizeof(temp));  // Clear the temporary buffer
        
        int chars_written = int_to_ascii(i, temp, sizeof(temp));
        if (chars_written < 0) {
            fprintf(stderr, "Error converting integer to ASCII\n");
            fclose(file);
            return 1;
        }

        if (pos + 12 >= buffer_size) { // Maximum number of characters for any number representation
            fwrite(buffer, 1, pos, file); // Write the buffer to the file
            pos = 0; // Reset the position in the buffer
        }


        // Accumulate the converted number and a newline in the buffer
        memcpy(buffer + pos, temp, chars_written);  // Add the number
        pos += chars_written;

        buffer[pos] = '\n';
        //buffer[pos] = 0x0A;
        pos++;
        // this is shorthand and does the same thing; writes the value then adavances the buffer by 1
        //buffer[pos++] = '\n';  // Add a newline for separation
    }

    // If there's data in the buffer, flush it
    if (pos > 0) {
        fwrite(buffer, 1, pos, file);  // Write the remaining buffer to the file
    }

    // Close the file
    fclose(file);

    return 0;
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
            writeToFilesV2(start, end, i + 1);
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
