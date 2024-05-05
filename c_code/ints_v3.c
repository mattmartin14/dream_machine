#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

int main() {
    clock_t start_time = clock();

    // Open a file for writing
    FILE* file = fopen("numbers.txt", "w");
    if (file == NULL) {
        perror("Error opening file");
        return 1;
    }

    // Allocate memory for the buffer dynamically
    const int buffer_size = 10000000; // Increased buffer size
    char* buffer = (char*)malloc(buffer_size);
    if (buffer == NULL) {
        fprintf(stderr, "Memory allocation failed\n");
        fclose(file);
        return 1;
    }

    memset(buffer, 0, buffer_size);  // Clear the buffer

    int pos = 0;  // Position in the buffer
    int max_rows = 1000000000;

    for (int i = 1; i <= max_rows; i++) {
        char temp[20];  // Temporary buffer for each number
        int chars_written = sprintf(temp, "%d\n", i); // Direct conversion to string
        if (chars_written < 0) {
            fprintf(stderr, "Error converting integer to ASCII\n");
            fclose(file);
            free(buffer); // Free allocated memory
            return 1;
        }

        // If adding this number exceeds the buffer size, flush
        if (pos + chars_written >= buffer_size) { // No need to account for newline here
            // Write the buffer to the file
            fwrite(buffer, 1, pos, file);
            // Clear the buffer and reset the position
            memset(buffer, 0, buffer_size);
            pos = 0;
        }

        // Accumulate the converted number in the buffer
        memcpy(buffer + pos, temp, chars_written);
        pos += chars_written;
    }

    // If there's data in the buffer, flush it
    if (pos > 0) {
        fwrite(buffer, 1, pos, file);  // Write the remaining buffer to the file
    }

    // Close the file
    fclose(file);
    // Free allocated memory
    free(buffer);

    clock_t end_time = clock();

    // Calculate the elapsed time in seconds
    double elapsed_time = (double)(end_time - start_time) / CLOCKS_PER_SEC;

    printf("Numbers 1 - %d have been written to numbers.txt\n", max_rows);
    printf("Elapsed time: %.2f seconds\n", elapsed_time);

    return 0;
}
