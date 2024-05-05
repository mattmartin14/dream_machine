#include <stdio.h>
#include <string.h>

// Function to convert an integer to ASCII
int int_to_ascii(int value, char* buffer, size_t buffer_size) {
    if (buffer_size == 0) {
        return -1;  // No space in the buffer
    }

    int is_negative = value < 0;  // Check if the integer is negative
    if (is_negative) {
        value = -value;  // Convert to positive for extraction
    }

    char temp[12];  // Temporary buffer for the integer
    int temp_pos = 0;

    do {
        int digit = value % 10;
        temp[temp_pos++] = '0' + digit;  // Convert to ASCII
        value /= 10;
    } while (value > 0);

    if (is_negative) {
        temp[temp_pos++] = '-';  // Add the negative sign if needed
    }

    // Reverse the characters and copy to the output buffer
    if (temp_pos > buffer_size) {
        return -1;  // Buffer overflow
    }

    for (int i = 0; i < temp_pos; i++) {
        buffer[i] = temp[temp_pos - i - 1];  // Copy in the correct order
    }

    return temp_pos;  // Number of characters written
}

int main() {
    // Open a file for writing
    FILE* file = fopen("numbers.txt", "w");
    if (file == NULL) {
        perror("Error opening file");
        return 1;
    }

    // Buffer to accumulate multiple numbers
    char buffer[10000];  // Large enough to hold multiple numbers
    memset(buffer, 0, sizeof(buffer));  // Clear the buffer

    int pos = 0;  // Position in the buffer

    // Loop through numbers 1-1000
    int max_rows = 1000000000;

    for (int i = 1; i <= max_rows; i++) {
        // Convert the integer to ASCII
        char temp[20];  // Temporary buffer for each number
        memset(temp, 0, sizeof(temp));  // Clear the temporary buffer
        
        int chars_written = int_to_ascii(i, temp, sizeof(temp));
        if (chars_written < 0) {
            fprintf(stderr, "Error converting integer to ASCII\n");
            fclose(file);
            return 1;
        }

        // If adding this number exceeds the buffer size, flush
        if (pos + chars_written + 1 >= sizeof(buffer)) {  // +1 for newline
            // Write the buffer to the file
            fwrite(buffer, 1, pos, file);
            // Clear the buffer and reset the position
            memset(buffer, 0, sizeof(buffer));
            pos = 0;
        }

        // Accumulate the converted number and a newline in the buffer
        memcpy(buffer + pos, temp, chars_written);  // Add the number
        pos += chars_written;
        buffer[pos++] = '\n';  // Add a newline for separation
    }

    // If there's data in the buffer, flush it
    if (pos > 0) {
        fwrite(buffer, 1, pos, file);  // Write the remaining buffer to the file
    }

    // Close the file
    fclose(file);

    printf("Numbers 1 - %d have been written to numbers.txt\n", max_rows);

    return 0;
}