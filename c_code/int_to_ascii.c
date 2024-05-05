#include <stdio.h>
#include <string.h>

// got this writing 100k in less than a split second
// need to figure out how to fill the buffer and flush and reset the pointer

// Function to convert an integer to ASCII and return the number of bytes used
int int_to_ascii(int value, char* buffer, size_t buffer_size) {
    if (buffer_size == 0) {
        return -1; // No space in the buffer
    }

    int is_negative = value < 0;  // Check if the integer is negative
    if (is_negative) {
        value = -value;  // Make the number positive for conversion
    }

    // A temporary array to hold the converted digits
    char temp[12];  // Enough space to hold any 32-bit integer as ASCII
    int temp_pos = 0;

    // Extract digits and store them in reverse order
    do {
        int digit = value % 10;  // Get the last digit
        temp[temp_pos++] = '0' + digit;  // Convert to ASCII and store in temp
        value /= 10;  // Remove the last digit
    } while (value > 0);

    // If the number is negative, add the negative sign
    if (is_negative) {
        temp[temp_pos++] = '-';
    }

    // Copy the digits in reverse order to the buffer
    if (temp_pos > buffer_size) {
        return -1;  // Buffer too small
    }

    // Copy the characters from temp to the buffer in the correct order
    for (int i = 0; i < temp_pos; i++) {
        buffer[i] = temp[temp_pos - i - 1];
    }

    return temp_pos;  // Return the number of characters written
}

int main() {

    // Write the buffer to a file
    FILE* file = fopen("output2.txt", "w");
    if (file == NULL) {
        perror("Failed to open file");
        return 1;
    }
   
    int pos2 = 0;

    char buf[500000];
    memset(buf, 0, sizeof(buf));

    int batch_size = 10000;

    for (int i=1;i<=100000;i++) {
        
        int chars_written = int_to_ascii(i, buf + pos2, sizeof(buf) - pos2);
        if (chars_written < 0) {
            fprintf(stderr, "Error converting integer to ASCII\n");
            return 1;
        }

        pos2 += chars_written;

        // Add a separator
        buf[pos2++] = '\n'; 

        batch_size ++;

        fwrite(buf, 1, pos2, file);

        // if (pos2 >= batch_size) {
        //     fwrite(buf, sizeof(buf), pos2, file);
        //     memset(buf, 0, sizeof(buf));
        // }

    }

    if (pos2 >= 1) {
        fwrite(buf, 1, pos2, file);
    }
    
    fclose(file);

    //  // Integers to convert
    // int int1 = 42;
    // int int2 = 1000000000;

    // // Buffer to hold ASCII representations
    // char buffer[50];
    // memset(buffer, 0, sizeof(buffer));  // Clear the buffer

    // int pos = 0;  // To track position in the buffer

    // // Convert the first integer and store in the buffer
    // int chars_written = int_to_ascii(int1, buffer + pos, sizeof(buffer) - pos);
    // if (chars_written < 0) {
    //     fprintf(stderr, "Error converting integer to ASCII\n");
    //     return 1;
    // }
    // pos += chars_written;

    // // Add a separator
    // buffer[pos++] = '\n';  // Space as a separator

    // // Convert the second integer and store in the buffer
    // chars_written = int_to_ascii(int2, buffer + pos, sizeof(buffer) - pos);
    // if (chars_written < 0) {
    //     fprintf(stderr, "Error converting integer to ASCII\n");
    //     return 1;
    // }
    // pos += chars_written;

    // // Write the buffer to a file
    // FILE* file = fopen("output.txt", "w");
    // if (file == NULL) {
    //     perror("Failed to open file");
    //     return 1;
    // }

    // fwrite(buffer, 1, pos, file);  // Write only the relevant part of the buffer
    // fclose(file);

    // printf("Data has been written to output.txt\n");

    // return 0;
}