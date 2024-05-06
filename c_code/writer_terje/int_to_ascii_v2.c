// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
// #include <time.h>

// // #include <time.h>
// // #include <string.h>
// // #include <sys/types.h>

// /*
//     for validation, you can use these commands for quick lookups

//     first 5 rows > head -n 5 numbers.txt
//     last 5 rows > tail -n 5 numbers.txt

//     i've gotten this down to less than 32 seconds
// å
// */

// // Function to convert an integer to ASCII
// int int_to_ascii(int value, char* buffer, size_t buffer_size) {
//     if (buffer_size == 0) {
//         return -1;  // No space in the buffer
//     }

//     char temp[12];  // Temporary buffer for the integer
//     int temp_pos = 0;

//     do {
//         int digit = value % 10;
//         temp[temp_pos++] = '0' + digit;  // Convert to ASCII
//         value /= 10;
//     } while (value > 0);

//     // Reverse the characters and copy to the output buffer
//     if (temp_pos > buffer_size) {
//         return -1;  // Buffer overflow
//     }

//     for (int i = 0; i < temp_pos; i++) {
//         buffer[i] = temp[temp_pos - i - 1];  // Copy in the correct order
//     }

//     return temp_pos;  // Number of characters written
// }

// int main() {

//     clock_t start_time = clock();

//     char f_path[100];
//     snprintf(f_path, sizeof(f_path), "%s/test_dummy_data/c/data.txt", getenv("HOME"));

//     // Open a file for writing
//     FILE *file = fopen(f_path,"w");
//     if (file == NULL) {
//         perror("Error opening file");
//         return 1;
//     }

//     int buffer_size = 200*1024;

//     // Buffer to accumulate multiple numbers
//     char buffer[buffer_size];  // Large enough to hold multiple numbers
//     memset(buffer, 0, sizeof(buffer));  // Clear the buffer

//     int pos = 0;  // Position in the buffer
//     int max_rows = 1000000000;

//     for (int i = 1; i <= max_rows; i++) {
//         // Convert the integer to ASCII
//         char temp[20];  // Temporary buffer for each number
//         memset(temp, 0, sizeof(temp));  // Clear the temporary buffer
        
//         int chars_written = int_to_ascii(i, temp, sizeof(temp));
//         if (chars_written < 0) {
//             fprintf(stderr, "Error converting integer to ASCII\n");
//             fclose(file);
//             return 1;
//         }

//         // If adding this number exceeds the remaining buffer space, write buffer to file
//         // we know that each number at most is 10 characters + the 2 for a new line; thuse we check for 12
//         if (pos + 12 >= buffer_size) { // Maximum number of characters for any number representation
//             fwrite(buffer, 1, pos, file); // Write the buffer to the file
//             pos = 0; // Reset the position in the buffer
//         }

//         // Accumulate the converted number and a newline in the buffer
//         memcpy(buffer + pos, temp, chars_written);  // Add the number
//         pos += chars_written;

//         // add new line (1 byte) and advance the buffer
//         //buffer[pos] = '\n';
//         // the \n and 0x0A are same thing; 0x0A is hexidecimal/ascii byte rep of \n
//         // no speed diff between the 2
//         buffer[pos] = '\n';
//         //buffer[pos] = 0x0A;
//         pos++;
//         // this is shorthand and does the same thing; writes the value then adavances the buffer by 1
//         //buffer[pos++] = '\n';  // Add a newline for separation
//     }

//     // If there's data in the buffer, flush it
//     if (pos > 0) {
//         fwrite(buffer, 1, pos, file);  // Write the remaining buffer to the file
//     }

//     // Close the file
//     fclose(file);

//     clock_t end_time = clock();

//     // Calculate the elapsed time in seconds
//     double elapsed_time = (double)(end_time - start_time) / CLOCKS_PER_SEC;

//     printf("Numbers 1 - %d have been written to numbers.txt\n", max_rows);
//     printf("Elapsed time: %.2f seconds\n", elapsed_time);

//     return 0;
// }