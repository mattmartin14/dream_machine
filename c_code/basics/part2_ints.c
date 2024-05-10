#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
    FILE *filePtr;
    char line[100]; // Assuming maximum line length is 100 characters
    int firstLine = 1; // Flag to track if it's the first line
    
    // Open the file for reading
    filePtr = fopen("data.txt", "r");
    
    // Check if the file was opened successfully
    if (filePtr == NULL) {
        printf("Error opening file.\n");
        return 1; // Return with error code
    }

    // Read and print each line
    while (fgets(line, sizeof(line), filePtr) != NULL) {
        // Skip the first line (header)
        if (firstLine) {
            firstLine = 0;
            continue;
        }

        // Remove trailing newline character if present
        if (line[strlen(line) - 1] == '\n') {
            line[strlen(line) - 1] = '\0';
        }

        // Split the line into name and age using strtok
        char *name = strtok(line, "|");
        char *age = strtok(NULL, "|");

        // Print the name and age
        printf("Name: %s, Age: %s\n", name, age);
    }

    // Close the file
    fclose(filePtr);

    return 0; // Successful execution
}
