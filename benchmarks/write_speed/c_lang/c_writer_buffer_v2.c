#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

#define TOT_ROWS 100000
#define BUFFER_SIZE 100

/*
    When the buffer size is too small, it fails; need to figure out the proper way to track all this

    Wonder if my define is throwing it off?

*/

// to compile run in terminal clang -o c_writer c_writer.c
int main() {
    
    clock_t start_ts, end_ts;
    double elapsed;

    start_ts = clock();

    //int buffer_size = 8192*10;

    const char* home_dir = getenv("HOME");
    const char* file_sub_path = "/test_dummy_data/write_benchmark/c_generated.csv";

    //concaneates the home directory and file sub path
    size_t len = strlen(home_dir) + strlen(file_sub_path) + 1;
    char f_path[len];
    snprintf(f_path,len,"%s%s",home_dir,file_sub_path);

    //printf("File target: %s\n",f_path);

    FILE *file = fopen(f_path,"w");
    if (file == NULL) {
        printf("Error creating file\n");
        return 1;
    }

   

    // takes 40 seconds with this buffered approach
    
    char buffer[BUFFER_SIZE];
    int buffer_index = 0;

    for (int i = 0; i < TOT_ROWS; i++) {
        int n = snprintf(buffer + buffer_index, BUFFER_SIZE - buffer_index, "%d\n", i);

        if (n < 0 || n + buffer_index >= BUFFER_SIZE) {
            printf("row was at %d\n",i);
            fprintf(stderr, "Error adding number to the buffer\n");
            return 1;
        }

        buffer_index += n;

        if (buffer_index >= BUFFER_SIZE - 1) {
            fputs(buffer, file);
            buffer_index = 0;
        }

    }

    if (buffer_index > 0) {
            fputs(buffer, file);
            buffer_index = 0;
    }

    fclose(file);

    end_ts = clock();

    elapsed = (double)(end_ts - start_ts)/ CLOCKS_PER_SEC;

    printf("C Lang process complete. Total Run time to generate %d: %.2f seconds\n", TOT_ROWS, elapsed);
}