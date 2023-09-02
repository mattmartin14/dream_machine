#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

#define TOT_ROWS 1000000000

// to compile run in terminal clang -o c_writer c_writer.c
int main() {
    
    clock_t start_ts, end_ts;
    double elapsed;

    start_ts = clock();

    int buffer_size = 8192*10;

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

   


    for (int i=1; i<=TOT_ROWS; i++){
        fprintf(file, "%d\n", i);
        }


    // takes 40 seconds with this buffered approach
    /*

         char *buffer = (char *)malloc(buffer_size);
    if (buffer == NULL) {
        perror("Memory allocation failed");
        fclose(file);
        return 1;
    }

        size_t curr_row_cnt = 0;
    size_t curr_buffer_size = 0;

    while (curr_row_cnt < TOT_ROWS) {
        int num = curr_row_cnt;  

        int numChars = snprintf(buffer + curr_buffer_size, buffer_size - curr_buffer_size, "%d\n", num+1);

        if (numChars <0) {
            break;
        }

        if (numChars >= buffer_size - curr_buffer_size) {
            // Buffer is full or an error occurred, flush buffer
            fwrite(buffer, 1, curr_buffer_size, file);
            curr_buffer_size = 0;
        } else {
            curr_buffer_size += numChars;
        }

        curr_row_cnt++;
    }

    if (curr_buffer_size > 0) {
        fwrite(buffer, 1, curr_buffer_size, file);
    }    

    free(buffer);
    */
    
    // FILE *buf_stream = __fbufopen(fileno(file), "w");
    // if (buf_stream == NULL) {
    //     perror("Error creating buffered stream");
    //     fclose(file);
    //     return 1;
    // }

   // setvbuf(buf_stream, NULL, _IOFBF, 8192);  // Adjust buffer size as needed

    // Write integers to the buffered stream
    // for (int i = 1; i <= TOT_ROWS; ++i) {
    //     fprintf(buf_stream, "%d\n", i);
    // }

    //fclose(buf_stream);
    fclose(file);

    end_ts = clock();

    elapsed = (double)(end_ts - start_ts)/ CLOCKS_PER_SEC;

    printf("C Lang process complete. Total Run time to generate %d: %.2f seconds\n", TOT_ROWS, elapsed);
}