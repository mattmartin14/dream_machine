#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

#define TOT_ROWS 10000000

/*
    to do: 
    got code working for single write;
    got function working to build a buffer string
    -- need to test using the buffer string

*/

// to compile run in terminal clang -o c_writer c_writer.c

int build_fmt_string(char *fmt_string, int num_elements) {
    for (int i=1;i<=num_elements;i++) {
        strcat(fmt_string, "%d\n");
    }
    return 0;
}

int main() {

    clock_t start_ts, end_ts;
    double elapsed;

    start_ts = clock();


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
    
    char buffer[4096*2*2*2];
    int buffer_index = 0;

    char fmt_string_1_step[] = "%d\n";
    
    
    char fmt_string_10_step[10*4];

    build_fmt_string(fmt_string_10_step,10);


    printf("fmt buffer of 10 looks like %s. \n",fmt_string_10_step);

    int step_by = 1;

    int max_buffer_size = sizeof(buffer);

    int buffer_padding = 200;

    int flush_cnt = 0;

    for (int i = 0; i < TOT_ROWS; i+=step_by) {

        int chars_written = snprintf(buffer + buffer_index, max_buffer_size - buffer_index, fmt_string_1_step, i+1);

        if (chars_written < 0 || chars_written >= max_buffer_size - buffer_index) {
            printf("row was at %d\n",i);
            fprintf(stderr, "Error adding number to the buffer\n");
            return 1;
        }

        buffer_index += chars_written;

        //write out if we are close to the buffer padding so we dont hit overflow errors
        if (buffer_index >= max_buffer_size - buffer_padding) {
            fputs(buffer, file);
            buffer_index = 0;
            flush_cnt++;
        }

    }

    if (buffer_index > 0) {
            fputs(buffer, file);
            buffer_index = 0;
            flush_cnt++;
    }

    fclose(file);

    end_ts = clock();

    elapsed = (double)(end_ts - start_ts)/ CLOCKS_PER_SEC;

    printf("C Lang process complete. Total buffer flushes: %d. Total Run time to generate %d: %.2f seconds\n"
    , flush_cnt, TOT_ROWS, elapsed);
}