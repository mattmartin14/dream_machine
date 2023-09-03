#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

#define TOT_ROWS 1000000000

/*
    to do: 
    
    add a step by 30, 40, 50
    -- see if we can find an optimal amount

    -- tail calc working

    perf notes:
        step 20 runs in about 24 seconds
        step 50 only shaves half a second off
        increasing the buffer does not help on perf much at this point

*/

// to compile run in terminal clang -o c_writer c_writer.c

int build_fmt_string(char *fmt_string, int num_elements) {
    for (int i=1;i<=num_elements;i++) {
        strcat(fmt_string, "%d\n");
    }
    return 0;
}

int write_10(char *buffer, int buffer_index, int max_buffer_size, char* fmt_string_10_step, int row_index){
    int chars_written = snprintf(buffer + buffer_index, max_buffer_size - buffer_index, fmt_string_10_step
        ,row_index+1,row_index+2,row_index+3,row_index+4,row_index+5,row_index+6,row_index+7,row_index+8,row_index+9,row_index+10
    );
    return chars_written;
}

int write_20(char *buffer, int buffer_index, int max_buffer_size, char* fmt_string_20_step, int row_index){
    int chars_written = snprintf(buffer + buffer_index, max_buffer_size - buffer_index, fmt_string_20_step
        ,row_index+1,row_index+2,row_index+3,row_index+4,row_index+5,row_index+6,row_index+7,row_index+8,row_index+9,row_index+10
        ,row_index+11,row_index+12,row_index+13,row_index+14,row_index+15,row_index+16,row_index+17,row_index+18,row_index+19,row_index+20
    );
    return chars_written;
}

int write_30(char *buffer, int buffer_index, int max_buffer_size, char* fmt_string_30_step, int row_index){
    int chars_written = snprintf(buffer + buffer_index, max_buffer_size - buffer_index, fmt_string_30_step
        ,row_index+1,row_index+2,row_index+3,row_index+4,row_index+5,row_index+6,row_index+7,row_index+8,row_index+9,row_index+10
        ,row_index+11,row_index+12,row_index+13,row_index+14,row_index+15,row_index+16,row_index+17,row_index+18,row_index+19,row_index+20
        ,row_index+21,row_index+22,row_index+23,row_index+24,row_index+25,row_index+26,row_index+27,row_index+28,row_index+29,row_index+30
    );
    return chars_written;
}

int write_50(char *buffer, int buffer_index, int max_buffer_size, char* fmt_string_50_step, int row_index){
    int chars_written = snprintf(buffer + buffer_index, max_buffer_size - buffer_index, fmt_string_50_step
        ,row_index+1,row_index+2,row_index+3,row_index+4,row_index+5,row_index+6,row_index+7,row_index+8,row_index+9,row_index+10
        ,row_index+11,row_index+12,row_index+13,row_index+14,row_index+15,row_index+16,row_index+17,row_index+18,row_index+19,row_index+20
        ,row_index+21,row_index+22,row_index+23,row_index+24,row_index+25,row_index+26,row_index+27,row_index+28,row_index+29,row_index+30
        ,row_index+31,row_index+32,row_index+33,row_index+34,row_index+35,row_index+36,row_index+37,row_index+38,row_index+39,row_index+40
        ,row_index+41,row_index+42,row_index+43,row_index+44,row_index+45,row_index+46,row_index+47,row_index+48,row_index+49,row_index+50
    );
    return chars_written;
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
    
    char buffer[4096*16*2*2];
    int buffer_index = 0;

    char fmt_string_1_step[] = "%d\n";
    
    
    char fmt_string_10_step[10*4];
    build_fmt_string(fmt_string_10_step,10);

    char fmt_string_20_step[20*4];
    build_fmt_string(fmt_string_20_step,20);

    char fmt_string_30_step[30*4];
    build_fmt_string(fmt_string_30_step,30);

    char fmt_string_50_step[50*4];
    build_fmt_string(fmt_string_50_step,50);

    //printf("fmt buffer of 10 looks like %s. \n",fmt_string_10_step);

    int step_by = 50;

    int max_buffer_size = sizeof(buffer);

    int buffer_padding = 600;

    int flush_cnt = 0;

    int chars_written = 0;

    int tail = TOT_ROWS % step_by;

    int has_tail = 0; 
    if (tail > 0) {has_tail = 1;}

    printf("tail value = %d. has tail = %d\n", tail, has_tail);

    for (int i = 0; i < TOT_ROWS; i+=step_by) {

        if(has_tail == 1 && (TOT_ROWS - i) <= tail) {
            //printf("in the tail\n");
            step_by = 1;
            chars_written = snprintf(buffer + buffer_index, max_buffer_size - buffer_index, fmt_string_1_step, i+1);
        } else {
            chars_written = write_50(buffer, buffer_index, max_buffer_size, fmt_string_50_step, i);
            //chars_written = write_20(buffer, buffer_index, max_buffer_size, fmt_string_20_step, i);
        }

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