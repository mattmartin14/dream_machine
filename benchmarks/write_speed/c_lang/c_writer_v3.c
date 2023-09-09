#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

#define TOT_ROWS 1000000000 // 1B
//#define TOT_ROWS 10013
#define BUFFER_SIZE 1024 * 1024// 100MB

// clang -o c_writer c_writer_v3.c     

int build_fmt_string(char *fmt_string, int num_elements) {

    //if you don't put that initialization in, it prints some funky character first
    fmt_string[0] = '\0';

    for (int i=1;i<=num_elements;i++) {
        strcat(fmt_string, "%d\n");
    }
    return 0;
}

int write_100(char *buffer, int buffer_index, int max_buffer_size, char* fmt_string_100_step, int row_index){
    //int chars_written = snprintf(buffer + buffer_index, max_buffer_size - buffer_index, fmt_string_100_step
    int chars_written = sprintf(buffer + buffer_index, fmt_string_100_step
        ,row_index+1,row_index+2,row_index+3,row_index+4,row_index+5,row_index+6,row_index+7,row_index+8,row_index+9,row_index+10
        ,row_index+11,row_index+12,row_index+13,row_index+14,row_index+15,row_index+16,row_index+17,row_index+18,row_index+19,row_index+20
        ,row_index+21,row_index+22,row_index+23,row_index+24,row_index+25,row_index+26,row_index+27,row_index+28,row_index+29,row_index+30
        ,row_index+31,row_index+32,row_index+33,row_index+34,row_index+35,row_index+36,row_index+37,row_index+38,row_index+39,row_index+40
        ,row_index+41,row_index+42,row_index+43,row_index+44,row_index+45,row_index+46,row_index+47,row_index+48,row_index+49,row_index+50
        ,row_index+51,row_index+52,row_index+53,row_index+54,row_index+55,row_index+56,row_index+57,row_index+58,row_index+59,row_index+60
        ,row_index+61,row_index+62,row_index+63,row_index+64,row_index+65,row_index+66,row_index+67,row_index+68,row_index+69,row_index+70
        ,row_index+71,row_index+72,row_index+73,row_index+74,row_index+75,row_index+76,row_index+77,row_index+78,row_index+79,row_index+80
        ,row_index+81,row_index+82,row_index+83,row_index+84,row_index+85,row_index+86,row_index+87,row_index+88,row_index+89,row_index+90
        ,row_index+91,row_index+92,row_index+93,row_index+94,row_index+95,row_index+96,row_index+97,row_index+98,row_index+99,row_index+100
    );
    return chars_written;

}

int main() {
    clock_t start_ts, end_ts;
    double elapsed;

    start_ts = clock();

    const char* home_dir = getenv("HOME");
    const char* file_sub_path = "/test_dummy_data/write_benchmark/c_generated.csv";

    size_t len = strlen(home_dir) + strlen(file_sub_path) + 1;
    char f_path[len];
    snprintf(f_path,len,"%s%s",home_dir,file_sub_path);

    FILE *file = fopen(f_path,"wb");
    if (file == NULL) {
        printf("Error creating file\n");
        return 1;
    }


    char *buffer = (char *)malloc(BUFFER_SIZE);
    if (buffer == NULL) {
        printf("Error allocating memory for the buffer\n");
        return 1;
    }

    int buffer_index = 0;
    char fmt_string_1_step[] = "%d\n";

    char fmt_string_100_step[100*4];
    build_fmt_string(fmt_string_100_step,100);

    int step_by = 100;

    int buffer_padding = 1000;

    int flush_cnt = 0;

    int chars_written = 0;

    int bulk_operations = TOT_ROWS/step_by;

    int tail_operations = TOT_ROWS % step_by;

    int tail_start = 0;
    int tail_end = 0;

    if (tail_operations > 0) {
        tail_start = bulk_operations * step_by+1;
        tail_end = TOT_ROWS;
    }

    //printf("total bulk operations: %d\nTotal tail operations: %d\ntail start = %d; tail end = %d\n", bulk_operations, tail_operations, tail_start, tail_end);


    int tail = TOT_ROWS % step_by;

    int has_tail = 0; 
    if (tail > 0) {has_tail = 1;}

    // you are here; need to figure out the bulk operations loop
    int row_index = 0;
    for (int i = 0; i < bulk_operations; i ++) {
        
        chars_written = sprintf(buffer + buffer_index, fmt_string_100_step
            ,row_index+1,row_index+2,row_index+3,row_index+4,row_index+5,row_index+6,row_index+7,row_index+8,row_index+9,row_index+10
            ,row_index+11,row_index+12,row_index+13,row_index+14,row_index+15,row_index+16,row_index+17,row_index+18,row_index+19,row_index+20
            ,row_index+21,row_index+22,row_index+23,row_index+24,row_index+25,row_index+26,row_index+27,row_index+28,row_index+29,row_index+30
            ,row_index+31,row_index+32,row_index+33,row_index+34,row_index+35,row_index+36,row_index+37,row_index+38,row_index+39,row_index+40
            ,row_index+41,row_index+42,row_index+43,row_index+44,row_index+45,row_index+46,row_index+47,row_index+48,row_index+49,row_index+50
            ,row_index+51,row_index+52,row_index+53,row_index+54,row_index+55,row_index+56,row_index+57,row_index+58,row_index+59,row_index+60
            ,row_index+61,row_index+62,row_index+63,row_index+64,row_index+65,row_index+66,row_index+67,row_index+68,row_index+69,row_index+70
            ,row_index+71,row_index+72,row_index+73,row_index+74,row_index+75,row_index+76,row_index+77,row_index+78,row_index+79,row_index+80
            ,row_index+81,row_index+82,row_index+83,row_index+84,row_index+85,row_index+86,row_index+87,row_index+88,row_index+89,row_index+90
            ,row_index+91,row_index+92,row_index+93,row_index+94,row_index+95,row_index+96,row_index+97,row_index+98,row_index+99,row_index+100
        );

        //chars_written = write_100(buffer, buffer_index, BUFFER_SIZE, fmt_string_100_step, i+row_index);

        row_index += step_by;

        if (chars_written < 0 || chars_written >= BUFFER_SIZE - buffer_index) {
            printf("row was at %d\n",i);
            fprintf(stderr, "Error adding number to the buffer\n");
            return 1;
        }

        buffer_index += chars_written;

        //write out if we are close to the buffer padding so we dont hit overflow errors
        if (buffer_index >= BUFFER_SIZE - buffer_padding) {
            fwrite(buffer, 1, buffer_index, file);
            buffer_index = 0;
            flush_cnt++;
        }

    }

    if (tail_start > 0) {
        for (int i = tail_start; i<=tail_end; i++){
            chars_written = snprintf(buffer + buffer_index, BUFFER_SIZE - buffer_index, "%d\n", i);
            if (chars_written < 0 || chars_written >= BUFFER_SIZE - buffer_index) {
                printf("row was at %d\n",i);
                fprintf(stderr, "Error adding number to the buffer\n");
                return 1;
            }

            buffer_index += chars_written;

            //write out if we are close to the buffer padding so we dont hit overflow errors
            if (buffer_index >= BUFFER_SIZE - buffer_padding) {
                fwrite(buffer, 1, buffer_index, file);
                //fputs(buffer, file);
                buffer_index = 0;
                flush_cnt++;
            }
        }
    }


    if (buffer_index > 0) {
            fwrite(buffer, 1, buffer_index, file);
            buffer_index = 0;
            flush_cnt++;
    }

    free(buffer);

    fclose(file);

    end_ts = clock();

    elapsed = (double)(end_ts - start_ts) / CLOCKS_PER_SEC;

    printf("C Lang process complete. Total buffer flushes: %d. Buffer Size: %d. Total Run time to generate %d: %.2f seconds\n", flush_cnt, BUFFER_SIZE, TOT_ROWS, elapsed);
    return 0;
}
