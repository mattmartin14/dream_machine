#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

int main() {
    
    clock_t start_ts, end_ts;
    double elapsed;

    start_ts = clock();

    const char* home_dir = getenv("HOME");
    const char* file_sub_path = "/test_dummy_data/dummy_ints.csv";

    //concaneates the home directory and file sub path
    size_t len = strlen(home_dir) + strlen(file_sub_path) + 1;
    char f_path[len];
    snprintf(f_path,len,"%s%s",home_dir,file_sub_path);

    printf("File target: %s\n",f_path);

    FILE *file = fopen(f_path,"w");
    if (file == NULL) {
        printf("Error creating file\n");
        return 1;
    }

    //add headers
    //char headers[] = "index,first_name,rand_val";
    fprintf(file, "%s\n", "index");

    //size_t buffer_size = 100;
    //char buffer[sizeof(int)];

    int max_iterations = 100000000;
    for (int i=1; i<=max_iterations; i++){
        //int random_value = rand();

        //10M rows on fwrite with buffer is 1.82 seconds
        //10M rows on fprintf is 1.34 seconds

        //snprintf(buffer, sizeof(int), "%d\n", i);
        //fwrite(buffer, 1, strlen(buffer),file);

        //takes 0.59 seconds to use fprintf for the file for 10M ints
        // need to test how long with a buffer
        fprintf(file, "%d\n", i);
    }

    fclose(file);

    end_ts = clock();

    elapsed = (double)(end_ts - start_ts)/ CLOCKS_PER_SEC;

    printf("process complete. Total Run time to generate %d: %.2f seconds\n", max_iterations, elapsed);
}