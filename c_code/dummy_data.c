#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

// to compile run in terminal clang -o dummy_data_in_c dummy_data.c
int main() {
    
    clock_t start_ts, end_ts;
    double elapsed;

    srand(time(NULL));

    start_ts = clock();

    const char* home_dir = getenv("HOME");
    const char* file_sub_path = "/test_dummy_data/dummy_data3.csv";

    size_t len = strlen(home_dir) + strlen(file_sub_path) + 1;
    char f_path[len];
    snprintf(f_path,len,"%s%s",home_dir,file_sub_path);

	//const char* f_path = strcat(home_dir, "/test_dummy_data/dummy_data3.csv");
    printf("File target: %s\n",f_path);

    FILE *file = fopen(f_path,"w");
    if (file == NULL) {
        printf("Error creating file\n");
        return 1;
    }

    //add headers
    char headers[] = "dummy_header1,dummy_header2,dummy_header3";
    fprintf(file, "%s\n", headers);

    int max_iterations = 1000000000;
    for (int i=1; i<=max_iterations; i++){
        int random_value = rand();
        fprintf(file, "%s,%d,%d\n", "test", random_value, i);
    }

    fclose(file);

    end_ts = clock();

    elapsed = (double)(end_ts - start_ts)/ CLOCKS_PER_SEC;

    //free(f_path);

    printf("process complete. Total Run time to generate %d: %.2f seconds\n", max_iterations, elapsed);

}