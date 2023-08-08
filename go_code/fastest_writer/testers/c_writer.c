#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

//clang -o dummy_data_in_c dummy_data.c

int main() {

    clock_t start_ts, end_ts;
    double elapsed;

    start_ts = clock();

    const char* home_dir = getenv("HOME");
    const char* file_sub_path = "/test_dummy_data/dummy_data_c_writer.csv";

    //concaneates the home directory and file sub path
    size_t len = strlen(home_dir) + strlen(file_sub_path) + 1;
    char f_path[len];
    snprintf(f_path,len,"%s%s",home_dir,file_sub_path);

    printf("File target: %s\n",f_path);


    //const char *filename = "integers.txt";
    FILE *file = fopen(f_path, "w");

    if (file == NULL) {
        perror("Error opening file");
        return 1;
    }

    long long numIntegers = 1000000000; // 1 billion
    for (long long i = 1; i <= numIntegers; ++i) {
        fprintf(file, "%lld\n", i);
    }

    fclose(file);

    end_ts = clock();

    elapsed = (double)(end_ts - start_ts)/ CLOCKS_PER_SEC;

    printf("process complete. Total Run time to generate %lld: %.2f seconds\n", numIntegers, elapsed);

   // printf("1 billion integers written to %s\n", f_path);

    return 0;
}
