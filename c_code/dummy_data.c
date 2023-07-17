#include <stdio.h>
#include <stdlib.h>
#include <time.h>

// to compile run in terminal clang -o dummy_data_in_c dummy_data.c
int main() {
    
    clock_t start_ts, end_ts;
    double elapsed;

    srand(time(NULL));

    start_ts = clock();

    char buffer[] = "dummy_header1,dummy_header2,dummy_header3";

    FILE *file = fopen("dummy_data_in_c.csv","w");
    if (file == NULL) {
        printf("Error creating file\n");
        return 1;
    }

    //add headers
    fprintf(file, "%s\n", buffer);

    int max_iterations = 1000000;
    for (int i=1; i<=max_iterations; i++){
        int random_value = rand();
        fprintf(file, "%s,%d,%d\n", "test", random_value, i);
    }

    fclose(file);

    end_ts = clock();

    elapsed = (double)(end_ts - start_ts)/ CLOCKS_PER_SEC;

    printf("process complete. Total Run time to generate %d: %.2f seconds\n", max_iterations, elapsed);

}