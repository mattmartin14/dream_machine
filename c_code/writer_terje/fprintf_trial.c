#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>


#define ROW_CNT 1000


int writeViaFprintF(int row_cnt) {
    
    char f_path[100];
    snprintf(f_path, sizeof(f_path), "%s/test_dummy_data/c/fprintf.txt", getenv("HOME"));

    FILE *file = fopen(f_path,"w");
    if (file == NULL) {
        printf("Error creating file\n");
        return 1;
    }

    clock_t start_ts, end_ts;
    double elapsed;
    start_ts = clock();
    
    for (int i = 1; i <= row_cnt; i++) {
        fprintf(file, "%d\n", i);
    }

    fclose(file);

    end_ts = clock();
    elapsed = (double)(end_ts - start_ts)/ CLOCKS_PER_SEC;

    printf("Total Run time single thread via fprintf: %.2f seconds\n", elapsed);

    return 0;
}

int main() {

    writeViaFprintF(ROW_CNT);
}