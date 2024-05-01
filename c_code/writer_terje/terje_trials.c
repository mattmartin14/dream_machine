#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

#define ROW_CNT 10000

void writeViaTerjeNoPad() {
    printf("test");
}

int writeViaFprintF(const char *f_path, int row_cnt) {
    
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

    const char* home_dir = getenv("HOME");
    const char* file_sub_path = "/test_dummy_data/dummy_ints.csv";

    //concaneates the home directory and file sub path
    size_t len = strlen(home_dir) + strlen(file_sub_path) + 1;
    char f_path[len];
    snprintf(f_path,len,"%s%s",home_dir,file_sub_path);

    writeViaFprintF(f_path, ROW_CNT);
}