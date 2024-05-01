#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <sys/types.h>


#define ROW_CNT 1000

typedef uint32_t fix4_28;

void terjePadded(char *buf, uint32_t val) {

    fix4_28 const f1_10000 = (1 << 28) / 10000;
    fix4_28 tmplo, tmphi;

    uint32_t lo = val % 100000;
    uint32_t hi = val / 100000;

    tmplo = lo * (f1_10000 + 1) - (lo / 4);
    tmphi = hi * (f1_10000 + 1) - (hi / 4);

    for(size_t i = 0; i < 5; i++)
    {
        buf[i + 0] = '0' + (char)(tmphi >> 28);
        buf[i + 5] = '0' + (char)(tmplo >> 28);
        tmphi = (tmphi & 0x0fffffff) * 10;
        tmplo = (tmplo & 0x0fffffff) * 10;
    }

}

int writeViaTerjePadded(int row_cnt)
{

    char f_path[100];
    snprintf(f_path, sizeof(f_path), "%s/test_dummy_data/c/terjepadded.txt", getenv("HOME"));

    FILE *file = fopen(f_path,"w");
    if (file == NULL) {
        printf("Error creating file\n");
        return 1;
    }

    clock_t start_ts, end_ts;
    double elapsed;
    start_ts = clock();

    char buf[12]; // Buffer for holding converted numbers
    int buf_pos = 0;
    for (uint32_t i = 1; i <= row_cnt; i++) {
        terjePadded(buf, i);
        buf[buf_pos+10] = '\n';
        fwrite(buf, sizeof(char), 11, file);
    }

    fclose(file);

    end_ts = clock();
    elapsed = (double)(end_ts - start_ts)/ CLOCKS_PER_SEC;

    printf("Total Run time single thread via terje padded: %.2f seconds\n", elapsed);

    return 0;

}


int main() {
    writeViaTerjePadded(ROW_CNT);
}