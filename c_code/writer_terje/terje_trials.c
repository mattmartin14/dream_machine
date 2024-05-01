#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <sys/types.h>


#define ROW_CNT 1000

typedef uint32_t fix4_28;


void itoa_terje_impl(char *buf, uint32_t val)
{
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


    //return buf+10;
}
char* itoa_terje_nopad(char *buf, uint32_t val) {
    char tmp_buf[32];
    char* p = tmp_buf;
    memset(tmp_buf, '\n', sizeof(tmp_buf));
    itoa_terje_impl(tmp_buf, val);

    if (*((uint64_t *) p) == 0x3030303030303030)
        p += 8;

    if (*((uint32_t *) p) == 0x30303030)
        p += 4;

    if (*((uint16_t *) p) == 0x3030)
        p += 2;

    if (*((uint8_t *) p) == 0x30)
        p += 1;

    int s = ROW_CNT - (p - tmp_buf) + 1;
    memcpy(buf, p, s);
    return buf + s + 1;
}


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

int writeViaTerjeNonPadded(int row_cnt)
{

    char f_path[100];
    snprintf(f_path, sizeof(f_path), "%s/test_dummy_data/c/terjeNonpadded.txt", getenv("HOME"));

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

        char* result = itoa_terje_nopad(buf, i);
        fwrite(buf, sizeof(result), 11, file);
        //itoa_terje_nopad(buf, i);
        //buf[buf_pos+sizeof(i)] = '\n';
        //fwrite(buf, sizeof(char), 11, file);
    }

    fclose(file);

    end_ts = clock();
    elapsed = (double)(end_ts - start_ts)/ CLOCKS_PER_SEC;

    printf("Total Run time single thread via terje padded: %.2f seconds\n", elapsed);

    return 0;

}

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

    const char* home_dir = getenv("HOME");
    const char* file_sub_path = "/test_dummy_data/dummy_ints.csv";

    //concaneates the home directory and file sub path
    size_t len = strlen(home_dir) + strlen(file_sub_path) + 1;
    char f_path[len];
    snprintf(f_path,len,"%s%s",home_dir,file_sub_path);

    writeViaFprintF(ROW_CNT);
    writeViaTerjePadded(ROW_CNT);
    writeViaTerjeNonPadded(ROW_CNT);
}