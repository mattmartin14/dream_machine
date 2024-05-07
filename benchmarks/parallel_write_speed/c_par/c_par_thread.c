#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>


/// this version uses threads instead of processes/forks; its about the same speed

#define NUM_FILES 20

#define TOTAL_ROWS 1000000000

typedef unsigned int uint32_t;
typedef unsigned long long fix4_28;

int writeToFilesV2(int start, int end, int fileNum);

#define MAX_NUM 10

void reverse(char* str, int length) {
    int start = 0;
    int end = length - 1;
    while (start < end) {
        char temp = str[start];
        str[start] = str[end];
        str[end] = temp;
        start++;
        end--;
    }
}

void itoa_terje_impl(char *buf, uint32_t val) {
    fix4_28 const f1_10000 = (1 << 28) / 10000;
    fix4_28 tmplo, tmphi;
    uint32_t lo = val % 100000;
    uint32_t hi = val / 100000;

    tmplo = lo * (f1_10000 + 1) - (lo / 4);
    tmphi = hi * (f1_10000 + 1) - (hi / 4);

    for (size_t i = 0; i < 5; i++) {
        buf[i + 0] = '0' + (char)(tmphi >> 28);
        buf[i + 5] = '0' + (char)(tmplo >> 28);
        tmphi = (tmphi & 0x0FFFFFFF) * 10;
        tmplo = (tmplo & 0x0FFFFFFF) * 10;
    }
}

char* itoa_terje_nopad(char *buf, uint32_t val) {
    char tmp_buf[11];
    itoa_terje_impl(tmp_buf, val);

    int idx = 0;
    while (idx < 10 && tmp_buf[idx] == '0') idx++;

    int len = 10 - idx;
    memcpy(buf, tmp_buf + idx, len);
    buf[len] = '\0';
    return buf;
}

typedef struct {
    int start;
    int end;
    int fileNum;
} ThreadData;

void* threadFunction(void* arg) {
    ThreadData* data = (ThreadData*)arg;
    writeToFilesV2(data->start, data->end, data->fileNum);
    return NULL;
}

int writeToFilesV2(int start, int end, int fileNum) {
    char filePath[100];
    snprintf(filePath, sizeof(filePath), "%s/test_dummy_data/c/data_%d.txt", getenv("HOME"), fileNum);
    FILE *file = fopen(filePath, "w");
    if (!file) {
        perror("Error opening file");
        exit(1);
    }

    int buffer_size = 100*1024;
    char buffer[buffer_size];
    int pos = 0;

    for (int i = start; i <= end; i++) {
        char temp[12];
        itoa_terje_nopad(temp, i);

        int len = strlen(temp);
        if (pos + len + 1 >= buffer_size) {
            fwrite(buffer, pos, 1, file);
            pos = 0;
        }
        memcpy(buffer + pos, temp, len);
        pos += len;
        buffer[pos++] = '\n';
    }

    if (pos > 0) {
        fwrite(buffer, pos, 1, file);
    }

    fclose(file);
    return 0;
}

int main() {
    struct timespec start_time, end_time;

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    pthread_t threads[NUM_FILES];
    ThreadData data[NUM_FILES];
    const int rows_per_file = TOTAL_ROWS / NUM_FILES;

    for (int i = 0; i < NUM_FILES; ++i) {
        data[i].start = i * rows_per_file + 1;
        data[i].end = (i == NUM_FILES - 1) ? TOTAL_ROWS : (i + 1) * rows_per_file;
        data[i].fileNum = i + 1;
        pthread_create(&threads[i], NULL, threadFunction, &data[i]);
    }

    for (int i = 0; i < NUM_FILES; i++) {
        pthread_join(threads[i], NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &end_time);

    double elapsed_time = end_time.tv_sec - start_time.tv_sec;
    elapsed_time += (end_time.tv_nsec - start_time.tv_nsec) / 1000000000.0;

    printf("Total Run time for terje nonpadded: %.2f seconds\n", elapsed_time);

    return 0;
}
