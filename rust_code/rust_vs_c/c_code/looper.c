#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int det_loop() {

    long long int z = 0;
    clock_t start, end;
    double cpu_time_used;

    start = clock();

    for (long long int i = 1; i <= 1000000000; i++) {
        z += i;
    }

    end = clock();

    // elapsed ms
    cpu_time_used = ((double) (end - start)) * 1000.0 / CLOCKS_PER_SEC; 

    printf("accumulated value is %lld\n", z);
    printf("Time to compute deterministic 1B loop in C: %f milliseconds\n", cpu_time_used);

    return 0;
}

int non_det_loop() {

    long long int z = 0;
    clock_t start, end;
    double cpu_time_used;

    start = clock();

    srand(time(NULL)); // Seed the random number generator

    for (long long int i = 1; i <= 1000000000; i++) {

        int random_value = rand() % 100;
        z += (i + random_value);

    }

    end = clock();

    // elapsed ms
    cpu_time_used = ((double) (end - start)) * 1000.0 / CLOCKS_PER_SEC; 

    printf("accumulated value is %lld\n", z);
    printf("Time to compute non-deterministic 1B loop in C: %f milliseconds\n", cpu_time_used);

    return 0;
}

int main() {

    det_loop();
    non_det_loop();
}
