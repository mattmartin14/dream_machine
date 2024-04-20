#include <stdio.h>
#include <time.h>

int main() {

    int i = 0;
    long long int z = 0;
    clock_t start, end;
    double cpu_time_used;

    start = clock(); // Record the start time

    for (i = 1; i <= 1000000000; i++) {
        z += i;
    }

    end = clock(); // Record the end time

    // elapsed ms
    cpu_time_used = ((double) (end - start)) * 1000.0 / CLOCKS_PER_SEC; 

    printf("accumulated value is %lld\n", z);
    printf("Time to compute: %f milliseconds\n", cpu_time_used);

    return 0;
}