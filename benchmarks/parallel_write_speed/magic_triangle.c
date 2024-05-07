#include <stdio.h>

int main() {

    long long z = 0;

    long long n = 1000000000;

    for (long long i = 1; i <= n; i++) {
        z+=i;
    }

    printf("Final value is %lld\n",z);


    long long z_calc = (n * (n + 1)) / 2;

    printf("calculated value is %lld\n",z_calc);

}   

