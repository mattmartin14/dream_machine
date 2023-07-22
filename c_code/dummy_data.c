#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

const char* randomString(const char* strings[], int numStrings) {
    int randomIndex = rand() % numStrings;
    return strings[randomIndex];
}

const char* rand_name() {
    //srand(time(NULL));

    const char* first_names[] = {
        "Amy","Bill","Carol","Dean","Elizabeth","Ferra","Gerald","Harold","Ina","Jane","Kelly","Lisa","Matt","Nancy"
        ,"Omar","Pricilla","Quentin","Rachel","Sarah","Thomas","Uma","Vanna","Whitney","Xander","Yanni","Zach"

    };

    int total_elements = sizeof(first_names)/sizeof(first_names[0]);

    const char* rand_name = randomString(first_names,total_elements);

    return rand_name;

}

// to compile run in terminal clang -o dummy_data_in_c dummy_data.c
int main() {
    
    clock_t start_ts, end_ts;
    double elapsed;

    srand(time(NULL));

    start_ts = clock();

    const char* home_dir = getenv("HOME");
    const char* file_sub_path = "/test_dummy_data/dummy_data3.csv";

    //concaneates the home directory and file sub path
    size_t len = strlen(home_dir) + strlen(file_sub_path) + 1;
    char f_path[len];
    snprintf(f_path,len,"%s%s",home_dir,file_sub_path);

	//const char* f_path = strcat(home_dir, "/test_dummy_data/dummy_data3.csv");
    printf("File target: %s\n",f_path);

    FILE *file = fopen(f_path,"w");
    if (file == NULL) {
        printf("Error creating file\n");
        return 1;
    }

    //add headers
    char headers[] = "index,first_name,rand_val";
    fprintf(file, "%s\n", headers);

    int max_iterations = 1000;
    for (int i=1; i<=max_iterations; i++){
        int random_value = rand();
        fprintf(file, "%d,%s,%d\n", i, rand_name(), random_value);
    }

    fclose(file);

    end_ts = clock();

    elapsed = (double)(end_ts - start_ts)/ CLOCKS_PER_SEC;

    //free(f_path);

    printf("process complete. Total Run time to generate %d: %.2f seconds\n", max_iterations, elapsed);

}