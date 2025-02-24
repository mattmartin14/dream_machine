#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <unistd.h>
#include <time.h>

#define MAX_LINE_LENGTH 256
#define MAX_ORDERS 1000000
#define BUFFER_ROW_CNT 100

typedef struct {
    int order_id;
    int total_quantity;
    double total_price;
    char max_order_date[11];
} OrderSummary;

int compare_dates(const char *date1, const char *date2) {
    return strcmp(date1, date2) > 0;
}

void expand_home_dir(char *path, char *expanded_path) {
    if (path[0] == '~') {
        const char *home = getenv("HOME");
        if (home) {
            snprintf(expanded_path, PATH_MAX, "%s%s", home, path + 1);
        } else {
            strncpy(expanded_path, path, PATH_MAX);
        }
    } else {
        strncpy(expanded_path, path, PATH_MAX);
    }
}

void flush_buffer(FILE *output, OrderSummary *orders, int order_count) {
    for (int i = 0; i < order_count; i++) {
        fprintf(output, "%d,%d,%.2f,%s\n", orders[i].order_id, orders[i].total_quantity, orders[i].total_price, orders[i].max_order_date);
    }
}

int main() {
 
    clock_t start_time = clock();

    char input_path[PATH_MAX], output_path[PATH_MAX];
    expand_home_dir("~/dummy_data/c/data.csv", input_path);
    expand_home_dir("~/dummy_data/c/summary.txt", output_path);
    
    FILE *file = fopen(input_path, "r");
    FILE *output = fopen(output_path, "w");
    if (!file || !output) {
        perror("Error opening file");
        return 1;
    }

    OrderSummary orders[BUFFER_ROW_CNT]; 
    long long order_count = 0;
    char line[MAX_LINE_LENGTH];
    
    fgets(line, sizeof(line), file); // Skip header
    while (fgets(line, sizeof(line), file)) {
        int order_id, order_line_id, quantity;
        double price;
        char order_date[11];
        
        int fields_parsed = sscanf(line, "%d,%d,%10[^,],%d,%lf", &order_id, &order_line_id, order_date, &quantity, &price);
        if (fields_parsed != 5) {
            fprintf(stderr, "Error parsing line: %s", line);
            continue; 
        }

        int found = 0;
        for (int i = 0; i < order_count; i++) {
            if (orders[i].order_id == order_id) {
                orders[i].total_quantity += quantity;
                orders[i].total_price += price;
                if (compare_dates(order_date, orders[i].max_order_date)) {
                    strcpy(orders[i].max_order_date, order_date);
                }
                found = 1;
                break;
            }
        }
        if (!found && order_count < BUFFER_ROW_CNT) {
            orders[order_count].order_id = order_id;
            orders[order_count].total_quantity = quantity;
            orders[order_count].total_price = price;
            strcpy(orders[order_count].max_order_date, order_date);
            order_count++;
        }

        if (order_count == BUFFER_ROW_CNT) {
            flush_buffer(output, orders, order_count);
            order_count = 0;  
        }
    }
    fclose(file);

    // commit any tail
    if (order_count > 0) {
        flush_buffer(output, orders, order_count);
    }

    fclose(output);
    
    clock_t end_time = clock();
    
    double time_taken = (double)(end_time - start_time) / CLOCKS_PER_SEC;
    printf("Summary written to summary.txt\n");
    printf("Total time: %.2f seconds\n", time_taken);
    
    return 0;
}
