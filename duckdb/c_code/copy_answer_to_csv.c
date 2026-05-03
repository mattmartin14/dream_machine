#include <stdio.h>
#include <string.h>
#include "duckdb.h"

static int execute_sql(duckdb_connection con, const char *sql) {
    duckdb_result result;
    memset(&result, 0, sizeof(result));

    duckdb_state state = duckdb_query(con, sql, &result);
    if (state == DuckDBError) {
        fprintf(stderr, "SQL failed: %s\n", sql);
        fprintf(stderr, "DuckDB error: %s\n", duckdb_result_error(&result));
        duckdb_destroy_result(&result);
        return 0;
    }

    duckdb_destroy_result(&result);
    return 1;
}

static int print_query_result(duckdb_connection con, const char *sql) {
    duckdb_result result;
    memset(&result, 0, sizeof(result));

    duckdb_state state = duckdb_query(con, sql, &result);
    if (state == DuckDBError) {
        fprintf(stderr, "SQL failed: %s\n", sql);
        fprintf(stderr, "DuckDB error: %s\n", duckdb_result_error(&result));
        duckdb_destroy_result(&result);
        return 0;
    }

    idx_t column_count = duckdb_column_count(&result);
    idx_t row_count = duckdb_row_count(&result);

    for (idx_t col = 0; col < column_count; col++) {
        if (col > 0) {
            printf(",");
        }
        printf("%s", duckdb_column_name(&result, col));
    }
    printf("\n");

    for (idx_t row = 0; row < row_count; row++) {
        for (idx_t col = 0; col < column_count; col++) {
            if (col > 0) {
                printf(",");
            }
            char *value = duckdb_value_varchar(&result, col, row);
            if (value) {
                printf("%s", value);
                duckdb_free(value);
            } else {
                printf("NULL");
            }
        }
        printf("\n");
    }

    duckdb_destroy_result(&result);
    return 1;
}

int main(void) {
    duckdb_database db = NULL;
    duckdb_connection con = NULL;

    if (duckdb_open(NULL, &db) == DuckDBError) {
        fprintf(stderr, "duckdb_open failed\n");
        return 1;
    }

    if (duckdb_connect(db, &con) == DuckDBError) {
        fprintf(stderr, "duckdb_connect failed\n");
        duckdb_close(&db);
        return 1;
    }

    const char *create_view_sql =
        "CREATE OR REPLACE VIEW orders AS "
        "SELECT * FROM (VALUES "
        "(1, DATE '2026-05-01', 99.95), "
        "(2, DATE '2026-05-02', 149.50), "
        "(3, DATE '2026-05-03', 75.00)"
        ") AS t(order_id, order_date, order_amt)";

    if (!execute_sql(con, create_view_sql)) {
        duckdb_disconnect(&con);
        duckdb_close(&db);
        return 1;
    }

    const char *copy_sql =
        "COPY (SELECT order_id, order_date, order_amt FROM orders) "
        "TO 'data.csv' (FORMAT CSV, HEADER)";

    if (!execute_sql(con, copy_sql)) {
        duckdb_disconnect(&con);
        duckdb_close(&db);
        return 1;
    }

    printf("Wrote data.csv from view orders.\n");
    printf("Read-back query from CSV:\n");

    const char *read_back_sql =
        "SELECT order_id, order_date, order_amt "
        "FROM read_csv_auto('data.csv') ORDER BY order_id";

    if (!print_query_result(con, read_back_sql)) {
        duckdb_disconnect(&con);
        duckdb_close(&db);
        return 1;
    }

    duckdb_disconnect(&con);
    duckdb_close(&db);
    return 0;
}
