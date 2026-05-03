# DuckDB C Client Example (macOS)

This folder contains a minimal C example that uses the DuckDB C client API to run:

1. Create a DuckDB view named orders with 3 columns:
    - order_id
    - order_date
    - order_amt
2. Copy that view to data.csv
3. Read data.csv back using a DuckDB SQL statement and print the rows from C

The example source file is:
- copy_answer_to_csv.c

The Makefile is:
- Makefile

## What You Need

1. macOS with C compiler tools installed
2. DuckDB C package (libduckdb), which includes:
- duckdb.h
- libduckdb.dylib

DuckDB install page for C/C++:
- https://duckdb.org/install/?environment=c

Direct archive used here (current stable at time of writing):
- https://install.duckdb.org/v1.5.2/libduckdb-osx-universal.zip

## 1) Download and Extract DuckDB C Package

Run these from this folder:

    cd ~/dream_machine/duckdb/c_code
    curl -L -o libduckdb-osx-universal.zip https://install.duckdb.org/v1.5.2/libduckdb-osx-universal.zip
    mkdir -p libduckdb
    unzip -o -q libduckdb-osx-universal.zip -d libduckdb

Optional check that key files are present:

    find libduckdb -type f | egrep 'duckdb.h|libduckdb\.(dylib|a)$'

Expected key paths:
- libduckdb/duckdb.h
- libduckdb/libduckdb.dylib

## 2) Build With Make (Recommended)

Build the program:

    make build

Run the program:

    make run

You can also run the binary directly:

    ./copy_answer_to_csv

Clean the compiled binary:

    make clean

## 3) Compile the C Example Manually (Alternative)

Use Apple clang on macOS:

    /usr/bin/clang -std=c11 -Wall -Wextra -I./libduckdb copy_answer_to_csv.c -L./libduckdb -lduckdb -Wl,-rpath,@loader_path/libduckdb -o copy_answer_to_csv

## 4) Run the Program Manually

Run the binary directly:

    ./copy_answer_to_csv

Then inspect the output file:

    cat data.csv

Expected CSV output:

    order_id,order_date,order_amt
    1,2026-05-01,99.95
    2,2026-05-02,149.50
    3,2026-05-03,75.00

Expected program output includes the read-back query result too, for example:

    Wrote data.csv from view orders.
    Read-back query from CSV:
    order_id,order_date,order_amt
    1,2026-05-01,99.95
    2,2026-05-02,149.5
    3,2026-05-03,75.0

## Troubleshooting

### Compiler cannot find standard C headers (for example stdio.h)

If this happens with Homebrew clang, use Apple clang explicitly:

    /usr/bin/clang ...

In this environment, Homebrew clang was configured with a missing SDK path, while Apple clang worked.

### Runtime error loading libduckdb.dylib

The Makefile links with an rpath (`@loader_path/libduckdb`) so direct execution should work.
If you build in a different way and hit a runtime loading error, this fallback still works:

    DYLD_LIBRARY_PATH=./libduckdb ./copy_answer_to_csv

### Command not found: rg

The setup instructions do not require rg. Use the find and egrep check shown above.

## Notes

- The program opens an in-memory DuckDB database (no database file on disk).
- The result object is always destroyed after duckdb_query, including on query failure, matching DuckDB C API guidance.
