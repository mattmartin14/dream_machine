CC := /usr/bin/clang
CFLAGS := -std=c11 -Wall -Wextra -I./libduckdb
LDFLAGS := -L./libduckdb -lduckdb -Wl,-rpath,@loader_path/libduckdb
TARGET := copy_answer_to_csv
SRC := copy_answer_to_csv.c

.PHONY: all build run clean

all: build

build: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) $(LDFLAGS) -o $(TARGET)

run: build
	./$(TARGET)

clean:
	rm -f $(TARGET)
