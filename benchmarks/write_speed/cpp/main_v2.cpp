#include <iostream>
#include <fstream>
#include <chrono>
#include <string>
#include <cstdlib> // For getenv

// to compile: g++ -std=c++11 -o cpp_test main_v2.cpp

// CPP file runs in 29 seconds

const int num_rows = 1000000000;
const int buffer_size = 1024 * 1024 * 10; // 10 MB buffer size

void writeWithBuffer(const std::string& file_name) {
    std::ofstream outfile(file_name); // Open the file in text mode

    if (!outfile.is_open()) {
        std::cerr << "Error opening the file: " << file_name << std::endl;
        return;
    }

    char* buffer = new char[buffer_size];
    std::fill(buffer, buffer + buffer_size, ' ');

    const char* fmt_string = "%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n";

    int batch_size = 0;
    char* buffer_ptr = buffer;
    
    for (int i = 1; i <= num_rows; i += 20) {
        int chars_written = snprintf(buffer_ptr, buffer_size, fmt_string
            , i, i + 1, i + 2, i + 3, i + 4, i + 5, i + 6, i + 7, i + 8, i + 9
            , i + 10, i + 11, i + 12, i + 13, i + 14, i + 15, i + 16, i + 17, i + 18, i + 19
        );
        buffer_ptr += chars_written;
        batch_size++;

        if (batch_size == 10000) {
            outfile.write(buffer, buffer_ptr - buffer);
            buffer_ptr = buffer;
            batch_size = 0;
        }
    }

    // Ensure any remaining data in the buffer is written
    if (batch_size > 0) {
        outfile.write(buffer, buffer_ptr - buffer);
        batch_size = 0;
    }

    delete[] buffer;
    outfile.close();
}

int main() {
    const char* home_dir = getenv("HOME");
    std::string file_name = std::string(home_dir) + "/test_dummy_data/write_benchmark/cpp_generated.csv";

    auto start_ts = std::chrono::high_resolution_clock::now();

    if (!file_name.empty()) {
        writeWithBuffer(file_name);
    }

    auto end_ts = std::chrono::high_resolution_clock::now();

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end_ts - start_ts);

    //std::cout << "Elapsed time: " << elapsed.count() << " seconds" << std::endl;

    std::cout << "C++ benchmark complete. Wrote " << num_rows << " rows in " << elapsed.count() << " seconds" << std::endl;

    return 0;
}
