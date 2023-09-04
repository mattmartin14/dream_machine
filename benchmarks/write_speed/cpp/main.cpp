#include <iostream>
#include <fstream>
#include <chrono>
#include <string>
#include <cstdlib> // For getenv

const int num_rows = 1000000000;
const int buffer_size = 1024 * 1024; 

void writeWithBuffer(const std::string& file_name) {
    std::ofstream outfile(file_name, std::ios::binary); // Open the file in binary mode

    if (!outfile.is_open()) {
        std::cerr << "Error opening the file: " << file_name << std::endl;
        return;
    }

    char* buffer = new char[buffer_size];
    std::fill(buffer, buffer + buffer_size, ' ');

    const char* fmt_string = "%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n%d\n";

    for (int i = 1; i <= num_rows; i+=20) {
        snprintf(buffer, buffer_size, fmt_string
            , i,i+1,i+2,i+3,i+4,i+5,i+6,i+7,i+8,i+9
            , i+10,i+11,i+12,i+13,i+14,i+15,i+16,i+17,i+18,i+19
        );
        outfile.write(buffer, strlen(buffer));
    }

    delete[] buffer;
    outfile.close();
}

int main() {
    
    std::cout << "hello world" << std::endl;
    
    const char* home_dir = getenv("HOME");
    std::string file_name = std::string(home_dir) + "/test_dummy_data/write_benchmark/cpp_generated.csv";

    auto start_ts = std::chrono::high_resolution_clock::now();

    if (!file_name.empty()) {
        writeWithBuffer(file_name);
    }

    auto end_ts = std::chrono::high_resolution_clock::now();

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end_ts - start_ts);

    std::cout << "Elapsed time: " << elapsed.count() << " seconds" << std::endl;

    std::cout << "Wrote " << num_rows << " integer rows to " << file_name << std::endl;

    return 0;
}
