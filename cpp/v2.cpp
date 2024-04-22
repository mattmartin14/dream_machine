#include <iostream>
#include <fstream>
#include <sys/types.h>
#include <unistd.h>
#include <sys/wait.h>
#include <cstdlib>
#include <chrono> 
#include <locale> 

using namespace std;
using namespace std::chrono;

/*
    Notes:
        I can get forking (similar to python multiprocess) working just fine
        can't seem to get threading working though.

        10 files for 1B rows writen in under 8 seconds

*/


// under 7 seconds to write 1B rows to 10 files
const int tot_rows = 2000000000;
const int numFiles = 10;

void writeToFiles(int start, int end, int fileNum) {
    string homeDir = getenv("HOME");
    string filePath = homeDir + "/test_dummy_data/cpp/data_" + to_string(fileNum) + ".txt";
    ofstream outFile(filePath);

    // Create a buffer to hold the numbers
    const int bufferSize = 8192; 
    char buffer[bufferSize];
    int bufferIndex = 0;

    for (int i = start; i <= end; ++i) {
        // Convert integer to string representation
        string numStr = to_string(i);

        // Check if adding the number will exceed buffer size
        if (bufferIndex + numStr.length() + 1 >= bufferSize) {
            // Write buffer to file
            outFile.write(buffer, bufferIndex);
            // Reset buffer index
            bufferIndex = 0;
        }

        // Copy the string to the buffer
        memcpy(buffer + bufferIndex, numStr.c_str(), numStr.length());
        bufferIndex += numStr.length();

        // Add newline character
        buffer[bufferIndex++] = '\n';
    }

    // Write remaining buffer to file
    if (bufferIndex > 0) {
        outFile.write(buffer, bufferIndex);
    }

    outFile.close();
}



int main() {
    
    const int rows_per_file = tot_rows / numFiles;

    high_resolution_clock::time_point start_time = high_resolution_clock::now();

    pid_t pid;

    for (int i = 0; i < numFiles; ++i) {
        pid = fork();

        if (pid < 0) {
            cerr << "Fork failed!" << endl;
            return 1;
        } else if (pid == 0) {
            // Child process
            int start = i * rows_per_file + 1;
            int end = start + rows_per_file - 1;
            writeToFiles(start, end, i + 1);
            return 0;
        }
    }

    // Parent process waits for all child processes to finish
    for (int i = 0; i < numFiles; ++i) {
        wait(NULL);
    }

    high_resolution_clock::time_point stop_time = high_resolution_clock::now();
    milliseconds duration = duration_cast<milliseconds>(stop_time - start_time);

    // Format tot_rows with commas
    locale loc("");
    cout.imbue(loc);
    cout << "A total of " << numFiles << " files have been written successfully with " << fixed << tot_rows << " in " << duration.count() << " milliseconds." << endl;

    return 0;
}
