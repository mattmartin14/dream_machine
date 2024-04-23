import os
import time
from concurrent.futures import ProcessPoolExecutor

NUM_FILES = 10
TOTAL_ROWS = 1000000000

def writeToFiles(start, end, fileNum):
    file_path = f"{os.getenv('HOME')}/test_dummy_data/c/data_{fileNum}.txt"
    with open(file_path, "w") as outFile:
        for i in range(start, end + 1):
            outFile.write(f"{i}\n")

def main():
    rows_per_file = TOTAL_ROWS // NUM_FILES
    start_time = time.time()

    with ProcessPoolExecutor() as executor:
        for i in range(NUM_FILES):
            start = i * rows_per_file + 1
            end = start + rows_per_file - 1
            executor.submit(writeToFiles, start, end, i + 1)

    stop_time = time.time()
    duration = (stop_time - start_time)

    msg = "Using Python, a total of {} files have been written successfully with {:,} rows in {} seconds".format(NUM_FILES, TOTAL_ROWS, duration)
    print(msg)
   
if __name__ == "__main__":
    main()
