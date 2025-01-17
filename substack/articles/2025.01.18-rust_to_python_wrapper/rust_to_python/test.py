#import rust_to_python
#rust_to_python.write_fast()

import csv
import os
import time
import sys


def create_csv_int_file():
    
    tot_rows = 100_000_000
    buffer_size = 1024 * 1024
    print('start')
    start_ts = time.time()

    home_dir = os.path.expanduser("~")
    f_path = "{0}/test_dummy_data/python/test_data.csv".format(home_dir)
    with open(f_path, 'w', newline='\n', buffering=buffer_size) as csvfile:
            csv_writer = csv.writer(csvfile)

            for i in range(1,tot_rows+1):
                csv_writer.writerow([i])

    end_ts = time.time()

    elapsed_time = round(end_ts - start_ts,2)

    msg = "Wrote {0} rows in {1} seconds".format('{:,}'.format(tot_rows),elapsed_time)

    print(msg)

if __name__ == "__main__": 
     create_csv_int_file()