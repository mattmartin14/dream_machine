#parallel writer

import pandas as pd
import concurrent.futures
import os
import time

home_dir = os.path.expanduser("~")
f_path = "{0}/test_dummy_data/python/".format(home_dir)

def generate_data(start, end):
    return list(range(start, end))

def write_csv_chunk(chunk_number, chunk_size):
    data = generate_data(chunk_number * chunk_size, (chunk_number + 1) * chunk_size)
    df = pd.DataFrame({'Value': data})
    df.to_csv(f_path+ f'output_{chunk_number}.csv', index=False)

if __name__ == "__main__":

    start_ts = time.time()

    num_rows = 1000000000  # 1 billion rows
    chunk_size = 10000000  # Number of rows in each chunk
    num_chunks = num_rows // chunk_size

    #lock = threading.Lock()

    # Generate and write data in parallel using multi-threading
    #with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(write_csv_chunk, i, chunk_size) for i in range(num_chunks)]

        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            pass
    
    end_ts = time.time()

    elapsed_time = round(end_ts - start_ts,2)

    msg = "Wrote {0} rows in {1} seconds".format('{:,}'.format(num_rows),elapsed_time)

    print(msg)
