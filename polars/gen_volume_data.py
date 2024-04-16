"""
    Author: Matt Martin
    Date: 4/15/24
    Desc: Generates dummy data csv files in parallel using mimesis
"""

import time
import os
from mimesis import Person, Address, Numeric
import csv
import io
from concurrent.futures import ProcessPoolExecutor, as_completed

# Set parameters
ROW_CNT = 10_000_000
BATCH_SIZE = 10_000
NUM_BATCHES = 15
MAX_WORKERS = 8

# Define data generation function
def generate_data(batch_nbr):
    try: 
        peep = Person()
        adrs = Address()
        num = Numeric()

        #check if on last batch
        if batch_nbr == NUM_BATCHES - 1:  
            num_rows = ROW_CNT - (ROW_CNT // NUM_BATCHES) * (NUM_BATCHES - 1)
        else:
            num_rows = ROW_CNT // NUM_BATCHES

        # Generate data
        data = []
        for _ in range(num_rows):
            data.append([
                peep.first_name(),
                peep.last_name(),
                peep.gender(),
                peep.weight(),
                peep.height(),
                adrs.address(),
                adrs.zip_code(),
                adrs.city(),
                adrs.state(),
                num.integer_number(0, 50000)
            ])

        # Write data to CSV file
        output_path = os.path.expanduser(f'~/test_dummy_data/polars2/data{batch_nbr}.csv')
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['first_name', 'last_name', 'gender','weight','height','address_txt', 'zip_cd', 'city', 'state', 'net_worth'])
            writer.writerows(data)
    except Exception as e:
        raise RuntimeError(f"Error occurred in generate_data for batch {batch_nbr}: {e}")


def main():
    start_time = time.time()

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(generate_data, batch_nbr) for batch_nbr in range(NUM_BATCHES)]

        # Capture exceptions from futures
        for future in as_completed(futures):
            exception = future.exception()
            if exception:
                raise exception

    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total time to generate {ROW_CNT}: {total_time} seconds")

if __name__ == "__main__":
    main()