from mimesis import Person, Address, Datetime, Numeric
from mimesis.locales import Locale
import polars as pl
import uuid
import concurrent.futures
import time

sch = {
    "first_name": pl.String,
    "last_name": pl.String,
    "birth_dt": pl.Date,
    "email_adrs": pl.String,
    "country": pl.String,
    "zip_cd": pl.String,
    "city": pl.String,
    "lat": pl.String,
    "long": pl.String,
    "hire_dt": pl.Date,
    "txn_key": pl.String,
    "net_worth": pl.Int32
}

def write_data(file_num, rows):

    peep = Person(Locale.EN)
    adrs = Address(Locale.EN)
    dt = Datetime(Locale.EN)
    n = Numeric()

    data = []
    for i in range(rows):
        data.append([peep.first_name(), peep.last_name(), dt.date(), peep.email()
                    , adrs.country(), adrs.zip_code(), adrs.city(), str(adrs.latitude(False)), str(adrs.longitude(False))
                    , dt.date()
                    , str(uuid.uuid4())
                    , n.integer_number(1_000, 100_000)
                    ])

    df = pl.DataFrame(data, schema=sch)
    df.write_parquet(f'~/test_dummy_data/polars/test{file_num}.parquet')

def main():

    start_time = time.time()

    num_files = 10
    rows_per_file = 1_000_000
    tot_rows = num_files * rows_per_file

    with concurrent.futures.ProcessPoolExecutor(max_workers=num_files) as executor:
        futures = [executor.submit(write_data, i + 1, rows_per_file) for i in range(num_files)]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred: {e}")    

    end_time = time.time()
    print(f"Total time to create dataset with {tot_rows:,}: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()