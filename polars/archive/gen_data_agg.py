""" 
Author: Matt Martin
Date: 3/8/24
Desc: Generating some random data, using polars to transform and write out to excel and parquet
"""

import polars as pl
from mimesis import Person, Address, Numeric
import xlsxwriter
import time

start_ts = time.time()

peep = Person()
adrs = Address()
num = Numeric()

num_rows = 1_000_000

data = []
for _ in range(num_rows):
    data.append({'name':peep.full_name(), 'address':adrs.address(), 'zip_cd':adrs.zip_code()
                 , 'state':adrs.state(), 'net_worth':num.integer_number(5000,50000)})
    
df = pl.DataFrame(data)

## agg and transform
res = df.group_by(pl.col('zip_cd')).agg(pl.col('name').n_unique().name.suffix("_cnt")
                                        ,pl.col('address').count().name.suffix("_cnt")
                                        ,pl.col('net_worth').sum().name.suffix("_tot")
                            )

## write out res
res.write_parquet("./agg_pl.parquet")
res.write_excel("./test.xlsx")

end_ts = time.time()

elapsed = end_ts - start_ts

print("total time to generate {0} rows and aggregate: {1} seconds".format(num_rows, elapsed))