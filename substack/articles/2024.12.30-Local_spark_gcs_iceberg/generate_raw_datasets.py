### generates raw datasets in csv and pumps them up to gcs using duckdb

import duckdb
from fsspec import filesystem
import os

cn = duckdb.connect()
cn.register_filesystem(filesystem('gcs'))

dataset_path = f"gs://{os.getenv("GCS_BUCKET")}/test_data"

#get the tpch datasets
cn.execute("install tpch; load tpch")
cn.execute("call dbgen(sf=1)")

sql_order_header = "select * from orders where o_orderkey between 1 and 10000"
cn.execute(f"copy ({sql_order_header}) to '{dataset_path}/order_hdr.parquet'")
print("order header data copied")

sql_order_detail = "select * from lineitem where l_orderkey between 1 and 10000"
cn.execute(f"copy ({sql_order_detail}) to '{dataset_path}/order_dtl.parquet'")
print("order header data copied")


