import duckdb
from pyiceberg.catalog.sql import SqlCatalog


warehouse_path = "./icehouse"
catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{warehouse_path}/icyhot.db",
        "warehouse": f"file://{warehouse_path}",
    },
)

namespace = "test_ns"
catalog.create_namespace(namespace)

rows = 5000

#duckdb
sql = f"""
    select t.row_id, uuid() as txn_key, current_date as rpt_dt
        ,round(random() * 100,2) as some_val
    from generate_series(1,{rows}) t(row_id)
"""

ds = duckdb.execute(sql).arrow()

table_name = "test_data"
ice_table = catalog.create_table(f"{namespace}.{table_name}",schema = ds.schema)
ice_table.append(ds)

print('table created and loaded')