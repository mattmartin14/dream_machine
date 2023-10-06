import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

# Define your BigQuery dataset and table names
dataset_id = 'nested_example'
table_name = 'po_detail'


po_nbr = ["A1", "A1", "A1", "B1", "B1", "B1", "C2", "C2", "C2", "C2"]
po_line_nbr = [1, 2, 3, 1, 2, 3, 1, 2, 3, 4]
item_nbr = [1234, 3432, 3523, 25365, 63564, 23456, 48586, 32438, 975563, 34234]
ord_qty = [6, 22, 1, 34, 6, 4, 17, 5, 3, 14]
unit_retl_amt = [1.25, 6.22, 8.99, 4.45, 23.22, 55.14, 32.99, 28.99, 44.99, 41.99]



data = {'po_nbr': po_nbr, 'po_line_nbr': po_line_nbr, 'item_nbr':item_nbr, 'ord_qty':ord_qty
        ,'unit_retl_amt':unit_retl_amt}

df = pd.DataFrame(data)
df['unit_retl_amt'] = df['unit_retl_amt'].astype(float)


print(df)
print(df.dtypes)

table_ref = client.dataset(dataset_id).table(table_name)
client.load_table_from_dataframe(df, table_ref).result()


sql = 'SELECT * FROM `nested_example.po_detail` LIMIT 10'

df = pd.read_gbq(sql)
print(df.head(5).to_string())


"""
sql to create nested table:

CREATE OR REPLACE TABLE `nested_example.po_nested`
as
SELECT hdr.po_nbr, hdr.order_dt, hdr.loc_nbr
  ,array_agg(struct(dtl.po_line_nbr, dtl.item_nbr, dtl.ord_qty, dtl.unit_retl_amt)) as dtl
FROM `nested_example.po_header` as hdr
  join `nested_example.po_detail` as dtl
    using(po_nbr)
  group by 1,2,3
"""