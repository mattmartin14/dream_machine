
"""
  Author: Matt Martin
  Date: 2023-10-06
  Desc: Creates a header/detail table and then a nested combined table in GBQ
"""


import pandas as pd
from google.cloud import bigquery


def create_load_po_detail():
    
  dataset_id = 'nested_example'
  table_name = 'po_detail'

  #make some data
  po_nbr = ["A1", "A1", "A1", "B1", "B1", "B1", "C2", "C2", "C2", "C2"]
  po_line_nbr = [1, 2, 3, 1, 2, 3, 1, 2, 3, 4]
  item_nbr = [1234, 3432, 3523, 25365, 63564, 23456, 48586, 32438, 975563, 34234]
  ord_qty = [6, 22, 1, 34, 6, 4, 17, 5, 3, 14]
  unit_retl_amt = [1.25, 6.22, 8.99, 4.45, 23.22, 55.14, 32.99, 28.99, 44.99, 41.99]

  data = {'po_nbr': po_nbr, 'po_line_nbr': po_line_nbr, 'item_nbr':item_nbr, 'ord_qty':ord_qty
          ,'unit_retl_amt':unit_retl_amt}

  df = pd.DataFrame(data)

  client = bigquery.Client()
  table_ref = client.dataset(dataset_id).table(table_name)

  ## set the config to create the table on the fly; if already exists, overwrite the data so we dont make dups
  job_config = bigquery.LoadJobConfig()
  job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
  job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

  client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()

def create_load_po_header():
    
  dataset_id = 'nested_example'
  table_name = 'po_header'

  po_nbrs = ['A1', 'B1', 'C2']
  order_dates = ['2023-05-01','2023-06-05','2023-08-01']
  loc_nbrs = [101, 105, 110]

  # Create a dictionary with arrays as values
  data = {'po_nbr': po_nbrs, 'order_dt': order_dates, 'loc_nbr':loc_nbrs}

  df = pd.DataFrame(data)

  #fix order date to date
  df['order_dt'] = pd.to_datetime(df['order_dt'])
  
  client = bigquery.Client()
  table_ref = client.dataset(dataset_id).table(table_name)

  job_config = bigquery.LoadJobConfig()
  job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
  job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

  client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()


def build_nested_table():
  sql = """
  CREATE OR REPLACE TABLE `nested_example.po_nested`
  as
  SELECT hdr.po_nbr, hdr.order_dt, hdr.loc_nbr
    ,CURRENT_DATETIME('America/New_York') AS last_upd_ts
    ,array_agg(struct(dtl.po_line_nbr, dtl.item_nbr, dtl.ord_qty, dtl.unit_retl_amt)) as dtl
  FROM `nested_example.po_header` as hdr
    join `nested_example.po_detail` as dtl
      using(po_nbr)
    group by 1,2,3
  """

  client = bigquery.Client()
  client.query(sql).result()
  


def run_all():
  create_load_po_header()
  print('po header table created')
  create_load_po_detail()
  print('po detail table created')
  build_nested_table()
  print('nested table created')


if __name__ == "__main__":
  run_all()
