import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

# Define your BigQuery dataset and table names
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
print(df)

table_ref = client.dataset(dataset_id).table(table_name)
client.load_table_from_dataframe(df, table_ref).result()


sql = 'SELECT * FROM `nested_example.po_header` LIMIT 10'

df = pd.read_gbq(sql)
print(df.head(5).to_string())