import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

# Define your BigQuery dataset and table names
dataset_id = 'ds1_test'
table_name = 'users2'

# Path to your CSV file
csv_file_path = '~/test_dummy_data/gbq/test_data.csv'

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file_path)

table_ref = client.dataset(dataset_id).table(table_name)
client.load_table_from_dataframe(df, table_ref).result()


sql = 'SELECT * FROM `ds1_test.users2` LIMIT 10'

df = pd.read_gbq(sql)
print(df.head(5).to_string())