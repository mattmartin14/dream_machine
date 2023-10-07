import pandas as pd
from google.cloud import bigquery
import os

client = bigquery.Client()

# Define your BigQuery dataset and table names
dataset_id = 'ds1_test'
table_name = 'customers'

# Path to your CSV file
csv_file_path = '~/test_dummy_data/gbq/cust.csv'

# we can define the schema here or do an auto detect
# schema = [
#     bigquery.SchemaField('cust_id', 'INTEGER'),
#     bigquery.SchemaField('cust_name', 'STRING'),
#     bigquery.SchemaField('create_date', 'DATE'),
#     bigquery.SchemaField('zip_cd', 'STRING')
# ]

job_config = bigquery.LoadJobConfig(
    create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED,
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
    skip_leading_rows=1,
    source_format = bigquery.SourceFormat.CSV,
    #schema = schema
    autodetect=True #autodetect the schema,
    
)

table_ref = client.dataset(dataset_id).table(table_name)


with open(os.path.expanduser(csv_file_path), 'rb') as f:
    client.load_table_from_file(f, table_ref, job_config=job_config).result()


#validate
sql = 'SELECT * FROM `ds1_test.customers` LIMIT 10'

df = pd.read_gbq(sql)
print(df.head(5).to_string())