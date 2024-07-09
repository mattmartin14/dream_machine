import pandas as pd
from google.cloud import bigquery
import os
import warnings

warnings.filterwarnings("ignore", message="Your application has authenticated using end user credentials from Google Cloud SDK")

def load_csv_to_bigquery(file_path, table_id):
    df = pd.read_csv(file_path)

    filename = os.path.splitext(os.path.basename(file_path))[0]
    df['filename'] = filename

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
    
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    # Wait for the load job to complete
    job.result()

    print(f"Loaded {df.shape[0]} rows into {table_id} from {file_path}")

def main():
    # Paths to your CSV files
    csv_files = ['./files/California.csv', './files/Georgia.csv']
    table_id = 'test_ds.states_flat'  # Replace with your project ID, dataset ID, and table ID

    for csv_file in csv_files:
        load_csv_to_bigquery(csv_file, table_id)

if __name__ == "__main__":
    main()