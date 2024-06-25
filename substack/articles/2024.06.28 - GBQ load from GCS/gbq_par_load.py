"""
Author: Matt Martin
Date: 6/25/24
Desc: Loads bigquery table from gcs files in parallel/async
"""

import os
from google.cloud import bigquery, storage
import warnings
import concurrent.futures

warnings.filterwarnings("ignore", message="Your application has authenticated using end user credentials from Google Cloud SDK")

def load_table_file(bq_client, dataset_id, table_id, bucket_name, blob_name):
    uri = f"gs://{bucket_name}/{blob_name}"
    print(f"Loading {blob_name} to BQ table {table_id}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
    )

    load_job = bq_client.load_table_from_uri(
        uri,
        bq_client.dataset(dataset_id).table(table_id),
        job_config=job_config
    )

    load_job.result()  # Wait for the job to complete
    print(f"Loaded {blob_name} into {table_id}")

def load_table(dataset_id: str, table_id: str, bucket_name: str, gcs_folder: str) -> None:
    bq_client = bigquery.Client()
    gcs_client = storage.Client()

    bucket = gcs_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=gcs_folder)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for blob in blobs:
            if blob.name.endswith('.csv'):
                futures.append(
                    executor.submit(load_table_file, bq_client, dataset_id, table_id, bucket_name, blob.name)
                )
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Generated an exception: {exc}")
                

def main() -> None:
    dataset_id = "test_ds"
    table_id = "test_tbl"
    bucket_name = os.getenv("GCS_BUCKET")
    gcs_folder = "test_data"
    load_table(dataset_id, table_id, bucket_name, gcs_folder)

if __name__ == "__main__":
    main()
