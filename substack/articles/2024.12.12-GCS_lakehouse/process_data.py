import polars as pl
from google.cloud import storage
import os
import warnings

# Suppress specific Google Cloud SDK warning
warnings.filterwarnings(
    "ignore",
    message="Your application has authenticated using end user credentials.*",
    category=UserWarning,
    module="google.auth._default"
)

# Check if the Delta Table exists
def delta_table_exists(gcs_path: str) -> bool:
    bucket_name, prefix = gcs_path.replace("gs://", "").split("/", 1)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(storage_client.list_blobs(bucket, prefix=prefix))
    return any(blob.name.startswith(prefix) for blob in blobs)

#creates or merges the data
def create_or_merge_table(table_path: str, df: pl.DataFrame, merge_predicate: str):
    
    if delta_table_exists(table_path):

        df.write_delta(target=table_path, mode="merge", 
                    delta_merge_options={
                        "predicate":merge_predicate,
                        "source_alias" :"s",
                        "target_alias":"t",
                    },
                
        ).when_matched_update_all() \
        .when_not_matched_insert_all() \
        .execute()

        print('merge complete')

    else:
        df.write_delta(target=table_path, mode='overwrite')
        print('table created')



def process_datasets():
    bucket = os.getenv("GCS_BUCKET")
    warehouse_path = f'gs://{bucket}/bicycle_shop'

    header_table_path = f'{warehouse_path}/raw_data/ord_hdr/'
    detail_table_path = f'{warehouse_path}/raw_data/ord_dtl/'

    df_order_header = pl.scan_csv(header_table_path+"*.csv").collect()
    df_order_detail = pl.scan_csv(detail_table_path+"*.csv").collect()

    delta_header_path = f'{warehouse_path}/processed/ord_hdr/'
    delta_detail_path = f'{warehouse_path}/processed/ord_dtl/'

    create_or_merge_table(delta_header_path, df_order_header, "s.order_id = t.order_id")
    create_or_merge_table(delta_detail_path, df_order_detail, "s.order_id = t.order_id and s.product_id = t.product_id")

if __name__ == "__main__":
    process_datasets()

   
