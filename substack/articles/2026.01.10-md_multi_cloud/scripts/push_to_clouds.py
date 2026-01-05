
import os
import boto3
from google.cloud import storage

def upload_orders_to_s3():
    bucket = os.getenv("aws_bucket")
    key = "multi_cloud/orders.parquet"
    local_path = '../data/orders.parquet'
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.upload_file(local_path, bucket, key)

def upload_insights_to_gcs():
    bucket = os.getenv("GCS_BUCKET")
    key = "multi_cloud/customer_insights.parquet"
    local_path = '../data/customer_insights.parquet'

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(key)
    blob.upload_from_filename(local_path)

if __name__ == "__main__":
    upload_orders_to_s3()
    print('orders uploaded to s3')
    upload_insights_to_gcs()
    print('insights uploaded to gcs')
