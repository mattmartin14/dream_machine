"""A Google Cloud Python Pulumi program"""

import pulumi
from pulumi_gcp import storage

# Create a GCP resource (Storage Bucket)
bucket_name = "test-matt-super-secret-bucket"
bucket = storage.Bucket(resource_name=bucket_name,name=bucket_name, location="US")

# Export the DNS name of the bucket
pulumi.export('bucket_name', bucket.url)
