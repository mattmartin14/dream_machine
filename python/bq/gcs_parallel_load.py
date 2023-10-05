"""
Author: Matt Martin
Date: 2023-10-05
Desc: Parallel Load to GCS

Notes: Noticed that processpoolexecutor does not play well with the GCS api; 
had to use threadpoolexecutor
"""

import concurrent.futures
import glob
from google.cloud import storage
import os

def upload_file_to_gcs(local_file_path):
    gcs_bucket_name = "data-mattm-test-sbx"
    gcs_key_path = "test_files/"+os.path.basename(local_file_path)
    client = storage.Client()
    bucket = client.get_bucket(gcs_bucket_name) 
    blob = bucket.blob(gcs_key_path)
    blob.upload_from_filename(local_file_path)
    print("file {0} uploaded successfully".format(os.path.basename(local_file_path)))


def load_parallel_gcs():

    local_folder_path = os.path.expanduser("~/test_dummy_data/gcs/*.csv")
    csv_file_list = glob.glob(local_folder_path)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(upload_file_to_gcs, csv_file_list)

if __name__ == "__main__":
    load_parallel_gcs()