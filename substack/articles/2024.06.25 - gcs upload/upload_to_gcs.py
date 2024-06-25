import os
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.auth.exceptions import RefreshError

def upload_file(bucket, f_path, gcs_path):
    try:
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(f_path)
    except Exception as e:
        print(f"Failed to upload {f_path}: {e}")

def upload_files_par_to_gcs_bucket(local_dir: str, gcs_folder: str) -> None:
    try:
        client = storage.Client()
        gcs_bucket_name = os.getenv("GCS_BUCKET")
        
        bucket = client.bucket(gcs_bucket_name)
        local_dir = os.path.expanduser(local_dir)

        with ThreadPoolExecutor() as executor:
            for fname in os.listdir(local_dir):
                if fname.endswith(".csv"):
                    f_path = os.path.join(local_dir, fname)
                    gcs_path = f"{gcs_folder}/{fname}"
                    
                    executor.submit(upload_file, bucket, f_path, gcs_path)
    except RefreshError as e:
        print(f"RefreshError: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def main() -> None: 
    local_dir = "~/test_dummy_data/fd/"
    gcs_folder = "test_data"
    upload_files_par_to_gcs_bucket(local_dir, gcs_folder)

if __name__ == "__main__":
    main()