from google.cloud import storage
import time
import os

BUCKET_NAME = os.environ["GCP_GCS_BUCKET"]
LOCAL_FOLDER = "/opt/airflow/data"

def upload_with_retry(blob, local_path, retries=3):
    blob.chunk_size = 5 * 1024 * 1024  # 5MB chunks

    for attempt in range(retries):
        try:
            blob.upload_from_filename(local_path, timeout=300)
            print(f"Uploaded {local_path}")
            return True
        except Exception as e:
            print(f"Retry {attempt+1} failed: {local_path} → {e}")
            time.sleep(2 ** attempt)

    print(f"FAILED permanently: {local_path}")
    return False
def upload_folder():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    failures = []
    for root, _, files in os.walk(LOCAL_FOLDER):
        for file in files:
            local_path = os.path.join(root, file)

            # preserve folder structure inside GCS
            relative_path = os.path.relpath(local_path, LOCAL_FOLDER)
            gcs_path = f"raw_test/{relative_path}"

            blob = bucket.blob(gcs_path)
            if blob.exists():
                print(f"Already exists: {gcs_path}")
                continue
            
            success = upload_with_retry(blob, local_path)
            if not success:
                failures.append(local_path)
    if failures:
        raise Exception(f"{len(failures)} files failed to upload")
if __name__ == "__main__":
    upload_folder()