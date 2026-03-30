from google.cloud import storage
import os

BUCKET_NAME = os.environ["GCP_GCS_BUCKET"]
LOCAL_FOLDER = "/opt/airflow/data/raw_test"

def upload_folder():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    for root, _, files in os.walk(LOCAL_FOLDER):
        for file in files:
            local_path = os.path.join(root, file)

            # preserve folder structure inside GCS
            relative_path = os.path.relpath(local_path, LOCAL_FOLDER)
            gcs_path = f"raw_test/{relative_path}"

            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)

            print(f"Uploaded {local_path} → {gcs_path}")

if __name__ == "__main__":
    upload_folder()