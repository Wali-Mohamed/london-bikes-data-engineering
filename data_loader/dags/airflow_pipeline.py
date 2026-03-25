from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
print('hi')
with DAG(
    dag_id="tfl_bike_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tfl", "bikes"]
) as dag:

    extract_task = BashOperator(
        task_id="extract_tfl_data",
        bash_command="echo 'Downloading TfL bike data'"
    )

    upload_task = BashOperator(
        task_id="upload_to_gcs",
        bash_command="echo 'Uploading data to GCS'"
    )

    spark_task = BashOperator(
        task_id="spark_transform",
        bash_command="echo 'Running Spark transformation'"
    )

    load_task = BashOperator(
        task_id="load_bigquery",
        bash_command="echo 'Loading into BigQuery'"
    )

    extract_task >> upload_task >> spark_task >> load_task