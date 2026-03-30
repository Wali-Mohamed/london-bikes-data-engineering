from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="tfl_bike_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["tfl", "bikes"]
) as dag:

    extract_task = BashOperator(
        task_id="extract_tfl_data",
        bash_command="python /opt/airflow/scripts/extract_tfl_data.py"
    )

    upload_task = BashOperator(
        task_id="upload_to_gcs",
        bash_command="python /opt/airflow/scripts/upload_to_gcs.py"
    )

    schema_task = BashOperator(
        task_id="standardize",
        bash_command="python /opt/airflow/scripts/polars_bronze_to_silver_standardizer.py"
    )
   

    spark_task = BashOperator(
        task_id="spark_transform",
        bash_command="python /opt/airflow/spark/spark_bronze_to_gold_unified_pipeline.py"
    )
   

    extract_task >> upload_task >> schema_task >> spark_task