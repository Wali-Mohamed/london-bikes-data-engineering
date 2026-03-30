git add .
git commit -m 'initial commit'
git commit -am 'prepare local repo with main folder'


1- docker-compose up -d postgres
# Wait 5 seconds for Postgres to wake up
2- docker-compose run --rm airflow-webserver airflow db init

## Inspect CSV Schemas

Check how many different schemas exist in the raw TfL CSV files.

```bash
head -q -n 1 data/raw/*.csv | sort | uniq -c
```
```bash
head -q -n 1 data/test/*.parquet | sort | uniq -c
```
```bash
rm -rf data/clean
```
## check folder size

```bash
du -sh data/curated
```
count files

```bash
find data/raw -name "*.csv" | wc -l
```
count all rows
```bash

```

cloud commands

from cloud shell send the code to vm
```bash 
gcloud compute scp ./polars_bronze_to_silver_standardizer.py bike-cluster-m:~/ --zone=europe-west2-c
```

Run the vm
```bash
gcloud compute ssh bike-cluster-m --zone=europe-west2-c --command="python3 ~/polars_bronze_to_silver_standardizer.py"
```

count the files
```bash
gsutil ls gs://london_bikes_data_lake_santander-bikes-pipeline/silver/*.parquet | wc -l
```

run data proc
```bash
gcloud dataproc jobs submit pyspark \
    gs://london_bikes_data_lake_santander-bikes-pipeline/scripts/spark_bronze_to_gold_unified_pipeline.py \
    --cluster=bike-cluster \
    --region=europe-west2 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --properties="spark.conf.set.temporaryGcsBucket=dataproc-temp-europe-west2-335511994446-buzuqs49"

```
count files
```
gcloud storage ls "gs://london_bikes_data_lake_santander-bikes-pipeline/silver/*.parquet" | wc -l
```
docker check files

```
docker exec -it london-bikes-data-engineering-airflow-scheduler-1 bash
```