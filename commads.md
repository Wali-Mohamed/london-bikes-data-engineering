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