from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, coalesce, trim,
    regexp_extract, lit,
    year, month, dayofmonth, hour
)

# ---------------------------
# 1. Spark session (Dataproc-ready)
# ---------------------------
spark = SparkSession.builder \
    .appName("BikePipeline") \
    .getOrCreate()

# ---------------------------
# 2. Config
# ---------------------------
# RAW_PATH = "gs://london_bikes_data_lake_santander-bikes-pipeline/data/raw/*.csv"
# CURATED_PATH = "gs://london_bikes_data_lake_santander-bikes-pipeline/data/curated"
RAW_PATH = "data/raw/*.csv"
test_path = "data/test1"
CURATED_PATH = "data/test"
# ---------------------------
# 3. Schema harmonisation setup
# ---------------------------
TARGET_COLUMNS = [
    "ride_id",
    "start_time",
    "start_station_id",
    "start_station_name",
    "end_time",
    "end_station_id",
    "end_station_name",
    "bike_id",
    "bike_model",
    "duration_seconds"
]

RENAME_MAP = {
    "Rental Id": "ride_id",
    "Number": "ride_id",

    "Start Date": "start_time",
    "Start date": "start_time",

    "End Date": "end_time",
    "End date": "end_time",

    "StartStation Id": "start_station_id",
    "Start station number": "start_station_id",
    "Start Station Id": "start_station_id",

    "StartStation Name": "start_station_name",
    "Start station": "start_station_name",
    "Start Station Name": "start_station_name",

    "EndStation Id": "end_station_id",
    "End station number": "end_station_id",
    "End Station Id": "end_station_id",

    "EndStation Name": "end_station_name",
    "End station": "end_station_name",
    "End Station Name": "end_station_name",

    "Bike Id": "bike_id",
    "Bike number": "bike_id",

    "Bike model": "bike_model",

    "Duration": "duration_seconds",
    "Duration_Seconds": "duration_seconds",
    "Total duration": "duration_seconds"
}

# ---------------------------
# 4. Read RAW from GCS
# ---------------------------
df = spark.read.csv(RAW_PATH, header=True)

# ---------------------------
# 5. Rename columns
# ---------------------------
for old, new in RENAME_MAP.items():
    if old in df.columns:
        df = df.withColumnRenamed(old, new)

# ---------------------------
# 6. Add missing columns
# ---------------------------
for c in TARGET_COLUMNS:
    if c not in df.columns:
        df = df.withColumn(c, lit(None))

# ---------------------------
# 7. Select target schema
# ---------------------------
df = df.select(TARGET_COLUMNS)

# ---------------------------
# 8. Cast types
# ---------------------------
df = df.withColumn("ride_id", col("ride_id").cast("long"))
df = df.withColumn("bike_id", col("bike_id").cast("long"))

df = df.withColumn("duration_seconds", col("duration_seconds").cast("string"))

df = df.withColumn("start_station_id", col("start_station_id").cast("string"))
df = df.withColumn("end_station_id", col("end_station_id").cast("string"))

df = df.withColumn("start_station_name", col("start_station_name").cast("string"))
df = df.withColumn("end_station_name", col("end_station_name").cast("string"))

df = df.withColumn("bike_model", col("bike_model").cast("string"))

df = df.withColumn("start_time", col("start_time").cast("string"))
df = df.withColumn("end_time", col("end_time").cast("string"))

df.write \
    .mode("overwrite") \
    .parquet(f"{test_path}/trips")
df = df.limit(100000)
# ---------------------------
# 9. Duration fix (regex + numeric)
# ---------------------------
hours = regexp_extract("duration_seconds", r"(\\d+)h", 1).cast("int")
minutes = regexp_extract("duration_seconds", r"(\\d+)m", 1).cast("int")
seconds = regexp_extract("duration_seconds", r"(\\d+)s", 1).cast("int")

regex_seconds = (
    coalesce(hours, lit(0)) * 3600 +
    coalesce(minutes, lit(0)) * 60 +
    coalesce(seconds, lit(0))
)

df = df.withColumn(
    "duration_seconds",
    coalesce(
        col("duration_seconds").cast("long"),
        regex_seconds
    )
)

#df = df.filter(col("duration_seconds").isNotNull())

# ---------------------------
# 10. Timestamp parsing
# ---------------------------
df = df.withColumn("start_time", trim("start_time"))

df = df.withColumn(
    "start_time",
    coalesce(
        to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss"),
        to_timestamp("start_time", "yyyy-MM-dd HH:mm"),
        to_timestamp("start_time", "dd/MM/yyyy HH:mm:ss"),
        to_timestamp("start_time", "dd/MM/yyyy HH:mm")
    )
)

#df = df.filter(col("start_time").isNotNull())

# ---------------------------
# 11. Feature engineering
# ---------------------------
df = df.withColumn("year", year("start_time"))
df = df.withColumn("month", month("start_time"))
df = df.withColumn("day", dayofmonth("start_time"))
df = df.withColumn("hour", hour("start_time"))

# ---------------------------
# 12. WRITE BASE CURATED TABLE (MOST IMPORTANT)
# ---------------------------
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"{CURATED_PATH}/trips")

# ---------------------------
# 13. OPTIONAL aggregations
# ---------------------------
# df.groupBy("year", "month", "hour") \
#   .count() \
#   .write.mode("overwrite") \
#   .partitionBy("year", "month") \
#   .parquet(f"{CURATED_PATH}/trips_by_hour")

# df.groupBy("start_station_name") \
#   .count() \
#   .write.mode("overwrite") \
#   .parquet(f"{CURATED_PATH}/trips_by_station")

print("Pipeline completed successfully")

spark.stop()