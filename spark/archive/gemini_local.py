from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, coalesce, trim,
    regexp_extract, lit,
    year, month, dayofmonth, hour
)
import os
import shutil

# 1. Setup with high memory for 451 files
spark = SparkSession.builder \
    .appName("LondonBikes_Universal_Pipeline") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

RAW_PATH = "data/raw/*.csv"
OUTPUT_PATH = "data/test/fact_trips"

# CLEANUP: Always wipe the old broken metadata/folders first
if os.path.exists("data/test"):
    shutil.rmtree("data/test")

# 2. READ with mergeSchema (The fix for the 2023-2025 shift)
# This forces Spark to look at HEADERS by name, not by position
df_raw = spark.read \
    .option("header", "true") \
    .option("mergeSchema", "true") \
    .option("inferSchema", "false") \
    .csv(RAW_PATH)

# 3. HELPER: The "Safe Column" Picker
def safe_col(df, *names):
    existing = df.columns
    for name in names:
        # Check for exact name (case-sensitive)
        if name in existing:
            return col(name)
    return lit(None)

# 4. HARMONIZE: Map all 15 years of different headers to 1 schema
df_clean = df_raw.select(
    safe_col(df_raw, "Number", "Rental Id").cast("string").alias("ride_id"),
    safe_col(df_raw, "Start date", "Start Date").cast("string").alias("start_time"),
    safe_col(df_raw, "End date", "End Date").cast("string").alias("end_time"),
    safe_col(df_raw, "Start station number", "StartStation Id", "Start Station Id").cast("string").alias("start_station_id"),
    safe_col(df_raw, "Start station", "StartStation Name", "Start Station Name").cast("string").alias("start_station_name"),
    safe_col(df_raw, "End station number", "EndStation Id", "End Station Id").cast("string").alias("end_station_id"),
    safe_col(df_raw, "End station", "EndStation Name", "End Station Name").cast("string").alias("end_station_name"),
    safe_col(df_raw, "Bike number", "Bike Id").cast("string").alias("bike_id"),
    safe_col(df_raw, "Bike model").cast("string").alias("bike_model"),
    safe_col(df_raw, "Duration", "Duration_Seconds", "Total duration").cast("string").alias("duration_seconds")
)

# 5. TRANSFORM: Durations and Timestamps
# Logic for "4h 45m" vs "285" seconds
h = regexp_extract("duration_seconds", r"(\d+)h", 1).cast("int")
m = regexp_extract("duration_seconds", r"(\d+)m", 1).cast("int")
s = regexp_extract("duration_seconds", r"(\d+)s", 1).cast("int")
regex_seconds = (coalesce(h, lit(0)) * 3600 + coalesce(m, lit(0)) * 60 + coalesce(s, lit(0)))

df_final = df_clean.withColumn(
    "duration_seconds", 
    coalesce(col("duration_seconds").cast("long"), regex_seconds)
).withColumn(
    "start_time",
    coalesce(
        to_timestamp(trim("start_time"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(trim("start_time"), "dd/MM/yyyy HH:mm:ss"),
        to_timestamp(trim("start_time"), "yyyy-MM-dd HH:mm"),
        to_timestamp(trim("start_time"), "dd/MM/yyyy HH:mm")
    )
)

# 6. FEATURE ENGINEERING: Year/Month/Day/Hour
df_final = df_final.filter(col("start_time").isNotNull()) \
    .withColumn("year", year("start_time")) \
    .withColumn("month", month("start_time")) \
    .withColumn("day", dayofmonth("start_time")) \
    .withColumn("hour", hour("start_time"))

# 7. WRITE: Partitioned by Year and Month
print(f"🚀 Starting Final Write to {OUTPUT_PATH}...")
df_final.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(OUTPUT_PATH)

print("✅ SUCCESS! All data from 2010 to 2025 has been processed and partitioned.")
spark.stop()