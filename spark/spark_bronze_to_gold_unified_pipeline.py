from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, year, month, avg, dayofmonth, coalesce, trim, regexp_extract, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
# 1. Initialize for Dataproc & BigQuery
# No master("local") or hardcoded memory here; Dataproc handles that.
spark = SparkSession.builder \
    .appName("Silver_to_Gold_BigQuery") \
    .getOrCreate()

# 2. Define Cloud Paths
BUCKET = "london_bikes_data_lake_santander-bikes-pipeline"
INPUT_PATH = f"gs://{BUCKET}/silver/*.parquet"
# Format BigQuery dataset name (usually project_id.dataset_name)
# We will use the bucket name with underscores as the dataset ID by default
BQ_DATASET = "london_bikes_dw"

# 3. Read Cleaned Parquet from Silver Layer
# We use StringType for IDs to ensure total compatibility
silver_schema = StructType([
    StructField("ride_id", LongType(), True),
    StructField("start_time", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("bike_id", LongType(), True),
    StructField("bike_model", StringType(), True),
    StructField("duration_seconds", StringType(), True)
])

# Update your read line to use this schema
df = spark.read.schema(silver_schema).parquet(INPUT_PATH)


# Check and Repartition if necessary for distributed processing
if df.rdd.getNumPartitions() < 100:
    df = df.repartition(400)

raw_count = df.count()

# 4. Transform: Duration Logic
df = df.withColumn("start_time", trim("start_time"))

h = regexp_extract("duration_seconds", r"(\d+)h", 1).cast("int")
m = regexp_extract("duration_seconds", r"(\d+)m", 1).cast("int")
s = regexp_extract("duration_seconds", r"(\d+)s", 1).cast("int")

regex_seconds = (
    coalesce(h, lit(0)) * 3600 +
    coalesce(m, lit(0)) * 60 +
    coalesce(s, lit(0))
)

df = df.withColumn(
    "duration_seconds",
    coalesce(
        col("duration_seconds").cast("long"),   # Old numeric format
        regex_seconds                           # New "1h 5m" format
    )
)

# 5. Transform: Timestamp & Features
df = df.withColumn(
    "start_time",
    coalesce(
        to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss"),
        to_timestamp("start_time", "yyyy-MM-dd HH:mm"),
        to_timestamp("start_time", "dd/MM/yyyy HH:mm:ss"),
        to_timestamp("start_time", "dd/MM/yyyy HH:mm")
    )
).filter(col("start_time").isNotNull())

df = df.withColumn("year", year("start_time")) \
       .withColumn("month", month("start_time")) \
       .withColumn("day", dayofmonth("start_time")) \
       .withColumn("hour", hour("start_time"))

clean_count = df.count()

# 6. WRITE TO BIGQUERY (The Gold Table: trips)
# We use partitionBy on BigQuery to optimize query costs
# df.write.format("bigquery") \
#     .option("table", f"{BQ_DATASET}.trips") \
#     .option("temporaryGcsBucket", BUCKET) \
#     .mode("overwrite") \
#     .partitionBy("year", "month") \
#     .save()

# Constants
MY_BUCKET = "london_bikes_data_lake_santander-bikes-pipeline"
MY_TABLE = "london_bikes_dw.trips_partitioned"

# The "Full Marks" Write Operation
df.write.format("bigquery") \
    .option("table", MY_TABLE) \
    .option("temporaryGcsBucket", MY_BUCKET) \
    .option("partitionField", "start_time") \
    .option("partitionType", "DAY") \
    .option("clusteredFields", "start_station_id") \
    .mode("overwrite") \
    .save()