from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, year, month, avg, dayofmonth,  coalesce, trim, regexp_extract,lit
from pyspark.sql.types import StructType, StructField, StringType, LongType

# 1. Initialize the session with safety "guardrails"
spark = SparkSession.builder \
    .appName("DataEngineeringTask") \
    .master("local[6]") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "800") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.sql.files.maxPartitionBytes", "33554432") \
    .getOrCreate()

# read cleaned parquet


df = spark.read.parquet("data/clean/*.parquet")
# Check the 'funnel' size
print(f"Starting with {df.rdd.getNumPartitions()} partitions")
# 3. FIX: Ensure the data is distributed immediately
# This prevents the '1-core-choke' problem
if df.rdd.getNumPartitions() < 100:
    print(f"Low partition count detected ({df.rdd.getNumPartitions()}). Repartitioning for laptop safety...")
    df = df.repartition(400)
raw=df.count()
df = df.withColumn("start_time", trim("start_time"))
hours = regexp_extract("duration_seconds", r"(\d+)h", 1).cast("int")
minutes = regexp_extract("duration_seconds", r"(\d+)m", 1).cast("int")
seconds = regexp_extract("duration_seconds", r"(\d+)s", 1).cast("int")
regex_seconds = (
    coalesce(hours, lit(0)) * 3600 +
    coalesce(minutes, lit(0)) * 60 +
    coalesce(seconds, lit(0))
)

df = df.withColumn(
    "duration_seconds",
    coalesce(
        col("duration_seconds").cast("long"),   # old format
        regex_seconds                           # new format
    )
)


df = df.filter(col("duration_seconds").isNotNull())

#convert timestamp

df = df.withColumn(
    "start_time",
    coalesce(
        to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss"),
        to_timestamp("start_time", "yyyy-MM-dd HH:mm"),
        to_timestamp("start_time", "dd/MM/yyyy HH:mm:ss"),
        to_timestamp("start_time", "dd/MM/yyyy HH:mm")
    )
)

df = df.filter(col("start_time").isNotNull())
clean=df.count()
# df = df.withColumn(
#     "end_time",
#     coalesce(
#         to_timestamp("end_time", "yyyy-MM-dd HH:mm:ss"),
#         to_timestamp("end_time", "yyyy-MM-dd HH:mm"),
#         to_timestamp("end_time", "dd/MM/yyyy HH:mm:ss"),
#         to_timestamp("end_time", "dd/MM/yyyy HH:mm")
#     )
# )


# derive time features
df = df.withColumn("year", year("start_time"))
df = df.withColumn("month", month("start_time"))
df = df.withColumn("day", dayofmonth("start_time"))
df = df.withColumn("hour", hour("start_time"))

df = df.cache()
df.count() 
df.groupBy("year").count().orderBy("year").show()
# aggregation example
trips_by_hour = df.groupBy("year", "month", "hour").count()

# write curated table
trips_by_hour.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("data/curated/trips_by_hour")


trips_by_station = df.groupBy("start_station_name").count()

trips_by_station.write \
    .mode("overwrite") \
    .parquet("data/curated/trips_by_station")

avg_trip_duration = df.groupBy("start_station_name") \
    .agg(avg("duration_seconds").alias("avg_duration"))

avg_trip_duration.write \
    .mode("overwrite") \
    .parquet("data/curated/avg_trip_duration")

routes = df.groupBy("start_station_name", "end_station_name").count()

routes.write \
    .mode("overwrite") \
    .parquet("data/curated/popular_routes")

trips_per_day = df.groupBy("year", "month", "day").count()

trips_per_day.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("data/curated/trips_per_day")

print(f'raw:{raw}')
print(f'clean:{clean}')