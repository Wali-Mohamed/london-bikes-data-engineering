from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, year, month, avg,col, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, LongType
spark = SparkSession.builder \
    .appName("Santander Bikes Transformations") \
    .getOrCreate()

schema = StructType([
    StructField("start_station_name", StringType(), True),
    StructField("duration_seconds", StringType(), True)
])




# read cleaned parquet
df = spark.read.parquet("data/clean/*.parquet")
df = df.withColumn(
    "duration_seconds",
    col("duration_seconds").cast("long")
)
df = df.filter(col("duration_seconds").isNotNull())

# convert timestamp
df = df.withColumn(
    "start_time",
    to_timestamp(col("start_time"), "dd/MM/yyyy HH:mm")
)

# derive time features
df = df.withColumn("year", year("start_time"))
df = df.withColumn("month", month("start_time"))
df = df.withColumn("day", dayofmonth("start_time"))
df = df.withColumn("hour", hour("start_time"))

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

df.select(year("start_time")).distinct().orderBy("year(start_time)").show()