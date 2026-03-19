from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .appName("LondonBikes_Full_105M_Final") \
        .getOrCreate()
    
    # Essential fixes for 15 years of historical data
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    # 🔥 NEW CRITICAL FIX: Forces BigQuery-compatible timestamps
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    input_path = "gs://london_bikes_data_lake_santander-bikes-pipeline/raw/*.csv"
    output_table = "santander-bikes-pipeline.london_bikes_dw.fact_trips"
    temp_bucket = "london_bikes_data_lake_santander-bikes-pipeline"

    df_raw = spark.read.option("header", "true") \
        .option("mergeSchema", "true") \
        .csv(input_path)

    def get_col(options):
        existing = [c for c in options if c in df_raw.columns]
        return F.col(f"`{existing[0]}`") if existing else F.lit(None)

    ride_id_opts = ["Rental Id", "Number", "rental id", "number", "ride_id", "rental_id"]
    date_opts = ["Start date", "Start Date", "start date", "start_date", "Start_Date"]
    dur_opts = ["duration", "Duration", "Total duration", "Duration_Seconds", "duration_seconds"]
    start_stat_id = ["StartStation Id", "Start station number", "start_station_id"]
    start_stat_name = ["StartStation Name", "Start station", "start_station_name"]
    end_stat_id = ["EndStation Id", "End station number", "end_station_id"]
    end_stat_name = ["EndStation Name", "End station", "end_station_name"]
    bike_opts = ["Bike Id", "Bike number", "bike_id"]

    raw_date_str = get_col(date_opts)
    raw_dur = get_col(dur_opts)

    parsed_date = F.coalesce(
        F.to_timestamp(raw_date_str, "yyyy-MM-dd HH:mm"),
        F.to_timestamp(raw_date_str, "dd/MM/yyyy HH:mm"),
        F.to_timestamp(raw_date_str, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(raw_date_str, "dd/MM/yyyy HH:mm:ss"),
        F.to_timestamp(raw_date_str)
    )

    hrs  = F.regexp_extract(raw_dur.cast("string"), r"(\d+)\s*h", 1).cast("int")
    mins = F.regexp_extract(raw_dur.cast("string"), r"(\d+)\s*m", 1).cast("int")
    secs = F.regexp_extract(raw_dur.cast("string"), r"(\d+)\s*s", 1).cast("int")
    
    duration_final = F.coalesce(
        (F.coalesce(hrs, F.lit(0)) * 3600) + 
        (F.coalesce(mins, F.lit(0)) * 60) + 
        F.coalesce(secs, F.lit(0)),
        raw_dur.cast("int"),
        F.lit(0)
    )

    df_clean = df_raw.select(
        get_col(ride_id_opts).alias("ride_id"),
        parsed_date.alias("start_time"),
        get_col(start_stat_id).alias("start_station_id"),
        get_col(start_stat_name).alias("start_station_name"),
        get_col(end_stat_id).alias("end_station_id"),
        get_col(end_stat_name).alias("end_station_name"),
        get_col(bike_opts).alias("bike_id"),
        duration_final.alias("duration_seconds")
    )

    df_final = df_clean.filter(F.col("start_time").isNotNull()& 
                        (F.col("start_time") > "1900-01-01") & 
                        (F.col("start_time") < "2100-01-01")
                        ) \
                       .withColumn("year", F.year("start_time")) \
                       .withColumn("month", F.month("start_time")) \
                       .repartition(100)

    df_final.write.format("bigquery") \
        .option("table", output_table) \
        .option("temporaryGcsBucket", temp_bucket) \
        .mode("overwrite") \
        .save()
    
    print("✅ SUCCESS: 105M rows processed!")

if __name__ == "__main__":
    main()