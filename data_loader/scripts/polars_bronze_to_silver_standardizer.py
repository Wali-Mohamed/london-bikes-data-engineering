import polars as pl
import gcsfs

# 1. Define your Bucket Paths
# Replace 'your-bucket-name' with london_bikes_data_lake_santander-bikes-pipeline
BUCKET_NAME = "london_bikes_data_lake_santander-bikes-pipeline"
RAW_GCS_PATH = f"gs://{BUCKET_NAME}/raw/"
SILVER_GCS_PATH = f"gs://{BUCKET_NAME}/silver/"

TARGET_COLUMNS = [
    "ride_id", "start_time", "start_station_id", "start_station_name",
    "end_time", "end_station_id", "end_station_name", "bike_id",
    "bike_model", "duration_seconds"
]

RENAME_MAP = {
    "Rental Id": "ride_id", "Number": "ride_id",
    "Start Date": "start_time", "Start date": "start_time",
    "End Date": "end_time", "End date": "end_time",
    "StartStation Id": "start_station_id", "Start station number": "start_station_id",
    "StartStation Name": "start_station_name", "Start station": "start_station_name",
    "EndStation Id": "end_station_id", "End station number": "end_station_id",
    "EndStation Name": "end_station_name", "End station": "end_station_name",
    "Bike Id": "bike_id", "Bike number": "bike_id",
    "Duration": "duration_seconds", "Duration_Seconds": "duration_seconds",
    "Total duration": "duration_seconds"
}

# 2. Use GCSFS to list files in the cloud
fs = gcsfs.GCSFileSystem()
files = fs.glob(f"{RAW_GCS_PATH}*.csv")

for file_path in files:
    full_path = f"gs://{file_path}"
    file_name = file_path.split("/")[-1]
    print(f"Cloud Processing: {file_name}")

    # Polars reads directly from GCS
    df = pl.read_csv(
        full_path,
        infer_schema_length=10000,
        schema_overrides={
            "Start station number": pl.Utf8,
            "End station number": pl.Utf8,
            "StartStation Id": pl.Utf8,
            "EndStation Id": pl.Utf8,
        }
    )

    valid_map = {k: v for k, v in RENAME_MAP.items() if k in df.columns}
    df = df.rename(valid_map)

    # Standardize columns (same as your local logic)
    for col in TARGET_COLUMNS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    df = df.select(TARGET_COLUMNS)
    
  
    df = df.with_columns([
        pl.col("ride_id").cast(pl.Int64, strict=False),
        pl.col("bike_id").cast(pl.Int64, strict=False),
    
        pl.col("duration_seconds").cast(pl.Utf8),
        pl.col("start_station_id").cast(pl.Utf8),
        pl.col("end_station_id").cast(pl.Utf8),

        pl.col("start_station_name").cast(pl.Utf8),
        pl.col("end_station_name").cast(pl.Utf8),
        pl.col("bike_model").cast(pl.Utf8),

        pl.col("start_time").cast(pl.Utf8),
        pl.col("end_time").cast(pl.Utf8),
    ])

    # Write cleaned Parquet back to the Silver layer in GCS
    output_name = file_name.replace(".csv", ".parquet")
    df.write_parquet(f"{SILVER_GCS_PATH}{output_name}")