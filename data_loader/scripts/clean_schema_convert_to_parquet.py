import polars as pl
from pathlib import Path

raw_path = Path("data/raw")
clean_path = Path("data/clean")

clean_path.mkdir(parents=True, exist_ok=True)

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

for file in raw_path.glob("*.csv"):

    print(f"Processing {file.name}")

    df = pl.read_csv(
    file,
    infer_schema_length=10000,
    schema_overrides={
        "Start station number": pl.Utf8,
        "End station number": pl.Utf8,
        "StartStation Id": pl.Utf8,
        "EndStation Id": pl.Utf8,
    }
)
    # rename known columns
    valid_map = {k: v for k, v in RENAME_MAP.items() if k in df.columns}
    df = df.rename(valid_map)

    # drop junk columns
    df = df.drop([
        col for col in df.columns
        if col.startswith("_duplicated")
        or col in ["", "Total duration (ms)", "endStationPriority_id",
                   "EndStation Logical Terminal", "StartStation Logical Terminal"]
    ])

    # add missing columns
    for col in TARGET_COLUMNS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    # enforce column order
    df = df.select(TARGET_COLUMNS)
    df = df.with_columns([
    pl.col("ride_id").cast(pl.Int64, strict=False),
    pl.col("bike_id").cast(pl.Int64, strict=False),
    pl.col("duration_seconds").cast(pl.Int64, strict=False),

    pl.col("start_station_id").cast(pl.Utf8),
    pl.col("end_station_id").cast(pl.Utf8),

    pl.col("start_station_name").cast(pl.Utf8),
    pl.col("end_station_name").cast(pl.Utf8),
    pl.col("bike_model").cast(pl.Utf8),

    pl.col("start_time").cast(pl.Utf8),
    pl.col("end_time").cast(pl.Utf8),
])

    df.write_parquet(clean_path / f"{file.stem}.parquet")