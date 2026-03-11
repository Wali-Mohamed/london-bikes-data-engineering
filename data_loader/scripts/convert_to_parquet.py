import polars as pl
from pathlib import Path

raw_path = Path("data/raw")
clean_path = Path("data/clean")

clean_path.mkdir(exist_ok=True)

for file in raw_path.glob("*.csv"):

    
    df = pl.read_csv(
    file,
    infer_schema_length=10000,
    schema_overrides={
        "End station number": pl.Utf8,
        "Start station number": pl.Utf8,
        "StartStation Id": pl.Utf8,
        "EndStation Id": pl.Utf8
    }
)

    rename_map = {
        "Rental Id": "ride_id",
        "Duration": "duration_seconds",
        "Duration_Seconds": "duration_seconds",
        "Bike Id": "bike_id",
        "Start Date": "start_time",
        "End Date": "end_time",
        "StartStation Id": "start_station_id",
        "StartStation Name": "start_station_name",
        "EndStation Id": "end_station_id",
        "EndStation Name": "end_station_name",
        "Number": "ride_id",
        "Start date": "start_time",
        "End date": "end_time",
        "Start station number": "start_station_id",
        "Start station": "start_station_name",
        "End station number": "end_station_id",
        "End station": "end_station_name",
        "Bike number": "bike_id",
        "Bike model": "bike_model",
        "Total duration": "duration_seconds",
    }

    # rename only existing columns
    valid_map = {k: v for k, v in rename_map.items() if k in df.columns}

    df = df.rename(valid_map)

    df.write_parquet(clean_path / f"{file.stem}.parquet")
    print(f'written {file.stem}.parquet')