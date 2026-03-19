import polars as pl
from pathlib import Path
from collections import Counter

schema_counter = Counter()

for file in Path("data/clean").glob("*.parquet"):
    df = pl.read_parquet(file, n_rows=1)
    header = tuple(df.columns)
    schema_counter[header] += 1

print(f"Unique schemas: {len(schema_counter)}\n")

for schema, count in schema_counter.items():
    print(count, schema)