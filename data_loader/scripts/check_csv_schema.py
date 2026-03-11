import csv
from pathlib import Path
from collections import Counter

schema_counter = Counter()

for file in Path("data/raw").glob("*.csv"):
    with open(file, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = tuple(next(reader))
        schema_counter[header] += 1

print(f"Unique schemas: {len(schema_counter)}\n")

for schema, count in schema_counter.items():
    print(count, schema)