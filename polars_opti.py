# %%
import polars as pl

# create data frame with fake data on city, address, name, age, slary, and gender
data = {
    "city": [
        "New York",
        "New York",
        "Amsterdam",
        "Amsterdam",
        "Chicago",
        "Chicago",
    ],
    "address": ["123 A St", "456 B St", "789 C St", "101 D St", "112 E St", "131 F St"],
    "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"],
    "age": [25, 30, 35, 40, 45, 50],
    "salary": [100, 200, 300, 400, 500, 600],
    "gender": ["F", "M", "M", "M", "F", "M"],
}

df = pl.DataFrame(data)
df.write_csv("~/Desktop/example.csv")

# %%
query = (
    pl.scan_csv("~/Desktop/example.csv")
    .group_by("city")
    .agg(n_addresses=pl.col("address").n_unique(), n_people=pl.col("name").count())
    .filter(pl.col("city") == "Amsterdam")
)

query.show_graph()  # see here on how to interpret the query plan: https://docs.pola.rs/user-guide/lazy/query-plan/#optimized-query-plan


# %%
import pandas as pd
import numpy as np

# Create sample data
sample_size = 1000000
df = pd.DataFrame({"value": np.random.randint(1, 100, sample_size)})
df.to_csv("~/Desktop/sample_data.csv", index=False)

# Initialize list to store processed chunks
processed_chunks = []

# Read and process data in chunks
chunk_size = 100000
for chunk in pd.read_csv("~/Desktop/sample_data.csv", chunksize=chunk_size):
    # Process chunk by dividing values by 2
    chunk["value"] = chunk["value"] / 2

    # Store processed chunk
    processed_chunks.append(chunk)

# Combine all processed chunks into final result
final_result = pd.concat(processed_chunks, ignore_index=True)

# Show first few rows of result
print("First few rows of processed data:")
print(final_result.head())

# %%
# Show some basic statistics
print("\nBasic statistics of processed data:")
print(final_result.describe())


# %%
result = (
    pl.scan_csv("~/Desktop/sample_data.csv")
    .with_columns(pl.col("value") / 2)  # Process by dividing values by 2
    .collect(streaming=True)
)  # Use streaming in collect to process in chunks

query = (
    pl.scan_csv("~/Desktop/sample_data.csv")
    .with_columns(pl.col("value") / 2)
    .explain(streaming=True)
)

print(query)
