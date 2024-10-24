# %%
import duckdb as ddb
import polars as pl
import time

# %%
# Let's load NYC taxi dataset with Polars
nyc = pl.scan_parquet("nyc-taxi/**/*.parquet", hive_partitioning=True)
q1 = (
    nyc.group_by(["passenger_count"])
    .agg([pl.mean("tip_amount").alias("mean_tip")])
    .sort("passenger_count")
)

tic = time.time()
dat1 = q1.collect()
toc = time.time()
print(f"Time: {toc - tic}")
# Output: Time: 1.4287219047546387

# %%
# Let's do the same with DuckDB

# # Turn the paqrquet files into a DuckDB table
# con = ddb.connect("nyc_taxi.ddb")
# # Write the parquet files to a table
# con.sql("CREATE TABLE nyc_taxi AS SELECT * FROM read_parquet('nyc-taxi/**/*.parquet')")

# # # Alternatively: Register the parquet files as a table
# con.sql("CREATE VIEW nyc_taxi AS SELECT * FROM read_parquet('nyc-taxi/2012/*.parquet')")

# %%
import duckdb as ddb

con = ddb.connect("nyc_taxi.ddb")
query = """SELECT
  t0.passenger_count,
  AVG(t0.tip_amount) AS mean_tip
FROM main.nyc_taxi AS t0
GROUP BY
  1
  """

tic = time.time()
con.sql(query).show()
toc = time.time()
print(f"Time: {toc - tic}")
# Output: Time: 0.2068800926

# %%
# How many rows are in the table?
con.sql("SELECT COUNT(*) FROM nyc_taxi").show()
# %%
import duckdb as ddb

query_2 = """SELECT
  t0.passenger_count,
  AVG(t0.tip_amount) AS mean_tip
FROM read_parquet('nyc-taxi/**/*.parquet') AS t0
GROUP BY
  1
  """
con2 = ddb.connect()
tic = time.time()
con2.sql(query_2).show()
toc = time.time()
print(f"Time: {toc - tic}")

# %%
