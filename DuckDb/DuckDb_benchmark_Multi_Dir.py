"""DuckDB Partitioned Benchmark

Evaluates DuckDB query performance on Hive-partitioned Parquet files.
hive_partitioning=true enables partition pruning for optimized queries.
"""

import duckdb
import time
import pandas as pd
from datetime import datetime
import os
import gc
import numpy as np

# Configuration
ENGINE = "duckdb"
LAYOUT = "partitioned"
TRIPS_PATH = "../Datasets/NYC_Taxi_Yellow_Trip_Dataset_DirO"
ZONES_PATH = "../Datasets/taxi_zone_lookup.csv"
REPEATS = 15

os.makedirs("../results", exist_ok=True)

# Connect to DuckDB and configure
con = duckdb.connect(database=':memory:')
con.execute("PRAGMA threads=1")

# Queries
# Hive partitioning enables partition pruning for year/month filters
QUERIES = {
    "Q1_Count":              f"SELECT COUNT(*) FROM read_parquet('{TRIPS_PATH}/**/*.parquet', hive_partitioning=true)",
    "Q2_SumAvg":             f"SELECT SUM(total_amount), AVG(trip_distance) FROM read_parquet('{TRIPS_PATH}/**/*.parquet', hive_partitioning=true)",
    "Q3_YearAgg":            f"SELECT year, COUNT(*) AS trips, SUM(total_amount) AS revenue FROM read_parquet('{TRIPS_PATH}/**/*.parquet', hive_partitioning=true) GROUP BY year ORDER BY year",
    "Q4_HourDist":           f"SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour, COUNT(*) AS trips FROM read_parquet('{TRIPS_PATH}/**/*.parquet', hive_partitioning=true) GROUP BY hour ORDER BY hour",
    "Q5_FilterComplex":      f"SELECT COUNT(*) FROM read_parquet('{TRIPS_PATH}/**/*.parquet', hive_partitioning=true) WHERE trip_distance > 5 AND total_amount < 20",
    "Q6_TimeRange":          f"SELECT COUNT(*) FROM read_parquet('{TRIPS_PATH}/**/*.parquet', hive_partitioning=true) WHERE year = 2020 AND month = 6",
    "Q7_PaymentType":        f"SELECT payment_type, COUNT(*) FROM read_parquet('{TRIPS_PATH}/**/*.parquet', hive_partitioning=true) GROUP BY payment_type",
    "Q8_JoinBorough":        f"SELECT z.Borough, COUNT(*) AS trips FROM read_parquet('{TRIPS_PATH}/**/*.parquet', hive_partitioning=true) t JOIN read_csv_auto('{ZONES_PATH}') z ON t.PULocationID = z.LocationID GROUP BY z.Borough",
    "Q9_JoinBothZones":      f"""SELECT p.Zone AS pickup_zone, d.Zone AS dropoff_zone, COUNT(*) AS trips
                                  FROM read_parquet('{TRIPS_PATH}/**/*.parquet', hive_partitioning=true) t
                                  JOIN read_csv_auto('{ZONES_PATH}') p ON t.PULocationID = p.LocationID
                                  JOIN read_csv_auto('{ZONES_PATH}') d ON t.DOLocationID = d.LocationID
                                  GROUP BY pickup_zone, dropoff_zone
                                  ORDER BY trips DESC LIMIT 50""",
    "Q10_FullFilter":        f"SELECT * FROM read_parquet('{TRIPS_PATH}/**/*.parquet', hive_partitioning=true, union_by_name=true) WHERE passenger_count = 1 LIMIT 100000",
    "Q11_SortLimit":         f"SELECT * FROM read_parquet('{TRIPS_PATH}/**/*.parquet', hive_partitioning=true, union_by_name=true) ORDER BY tpep_pickup_datetime DESC LIMIT 50000",
}

def run_sql(sql):
    # Clear cache before each run
    gc.collect()
    
    start = time.perf_counter()
    result = con.execute(sql).fetchdf()
    elapsed = time.perf_counter() - start
    return elapsed, result

# Run benchmarks
print(f"\nSTARTING: {ENGINE.upper()} + {LAYOUT} | {REPEATS} runs per query\n")
all_sheets = {}

for qname, sql in QUERIES.items():
    print(f"{qname} running...")
    timings = []
    first_result = None

    for i in range(REPEATS):
        duration, result = run_sql(sql)
        timings.append(duration)
        if i == 0:
            first_result = result if isinstance(result, pd.DataFrame) else pd.DataFrame(result)
        print(f"  Run {i+1:2d}: {duration:.3f}s")

    min_t = min(timings)
    max_t = max(timings)
    avg_t = np.mean(timings)
    median_t = np.median(timings)
    std_t = np.std(timings, ddof=1)
    print(f"  → Min: {min_t:.3f}s | Median: {median_t:.3f}s | Mean: {avg_t:.3f}s | Max: {max_t:.3f}s | StdDev: {std_t:.3f}s\n")

    # Build sheet
    timing_df = pd.DataFrame({
        "run": range(1, REPEATS+1),
        "duration_sec": timings
    })

    summary_df = pd.DataFrame([{
        "query": qname,
        "min_sec": min_t,
        "median_sec": median_t,
        "mean_sec": avg_t,
        "max_sec": max_t,
        "std_dev_sec": std_t,
        "runs": REPEATS
    }])

    sheet = pd.concat([
        summary_df,
        pd.DataFrame([{"": ""}]),
        timing_df,
        pd.DataFrame([{"": ""}]),
        pd.DataFrame({"FIRST RUN OUTPUT (for correctness check)": [""]}),
        first_result.head(100).reset_index(drop=True)
    ], ignore_index=True)

    all_sheets[qname] = sheet

# Save
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
outfile = f"../results/BENCHMARK_{ENGINE}_{LAYOUT}_{timestamp}.xlsx"

with pd.ExcelWriter(outfile, engine="openpyxl") as writer:
    for qname, df in all_sheets.items():
        df.to_excel(writer, sheet_name=qname[:31], index=False)  # Excel sheet name limit

print(f"Saved: {outfile}")