"""chDB Flat Directory Benchmark

Evaluates chDB query performance on flat directory with multiple Parquet files.
"""

import chdb
import time
import pandas as pd
from datetime import datetime
import os
import gc
import numpy as np

# Configuration
ENGINE = "chdb"
LAYOUT = "flat_dir"
TRIPS_PATH = "../Datasets/NYC_Taxi_Yellow_Trip_Dataset"
ZONES_PATH = "../Datasets/taxi_zone_lookup.csv"
REPEATS = 15


os.makedirs("../results", exist_ok=True)

# Data source for chDB queries
CHDB_SOURCE = f"file('{TRIPS_PATH}/*.parquet', Parquet)"

# Queries
CHDB_QUERIES = {
    "Q1_Count":              f"SELECT count() FROM {CHDB_SOURCE}",
    "Q2_SumAvg":             f"SELECT sum(total_amount), avg(trip_distance) FROM {CHDB_SOURCE}",
    "Q3_YearAgg":            f"SELECT toYear(toTimeZone(tpep_pickup_datetime, 'UTC')) AS year, count() AS trips, sum(total_amount) AS revenue FROM {CHDB_SOURCE} GROUP BY year ORDER BY year",
    "Q4_HourDist":           f"SELECT toHour(toTimeZone(tpep_pickup_datetime, 'UTC')) AS hour, count() AS trips FROM {CHDB_SOURCE} GROUP BY hour ORDER BY hour",
    "Q5_FilterComplex":      f"SELECT count() FROM {CHDB_SOURCE} WHERE trip_distance > 5 AND total_amount < 20",
    "Q6_TimeRange":          f"SELECT count() FROM {CHDB_SOURCE} WHERE toTimeZone(tpep_pickup_datetime, 'UTC') BETWEEN '2020-06-01' AND '2020-06-30'",
    "Q7_PaymentType":        f"SELECT payment_type, count() FROM {CHDB_SOURCE} GROUP BY payment_type",
    "Q8_JoinBorough":        f"SELECT z.Borough, count() FROM {CHDB_SOURCE} AS t ANY LEFT JOIN file('{ZONES_PATH}', CSVWithNames) AS z ON t.PULocationID = z.LocationID GROUP BY z.Borough",
    "Q9_JoinBothZones":      f"""SELECT p.Zone AS pickup_zone, d.Zone AS dropoff_zone, count() AS trips
                                  FROM {CHDB_SOURCE} AS t
                                  ANY LEFT JOIN file('{ZONES_PATH}', CSVWithNames) AS p ON t.PULocationID = p.LocationID
                                  ANY LEFT JOIN file('{ZONES_PATH}', CSVWithNames) AS d ON t.DOLocationID = d.LocationID
                                  GROUP BY pickup_zone, dropoff_zone ORDER BY trips DESC LIMIT 50""",
    "Q10_FullFilter":        f"SELECT * FROM {CHDB_SOURCE} WHERE passenger_count = 1 LIMIT 100000",
    "Q11_SortLimit":         f"SELECT * FROM {CHDB_SOURCE} ORDER BY toTimeZone(tpep_pickup_datetime, 'UTC') DESC LIMIT 50000",
}

def run_sql(sql):
    # Clear cache before each run
    gc.collect()
    
    # Add settings to handle schema differences (airport_fee column)
    sql_with_settings = sql + " SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference=1"
    
    start = time.perf_counter()
    result = chdb.query(sql_with_settings, "DataFrame")
    elapsed = time.perf_counter() - start
    return elapsed, result

# Run benchmarks
print(f"\nSTARTING: {ENGINE.upper()} + {LAYOUT} | {REPEATS} runs per query\n")
all_sheets = {}

for qname, sql in CHDB_QUERIES.items():
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

    # Remove timezone from datetime columns for Excel compatibility
    first_result_clean = first_result.head(100).reset_index(drop=True)
    for col in first_result_clean.select_dtypes(include=['datetimetz']).columns:
        first_result_clean[col] = first_result_clean[col].dt.tz_localize(None)
    
    sheet = pd.concat([
        summary_df,
        pd.DataFrame([{"":""}]),
        timing_df,
        pd.DataFrame([{"":""}]),
        pd.DataFrame({"FIRST RUN OUTPUT (for correctness check)":[""]}),
        first_result_clean
    ], ignore_index=True)

    all_sheets[qname] = sheet

# Save
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
outfile = f"../results/BENCHMARK_{ENGINE}_{LAYOUT}_{timestamp}.xlsx"

with pd.ExcelWriter(outfile, engine="openpyxl") as writer:
    for qname, df in all_sheets.items():
        df.to_excel(writer, sheet_name=qname[:31], index=False)  # Excel sheet name limit

print(f"Saved: {outfile}")
