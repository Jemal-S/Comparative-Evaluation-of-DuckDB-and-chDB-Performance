"""DuckDB Single File Benchmark

Evaluates DuckDB query performance on single Parquet file layout.
Methodology: 15 runs per query, first run discarded as warm-up,
OS caches cleared between runs, statistics computed on remaining 14 runs.
"""

import duckdb
import time
import pandas as pd
from datetime import datetime
import os
import gc
import subprocess
import numpy as np

# Configuration
ENGINE = "duckdb"
LAYOUT = "single_file"
TRIPS_PATH = "../Datasets/yellow_merged.parquet"
ZONES_PATH = "../Datasets/taxi_zone_lookup.csv"
REPEATS = 15  # First run is warm-up, remaining 14 used for stats

os.makedirs("../results", exist_ok=True)

# Connect to DuckDB and configure
con = duckdb.connect(database=':memory:')
con.execute("PRAGMA threads=1")

# Pre-load/attach datasets before timing
print("Pre-loading datasets...")
con.execute(f"CREATE VIEW trips AS SELECT * FROM read_parquet('{TRIPS_PATH}')")
con.execute(f"CREATE VIEW zones AS SELECT * FROM read_csv_auto('{ZONES_PATH}')")
print("Datasets loaded.\n")

# Queries
QUERIES = {
    "Q1_Count":              "SELECT COUNT(*) FROM trips",
    "Q2_SumAvg":             "SELECT SUM(total_amount), AVG(trip_distance) FROM trips",
    "Q3_YearAgg":            "SELECT YEAR(tpep_pickup_datetime) AS year, COUNT(*) AS trips, SUM(total_amount) AS revenue FROM trips GROUP BY year ORDER BY year",
    "Q4_HourDist":           "SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour, COUNT(*) AS trips FROM trips GROUP BY hour ORDER BY hour",
    "Q5_FilterComplex":      "SELECT COUNT(*) FROM trips WHERE trip_distance > 5 AND total_amount < 20",
    "Q6_TimeRange":          "SELECT COUNT(*) FROM trips WHERE tpep_pickup_datetime BETWEEN '2020-06-01' AND '2020-06-30'",
    "Q7_PaymentType":        "SELECT payment_type, COUNT(*) FROM trips GROUP BY payment_type",
    "Q8_JoinBorough":        "SELECT z.Borough, COUNT(*) AS trips FROM trips t JOIN zones z ON t.PULocationID = z.LocationID GROUP BY z.Borough",
    "Q9_JoinBothZones":      """SELECT p.Zone AS pickup_zone, d.Zone AS dropoff_zone, COUNT(*) AS trips
                                  FROM trips t
                                  JOIN zones p ON t.PULocationID = p.LocationID
                                  JOIN zones d ON t.DOLocationID = d.LocationID
                                  GROUP BY pickup_zone, dropoff_zone
                                  ORDER BY trips DESC LIMIT 50""",
    "Q10_FullFilter":        "SELECT * FROM trips WHERE passenger_count = 1 LIMIT 100000",
    "Q11_SortLimit":         "SELECT * FROM trips ORDER BY tpep_pickup_datetime DESC LIMIT 50000",
}

def clear_os_cache():
    """Clear OS-level caches for cold-start equivalence.
    
    Note: Requires sudo privileges on Linux. Silently skips on Windows or
    if sudo is not available.
    """
    try:
        subprocess.run(['sync'], check=True, capture_output=True)
        subprocess.run(['sudo', 'sh', '-c', 'echo 3 > /proc/sys/vm/drop_caches'], 
                      check=True, capture_output=True, timeout=5)
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        pass

def run_sql(sql):
    # Clear Python and OS caches before each run
    gc.collect()
    clear_os_cache()
    
    start = time.perf_counter()
    result = con.execute(sql).fetchdf()
    elapsed = time.perf_counter() - start
    return elapsed, result

# Run benchmarks
print(f"STARTING: {ENGINE.upper()} + {LAYOUT} | {REPEATS} runs per query (first is warm-up)\n")
all_sheets = {}

for qname, sql in QUERIES.items():
    print(f"{qname} running...")
    timings = []
    first_result = None

    for i in range(REPEATS):
        duration, result = run_sql(sql)
        
        if i == 0:
            # First run is warm-up - capture result but mark timing
            first_result = result if isinstance(result, pd.DataFrame) else pd.DataFrame(result)
            print(f"  Run {i+1:2d}: {duration:.3f}s (WARM-UP - discarded)")
        else:
            # Runs 2-15 are used for statistics
            timings.append(duration)
            print(f"  Run {i+1:2d}: {duration:.3f}s")

    # Calculate statistics on the 14 timed runs (excluding warm-up)
    min_t = min(timings)
    max_t = max(timings)
    avg_t = np.mean(timings)
    median_t = np.median(timings)
    std_t = np.std(timings, ddof=1)  # Sample standard deviation
    
    print(f"  → Min: {min_t:.3f}s | Median: {median_t:.3f}s | Mean: {avg_t:.3f}s | Max: {max_t:.3f}s | StdDev: {std_t:.3f}s\n")

    # Build sheet
    timing_df = pd.DataFrame({
        "run": ["WARM-UP"] + list(range(2, REPEATS+1)),
        "duration_sec": [None] + timings,  # Explicitly mark warm-up as excluded
        "included_in_stats": [False] + [True] * len(timings)
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
        pd.DataFrame({"WARM-UP RUN OUTPUT (for correctness check)": [""]}),
        first_result.head(100).reset_index(drop=True)
    ], ignore_index=True)

    all_sheets[qname] = sheet

# Save
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
outfile = f"../results/BENCHMARK_{ENGINE}_{LAYOUT}_{timestamp}.xlsx"

with pd.ExcelWriter(outfile, engine="openpyxl") as writer:
    for qname, df in all_sheets.items():
        df.to_excel(writer, sheet_name=qname[:31], index=False)

print(f"Saved: {outfile}")
