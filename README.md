# Comparative Evaluation of DuckDB and chDB on NYC Taxi Parquet Layouts

[![Python - 3.8+](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![DuckDB - 1.4.1](https://img.shields.io/badge/DuckDB-1.4.1-gold)](https://duckdb.org/)
[![chDB - 3.4.0](https://img.shields.io/badge/chDB-3.4.0-orange)](https://github.com/chdb-io/chdb)
[![Dataset - NYC TLC Yellow Taxi](https://img.shields.io/badge/Dataset-NYC%20TLC-yellow)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
[![Status - Research Benchmark](https://img.shields.io/badge/Status-Research%20Benchmark-lightgrey)](#)
[![DOI](https://zenodo.org/badge/1193504965.svg)](https://doi.org/10.5281/zenodo.19254112)


A comprehensive performance comparison between **chDB** and **DuckDB** using the NYC Taxi Yellow Trip Dataset across different data layouts.

## Abstract

This study investigates how physical Parquet layout affects end-to-end query latency under two embedded engines. We evaluate three storage layouts over the **NYC Taxi Yellow Trip dataset (2019-2024)**: single-file, flat multi-file directory, and Hive-partitioned directory. The benchmark executes 11 standardized analytical queries repeatedly and reports summary statistics (min, median, mean, max, standard deviation), per-run timings, and sample outputs for result validation. The artifact is designed to support reproducible comparative experiments, sensitivity analysis by layout type, and methodologically transparent reporting.

## Experimental Scope

### Engines & Data
- **Engines:** DuckDB v1.4.1 and chDB v3.4.0
- **Primary Dataset:** [NYC Taxi Yellow Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (2019-2024)
  - **Source:** NYC Taxi and Limousine Commission (TLC)
  - **Time Period:** 6 years of historical trip data (January 2019 through December 2024)
  - **Content:** Includes pickup/dropoff locations, timestamps, fare amounts, passenger counts, payment types, etc.
  - **Supplementary Data:** NYC Taxi Zone Lookup table (`taxi_zone_lookup.csv`) for borough and zone classifications
- **Layouts Tested:**
  - Single file (`yellow_merged.parquet`) — consolidated dataset
  - Flat directory (multiple Parquet files) — unpartitioned multi-file layout
  - Partitioned directory (`year=YYYY/month=MM/`) — Hive-partitioned directory structure
- **Analytical Workload:** 11 standardized SQL queries
- **Output artifact:** Excel workbooks with per-query statistics and run-level traces

## Methodology

### Execution Protocol

- **Repetitions:** 15 runs per query
- **Warm-up policy:**
  - Single-file layout: first run discarded as warm-up; statistics computed on runs 2-15
  - Flat and partitioned layouts: all 15 runs included in statistics
- **Cache handling:**
  - Single-file benchmarks: Python GC + OS cache clearing attempted between runs on Linux environments requiring elevated privileges
  - Flat and partitioned benchmarks: Python GC before each run (no OS cache drop command)
- **Runtime hygiene:** garbage collection invoked before each run

### Dependent Variables (Measured Metrics)

- Wall-clock execution time (seconds)
- Descriptive statistics: **min, median, mean, max, stddev**
- Per-run timing series
- Query output sample (first 100 rows) for sanity checking and cross-engine result inspection

### Workload Definition (Q1-Q11)

| Query | Description |
|-------|-------------|
| **Q1_Count** | Total row count |
| **Q2_SumAvg** | Aggregate sum and average on fare columns |
| **Q3_YearAgg** | Year-wise trip count and revenue |
| **Q4_HourDist** | Hourly trip distribution |
| **Q5_FilterComplex** | Multi-condition filtering |
| **Q6_TimeRange** | Time-based range filtering |
| **Q7_PaymentType** | Group by payment type |
| **Q8_JoinBorough** | Join with zone lookup (borough) |
| **Q9_JoinBothZones** | Join with both pickup and dropoff zones |
| **Q10_FullFilter** | Full row retrieval with filtering |
| **Q11_SortLimit** | Sorting with limit |

## Reproducibility

### Software Requirements

```bash
pip install chdb duckdb pandas numpy openpyxl
```

- Python 3.8+
- chdb==3.4.0
- duckdb==1.4.1
- pandas
- numpy
- openpyxl

### Repository Structure

```
.
├── ChDb/
│   ├── chDb_benchmark_Dir.py
│   ├── chDb_benchmark_Multi_Dir.py
│   └── chDb_benchmark_Single.py
├── DuckDb/
│   ├── DuckDb_benchmark_Dir.py
│   ├── DuckDb_benchmark_Multi_Dir.py
│   └── DuckDb_benchmark_Single.py
├── Datasets/
│   ├── NYC_Taxi_Yellow_Trip_Dataset/                 [Flat multi-file directory layout]
│   ├── NYC_Taxi_Yellow_Trip_Partitioned_Dataset/     [Hive-partitioned directory layout: year=YYYY/month=MM_MonthName/]
│   ├── yellow_merged.parquet                         [Single consolidated file (2019-2024)]
│   └── taxi_zone_lookup.csv                          [Zone reference table for joins]
└── results/
```

### Dataset Details

**NYC Taxi Yellow Trip Records (2019-2024):**
- **Time Span:** January 2019 through December 2024 (6 complete years)
- **Source:** [NYC Taxi and Limousine Commission (TLC) Official Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Key Fields:** Pickup/dropoff datetime, pickup/dropoff location IDs, fare amounts, tip amounts, total cost, passenger count, payment type, trip distance, RatecodeID, and more
- **Data Volume Variations Across Layouts:**
  - `yellow_merged.parquet` — Entire dataset (6 years) consolidated into one Parquet file (created by merging all files from `NYC_Taxi_Yellow_Trip_Dataset/`)
  - `NYC_Taxi_Yellow_Trip_Dataset/` — Split across multiple Parquet files (flat structure, no partitioning)
  - `NYC_Taxi_Yellow_Trip_Partiontioned_Dataset/` — Organized as Hive-partitioned by `year=YYYY/month=MM_MonthName/` (e.g., `2019/01_January/`, `2024/12_December/`) for hierarchical access patterns
- **Data Preparation:** The single-file layout (`yellow_merged.parquet`) must be generated by merging all individual Parquet files from the flat directory layout before benchmarking

**NYC Taxi Zone Lookup (`taxi_zone_lookup.csv`):**
- **Purpose:** Reference table for trip location enrichment and aggregation
- **Columns:** LocationID, Borough, Zone, service_zone
- **Used in Queries:** Q8_JoinBorough, Q9_JoinBothZones for zone-based grouping and borough classification
- **Critical for:** Testing join performance between fact table (trips) and dimension table (zones)

### Running the Experiments

```bash
# chDB
python ChDb/chDb_benchmark_Single.py
python ChDb/chDb_benchmark_Dir.py
python ChDb/chDb_benchmark_Multi_Dir.py

# DuckDB
python DuckDb/DuckDb_benchmark_Single.py
python DuckDb/DuckDb_benchmark_Dir.py
python DuckDb/DuckDb_benchmark_Multi_Dir.py
```

### Configuration Interface

Each benchmark script exposes a local `configuration` block for experiment control:

```python
ENGINE = "chdb"            # or "duckdb"
LAYOUT = "single_file"     # or "flat_dir" or "partitioned"
TRIPS_PATH = "../Datasets/yellow_merged.parquet"  # Target dataset path
ZONES_PATH = "../Datasets/taxi_zone_lookup.csv"   # Zone reference table
REPEATS = 15               # Number of repetitions per query
```

**Data Path Mapping (as referenced in scripts):**
- `single_file` layout → `../Datasets/yellow_merged.parquet` (2019-2024 consolidated)
- `flat_dir` layout → `../Datasets/NYC_Taxi_Yellow_Trip_Dataset/` (multiple unpartitioned Parquet files)
- `partitioned` layout → `../Datasets/NYC_Taxi_Yellow_Trip_Dataset_DirO/` Hive-partitioned by `year=YYYY/month=MM_MonthName/`
- All layouts use the same `../Datasets/taxi_zone_lookup.csv` reference table for join operations

## Output and Data Products

Results from running benchmarks against the above datasets are written to `results/` as:

```text
BENCHMARK_{engine}_{layout}_{timestamp}.xlsx
```

For example, running chDB benchmarks on a single-file layout would produce: `BENCHMARK_chdb_single_file_2024-03-15T10-30-45.xlsx`

Each workbook includes one sheet per query containing:

1. **Summary Statistics** — min, median, mean, max, stddev computed from per-run timings
2. **Per-Run Timings** — Wall-clock execution time (seconds) for each of the 15 repetitions
3. **Sample Result Rows** — First 100 rows of query output for validation and sanity checking

## Validity Notes and Limitations

- Cache-clearing behavior is OS-dependent and may not be equivalent across environments.
- Differences in SQL function semantics between engines can introduce query adaptation overhead.
- Hardware and filesystem characteristics may materially influence I/O-heavy workloads.
- This artifact emphasizes performance observables; external validity depends on workload similarity to target production systems.

