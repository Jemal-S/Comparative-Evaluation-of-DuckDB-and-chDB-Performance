# Comparative Evaluation of DuckDB and chDB on NYC Taxi Parquet Layouts

[![Python - 3.8+](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![DuckDB - 1.4.1](https://img.shields.io/badge/DuckDB-1.4.1-gold)](https://duckdb.org/)
[![chDB - 3.4.0](https://img.shields.io/badge/chDB-3.4.0-orange)](https://github.com/chdb-io/chdb)
[![Dataset - NYC TLC Yellow Taxi](https://img.shields.io/badge/Dataset-NYC%20TLC-yellow)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
[![Status - Research Benchmark](https://img.shields.io/badge/Status-Research%20Benchmark-lightgrey)](#)
[![DOI](https://zenodo.org/badge/1193504965.svg)](https://doi.org/10.5281/zenodo.19254112)


A comprehensive performance comparison between **chDB** and **DuckDB** using the NYC Taxi Yellow Trip Dataset across different data layouts.

## Abstract

This study investigates how physical Parquet layout affects end-to-end query latency under two embedded engines. We evaluate three storage layouts over the NYC Taxi Yellow Trip dataset (2018-2023): single-file, flat multi-file directory, and Hive-partitioned directory. The benchmark executes 11 standardized analytical queries repeatedly and reports summary statistics (min, median, mean, max, standard deviation), per-run timings, and sample outputs for result validation. The artifact is designed to support reproducible comparative experiments, sensitivity analysis by layout type, and methodologically transparent reporting.

## Experimental Scope

- **Engines:** DuckDB v1.4.1 and chDB v3.4.0
- **Dataset:** NYC Taxi Yellow Trip (2018-2023)
- **Layouts:**
  - Single file (`yellow_merged.parquet`)
  - Flat directory (multiple Parquet files)
  - Partitioned directory (`year=YYYY/month=MM/`)
- **Workload size:** 11 standardized SQL queries
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
│   ├── NYC_Taxi_Yellow_Trip_Dataset/
│   ├── NYC_Taxi_Yellow_Trip_Dataset_DirO/
│   ├── yellow_merged.parquet
│   └── taxi_zone_lookup.csv
└── results/
```

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
TRIPS_PATH = "../Datasets/yellow_merged.parquet"
ZONES_PATH = "../Datasets/taxi_zone_lookup.csv"
REPEATS = 15
```

## Output and Data Products

Results are written to `results/` as:

```text
BENCHMARK_{engine}_{layout}_{timestamp}.xlsx
```

Each workbook includes one sheet per query containing:

1. Summary statistics
2. Per-run timings
3. Sample result rows (first 100)

## Validity Notes and Limitations

- Cache-clearing behavior is OS-dependent and may not be equivalent across environments.
- Differences in SQL function semantics between engines can introduce query adaptation overhead.
- Hardware and filesystem characteristics may materially influence I/O-heavy workloads.
- This artifact emphasizes performance observables; external validity depends on workload similarity to target production systems.

