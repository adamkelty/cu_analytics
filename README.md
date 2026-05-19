# cu_analytics

A local data pipeline for benchmarking credit union financial performance using NCUA quarterly call report data.

## Why this exists

I work as a core analyst at a credit union, where peer benchmarking is a recurring need but the tooling is limited. This project rebuilds that analysis from scratch as a proper data pipeline, using public NCUA data, to answer a practical question: how does a given credit union compare to its peers across key financial ratios, and how has that changed over time?

## Architecture

The pipeline follows a medallion pattern with three layers.

**Bronze** holds raw NCUA call report CSVs loaded into DuckDB exactly as they arrive, with no transformations or type casting. The loader handles schema evolution automatically, detecting and adding new columns as the NCUA changes its reporting fields across quarters.

**Silver** is a set of dbt views that rename cryptic NCUA account codes into readable column names and apply explicit type casting. `stg_credit_unions` pulls institutional data from FOICU, and `stg_financials` pulls financial figures from FS220.

**Gold** is the business layer, built on silver and designed to expand over time. The first model, `peer_comparison`, joins institutional and financial data, filters to a defined peer group, and calculates core ratios. Planned models cover growth metrics, asset quality, liquidity, capital adequacy, and loan portfolio mix.

## Stack

Python for data ingestion and visualization, DuckDB as the local analytical database, dbt for the transformation layer, and Plotly for charting.

## Data

NCUA quarterly call report bulk data, publicly available at [ncua.gov](https://www.ncua.gov/analysis/credit-union-corporate-call-report-data/quarterly-data). Currently loaded with 16 quarters spanning 2022 through 2025. The peer group is ten credit unions in and around Fort Wayne, Indiana, ranging from roughly $2M to $2.7B in assets.

## Running the pipeline

```bash
# 1. Download quarterly ZIPs from NCUA and place in ~/Downloads/Data Import/NCUA/

# 2. Activate environment
source .venv/bin/activate

# 3. Move and extract ZIPs
python scripts/01_import_data.py

# 4. Load into DuckDB bronze layer
python scripts/02_load_bronze.py

# 5. Build silver and gold models
dbt run --project-dir cu_models
```

New quarters are detected automatically. Quarters already present in the database are skipped, so reruns are fast and adding a new quarter is just a matter of dropping in the ZIP and running the scripts.

## Output

`gold.peer_comparison` produces one row per credit union per quarter, combining balance sheet figures with calculated ratios including loan-to-share, delinquency rate, charge-off rate, and reserve coverage. It can be queried directly in DuckDB or used as the source for trend charts.

## Planned work

Expanding the gold layer with additional metric categories, building an interactive Dash dashboard for exploring peer comparisons, and adding geographic filtering to define peer groups by region.