# cu_analytics

A data pipeline for analyzing NCUA call report data across federally insured credit unions.

## Goals
- Download and store quarterly NCUA 5300 call report bulk data
- Build a medallion architecture (bronze/silver/gold) using DuckDB and dbt
- Generate peer comparison analysis and visualizations

## Stack
- Python
- DuckDB
- dbt
- Plotly

## Project Structure
- `data/` - raw and extracted NCUA data files
- `scripts/` - download and ingestion scripts
- `dbt/` - transformation models (bronze/silver/gold)