# Project Notes

## Status
Just getting started -- bronze ingestion pipeline in progress.

## Todo
- [ ] Finish and test 02_load_bronze.py
- [ ] Explore data with Jupyter notebook
- [ ] Set up dbt project
- [ ] Build silver layer models
- [ ] Build gold layer peer comparison models
- [ ] Build Plotly charts
- [ ] Build Dash dashboard (stretch)

## Decisions
- Using DuckDB with schema dot notation (bronze.fs220)
- Medallion architecture: bronze/silver/gold
- Dynamic file loading -- no hardcoded table names
- Numbered script prefixes for pipeline order

## Peers
- Partners 1st FCU: 7688
- Inova: TBD
- Three Rivers: TBD
- Fort Financial: TBD