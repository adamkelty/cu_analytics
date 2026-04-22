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

## Peer Group CU Numbers
- 159   -- Three Rivers FCU (Fort Wayne, IN)
- 620   -- Power One FCU (Fort Wayne, IN)
- 1427  -- Midwest America FCU (Fort Wayne, IN)
- 4968  -- Inova FCU (Elkhart, IN)
- 5431  -- Fort Financial FCU (Fort Wayne, IN)
- 7688  -- Partners 1st FCU (Fort Wayne, IN) -- primary
- 17012 -- ProFed FCU (Fort Wayne, IN)
- 21593 -- Fire Police City County FCU (Fort Wayne, IN)
- 24781 -- Urban Beginnings Choice FCU (Fort Wayne, IN)
- 64275 -- Public Service #3 FCU (Fort Wayne, IN)


## Style Guide
### SQL
- Lowercase everything
- One clause per line
- CTEs over subqueries
- Always alias tables
#### Naming
- snake_case for everything
- Tables are plural -- credit_unions not credit_union
- Boolean columns start with is_ or has_ -- is_active, has_members
- Date columns end with _at or _date -- reported_at, charter_date
#### dbt models specifically
- Bronze/staging models prefixed with stg_
- Final models named for what they contain -- peer_comparisons, credit_union_metrics