import duckdb
from pathlib import Path

# Paths
extracted = Path("data/extracted")
db_path = Path("data/cu_analytics.duckdb")

# Connect to DuckDB
con = duckdb.connect(str(db_path))

# Create schemas
con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
con.execute("CREATE SCHEMA IF NOT EXISTS silver")
con.execute("CREATE SCHEMA IF NOT EXISTS gold")

# Files to skip -- not real data
skip_files = ["readme", "report1"]

# Loop through every quarter folder in data/extracted/
quarter_folders = sorted(extracted.iterdir())

for folder in quarter_folders:
    if not folder.is_dir():
        continue

    # Parse quarter from folder name e.g. call-report-data-2025-12 -> 2025-12
    quarter = "-".join(folder.name.split("-")[-2:])
    print(f"\nLoading quarter: {quarter}")

    # Dynamically build table list from files in this folder
    for file in sorted(folder.glob("*.txt")):
        # Skip non-data files
        if any(s in file.stem.lower() for s in skip_files):
            print(f"  Skipping {file.name}")
            continue

        # Clean filename into a valid table name
        table_name = file.stem.lower().replace(" ", "_").replace("-", "_")
        full_table = f"bronze.{table_name}"

        print(f"  Loading {file.name} into {full_table}...")

        # Stage the data
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW staging AS
            SELECT *, '{quarter}' AS quarter
            FROM read_csv_auto('{file}', header=true, ignore_errors=true)
        """)
        # Raw count before loading
        raw_count = con.execute(f"""
            SELECT COUNT(*) FROM read_csv_auto('{file}', header=true, ignore_errors=true)
        """).fetchone()[0]

        # Create the table if it doesn't exist
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_table} AS
            SELECT * FROM staging WHERE 1=0
        """)

        # Delete existing data for this quarter
        con.execute(f"""
            DELETE FROM {full_table} WHERE quarter = '{quarter}'
        """)

        # Insert the new data
        con.execute(f"""
            INSERT INTO {full_table} SELECT * FROM staging
        """)

        # Check for skipped rows
        loaded_count = con.execute(f"""
            SELECT COUNT(*) FROM {full_table}
            WHERE quarter = '{quarter}'
        """).fetchone()[0]

        if raw_count != loaded_count:
            print(
                f"  ⚠️  Warning: raw={raw_count}, loaded={loaded_count}, skipped={raw_count - loaded_count}"
            )
        else:
            print(f"  Loaded {loaded_count:,} rows")

print("\nDone! All quarters loaded.")
con.close()
