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
            FROM read_csv_auto('{file}', header=true)
        """)

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

        count = con.execute(f"""
            SELECT COUNT(*) FROM {full_table}
            WHERE quarter = '{quarter}'
        """).fetchone()[0]
        print(f"  Loaded {count:,} rows")

print("\nDone! All quarters loaded.")
con.close()
