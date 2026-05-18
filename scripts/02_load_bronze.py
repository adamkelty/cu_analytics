import duckdb
from pathlib import Path

# Paths
extracted = Path("data/extracted")
db_path = Path("data/cu_analytics.duckdb")

# Connect to DuckDB
con = duckdb.connect(str(db_path))

# Create schemas
con.execute("create schema if not exists bronze")
con.execute("create schema if not exists silver")
con.execute("create schema if not exists gold")

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

        # Get raw column names from the CSV first
        raw_cols = [
            col[0]
            for col in con.execute(f"""
            describe (select * from read_csv_auto('{file}', header=true, ignore_errors=true, all_varchar=true) limit 0)
        """).fetchall()
        ]

        # Strip any embedded quotes from column names
        clean_cols = [c.strip('"') for c in raw_cols]

        # Build select list with aliases to normalize names
        def sql_id(raw):
            return f'"{raw.replace(chr(34), chr(34) + chr(34))}"'

        col_select = ", ".join(
            f'{sql_id(raw)} AS "{clean}"' for raw, clean in zip(raw_cols, clean_cols)
        )

        # Stage the data with clean column names
        con.execute(f"""
            create or replace temp view staging as
            select {col_select}, '{quarter}' as quarter
            from read_csv_auto('{file}', header=true, ignore_errors=true, all_varchar=true)
        """)
        # Raw count before loading
        raw_count = con.execute(f"""
            select count(*) from read_csv_auto('{file}', header=true, ignore_errors=true)
        """).fetchone()[0]

        # Check if table exists
        table_exists = con.execute(f"""
            select count(*) from information_schema.tables
            where table_schema = '{full_table.split(".")[0]}'
            and table_name = '{full_table.split(".")[1]}'
        """).fetchone()[0]

        if not table_exists:
            con.execute(f"""
                create table {full_table} as
                select * from staging
            """)
        else:
            con.execute(f"""
               delete from {full_table} where quarter = '{quarter}'
           """)
            staging_cols = [
                col[0] for col in con.execute("describe staging").fetchall()
            ]
            table_cols = [
                col[0] for col in con.execute(f"describe {full_table}").fetchall()
            ]
            common_cols = [c for c in staging_cols if c in table_cols]
            cols_str = ", ".join(f'"{c}"' for c in common_cols)
            con.execute(f"""
                insert into {full_table} ({cols_str})
                select {cols_str} from staging
            """)

        loaded_count = con.execute(f"""
           select count(*) from {full_table}
           where quarter = '{quarter}'
       """).fetchone()[0]

        if raw_count != loaded_count:
            print(
                f"  ⚠️  Warning: raw={raw_count}, loaded={loaded_count}, skipped={raw_count - loaded_count}"
            )
        else:
            print(f"  Loaded {loaded_count:,} rows")

print("\nDone! All quarters loaded.")
con.close()
