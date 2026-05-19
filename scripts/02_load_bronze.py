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

    # Skip if this quarter is already loaded
    table_exists = con.execute("""
        select count(*) from information_schema.tables
        where table_schema = 'bronze'
        and table_name = 'fs220'
    """).fetchone()[0]

    if table_exists:
        quarter_exists = con.execute(f"""
            select count(*) from bronze.fs220
            where quarter = '{quarter}'
        """).fetchone()[0]

        if quarter_exists:
            print("  Already loaded, skipping")
            continue

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

        # Stage the data -- normalize_names cleans column names,
        # union_by_name handles schema differences across quarters
        con.execute(f"""
            create or replace temp view staging as
            select *, '{quarter}' as quarter
            from read_csv('{file}',
                header=true,
                all_varchar=true,
                ignore_errors=true,
                normalize_names=true,
                union_by_name=true)
        """)

        # Raw count before loading
        raw_count = con.execute(f"""
            select count(*) from read_csv('{file}',
                header=true,
                ignore_errors=true,
                normalize_names=true)
        """).fetchone()[0]

        # Create table if new, otherwise insert by name
        table_exists = con.execute(f"""
            select count(*) from information_schema.tables
            where table_schema = '{full_table.split(".")[0]}'
            and table_name = '{full_table.split(".")[1]}'
        """).fetchone()[0]

        if not table_exists:
            con.execute(f"create table {full_table} as select * from staging")
        else:
            con.execute(f"delete from {full_table} where quarter = '{quarter}'")

            # Find columns in staging that the table doesn't have yet
            staging_cols = {
                c[0]: c[1] for c in con.execute("describe staging").fetchall()
            }
            table_cols = {
                c[0] for c in con.execute(f"describe {full_table}").fetchall()
            }
            new_cols = [c for c in staging_cols if c not in table_cols]

            # Add any new columns to the table (schema evolution)
            for col in new_cols:
                con.execute(
                    f'alter table {full_table} add column "{col}" {staging_cols[col]}'
                )
                print(f"    + added new column: {col}")

            # Insert by name -- fills missing columns with null
            con.execute(f"insert into {full_table} by name select * from staging")

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
