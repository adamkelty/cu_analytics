import os
import shutil
import zipfile
from pathlib import Path

# Paths
downloads = Path.home() / "Downloads" / "Data Import" / "NCUA"
raw = Path("data/raw")
extracted = Path("data/extracted")

# Find NCUA zip files in Downloads
ncua_files = list(downloads.glob("call-report-data-*.zip"))

if not ncua_files:
    print("No NCUA zip files found in Downloads")
else:
    for zip_file in ncua_files:
        print(f"Found: {zip_file.name}")

        # Move to raw
        dest = raw / zip_file.name
        shutil.move(str(zip_file), str(dest))
        print(f"Moved to data/raw/")

        # Extract to extracted
        with zipfile.ZipFile(dest, "r") as z:
            z.extractall(extracted / zip_file.stem)
        print(f"Extracted to data/extracted/{zip_file.stem}")
