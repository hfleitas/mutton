# Fabric Notebook — Auto-load CSV files from Files/ into Delta tables in Tables/
#
# How to use:
#   1. Create a new Fabric Notebook attached to your LH1 Lakehouse.
#   2. Paste each section below into separate cells.
#   3. Run manually, or attach to a Pipeline / Schedule for automation.
#
# Automation options:
#   A) Schedule: Notebook settings → Schedule → set a recurring trigger.
#   B) Pipeline: Data Factory pipeline → add Notebook activity → trigger on
#      schedule, event, or after your AzCopy sync completes.

# ─── Cell 1: Configuration ───────────────────────────────────────────────────

# The default lakehouse mount point in Fabric notebooks
FILES_ROOT = "Files/"          # CSVs land here (via AzCopy sync or manual upload)
TABLES_ROOT = "Tables/"        # Delta tables are written here

# Map each CSV filename (without extension) to its target table name.
# Set to None to auto-discover all CSVs in FILES_ROOT.
TABLE_MAP = None  # e.g. {"claims_fact": "claims_fact", "customer_dim": "customer_dim"}

# Write mode: "overwrite" replaces the table each run; "append" adds rows.
WRITE_MODE = "overwrite"

# ─── Cell 2: Load CSVs as Delta Tables ───────────────────────────────────────

from pyspark.sql import SparkSession
from notebookutils import mssparkutils
import os

spark = SparkSession.builder.getOrCreate()

# Discover CSV files
if TABLE_MAP:
    csv_files = {name: f"{FILES_ROOT}{name}.csv" for name in TABLE_MAP}
else:
    # Auto-discover: list all CSVs in the Files root
    file_infos = mssparkutils.fs.ls(f"abfss://{FILES_ROOT}")
    csv_files = {}
    for f in file_infos:
        if f.name.lower().endswith(".csv"):
            table_name = os.path.splitext(f.name)[0]
            csv_files[table_name] = f"{FILES_ROOT}{f.name}"

print(f"Found {len(csv_files)} CSV file(s) to load:")
for table_name, path in csv_files.items():
    print(f"  {path} -> Tables/{table_name}")

# ─── Cell 3: Process each file ───────────────────────────────────────────────

loaded = []
failed = []

for table_name, csv_path in csv_files.items():
    try:
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(csv_path))

        row_count = df.count()

        (df.write
         .format("delta")
         .mode(WRITE_MODE)
         .saveAsTable(table_name))

        print(f"  ✓ {table_name}: {row_count} rows loaded ({WRITE_MODE})")
        loaded.append(table_name)
    except Exception as e:
        print(f"  ✗ {table_name}: {e}")
        failed.append(table_name)

# ─── Cell 4: Summary ─────────────────────────────────────────────────────────

print(f"\nDone. Loaded: {len(loaded)}, Failed: {len(failed)}")
if failed:
    print(f"Failed tables: {', '.join(failed)}")
