# ============================================================================
# Sync-WarehouseDirect.py
# ============================================================================
# Run this in a Fabric notebook in the BCDR2 workspace.
#
# PURPOSE:
#   Syncs DW1 Warehouse tables from BCDR1 → BCDR2 directly.
#   NO staging Lakehouse needed.
#   - READS from BCDR1's DW1 via OneLake (Spark delta reader)
#   - WRITES to BCDR2's DW1 via JDBC (the Warehouse's native SQL endpoint)
#
# PREREQUISITES:
#   1. DW1 Warehouse exists in BCDR2
#   2. Get the SQL connection string for DW1 in BCDR2:
#        Fabric portal → BCDR2 workspace → DW1 → Settings (gear icon)
#        → "SQL connection string"
#        It looks like: xxxxxxxx.datawarehouse.fabric.microsoft.com
#   3. Your notebook identity has read access to BCDR1 workspace
#
# USAGE:
#   - Set LOAD_MODE = "full" for initial load (truncate + insert)
#   - Set LOAD_MODE = "incremental" for ongoing syncs (MERGE/UPSERT)
# ============================================================================

# ── CONFIGURATION ──────────────────────────────────────────────────────────

# Source: BCDR1 workspace, DW1 Warehouse (read via OneLake)
SOURCE_WORKSPACE = "BCDR1"
SOURCE_WAREHOUSE = "DW1"
SOURCE_ITEM_TYPE = "Warehouse"  # "Warehouse" or "Lakehouse"

# Target: DW1 Warehouse in BCDR2
TARGET_WAREHOUSE = "DW1"

# ⚠️ REQUIRED: Paste your DW1 SQL endpoint from BCDR2 here
# Find it: Fabric portal → BCDR2 → DW1 → Settings → SQL connection string
SQL_ENDPOINT = "your-endpoint.datawarehouse.fabric.microsoft.com"  # <-- UPDATE THIS

# Schema
SCHEMA = "dbo"

# Load mode: "full" = truncate & reload, "incremental" = merge/upsert
LOAD_MODE = "full"  # Change to "incremental" after initial load

# Tables to sync
# Format: { "table_name": "primary_key_column" }
# Primary key is required for incremental mode (MERGE)
# Set value to None if unknown (will fall back to full load)
TABLES = {
    "TABLE1": "id",  # <-- UPDATE with your actual table name and PK column
    # "TABLE2": "pk_column",
}

# ── HELPERS ────────────────────────────────────────────────────────────────

def onelake_path(table_name):
    """Build the ABFS path to read a table from the source warehouse via OneLake."""
    return (
        f"abfss://{SOURCE_WORKSPACE}@onelake.dfs.fabric.microsoft.com/"
        f"{SOURCE_WAREHOUSE}.{SOURCE_ITEM_TYPE}/Tables/{SCHEMA}/{table_name}"
    )


def get_access_token():
    """Get an AAD token for the Warehouse SQL endpoint."""
    try:
        return notebookutils.credentials.getToken("https://database.windows.net")
    except NameError:
        return mssparkutils.credentials.getToken("https://database.windows.net")


def get_jdbc_url():
    """Build the JDBC connection URL for the target Warehouse."""
    return (
        f"jdbc:sqlserver://{SQL_ENDPOINT}:1433;"
        f"database={TARGET_WAREHOUSE};"
        f"encrypt=true;trustServerCertificate=false;"
        f"hostNameInCertificate=*.datawarehouse.fabric.microsoft.com;"
        f"loginTimeout=30"
    )


def get_jdbc_properties():
    """Build JDBC connection properties with AAD token auth."""
    return {
        "accessToken": get_access_token(),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }


def execute_warehouse_sql(sql):
    """Execute a T-SQL statement directly against the Warehouse via JDBC."""
    token = get_access_token()
    url = get_jdbc_url() + f";accessToken={token}"
    conn = spark._jvm.java.sql.DriverManager.getConnection(url)
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()


# ── LOAD FUNCTIONS ─────────────────────────────────────────────────────────

def load_table_full(table_name):
    """Full load: read from BCDR1 OneLake, write to BCDR2 Warehouse via JDBC."""
    print(f"[FULL LOAD] {SCHEMA}.{table_name}")

    # Step 1: Read from BCDR1's DW1 via OneLake (read is allowed)
    source_path = onelake_path(table_name)
    print(f"  Reading from: {source_path}")
    source_df = spark.read.format("delta").load(source_path)

    row_count = source_df.count()
    print(f"  Rows read: {row_count}")

    if row_count == 0:
        print(f"  Skipping {table_name} — no rows.")
        return

    # Step 2: Write to Warehouse via JDBC (the Warehouse's native SQL write path)
    print(f"  Writing to {TARGET_WAREHOUSE}.{SCHEMA}.{table_name} via JDBC...")
    jdbc_url = get_jdbc_url()
    properties = get_jdbc_properties()
    properties["truncate"] = "true"  # Truncate instead of DROP+CREATE

    source_df.write.jdbc(
        url=jdbc_url,
        table=f"{SCHEMA}.{table_name}",
        mode="overwrite",
        properties=properties
    )

    print(f"  Loaded {row_count} rows into {TARGET_WAREHOUSE}.{SCHEMA}.{table_name}")


def load_table_incremental(table_name, primary_key):
    """Incremental load: MERGE new/changed rows into target Warehouse via JDBC."""
    print(f"[INCREMENTAL] {SCHEMA}.{table_name} (PK: {primary_key})")

    if not primary_key:
        print(f"  No primary key defined. Falling back to full load.")
        load_table_full(table_name)
        return

    # Read from source OneLake
    source_path = onelake_path(table_name)
    print(f"  Reading from: {source_path}")
    source_df = spark.read.format("delta").load(source_path)

    row_count = source_df.count()
    print(f"  Rows in source: {row_count}")

    if row_count == 0:
        print(f"  Skipping {table_name} — no rows.")
        return

    # Step 1: Write source data to a temp table in the Warehouse via JDBC
    temp_table = f"_sync_tmp_{table_name}"
    print(f"  Writing to temp table {temp_table}...")

    jdbc_url = get_jdbc_url()
    properties = get_jdbc_properties()

    source_df.write.jdbc(
        url=jdbc_url,
        table=f"{SCHEMA}.{temp_table}",
        mode="overwrite",
        properties=properties
    )

    # Step 2: MERGE from temp table into target table via T-SQL
    columns = source_df.columns
    update_set = ", ".join([f"target.[{c}] = source.[{c}]" for c in columns if c != primary_key])
    insert_cols = ", ".join([f"[{c}]" for c in columns])
    insert_vals = ", ".join([f"source.[{c}]" for c in columns])

    # Check if target exists; if not, just rename temp to target
    try:
        execute_warehouse_sql(f"SELECT TOP 1 1 FROM {SCHEMA}.{table_name}")
    except Exception:
        print(f"  Target table doesn't exist. Renaming temp table.")
        execute_warehouse_sql(f"EXEC sp_rename '{SCHEMA}.{temp_table}', '{table_name}'")
        print(f"  Created {TARGET_WAREHOUSE}.{SCHEMA}.{table_name} with {row_count} rows")
        return

    merge_sql = f"""
    MERGE INTO {SCHEMA}.{table_name} AS target
    USING {SCHEMA}.{temp_table} AS source
    ON target.[{primary_key}] = source.[{primary_key}]
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ({insert_cols}) VALUES ({insert_vals})
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
    """

    try:
        print(f"  Executing MERGE...")
        execute_warehouse_sql(merge_sql)
        print(f"  Merged into {TARGET_WAREHOUSE}.{SCHEMA}.{table_name}")
    except Exception as e:
        print(f"  MERGE failed: {e}")
        print(f"  Falling back to full load...")
        load_table_full(table_name)
    finally:
        # Clean up temp table
        try:
            execute_warehouse_sql(f"DROP TABLE IF EXISTS {SCHEMA}.{temp_table}")
        except Exception:
            pass


# ── MAIN ───────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  Warehouse Direct Sync (no staging)")
    print(f"  Source : {SOURCE_WORKSPACE}/{SOURCE_WAREHOUSE} (OneLake)")
    print(f"  Target : {TARGET_WAREHOUSE} (JDBC: {SQL_ENDPOINT})")
    print(f"  Mode   : {LOAD_MODE}")
    print("=" * 60)

    if SQL_ENDPOINT == "your-endpoint.datawarehouse.fabric.microsoft.com":
        print("ERROR: You must set SQL_ENDPOINT to your actual Warehouse SQL connection string.")
        print("       Find it: Fabric portal → BCDR2 → DW1 → Settings → SQL connection string")
        return

    tables_to_load = TABLES

    if not tables_to_load:
        print("ERROR: No tables configured. Add entries to the TABLES dict.")
        return

    success = 0
    failed = 0

    for table_name, primary_key in tables_to_load.items():
        try:
            if LOAD_MODE == "full":
                load_table_full(table_name)
            else:
                load_table_incremental(table_name, primary_key)
            success += 1
        except Exception as e:
            print(f"  ERROR loading {table_name}: {e}")
            failed += 1

    print("\n" + "=" * 60)
    print(f"  Complete. Success: {success}, Failed: {failed}")
    print("=" * 60)


main()
