# Fabric Lakehouse BCDR Sync with AzCopy

Syncs a Microsoft Fabric Lakehouse between two workspaces (BCDR1 → BCDR2) using AzCopy over OneLake. Supports initial full copy, incremental sync, and a Delta-aware mirror 
mode for tables.

##  DISCLAIMER

The information contained in this code-repository and any accompanying materials (including, but not limited to, scripts, sample codes, etc.) are provided "AS-IS" and "WITH ALL FAULTS." Any estimated pricing information is provided solely for demonstration purposes and does not represent final pricing. Microsoft, Hiram Fleitas, and/or contributors of this repo (refered to as Parties thereof), assume no liability arising from your use of the information or any accompanying material. The Parties, as mentioned previously, make NO GUARANTEES OR WARRANTIES OF ANY KIND, WHETHER EXPRESSED OR IMPLIED, in providing this information, including any pricing information.

## Prerequisites

### Software

| Tool | Install | Verify |
|------|---------|--------|
| **PowerShell 7+** | `winget install Microsoft.PowerShell` | `pwsh --version` |
| **AzCopy v10.x** | `winget install Microsoft.AzCopy` or download from [aka.ms/azcopy](https://aka.ms/azcopy) | `azcopy --version` |

### Authentication

Log in to AzCopy before running any scripts:

```powershell
# Interactive AAD login (VS Code / local dev)
azcopy login

# OR managed identity (Fabric notebook / VM)
azcopy login --identity

# OR service principal
azcopy login --tenant-id <tenant-id> --service-principal --application-id <app-id>
```

### Manual Fabric Setup

Complete these one-time steps in the [Fabric portal](https://app.fabric.microsoft.com):

1. **Create workspace `BCDR1`** (primary) — this is the source of truth.
2. **Create workspace `BCDR2`** (replica) — this is the disaster-recovery target.
3. **In `BCDR1`, create a Lakehouse named `LH1`.**
4. **In `BCDR2`, create a Lakehouse named `LH1`** (same name).
5. Ensure your identity (or service principal) has **Contributor** or higher on both workspaces.

> The scripts expect these exact names: workspaces `BCDR1` / `BCDR2` and lakehouse `LH1`. To customize, edit the URIs in `Run-Sync.ps1`.

## Repository Contents

| File | Description |
|------|-------------|
| `Sync-FabricLakehouse.ps1` | Main sync engine — supports `copy`, `sync`, and `mirror` modes |
| `Run-Sync.ps1` | Wrapper that syncs both Tables (mirror) and Files (sync) in one run |
| `LH1LoadTables.ipynb` | Fabric notebook — auto-loads CSVs from Files/ into Delta tables |
| `LH1Changes.ipynb` | Fabric notebook — simulates UPDATE and DELETE on `adjuster_dim` |
| `lh1_files/` | Sample CSV files (8 tables: claims, customers, policies, etc.) |
| `notebook_helper.py` | Helper to invoke sync from a Fabric notebook via subprocess |
| `load_files_to_tables.py` | Standalone Python version of the table-load notebook |
| `Sync-WarehouseDirect.py` | Fabric notebook script — syncs Warehouse tables directly via JDBC (no staging Lakehouse) |
| `logs/` | Structured JSONL logs written by the sync scripts |

## Sync Modes

| Mode | Command | Use Case |
|------|---------|----------|
| `copy` | `azcopy copy` | Initial full load — copies all files, skips if source is older |
| `sync` | `azcopy sync --delete-destination=true` | Incremental sync for **Files/** — copies changed files, deletes extras |
| `mirror` | `azcopy copy --overwrite=true` then `azcopy sync --delete-destination=true` | Delta-aware sync for **Tables/** — two-pass to capture UPDATE/DELETE changes |

## Test Playbook

Run these stages in sequence to validate the full BCDR sync lifecycle.

### Stage 1 — Upload Files & Initial Copy

Upload the sample CSVs to BCDR1 and do the first full copy to BCDR2.

```powershell
# 1a. Upload CSV files to BCDR1 LH1 Files section
azcopy copy "lh1_files/*" "https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Files" `
    --recursive=true --overwrite=ifSourceNewer `
    --trusted-microsoft-suffixes=onelake.dfs.fabric.microsoft.com

# 1b. Initial full copy — Files
.\Sync-FabricLakehouse.ps1 `
    -SourceUri "https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Files" `
    -DestUri   "https://onelake.dfs.fabric.microsoft.com/BCDR2/LH1.Lakehouse/Files" `
    -Mode copy -SafetyTag CONFIRMED
```

**Validate:** Open BCDR2 → LH1 → Files and confirm all 8 CSVs are present.

### Stage 2 — Create Delta Tables from CSVs

Run the `LH1LoadTables.ipynb` notebook in Fabric (attached to BCDR1 / LH1):

1. Open the Fabric portal → BCDR1 workspace → create or import `LH1LoadTables` notebook.
2. Attach the notebook to the `LH1` Lakehouse.
3. Run all cells — this reads every CSV from Files/ and writes Delta tables to Tables/.

Then sync the tables to BCDR2:

```powershell
# 2. Initial full copy — Tables
.\Sync-FabricLakehouse.ps1 `
    -SourceUri "https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Tables" `
    -DestUri   "https://onelake.dfs.fabric.microsoft.com/BCDR2/LH1.Lakehouse/Tables" `
    -Mode copy -SafetyTag CONFIRMED
```

**Validate:** Open BCDR2 → LH1 → Tables and confirm all 8 Delta tables appear with correct row counts.

### Stage 3 — File Changes (Add & Delete)

Test that incremental sync picks up file additions and deletions.

```powershell
# 3a. Delete a file from BCDR1
azcopy remove "https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Files/add_adjuster.csv" `
    --trusted-microsoft-suffixes=onelake.dfs.fabric.microsoft.com

# 3b. Add a new file to BCDR1 (example: re-upload one with changes, or any new CSV)
azcopy copy "lh1_files/adjuster_dim.csv" `
    "https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Files/adjuster_dim_v2.csv" `
    --trusted-microsoft-suffixes=onelake.dfs.fabric.microsoft.com

# 3c. Sync Files to BCDR2
.\Sync-FabricLakehouse.ps1 `
    -SourceUri "https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Files" `
    -DestUri   "https://onelake.dfs.fabric.microsoft.com/BCDR2/LH1.Lakehouse/Files" `
    -Mode sync -SafetyTag CONFIRMED
```

**Validate:**
- `add_adjuster.csv` is **removed** from BCDR2 Files.
- `adjuster_dim_v2.csv` is **added** to BCDR2 Files.

### Stage 4 — Table Changes (UPDATE & DELETE rows)

Test that mirror mode captures Delta table mutations.

Run the `LH1Changes.ipynb` notebook in Fabric (attached to BCDR1 / LH1). This notebook:
- Runs `SELECT * FROM adjuster_dim` (baseline)
- Runs `UPDATE adjuster_dim SET Experience_Years = 15 WHERE Adjuster_ID = 101`
- Runs `SELECT * FROM adjuster_dim` (verify update)
- Runs `DELETE FROM adjuster_dim WHERE Adjuster_ID = 109`

Then mirror the tables to BCDR2:

```powershell
# 4. Mirror Tables to BCDR2 (Delta-aware two-pass)
.\Sync-FabricLakehouse.ps1 `
    -SourceUri "https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Tables" `
    -DestUri   "https://onelake.dfs.fabric.microsoft.com/BCDR2/LH1.Lakehouse/Tables" `
    -Mode mirror -SafetyTag CONFIRMED
```

**Validate:**
- In BCDR2 `adjuster_dim`: Adjuster_ID 101 has `Experience_Years = 15`.
- In BCDR2 `adjuster_dim`: Adjuster_ID 109 is **deleted** (row no longer exists).

### Stage 5 — Reverse Sync / Failback (BCDR2 → BCDR1)

If the primary (BCDR1) needs to be restored from the replica (BCDR2), use the `-AllowReverse` switch to bypass the built-in direction safety guard.

```powershell
# 5a. Reverse copy — Files
.\Sync-FabricLakehouse.ps1 `
    -SourceUri "https://onelake.dfs.fabric.microsoft.com/BCDR2/LH1.Lakehouse/Files" `
    -DestUri   "https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Files" `
    -Mode copy -SafetyTag CONFIRMED -AllowReverse

# 5b. Reverse copy — Tables
.\Sync-FabricLakehouse.ps1 `
    -SourceUri "https://onelake.dfs.fabric.microsoft.com/BCDR2/LH1.Lakehouse/Tables" `
    -DestUri   "https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Tables" `
    -Mode copy -SafetyTag CONFIRMED -AllowReverse
```

**Validate:** Open BCDR1 → LH1 and confirm Files and Tables match BCDR2.

> **Note:** Without `-AllowReverse`, the script blocks any attempt to use BCDR2 as the source or BCDR1 as the destination. This prevents accidental overwrites during normal operations.

### Run All Stages at Once

After initial setup, use the wrapper to sync everything (Tables via mirror, Files via sync):

```powershell
.\Run-Sync.ps1
```

![Sync'ed](https://github.com/user-attachments/assets/99e4f6f2-c068-433f-aa6d-b67e46bc961f "Sync'ed")


## Warehouse Direct Sync (Sync-WarehouseDirect.py)

Syncs Warehouse tables from one workspace to another **directly via JDBC** — no intermediate staging Lakehouse required. Reads source tables from OneLake (Spark Delta reader) and writes to the target Warehouse's SQL endpoint.

### Prerequisites

1. A **Warehouse** (e.g., `DW1`) must exist in both the source and target workspaces.
2. Get the **SQL connection string** for the target Warehouse:
   - Fabric portal → target workspace → Warehouse → Settings (gear icon) → **SQL connection string**
   - It looks like: `xxxxxxxx.datawarehouse.fabric.microsoft.com`
3. Your notebook identity must have **read access** to the source workspace.

### Configuration

Open `Sync-WarehouseDirect.py` and update the variables at the top:

| Variable | Description | Example |
|----------|-------------|---------|
| `SOURCE_WORKSPACE` | Source workspace name | `BCDR1` |
| `SOURCE_WAREHOUSE` | Source warehouse name | `DW1` |
| `SOURCE_ITEM_TYPE` | `"Warehouse"` or `"Lakehouse"` | `Warehouse` |
| `TARGET_WAREHOUSE` | Target warehouse name | `DW1` |
| `SQL_ENDPOINT` | Target warehouse SQL connection string | `xxx.datawarehouse.fabric.microsoft.com` |
| `SCHEMA` | Schema name | `dbo` |
| `LOAD_MODE` | `"full"` (truncate + reload) or `"incremental"` (MERGE/upsert) | `full` |
| `TABLES` | Dict of `{ "table_name": "primary_key_column" }` | `{ "TABLE1": "id" }` |

### Usage

1. **Create a Fabric notebook** in the target workspace (e.g., BCDR2).
2. **Paste or import** `Sync-WarehouseDirect.py` into the notebook.
3. **Update** `SQL_ENDPOINT` and the `TABLES` dictionary with your actual values.
4. **Set `LOAD_MODE`:**
   - `"full"` — for initial load (truncates target table, then inserts all rows).
   - `"incremental"` — for ongoing syncs (MERGE/upsert using the primary key).
5. **Run all cells.**

```text
# Example output
============================================================
  Warehouse Direct Sync (no staging)
  Source : BCDR1/DW1 (OneLake)
  Target : DW1 (JDBC: your-endpoint.datawarehouse.fabric.microsoft.com)
  Mode   : full
============================================================
[FULL LOAD] dbo.TABLE1
  Reading from: abfss://BCDR1@onelake.dfs.fabric.microsoft.com/DW1.Warehouse/Tables/dbo/TABLE1
  Rows read: 1000
  Writing to DW1.dbo.TABLE1 via JDBC...
  Loaded 1000 rows into DW1.dbo.TABLE1
============================================================
  Complete. Success: 1, Failed: 0
============================================================
```

> **Tip:** Use `"full"` mode for the initial load, then switch to `"incremental"` for subsequent syncs. Incremental mode requires a primary key column for each table.

## Contributors

- Hiram Fleitas, Analytics GBB, Microsoft
- Simon Nuss, Prin PM, Microsoft
- Amol Manocha, Sr Sol Engineer, Microsoft
- Paras Sitaula, Prin Sol Engineer, Microsoft
- Sagar Bathe, Prin CSA, Microsoft
- Sachin Saraf, Prin CSA, Microsoft

## Thank You

Thanks for checking out this project! Named after the beautiful mutton snapper (*Lutjanus analis*) — a prized catch in the Florida Keys.

![Mutton Snapper](https://upload.wikimedia.org/wikipedia/commons/f/f4/Mutton-snapper-lutjanus-analis.jpg "Mutton Snapper")
