# Fabric Notebook Helper — Shell out to AzCopy sync from a Fabric Notebook
#
# Usage:
#   1. Upload Sync-FabricLakehouse.ps1 to a location accessible from the notebook
#      (e.g., the Lakehouse Files area or an attached volume).
#   2. Use the cell below (Python) to invoke it via subprocess.
#
# NOTE: In a Fabric notebook, the compute runs with the workspace managed identity.
#       AzCopy inherits that identity — no extra login step needed.

# ─── Python cell for a Fabric notebook ───
"""
import subprocess, json

# Path to the uploaded PowerShell script
script_path = "/lakehouse/default/Files/scripts/Sync-FabricLakehouse.ps1"

source = "https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Tables"
dest   = "https://onelake.dfs.fabric.microsoft.com/BCDR2/LH1.Lakehouse/Tables"
mode   = "sync"  # "copy" for initial full load

cmd = [
    "pwsh", "-NoProfile", "-File", script_path,
    "-SourceUri", source,
    "-DestUri",   dest,
    "-Mode",      mode,
    "-SafetyTag", "CONFIRMED",
    "-MaxRetries", "3"
]

print(f"Running: {' '.join(cmd)}")
result = subprocess.run(cmd, capture_output=True, text=True)

print("─── STDOUT ───")
print(result.stdout)
if result.stderr:
    print("─── STDERR ───")
    print(result.stderr)
print(f"Exit code: {result.returncode}")
"""
