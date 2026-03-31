<#
.SYNOPSIS
    Wrapper to invoke Sync-FabricLakehouse.ps1 from a Fabric notebook or VS Code terminal.

.DESCRIPTION
    This lightweight wrapper sets up authentication context and calls the main sync script.

    FROM A FABRIC NOTEBOOK:
      - Use a %%script or !pwsh cell to shell out to PowerShell.
      - Fabric notebooks run with the workspace identity, so AzCopy will use
        the ambient managed identity for OneLake access (no SAS tokens needed).

    FROM VS CODE TERMINAL:
      - Run this script directly. Ensure you have logged in first:
            azcopy login --identity          (managed identity)
          OR
            azcopy login                     (interactive AAD)
          OR
            azcopy login --tenant-id <tid>   (service principal)

.NOTES
    Adjust the URIs below to match your actual Fabric workspace/item paths.
#>

# ─────────────────────────────────────────────────────────────────────────────
# ██  CONFIGURE THESE VALUES FOR YOUR ENVIRONMENT
# ─────────────────────────────────────────────────────────────────────────────
#
# OneLake URI format:
#   https://onelake.dfs.fabric.microsoft.com/<WorkspaceName>/<ItemName>.<ItemType>/<SubPath>
#
# Examples:
#   Lakehouse Tables : https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Tables
#   Lakehouse Files  : https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Files
#   Warehouse Tables : https://onelake.dfs.fabric.microsoft.com/BCDR1/WH1.Warehouse/Tables
#
# Tip: Sync Tables AND Files separately if you need both.
# ─────────────────────────────────────────────────────────────────────────────

$SourceBase = "https://onelake.dfs.fabric.microsoft.com/BCDR1"
$DestBase   = "https://onelake.dfs.fabric.microsoft.com/BCDR2"

# Items to sync — add or remove entries as needed
# Mode: "mirror" for Tables (Delta-aware: copy+delete), "sync" for Files
$SyncPairs = @(
    @{ Source = "$SourceBase/LH1.Lakehouse/Tables";  Dest = "$DestBase/LH1.Lakehouse/Tables";  Mode = "mirror" }
    @{ Source = "$SourceBase/LH1.Lakehouse/Files";   Dest = "$DestBase/LH1.Lakehouse/Files";   Mode = "sync"   }
    # Uncomment below to also sync a Warehouse:
    # @{ Source = "$SourceBase/WH1.Warehouse/Tables"; Dest = "$DestBase/WH1.Warehouse/Tables"; Mode = "mirror" }
)

# Resolve the main script (assumed to be alongside this wrapper)
$SyncScript = Join-Path $PSScriptRoot "Sync-FabricLakehouse.ps1"
if (-not (Test-Path $SyncScript)) {
    Write-Error "Main sync script not found at: $SyncScript"
    exit 1
}

# ─────────────────────────────────────────────────────────────────────────────
# Choose mode: "copy" for initial full load, "sync" for incremental
# ─────────────────────────────────────────────────────────────────────────────
$DefaultMode = "sync"       # Default mode: "copy" for initial full load, "sync" for incremental
$ContinuousSync = $false    # Set to $true for an ongoing loop
$IntervalMinutes = 5

foreach ($pair in $SyncPairs) {
    # Use per-pair mode if specified, otherwise fall back to default
    $pairMode = if ($pair.Mode) { $pair.Mode } else { $DefaultMode }

    Write-Host "`n>>> Syncing ($pairMode): $($pair.Source) -> $($pair.Dest)" -ForegroundColor Cyan

    $params = @{
        SourceUri          = $pair.Source
        DestUri            = $pair.Dest
        Mode               = $pairMode
        SafetyTag          = "CONFIRMED"
        MaxRetries         = 3
        RetryDelaySeconds  = 10
    }

    if ($ContinuousSync -and $pairMode -eq "sync") {
        $params["Continuous"]         = $true
        $params["SyncIntervalMinutes"] = $IntervalMinutes
    }

    & $SyncScript @params
}
