<#
.SYNOPSIS
    Syncs a Microsoft Fabric Lakehouse/Warehouse between two workspaces using AzCopy.

.DESCRIPTION
    Performs an initial full copy (AzCopy copy) or incremental sync (AzCopy sync)
    from a source OneLake URI to a destination OneLake URI.

    Designed for BCDR scenarios: BCDR1 (primary) -> BCDR2 (replica).

    Supports:
      - Full initial copy mode
      - One-shot incremental sync
      - Continuous sync loop on a configurable interval
      - Dry-run mode (prints commands without executing)
      - Structured JSON logging
      - Retry with backoff for transient failures
      - Include/exclude filter patterns
      - Safety check to prevent accidental source/dest reversal

.PARAMETER SourceUri
    The OneLake URI for the source Lakehouse/Warehouse (BCDR1).
    Example: https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Tables

.PARAMETER DestUri
    The OneLake URI for the destination Lakehouse/Warehouse (BCDR2).
    Example: https://onelake.dfs.fabric.microsoft.com/BCDR2/LH1.Lakehouse/Tables

.PARAMETER Mode
    Operation mode: "copy" for initial full load, "sync" for incremental delta.

.PARAMETER LogPath
    Directory where structured log files are written. Defaults to ./logs.

.PARAMETER MaxRetries
    Maximum number of retry attempts for transient AzCopy failures. Default: 3.

.PARAMETER RetryDelaySeconds
    Base delay in seconds between retries (doubles each attempt). Default: 10.

.PARAMETER ExcludePatterns
    Semicolon-separated glob patterns to exclude (passed to --exclude-pattern).

.PARAMETER IncludePatterns
    Semicolon-separated glob patterns to include (passed to --include-pattern).

.PARAMETER DryRun
    If set, prints the AzCopy commands without executing them.

.PARAMETER Continuous
    If set with Mode=sync, runs sync in a loop every SyncIntervalMinutes.

.PARAMETER SyncIntervalMinutes
    Interval in minutes between continuous sync runs. Default: 5.

.PARAMETER SafetyTag
    A confirmation string that must equal "CONFIRMED" to proceed. Prevents
    accidental execution without explicit intent.

.PARAMETER AzCopyPath
    Path to the azcopy executable. Defaults to "azcopy" (assumes on PATH).

.EXAMPLE
    # Initial full copy
    .\Sync-FabricLakehouse.ps1 -SourceUri "https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Tables" `
        -DestUri "https://onelake.dfs.fabric.microsoft.com/BCDR2/LH1.Lakehouse/Tables" `
        -Mode copy -SafetyTag CONFIRMED

.EXAMPLE
    # One-shot incremental sync
    .\Sync-FabricLakehouse.ps1 -SourceUri "https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Tables" `
        -DestUri "https://onelake.dfs.fabric.microsoft.com/BCDR2/LH1.Lakehouse/Tables" `
        -Mode sync -SafetyTag CONFIRMED

.EXAMPLE
    # Continuous sync every 5 minutes
    .\Sync-FabricLakehouse.ps1 -SourceUri "https://onelake.dfs.fabric.microsoft.com/BCDR1/LH1.Lakehouse/Tables" `
        -DestUri "https://onelake.dfs.fabric.microsoft.com/BCDR2/LH1.Lakehouse/Tables" `
        -Mode sync -Continuous -SyncIntervalMinutes 5 -SafetyTag CONFIRMED
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string]$SourceUri,

    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string]$DestUri,

    [Parameter(Mandatory)]
    [ValidateSet("copy", "sync")]
    [string]$Mode,

    [string]$LogPath = (Join-Path $PSScriptRoot "logs"),

    [ValidateRange(0, 10)]
    [int]$MaxRetries = 3,

    [ValidateRange(1, 300)]
    [int]$RetryDelaySeconds = 10,

    [string]$ExcludePatterns,

    [string]$IncludePatterns,

    [switch]$DryRun,

    [switch]$Continuous,

    [ValidateRange(1, 1440)]
    [int]$SyncIntervalMinutes = 5,

    [Parameter(Mandatory)]
    [ValidateSet("CONFIRMED")]
    [string]$SafetyTag,

    [string]$AzCopyPath = "azcopy"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────
# Exit codes that AzCopy considers transient / retryable
$TransientExitCodes = @(1)   # 1 = partial failure / transient; 2 = permanent failure

# ─────────────────────────────────────────────
# Safety: prevent accidental source/dest reversal
# ─────────────────────────────────────────────
function Assert-DirectionSafety {
    <#
    .SYNOPSIS
        Validates that BCDR2 (replica) is never used as the source
        and BCDR1 (primary) is never used as the destination.
    #>

    # Extract workspace names from the OneLake URIs.
    # Expected format: https://onelake.dfs.fabric.microsoft.com/<workspace>/<item>
    $srcSegments = ([Uri]$SourceUri).AbsolutePath.Trim('/').Split('/')
    $dstSegments = ([Uri]$DestUri).AbsolutePath.Trim('/').Split('/')

    if ($srcSegments.Count -lt 1 -or $dstSegments.Count -lt 1) {
        throw "Unable to parse workspace names from the supplied URIs. Verify the URI format."
    }

    $srcWorkspace = $srcSegments[0]
    $dstWorkspace = $dstSegments[0]

    # Guard: source must not be the replica workspace
    if ($srcWorkspace -match '(?i)^BCDR2$') {
        throw @"
SAFETY BLOCK: Source workspace is '$srcWorkspace' (the REPLICA).
Syncing FROM the replica back to primary is not allowed.
If this is intentional, remove this safety check.
"@
    }

    # Guard: destination must not be the primary workspace
    if ($dstWorkspace -match '(?i)^BCDR1$') {
        throw @"
SAFETY BLOCK: Destination workspace is '$dstWorkspace' (the PRIMARY).
You would be overwriting the primary with replica data.
If this is intentional, remove this safety check.
"@
    }

    # Guard: source and dest must differ
    if ($srcWorkspace -eq $dstWorkspace) {
        throw "Source and destination workspaces are the same ('$srcWorkspace'). Nothing to sync."
    }

    Write-Host "[Safety] Direction OK: $srcWorkspace -> $dstWorkspace" -ForegroundColor Green
}

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
function Initialize-LogDirectory {
    if (-not (Test-Path $LogPath)) {
        New-Item -ItemType Directory -Path $LogPath -Force | Out-Null
    }
}

function Write-StructuredLog {
    param(
        [string]$Level,        # INFO, WARN, ERROR
        [string]$RunMode,      # copy | sync
        [string]$Command,
        [int]$ExitCode = -1,
        [string]$Message
    )

    $entry = [ordered]@{
        timestamp = (Get-Date -Format "o")
        level     = $Level
        mode      = $RunMode
        command   = $Command
        exitCode  = $ExitCode
        message   = $Message
    }

    $json = $entry | ConvertTo-Json -Compress
    $logFile = Join-Path $LogPath ("FabricSync_{0}.jsonl" -f (Get-Date -Format "yyyyMMdd"))

    # Write to log file and console
    Add-Content -Path $logFile -Value $json -Encoding UTF8
    $color = switch ($Level) {
        "ERROR" { "Red" }
        "WARN"  { "Yellow" }
        default { "Cyan" }
    }
    Write-Host "[$Level] $Message" -ForegroundColor $color
}

# ─────────────────────────────────────────────
# AzCopy command builder
# ─────────────────────────────────────────────
function Build-AzCopyArgs {
    param(
        [string]$RunMode
    )

    $args = [System.Collections.Generic.List[string]]::new()

    # Primary verb
    $args.Add($RunMode)

    # Source and destination (quoted to handle spaces)
    $args.Add("`"$SourceUri`"")
    $args.Add("`"$DestUri`"")

    # Recursive is essential for Lakehouse delta tables (nested _delta_log dirs)
    $args.Add("--recursive=true")

    # copy-only flags
    if ($RunMode -eq "copy") {
        # Place contents directly in dest, don't nest source folder as a subfolder
        $args.Add("--as-subdir=false")
        # Overwrite only if source is newer (avoids DELETE calls that OneLake rejects)
        $args.Add("--overwrite=ifSourceNewer")
        # OneLake doesn't support access tiers
        $args.Add("--s2s-preserve-access-tier=false")
        # Skip length check (OneLake doesn't always return Content-Length)
        $args.Add("--check-length=false")
    }

    # Include / Exclude patterns
    if ($IncludePatterns) {
        $args.Add("--include-pattern=`"$IncludePatterns`"")
    }
    if ($ExcludePatterns) {
        $args.Add("--exclude-pattern=`"$ExcludePatterns`"")
    }

    # Tell AzCopy to use AAD auth for the OneLake endpoint
    $args.Add("--trusted-microsoft-suffixes=onelake.dfs.fabric.microsoft.com")

    # Output format for easier log parsing
    $args.Add("--output-type=json")

    return $args
}

# ─────────────────────────────────────────────
# AzCopy executor with retry logic
# ─────────────────────────────────────────────
function Invoke-AzCopyWithRetry {
    param(
        [string]$RunMode
    )

    $azcopyArgs = Build-AzCopyArgs -RunMode $RunMode
    $fullCommand = "$AzCopyPath $($azcopyArgs -join ' ')"

    if ($DryRun) {
        Write-StructuredLog -Level "INFO" -RunMode $RunMode -Command $fullCommand -Message "DRY RUN — command would be: $fullCommand"
        return 0
    }

    $attempt = 0
    $delay = $RetryDelaySeconds

    while ($true) {
        $attempt++
        Write-StructuredLog -Level "INFO" -RunMode $RunMode -Command $fullCommand `
            -Message "Attempt $attempt of $($MaxRetries + 1) — executing AzCopy $RunMode"

        # Execute AzCopy
        $process = Start-Process -FilePath $AzCopyPath `
            -ArgumentList ($azcopyArgs -join ' ') `
            -NoNewWindow -Wait -PassThru `
            -RedirectStandardOutput (Join-Path $LogPath "azcopy_stdout_tmp.log") `
            -RedirectStandardError  (Join-Path $LogPath "azcopy_stderr_tmp.log")

        $exitCode = $process.ExitCode

        # Read captured output for logging
        $stdout = ""
        $stderr = ""
        $stdoutFile = Join-Path $LogPath "azcopy_stdout_tmp.log"
        $stderrFile = Join-Path $LogPath "azcopy_stderr_tmp.log"
        if (Test-Path $stdoutFile) { $stdout = Get-Content $stdoutFile -Raw -ErrorAction SilentlyContinue }
        if (Test-Path $stderrFile) { $stderr = Get-Content $stderrFile -Raw -ErrorAction SilentlyContinue }

        # Append raw output to a persistent run log
        $runLog = Join-Path $LogPath ("AzCopyRun_{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
        @"
=== AzCopy Run | Mode=$RunMode | Attempt=$attempt | Exit=$exitCode ===
STDOUT:
$stdout
STDERR:
$stderr
"@ | Out-File -FilePath $runLog -Encoding UTF8

        if ($exitCode -eq 0) {
            Write-StructuredLog -Level "INFO" -RunMode $RunMode -Command $fullCommand `
                -ExitCode $exitCode -Message "AzCopy $RunMode completed successfully."
            return 0
        }

        # Determine if transient
        $isTransient = $exitCode -in $TransientExitCodes

        if (-not $isTransient) {
            Write-StructuredLog -Level "ERROR" -RunMode $RunMode -Command $fullCommand `
                -ExitCode $exitCode -Message "AzCopy failed with NON-TRANSIENT exit code $exitCode. Aborting."
            return $exitCode
        }

        if ($attempt -gt $MaxRetries) {
            Write-StructuredLog -Level "ERROR" -RunMode $RunMode -Command $fullCommand `
                -ExitCode $exitCode -Message "Exhausted $MaxRetries retries. Last exit code: $exitCode."
            return $exitCode
        }

        Write-StructuredLog -Level "WARN" -RunMode $RunMode -Command $fullCommand `
            -ExitCode $exitCode -Message "Transient failure (exit $exitCode). Retrying in $delay seconds..."

        Start-Sleep -Seconds $delay
        $delay = [Math]::Min($delay * 2, 300)  # exponential backoff, capped at 5 min
    }
}

# ─────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────
function Start-LakehouseSync {

    Write-Host ""
    Write-Host "================================================================" -ForegroundColor White
    Write-Host "  Fabric Lakehouse Sync  |  Mode: $Mode  |  DryRun: $DryRun" -ForegroundColor White
    Write-Host "  Source : $SourceUri" -ForegroundColor Gray
    Write-Host "  Dest   : $DestUri" -ForegroundColor Gray
    Write-Host "================================================================" -ForegroundColor White
    Write-Host ""

    # Validate prerequisites
    Initialize-LogDirectory
    Assert-DirectionSafety

    # Verify AzCopy is reachable
    try {
        $ver = & $AzCopyPath --version 2>&1
        Write-Host "[Pre-flight] AzCopy version: $ver" -ForegroundColor DarkGray
    }
    catch {
        throw "AzCopy not found at '$AzCopyPath'. Install it or set -AzCopyPath."
    }

    # ── COPY mode: one-shot full load ──
    if ($Mode -eq "copy") {
        $result = Invoke-AzCopyWithRetry -RunMode "copy"
        exit $result
    }

    # ── SYNC mode ──
    if (-not $Continuous) {
        # Single sync run
        $result = Invoke-AzCopyWithRetry -RunMode "sync"
        exit $result
    }

    # Continuous sync loop
    Write-StructuredLog -Level "INFO" -RunMode "sync" -Command "" `
        -Message "Starting continuous sync loop. Interval: $SyncIntervalMinutes min. Press Ctrl+C to stop."

    $iteration = 0
    while ($true) {
        $iteration++
        Write-Host ""
        Write-Host "── Sync iteration $iteration ── $(Get-Date -Format 'HH:mm:ss') ──" -ForegroundColor Magenta

        $result = Invoke-AzCopyWithRetry -RunMode "sync"

        if ($result -ne 0) {
            Write-StructuredLog -Level "WARN" -RunMode "sync" -Command "" `
                -ExitCode $result -Message "Iteration $iteration failed (exit $result). Will retry next interval."
        }

        Write-Host "Sleeping $SyncIntervalMinutes minutes until next sync..." -ForegroundColor DarkGray
        Start-Sleep -Seconds ($SyncIntervalMinutes * 60)
    }
}

# ─────────────────────────────────────────────
# Run
# ─────────────────────────────────────────────
Start-LakehouseSync
