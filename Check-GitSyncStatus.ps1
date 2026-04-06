<#
.SYNOPSIS
    Checks whether a Fabric workspace has pending uncommitted changes in Git.

.DESCRIPTION
    Calls the Fabric REST API "Git - Get Status" endpoint to detect workspace items
    that have been added, modified, or deleted but not yet committed to the connected
    Git repository. This is critical for BCDR: if BCDR1 has uncommitted changes,
    syncing items to BCDR2 via Git would leave BCDR2 out-of-date.

    Reports:
      - Workspace-side changes (uncommitted to Git)
      - Remote-side changes (unsynced from Git)
      - Conflicts (modified on both sides)
      - Whether workspace HEAD matches the remote commit hash

.PARAMETER WorkspaceId
    The workspace ID to check.

.PARAMETER WorkspaceName
    The workspace display name (used to resolve WorkspaceId if not provided).

.PARAMETER FailOnPending
    If set, exits with code 1 when pending workspace changes are detected.
    Useful in CI/CD pipelines as a gate check.

.EXAMPLE
    # Check BCDR1 for uncommitted items
    .\Check-GitSyncStatus.ps1 -WorkspaceName "BCDR1"

.EXAMPLE
    # Gate check in a pipeline — fail if BCDR1 has pending commits
    .\Check-GitSyncStatus.ps1 -WorkspaceName "BCDR1" -FailOnPending
#>

[CmdletBinding()]
param(
    [string]$WorkspaceId,
    [string]$WorkspaceName,
    [switch]$FailOnPending
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$FabricApiBase = "https://api.fabric.microsoft.com/v1"

# ─────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────
function Get-FabricToken {
    try {
        $tok = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
        return $tok
    }
    catch {}

    try {
        $tok = (az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv)
        if ($tok) { return $tok }
    }
    catch {}

    throw "Unable to acquire a Fabric access token. Run Connect-AzAccount or az login first."
}

function Get-FabricHeaders {
    $token = Get-FabricToken
    return @{
        "Authorization" = "Bearer $token"
        "Content-Type"  = "application/json"
    }
}

# ─────────────────────────────────────────────
# Resolve workspace ID from display name
# ─────────────────────────────────────────────
function Resolve-WorkspaceId {
    param([string]$Name)

    $headers = Get-FabricHeaders
    $resp = Invoke-RestMethod -Uri "$FabricApiBase/workspaces" -Headers $headers -Method Get
    $ws = $resp.value | Where-Object { $_.displayName -eq $Name }

    if (-not $ws) { throw "Workspace '$Name' not found." }
    if ($ws -is [array]) { $ws = $ws[0] }

    Write-Host "[Resolve] Workspace '$Name' -> $($ws.id)" -ForegroundColor DarkGray
    return $ws.id
}

# ─────────────────────────────────────────────
# Poll long-running operation
# ─────────────────────────────────────────────
function Wait-FabricOperation {
    param([string]$OperationId)

    $headers = Get-FabricHeaders
    $maxWait = 120
    $elapsed = 0
    $interval = 5
    $opUrl = "$FabricApiBase/operations/$OperationId"

    while ($elapsed -lt $maxWait) {
        $status = Invoke-RestMethod -Uri $opUrl -Headers $headers -Method Get
        if ($status.status -eq "Succeeded") {
            # Fetch result
            $resultUrl = "$opUrl/result"
            $result = Invoke-RestMethod -Uri $resultUrl -Headers $headers -Method Get
            return $result
        }
        if ($status.status -eq "Failed") {
            throw "Git status operation failed: $($status | ConvertTo-Json -Depth 5)"
        }
        Write-Host "  Waiting for Git status... ($($status.status))" -ForegroundColor DarkGray
        Start-Sleep -Seconds $interval
        $elapsed += $interval
    }
    throw "Git status operation timed out after $maxWait seconds."
}

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

# Resolve workspace
if (-not $WorkspaceId -and $WorkspaceName) {
    $WorkspaceId = Resolve-WorkspaceId -Name $WorkspaceName
}
if (-not $WorkspaceId) { throw "Provide -WorkspaceId or -WorkspaceName." }

$label = if ($WorkspaceName) { $WorkspaceName } else { $WorkspaceId }

Write-Host ""
Write-Host "================================================================" -ForegroundColor White
Write-Host "  Git Sync Status Check — $label" -ForegroundColor White
Write-Host "================================================================" -ForegroundColor White
Write-Host ""

# Call Git - Get Status
$headers = Get-FabricHeaders
$statusUrl = "$FabricApiBase/workspaces/$WorkspaceId/git/status"

Write-Host "[1/2] Requesting Git status..." -ForegroundColor Cyan

$resp = Invoke-WebRequest -Uri $statusUrl -Headers $headers -Method Get -UseBasicParsing

if ($resp.StatusCode -eq 202) {
    # Long-running — poll
    $opId = ($resp.Headers["x-ms-operation-id"])
    if (-not $opId) {
        $loc = $resp.Headers["Location"]
        $opId = ($loc -split "/operations/" | Select-Object -Last 1).Trim("/")
    }
    $gitStatus = Wait-FabricOperation -OperationId $opId
}
else {
    $gitStatus = $resp.Content | ConvertFrom-Json
}

# ─────────────────────────────────────────────
# Analyze results
# ─────────────────────────────────────────────
Write-Host "[2/2] Analyzing status..." -ForegroundColor Cyan
Write-Host ""

$wsHead = $gitStatus.workspaceHead
$remoteHead = $gitStatus.remoteCommitHash
$changes = $gitStatus.changes

$inSync = ($wsHead -eq $remoteHead) -and ($changes.Count -eq 0)

Write-Host "  Workspace HEAD : $wsHead" -ForegroundColor Gray
Write-Host "  Remote HEAD    : $remoteHead" -ForegroundColor Gray
Write-Host "  Heads match    : $($wsHead -eq $remoteHead)" -ForegroundColor $(if ($wsHead -eq $remoteHead) { "Green" } else { "Yellow" })
Write-Host "  Total changes  : $($changes.Count)" -ForegroundColor $(if ($changes.Count -eq 0) { "Green" } else { "Yellow" })
Write-Host ""

# Categorize changes
$wsChanges     = $changes | Where-Object { $_.workspaceChange -and $_.workspaceChange -ne "None" }
$remoteChanges = $changes | Where-Object { $_.remoteChange -and $_.remoteChange -ne "None" }
$conflicts     = $changes | Where-Object { $_.conflictType -eq "Conflict" }

if ($wsChanges.Count -gt 0) {
    Write-Host "  UNCOMMITTED WORKSPACE CHANGES ($($wsChanges.Count)):" -ForegroundColor Yellow
    foreach ($c in $wsChanges) {
        $name = $c.itemMetadata.displayName
        $type = $c.itemMetadata.itemType
        $action = $c.workspaceChange
        Write-Host "    [$action] $type: $name" -ForegroundColor Yellow
    }
    Write-Host ""
}

if ($remoteChanges.Count -gt 0) {
    Write-Host "  UNSYNCED REMOTE CHANGES ($($remoteChanges.Count)):" -ForegroundColor Cyan
    foreach ($c in $remoteChanges) {
        $name = $c.itemMetadata.displayName
        $type = $c.itemMetadata.itemType
        $action = $c.remoteChange
        Write-Host "    [$action] $type: $name" -ForegroundColor Cyan
    }
    Write-Host ""
}

if ($conflicts.Count -gt 0) {
    Write-Host "  CONFLICTS ($($conflicts.Count)):" -ForegroundColor Red
    foreach ($c in $conflicts) {
        $name = $c.itemMetadata.displayName
        $type = $c.itemMetadata.itemType
        Write-Host "    [Conflict] $type: $name" -ForegroundColor Red
    }
    Write-Host ""
}

# Final verdict
Write-Host "─────────────────────────────────────────" -ForegroundColor White
if ($inSync) {
    Write-Host "  RESULT: $label is IN SYNC with Git." -ForegroundColor Green
    Write-Host "  BCDR2 will have the latest items after Git sync." -ForegroundColor Green
}
else {
    if ($wsChanges.Count -gt 0) {
        Write-Host "  RESULT: $label has $($wsChanges.Count) PENDING workspace change(s) NOT committed to Git." -ForegroundColor Yellow
        Write-Host "  WARNING: BCDR2 will be OUT OF DATE until these are committed." -ForegroundColor Yellow
    }
    if ($remoteChanges.Count -gt 0) {
        Write-Host "  RESULT: $label has $($remoteChanges.Count) remote change(s) not yet synced to the workspace." -ForegroundColor Cyan
    }
    if ($conflicts.Count -gt 0) {
        Write-Host "  RESULT: $($conflicts.Count) CONFLICT(s) detected — manual resolution required." -ForegroundColor Red
    }
}
Write-Host "─────────────────────────────────────────" -ForegroundColor White
Write-Host ""

if ($FailOnPending -and $wsChanges.Count -gt 0) {
    Write-Error "FAIL: $($wsChanges.Count) uncommitted workspace change(s) in $label. Commit to Git before proceeding with BCDR sync."
    exit 1
}
