<#
.SYNOPSIS
    Creates, enables, or disables a scheduled trigger for a Fabric Notebook.

.DESCRIPTION
    Manages scheduled triggers for a notebook across BCDR workspaces using the
    Fabric REST API Job Scheduler endpoints. Supports:

      - Creating a new Cron schedule on a notebook
      - Enabling/disabling an existing schedule
      - Failover mode: enables the schedule on the new primary workspace and
        disables it on the old primary in a single invocation

    Designed for the ExportEventhouseToLakehouse notebook so that only one
    workspace (the active primary) runs the scheduled export at a time.

.PARAMETER Action
    The operation to perform: "create", "enable", "disable", or "failover".

.PARAMETER WorkspaceId
    The workspace ID containing the notebook.

.PARAMETER WorkspaceName
    The workspace display name (used to resolve WorkspaceId if not provided).

.PARAMETER NotebookName
    The display name of the notebook.

.PARAMETER NotebookId
    The item ID of the notebook (used if NotebookName is not provided).

.PARAMETER IntervalMinutes
    For Action=create: the Cron interval in minutes. Default: 5.

.PARAMETER TimeZone
    For Action=create: the local time zone ID. Default: "Eastern Standard Time".

.PARAMETER ScheduleId
    The schedule ID to enable/disable. If omitted, the script auto-discovers
    the first schedule on the notebook.

.PARAMETER EnableWorkspaceName
    For Action=failover: the workspace to ENABLE the schedule on (new primary).

.PARAMETER DisableWorkspaceName
    For Action=failover: the workspace to DISABLE the schedule on (old primary).

.EXAMPLE
    # Create a schedule that runs every 5 minutes on BCDR1
    .\Manage-NotebookSchedule.ps1 -Action create -WorkspaceName "BCDR1" `
        -NotebookName "ExportEventhouseToLakehouse" -IntervalMinutes 5

.EXAMPLE
    # Disable the schedule on BCDR1
    .\Manage-NotebookSchedule.ps1 -Action disable -WorkspaceName "BCDR1" `
        -NotebookName "ExportEventhouseToLakehouse"

.EXAMPLE
    # Failover: enable on BCDR2, disable on BCDR1
    .\Manage-NotebookSchedule.ps1 -Action failover `
        -NotebookName "ExportEventhouseToLakehouse" `
        -EnableWorkspaceName "BCDR2" -DisableWorkspaceName "BCDR1"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet("create", "enable", "disable", "failover")]
    [string]$Action,

    [string]$WorkspaceId,
    [string]$WorkspaceName,
    [string]$NotebookName,
    [string]$NotebookId,

    [ValidateRange(1, 5270400)]
    [int]$IntervalMinutes = 5,

    [string]$TimeZone = "Eastern Standard Time",

    [string]$ScheduleId,

    # Failover-specific parameters
    [string]$EnableWorkspaceName,
    [string]$DisableWorkspaceName
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$FabricApiBase = "https://api.fabric.microsoft.com/v1"
$JobType = "DefaultJob"

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
# Resolve helpers
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

function Resolve-NotebookId {
    param([string]$WsId, [string]$Name)

    $headers = Get-FabricHeaders
    $resp = Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WsId/items?type=Notebook" -Headers $headers -Method Get
    $nb = $resp.value | Where-Object { $_.displayName -eq $Name }

    if (-not $nb) { throw "Notebook '$Name' not found in workspace $WsId." }
    if ($nb -is [array]) { $nb = $nb[0] }

    Write-Host "[Resolve] Notebook '$Name' -> $($nb.id)" -ForegroundColor DarkGray
    return $nb.id
}

# ─────────────────────────────────────────────
# Get existing schedules for a notebook
# ─────────────────────────────────────────────
function Get-NotebookSchedules {
    param([string]$WsId, [string]$NbId)

    $headers = Get-FabricHeaders
    $url = "$FabricApiBase/workspaces/$WsId/items/$NbId/jobs/$JobType/schedules"

    try {
        $resp = Invoke-RestMethod -Uri $url -Headers $headers -Method Get
        return $resp.value
    }
    catch {
        if ($_.Exception.Response.StatusCode -eq 404) {
            return @()
        }
        throw
    }
}

# ─────────────────────────────────────────────
# Create a new schedule
# ─────────────────────────────────────────────
function New-NotebookSchedule {
    param([string]$WsId, [string]$NbId)

    $headers = Get-FabricHeaders
    $url = "$FabricApiBase/workspaces/$WsId/items/$NbId/jobs/$JobType/schedules"

    $startTime = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    # Default end: 1 year from now
    $endTime = (Get-Date).AddYears(1).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

    $body = @{
        enabled       = $true
        configuration = @{
            type            = "Cron"
            startDateTime   = $startTime
            endDateTime     = $endTime
            localTimeZoneId = $TimeZone
            interval        = $IntervalMinutes
        }
    } | ConvertTo-Json -Depth 5

    Write-Host "  Creating schedule: every $IntervalMinutes min, TZ=$TimeZone" -ForegroundColor Cyan

    $resp = Invoke-RestMethod -Uri $url -Headers $headers -Method Post -Body $body
    Write-Host "  Schedule created: $($resp.id)" -ForegroundColor Green
    return $resp
}

# ─────────────────────────────────────────────
# Enable or disable an existing schedule
# ─────────────────────────────────────────────
function Set-NotebookScheduleState {
    param(
        [string]$WsId,
        [string]$NbId,
        [string]$SchedId,
        [bool]$Enabled
    )

    $headers = Get-FabricHeaders

    # First get the current schedule to preserve its configuration
    $getUrl = "$FabricApiBase/workspaces/$WsId/items/$NbId/jobs/$JobType/schedules"
    $schedules = (Invoke-RestMethod -Uri $getUrl -Headers $headers -Method Get).value
    $schedule = $schedules | Where-Object { $_.id -eq $SchedId }

    if (-not $schedule) {
        throw "Schedule '$SchedId' not found on notebook $NbId."
    }

    $url = "$FabricApiBase/workspaces/$WsId/items/$NbId/jobs/$JobType/schedules/$SchedId"

    $body = @{
        enabled       = $Enabled
        configuration = $schedule.configuration
    } | ConvertTo-Json -Depth 5

    $state = if ($Enabled) { "ENABLED" } else { "DISABLED" }
    Write-Host "  Setting schedule $SchedId -> $state" -ForegroundColor $(if ($Enabled) { "Green" } else { "Yellow" })

    Invoke-RestMethod -Uri $url -Headers $headers -Method Patch -Body $body | Out-Null
    Write-Host "  Done." -ForegroundColor Green
}

# ─────────────────────────────────────────────
# Auto-discover schedule ID
# ─────────────────────────────────────────────
function Get-FirstScheduleId {
    param([string]$WsId, [string]$NbId)

    $schedules = Get-NotebookSchedules -WsId $WsId -NbId $NbId
    if ($schedules.Count -eq 0) {
        throw "No schedules found on notebook $NbId. Use -Action create first."
    }
    $id = $schedules[0].id
    $state = if ($schedules[0].enabled) { "enabled" } else { "disabled" }
    Write-Host "  Auto-discovered schedule: $id ($state)" -ForegroundColor DarkGray
    return $id
}

# ─────────────────────────────────────────────
# Perform enable/disable on a single workspace
# ─────────────────────────────────────────────
function Invoke-SingleAction {
    param(
        [string]$WsId,
        [string]$WsName,
        [string]$NbId,
        [string]$NbName,
        [string]$Act
    )

    Write-Host ""
    Write-Host "  Workspace: $WsName ($WsId)" -ForegroundColor White
    Write-Host "  Notebook : $NbName ($NbId)" -ForegroundColor White

    switch ($Act) {
        "create" {
            $existing = Get-NotebookSchedules -WsId $WsId -NbId $NbId
            if ($existing.Count -gt 0) {
                Write-Host "  Schedule already exists: $($existing[0].id) (enabled=$($existing[0].enabled))" -ForegroundColor Yellow
                Write-Host "  Use -Action enable/disable to change state." -ForegroundColor Yellow
                return
            }
            New-NotebookSchedule -WsId $WsId -NbId $NbId
        }
        "enable" {
            $sid = if ($ScheduleId) { $ScheduleId } else { Get-FirstScheduleId -WsId $WsId -NbId $NbId }
            Set-NotebookScheduleState -WsId $WsId -NbId $NbId -SchedId $sid -Enabled $true
        }
        "disable" {
            $sid = if ($ScheduleId) { $ScheduleId } else { Get-FirstScheduleId -WsId $WsId -NbId $NbId }
            Set-NotebookScheduleState -WsId $WsId -NbId $NbId -SchedId $sid -Enabled $false
        }
    }
}

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

Write-Host ""
Write-Host "================================================================" -ForegroundColor White
Write-Host "  Manage Notebook Schedule — Action: $Action" -ForegroundColor White
Write-Host "================================================================" -ForegroundColor White

if ($Action -eq "failover") {
    # Failover mode: enable on one workspace, disable on the other
    if (-not $EnableWorkspaceName -or -not $DisableWorkspaceName) {
        throw "Failover requires -EnableWorkspaceName and -DisableWorkspaceName."
    }
    if (-not $NotebookName) {
        throw "Failover requires -NotebookName."
    }

    Write-Host ""
    Write-Host "  Failover: $DisableWorkspaceName (disable) -> $EnableWorkspaceName (enable)" -ForegroundColor Cyan
    Write-Host ""

    # Resolve both workspaces and notebooks
    $enableWsId  = Resolve-WorkspaceId -Name $EnableWorkspaceName
    $disableWsId = Resolve-WorkspaceId -Name $DisableWorkspaceName

    $enableNbId  = Resolve-NotebookId -WsId $enableWsId -Name $NotebookName
    $disableNbId = Resolve-NotebookId -WsId $disableWsId -Name $NotebookName

    # Disable old primary first (safety — avoid two running at once)
    Write-Host "`n── Disabling on $DisableWorkspaceName ──" -ForegroundColor Yellow
    Invoke-SingleAction -WsId $disableWsId -WsName $DisableWorkspaceName `
        -NbId $disableNbId -NbName $NotebookName -Act "disable"

    # Enable new primary
    Write-Host "`n── Enabling on $EnableWorkspaceName ──" -ForegroundColor Green
    Invoke-SingleAction -WsId $enableWsId -WsName $EnableWorkspaceName `
        -NbId $enableNbId -NbName $NotebookName -Act "enable"

    Write-Host "`nFailover complete." -ForegroundColor Green
}
else {
    # Single-workspace action
    if (-not $WorkspaceId -and $WorkspaceName) {
        $WorkspaceId = Resolve-WorkspaceId -Name $WorkspaceName
    }
    if (-not $WorkspaceId) { throw "Provide -WorkspaceId or -WorkspaceName." }

    if (-not $NotebookId -and $NotebookName) {
        $NotebookId = Resolve-NotebookId -WsId $WorkspaceId -Name $NotebookName
    }
    if (-not $NotebookId) { throw "Provide -NotebookId or -NotebookName." }

    $wsLabel = if ($WorkspaceName) { $WorkspaceName } else { $WorkspaceId }
    $nbLabel = if ($NotebookName) { $NotebookName } else { $NotebookId }

    Invoke-SingleAction -WsId $WorkspaceId -WsName $wsLabel `
        -NbId $NotebookId -NbName $nbLabel -Act $Action
}

Write-Host ""
