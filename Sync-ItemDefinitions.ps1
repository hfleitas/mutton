<#
.SYNOPSIS
    Syncs Fabric item definitions from one workspace to another via REST API (no Git required).

.DESCRIPTION
    Enumerates all items in a source workspace, downloads their definitions via
    "Get Item Definition", and either creates or updates them in the destination
    workspace using "Create Item" or "Update Item Definition".

    This provides a Git-free mechanism to keep item definitions (notebooks, pipelines,
    etc.) in sync between BCDR1 and BCDR2.

    Workflow:
      1. List all items in source workspace.
      2. Filter to types that support definitions (or use -ItemTypes to limit).
      3. For each item, GET the definition (base64 parts).
      4. Check if the item exists in the destination (by displayName + type).
         - If it exists: call Update Item Definition.
         - If it doesn't exist: call Create Item with the definition.
      5. Log results.

    NOT synced (by design):
      - SQLEndpoint, SemanticModel, and other auto-generated items are skipped by
        default since they are created automatically by their parent items.
      - Lakehouse/Warehouse items are created as empty shells (definition not
        supported for creation) — use AzCopy to sync their data.

.PARAMETER SourceWorkspaceId
    The source workspace ID.

.PARAMETER SourceWorkspaceName
    The source workspace display name (resolved to ID if SourceWorkspaceId is omitted).

.PARAMETER DestWorkspaceId
    The destination workspace ID.

.PARAMETER DestWorkspaceName
    The destination workspace display name (resolved to ID if DestWorkspaceId is omitted).

.PARAMETER ItemTypes
    Array of item types to sync. Defaults to types known to support definitions.
    Example: @("Notebook", "DataPipeline", "SparkJobDefinition")

.PARAMETER ExcludeItems
    Array of item display names to skip.

.PARAMETER DryRun
    If set, lists what would be created/updated without making changes.

.EXAMPLE
    # Sync all supported items from BCDR1 to BCDR2
    .\Sync-ItemDefinitions.ps1 -SourceWorkspaceName "BCDR1" -DestWorkspaceName "BCDR2"

.EXAMPLE
    # Sync only notebooks
    .\Sync-ItemDefinitions.ps1 -SourceWorkspaceName "BCDR1" -DestWorkspaceName "BCDR2" `
        -ItemTypes @("Notebook")

.EXAMPLE
    # Dry run
    .\Sync-ItemDefinitions.ps1 -SourceWorkspaceName "BCDR1" -DestWorkspaceName "BCDR2" -DryRun
#>

[CmdletBinding()]
param(
    [string]$SourceWorkspaceId,
    [string]$SourceWorkspaceName,
    [string]$DestWorkspaceId,
    [string]$DestWorkspaceName,

    [string[]]$ItemTypes = @(
        "Notebook",
        "DataPipeline",
        "SparkJobDefinition",
        "Environment",
        "KQLQueryset",
        "Eventstream",
        "Report",
        "Lakehouse",
        "Warehouse",
        "Eventhouse",
        "KQLDatabase",
        "KQLDashboard",
        "MLExperiment",
        "MLModel",
        "UserDataFunction",
        "CopyJob"
    ),

    [string[]]$ExcludeItems = @(),

    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$FabricApiBase = "https://api.fabric.microsoft.com/v1"

# Item types where getDefinition is NOT supported — create as empty shell only
$NoDefinitionTypes = @("Lakehouse", "Warehouse", "Eventhouse", "KQLDatabase", "MLExperiment", "MLModel")

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
# List all items in a workspace (handles pagination)
# ─────────────────────────────────────────────
function Get-AllWorkspaceItems {
    param([string]$WsId)

    $headers = Get-FabricHeaders
    $allItems = @()
    $url = "$FabricApiBase/workspaces/$WsId/items"

    do {
        $resp = Invoke-RestMethod -Uri $url -Headers $headers -Method Get
        $allItems += $resp.value

        $url = $resp.continuationUri
    } while ($url)

    return $allItems
}

# ─────────────────────────────────────────────
# Poll a long-running operation until complete
# ─────────────────────────────────────────────
function Wait-FabricOperation {
    param(
        [string]$OperationUri,
        [string]$Label = "operation"
    )

    $headers = Get-FabricHeaders
    $maxWait = 180
    $elapsed = 0
    $interval = 5

    while ($elapsed -lt $maxWait) {
        $status = Invoke-RestMethod -Uri $OperationUri -Headers $headers -Method Get
        if ($status.status -eq "Succeeded") {
            return $status
        }
        if ($status.status -eq "Failed") {
            throw "$Label failed: $($status | ConvertTo-Json -Depth 5)"
        }
        Write-Host "    Waiting for $Label... ($($status.status))" -ForegroundColor DarkGray
        Start-Sleep -Seconds $interval
        $elapsed += $interval
    }
    throw "$Label timed out after $maxWait seconds."
}

# ─────────────────────────────────────────────
# Get item definition (handles LRO)
# ─────────────────────────────────────────────
function Get-ItemDefinition {
    param([string]$WsId, [string]$ItemId, [string]$ItemName)

    $headers = Get-FabricHeaders
    $url = "$FabricApiBase/workspaces/$WsId/items/$ItemId/getDefinition"

    try {
        $resp = Invoke-WebRequest -Uri $url -Headers $headers -Method Post -UseBasicParsing

        if ($resp.StatusCode -eq 202) {
            $opId = $resp.Headers["x-ms-operation-id"]
            if (-not $opId) {
                $loc = $resp.Headers["Location"]
                $opId = ($loc -split "/operations/" | Select-Object -Last 1).Trim("/")
            }

            $opResult = Wait-FabricOperation -OperationUri "$FabricApiBase/operations/$opId" -Label "getDefinition($ItemName)"

            # Fetch the definition result
            $resultUrl = "$FabricApiBase/operations/$opId/result"
            $resultResp = Invoke-RestMethod -Uri $resultUrl -Headers (Get-FabricHeaders) -Method Get
            return $resultResp.definition
        }

        $content = $resp.Content | ConvertFrom-Json
        return $content.definition
    }
    catch {
        $statusCode = $null
        if ($_.Exception.Response) {
            $statusCode = [int]$_.Exception.Response.StatusCode
        }
        # OperationNotSupportedForItem or similar
        Write-Host "    [Skip] Cannot get definition for '$ItemName': $($_.Exception.Message)" -ForegroundColor DarkGray
        return $null
    }
}

# ─────────────────────────────────────────────
# Create item in destination workspace
# ─────────────────────────────────────────────
function New-FabricItem {
    param(
        [string]$WsId,
        [string]$DisplayName,
        [string]$ItemType,
        [string]$Description,
        $Definition
    )

    $headers = Get-FabricHeaders
    $url = "$FabricApiBase/workspaces/$WsId/items"

    $body = @{
        displayName = $DisplayName
        type        = $ItemType
    }

    if ($Description) {
        $body["description"] = $Description
    }

    if ($Definition) {
        $body["definition"] = $Definition
    }

    $json = $body | ConvertTo-Json -Depth 10

    try {
        $resp = Invoke-WebRequest -Uri $url -Headers $headers -Method Post -Body $json -UseBasicParsing

        if ($resp.StatusCode -eq 202) {
            $opId = $resp.Headers["x-ms-operation-id"]
            if ($opId) {
                Wait-FabricOperation -OperationUri "$FabricApiBase/operations/$opId" -Label "createItem($DisplayName)" | Out-Null
            }
        }
        return $true
    }
    catch {
        Write-Host "    [Error] Create '$DisplayName' ($ItemType): $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# ─────────────────────────────────────────────
# Update item definition in destination workspace
# ─────────────────────────────────────────────
function Update-FabricItemDefinition {
    param(
        [string]$WsId,
        [string]$ItemId,
        [string]$DisplayName,
        $Definition
    )

    $headers = Get-FabricHeaders
    $url = "$FabricApiBase/workspaces/$WsId/items/$ItemId/updateDefinition"

    $body = @{
        definition = $Definition
    } | ConvertTo-Json -Depth 10

    try {
        $resp = Invoke-WebRequest -Uri $url -Headers $headers -Method Post -Body $body -UseBasicParsing

        if ($resp.StatusCode -eq 202) {
            $opId = $resp.Headers["x-ms-operation-id"]
            if ($opId) {
                Wait-FabricOperation -OperationUri "$FabricApiBase/operations/$opId" -Label "updateDefinition($DisplayName)" | Out-Null
            }
        }
        return $true
    }
    catch {
        Write-Host "    [Error] Update '$DisplayName': $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

# Resolve workspace IDs
if (-not $SourceWorkspaceId -and $SourceWorkspaceName) {
    $SourceWorkspaceId = Resolve-WorkspaceId -Name $SourceWorkspaceName
}
if (-not $SourceWorkspaceId) { throw "Provide -SourceWorkspaceId or -SourceWorkspaceName." }

if (-not $DestWorkspaceId -and $DestWorkspaceName) {
    $DestWorkspaceId = Resolve-WorkspaceId -Name $DestWorkspaceName
}
if (-not $DestWorkspaceId) { throw "Provide -DestWorkspaceId or -DestWorkspaceName." }

$srcLabel = if ($SourceWorkspaceName) { $SourceWorkspaceName } else { $SourceWorkspaceId }
$dstLabel = if ($DestWorkspaceName) { $DestWorkspaceName } else { $DestWorkspaceId }

Write-Host ""
Write-Host "================================================================" -ForegroundColor White
Write-Host "  Sync Item Definitions via REST API" -ForegroundColor White
Write-Host "  Source : $srcLabel ($SourceWorkspaceId)" -ForegroundColor Gray
Write-Host "  Dest   : $dstLabel ($DestWorkspaceId)" -ForegroundColor Gray
Write-Host "  Types  : $($ItemTypes -join ', ')" -ForegroundColor Gray
Write-Host "  DryRun : $DryRun" -ForegroundColor Gray
Write-Host "================================================================" -ForegroundColor White
Write-Host ""

# Step 1: List items in source
Write-Host "[1/4] Listing items in source workspace ($srcLabel)..." -ForegroundColor Cyan
$sourceItems = Get-AllWorkspaceItems -WsId $SourceWorkspaceId

# Filter to requested types
$filteredItems = $sourceItems | Where-Object { $_.type -in $ItemTypes }

# Exclude specific items
if ($ExcludeItems.Count -gt 0) {
    $filteredItems = $filteredItems | Where-Object { $_.displayName -notin $ExcludeItems }
}

Write-Host "  Found $($sourceItems.Count) total items, $($filteredItems.Count) match type filter." -ForegroundColor DarkGray
Write-Host ""

if ($filteredItems.Count -eq 0) {
    Write-Host "No items to sync." -ForegroundColor Yellow
    exit 0
}

# Step 2: List items in destination (for matching)
Write-Host "[2/4] Listing items in destination workspace ($dstLabel)..." -ForegroundColor Cyan
$destItems = Get-AllWorkspaceItems -WsId $DestWorkspaceId
Write-Host "  Found $($destItems.Count) existing items." -ForegroundColor DarkGray
Write-Host ""

# Build a lookup: key = "type::displayName" -> item
$destLookup = @{}
foreach ($di in $destItems) {
    $key = "$($di.type)::$($di.displayName)"
    $destLookup[$key] = $di
}

# Step 3: Sync each item
Write-Host "[3/4] Syncing item definitions..." -ForegroundColor Cyan
Write-Host ""

$stats = @{ created = 0; updated = 0; skipped = 0; failed = 0 }
$results = @()

foreach ($item in $filteredItems) {
    $name = $item.displayName
    $type = $item.type
    $lookupKey = "$type::$name"
    $existsInDest = $destLookup.ContainsKey($lookupKey)

    Write-Host "  [$type] $name" -ForegroundColor White -NoNewline

    # Determine if this type supports definitions
    $supportsDefinition = $type -notin $NoDefinitionTypes

    if ($DryRun) {
        if ($existsInDest) {
            if ($supportsDefinition) {
                Write-Host " -> would UPDATE definition" -ForegroundColor Yellow
            }
            else {
                Write-Host " -> already exists (no definition to update)" -ForegroundColor DarkGray
            }
        }
        else {
            Write-Host " -> would CREATE" -ForegroundColor Green
        }
        continue
    }

    # Get definition from source (if supported)
    $definition = $null
    if ($supportsDefinition) {
        Write-Host " [fetching definition...]" -ForegroundColor DarkGray -NoNewline
        $definition = Get-ItemDefinition -WsId $SourceWorkspaceId -ItemId $item.id -ItemName $name
    }

    if ($existsInDest) {
        $destItem = $destLookup[$lookupKey]

        if ($definition) {
            # Update existing item's definition
            $success = Update-FabricItemDefinition -WsId $DestWorkspaceId -ItemId $destItem.id `
                -DisplayName $name -Definition $definition

            if ($success) {
                Write-Host " -> UPDATED" -ForegroundColor Yellow
                $stats.updated++
            }
            else {
                Write-Host " -> FAILED to update" -ForegroundColor Red
                $stats.failed++
            }
        }
        else {
            Write-Host " -> EXISTS (no definition to sync)" -ForegroundColor DarkGray
            $stats.skipped++
        }
    }
    else {
        # Create new item
        $defPayload = $null
        if ($definition) {
            $defPayload = @{
                parts = @($definition.parts | ForEach-Object {
                    @{
                        path        = $_.path
                        payload     = $_.payload
                        payloadType = $_.payloadType
                    }
                })
            }
        }

        $success = New-FabricItem -WsId $DestWorkspaceId -DisplayName $name `
            -ItemType $type -Description $item.description -Definition $defPayload

        if ($success) {
            Write-Host " -> CREATED" -ForegroundColor Green
            $stats.created++
        }
        else {
            Write-Host " -> FAILED to create" -ForegroundColor Red
            $stats.failed++
        }
    }

    $results += [PSCustomObject]@{
        Name   = $name
        Type   = $type
        Action = if ($existsInDest) { "Update" } else { "Create" }
    }
}

# Step 4: Summary
Write-Host ""
Write-Host "[4/4] Summary" -ForegroundColor Cyan
Write-Host "─────────────────────────────────────────" -ForegroundColor White

if ($DryRun) {
    Write-Host "  DRY RUN — no changes were made." -ForegroundColor Magenta
}
else {
    Write-Host "  Created : $($stats.created)" -ForegroundColor Green
    Write-Host "  Updated : $($stats.updated)" -ForegroundColor Yellow
    Write-Host "  Skipped : $($stats.skipped)" -ForegroundColor DarkGray
    Write-Host "  Failed  : $($stats.failed)" -ForegroundColor $(if ($stats.failed -gt 0) { "Red" } else { "Green" })
}

Write-Host "─────────────────────────────────────────" -ForegroundColor White
Write-Host ""

if ($stats.failed -gt 0) {
    exit 1
}
