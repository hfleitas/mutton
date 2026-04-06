<#
.SYNOPSIS
    Switches a Fabric Notebook's default Lakehouse connection from one workspace to another.

.DESCRIPTION
    Downloads the notebook item definition via the Fabric REST API, replaces the
    source workspace/lakehouse references with the target, and pushes the updated
    definition back. Useful for BCDR failover where a notebook in BCDR2 needs to
    point at the BCDR2 lakehouse instead of the BCDR1 lakehouse (or vice versa).

    Steps:
      1. Resolve workspace and item IDs from display names (if not provided).
      2. GET the notebook item definition (base64-encoded parts).
      3. Decode each part, replace source workspace/lakehouse IDs with target.
      4. POST the updated definition back via Update Item Definition.

.PARAMETER WorkspaceId
    The workspace ID where the notebook lives.

.PARAMETER WorkspaceName
    The workspace display name (used to resolve WorkspaceId if not provided).

.PARAMETER NotebookName
    The display name of the notebook to update.

.PARAMETER NotebookId
    The item ID of the notebook (used if NotebookName is not provided).

.PARAMETER SourceLakehouseId
    The lakehouse ID currently referenced by the notebook (old connection).

.PARAMETER TargetLakehouseId
    The lakehouse ID to switch to (new connection).

.PARAMETER SourceWorkspaceId
    The workspace ID currently referenced in the notebook definition (old).

.PARAMETER TargetWorkspaceId
    The workspace ID to replace with in the notebook definition (new).

.PARAMETER DryRun
    If set, prints the transformed definition parts without pushing the update.

.EXAMPLE
    # Switch LH1SentimentAnalysis from BCDR1 LH1 to BCDR2 LH1
    .\Switch-NotebookConnection.ps1 `
        -WorkspaceName "BCDR2" `
        -NotebookName "LH1SentimentAnalysis" `
        -SourceWorkspaceId "aaaa-bbbb-cccc" `
        -TargetWorkspaceId "dddd-eeee-ffff" `
        -SourceLakehouseId "1111-2222-3333" `
        -TargetLakehouseId "4444-5555-6666"
#>

[CmdletBinding()]
param(
    [string]$WorkspaceId,
    [string]$WorkspaceName,
    [string]$NotebookName,
    [string]$NotebookId,

    [Parameter(Mandatory)]
    [string]$SourceLakehouseId,

    [Parameter(Mandatory)]
    [string]$TargetLakehouseId,

    [string]$SourceWorkspaceId,
    [string]$TargetWorkspaceId,

    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$FabricApiBase = "https://api.fabric.microsoft.com/v1"

# ─────────────────────────────────────────────
# Auth: acquire a Fabric bearer token
# ─────────────────────────────────────────────
function Get-FabricToken {
    # Try Az PowerShell module first, fall back to az CLI
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
# Resolve notebook ID from display name
# ─────────────────────────────────────────────
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
# Poll a long-running operation until complete
# ─────────────────────────────────────────────
function Wait-FabricOperation {
    param([string]$OperationUrl)

    $headers = Get-FabricHeaders
    $maxWait = 120  # seconds
    $elapsed = 0
    $interval = 5

    while ($elapsed -lt $maxWait) {
        $status = Invoke-RestMethod -Uri $OperationUrl -Headers $headers -Method Get
        if ($status.status -eq "Succeeded") {
            return $status
        }
        if ($status.status -eq "Failed") {
            throw "Operation failed: $($status | ConvertTo-Json -Depth 5)"
        }
        Write-Host "  Waiting for operation... ($($status.status))" -ForegroundColor DarkGray
        Start-Sleep -Seconds $interval
        $elapsed += $interval
    }
    throw "Operation timed out after $maxWait seconds."
}

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

# Resolve IDs
if (-not $WorkspaceId -and $WorkspaceName) {
    $WorkspaceId = Resolve-WorkspaceId -Name $WorkspaceName
}
if (-not $WorkspaceId) { throw "Provide -WorkspaceId or -WorkspaceName." }

if (-not $NotebookId -and $NotebookName) {
    $NotebookId = Resolve-NotebookId -WsId $WorkspaceId -Name $NotebookName
}
if (-not $NotebookId) { throw "Provide -NotebookId or -NotebookName." }

Write-Host ""
Write-Host "================================================================" -ForegroundColor White
Write-Host "  Switch Notebook Connection" -ForegroundColor White
Write-Host "  Workspace : $WorkspaceId" -ForegroundColor Gray
Write-Host "  Notebook  : $NotebookId" -ForegroundColor Gray
Write-Host "  Lakehouse : $SourceLakehouseId -> $TargetLakehouseId" -ForegroundColor Gray
if ($SourceWorkspaceId -and $TargetWorkspaceId) {
    Write-Host "  WS Refs   : $SourceWorkspaceId -> $TargetWorkspaceId" -ForegroundColor Gray
}
Write-Host "================================================================" -ForegroundColor White
Write-Host ""

# Step 1: Get notebook definition
$headers = Get-FabricHeaders
$defUrl = "$FabricApiBase/workspaces/$WorkspaceId/items/$NotebookId/getDefinition"

Write-Host "[1/3] Downloading notebook definition..." -ForegroundColor Cyan
$defResponse = Invoke-WebRequest -Uri $defUrl -Headers $headers -Method Post -UseBasicParsing

if ($defResponse.StatusCode -eq 202) {
    # Long-running — poll for result
    $opUrl = $defResponse.Headers["Location"]
    if (-not $opUrl) {
        $opId = $defResponse.Headers["x-ms-operation-id"]
        $opUrl = "$FabricApiBase/operations/$opId"
    }
    $opResult = Wait-FabricOperation -OperationUrl $opUrl

    # Fetch the definition from the result URL
    $resultUrl = "$FabricApiBase/operations/$($opResult.id)/result" -replace "//result", "/result"
    $defResponse = Invoke-WebRequest -Uri "$defUrl" -Headers (Get-FabricHeaders) -Method Post -UseBasicParsing
}

$definition = ($defResponse.Content | ConvertFrom-Json).definition

if (-not $definition -or -not $definition.parts) {
    throw "Failed to retrieve notebook definition or definition has no parts."
}

Write-Host "  Found $($definition.parts.Count) definition part(s)." -ForegroundColor DarkGray

# Step 2: Transform — replace lakehouse/workspace references in each part
Write-Host "[2/3] Replacing connection references..." -ForegroundColor Cyan

$updatedParts = @()
$changesMade = 0

foreach ($part in $definition.parts) {
    $decoded = [System.Text.Encoding]::UTF8.GetString(
        [System.Convert]::FromBase64String($part.payload)
    )

    $original = $decoded

    # Replace lakehouse ID
    $decoded = $decoded -replace [regex]::Escape($SourceLakehouseId), $TargetLakehouseId

    # Replace workspace ID references if provided
    if ($SourceWorkspaceId -and $TargetWorkspaceId) {
        $decoded = $decoded -replace [regex]::Escape($SourceWorkspaceId), $TargetWorkspaceId
    }

    if ($decoded -ne $original) {
        $changesMade++
        Write-Host "  [Changed] $($part.path)" -ForegroundColor Yellow
    }
    else {
        Write-Host "  [No change] $($part.path)" -ForegroundColor DarkGray
    }

    $encoded = [System.Convert]::ToBase64String(
        [System.Text.Encoding]::UTF8.GetBytes($decoded)
    )

    $updatedParts += @{
        path        = $part.path
        payload     = $encoded
        payloadType = "InlineBase64"
    }
}

if ($changesMade -eq 0) {
    Write-Host "`nNo references to the source lakehouse were found in the definition." -ForegroundColor Yellow
    Write-Host "Verify that the SourceLakehouseId is correct." -ForegroundColor Yellow
    exit 0
}

Write-Host "  $changesMade part(s) updated." -ForegroundColor Green

if ($DryRun) {
    Write-Host "`n[DRY RUN] Would update the following parts:" -ForegroundColor Magenta
    foreach ($p in $updatedParts) {
        $content = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($p.payload))
        Write-Host "--- $($p.path) ---" -ForegroundColor White
        Write-Host $content
    }
    exit 0
}

# Step 3: Push updated definition
Write-Host "[3/3] Pushing updated definition..." -ForegroundColor Cyan

$body = @{
    definition = @{
        parts = $updatedParts
    }
} | ConvertTo-Json -Depth 10

$updateUrl = "$FabricApiBase/workspaces/$WorkspaceId/items/$NotebookId/updateDefinition"
$updateHeaders = Get-FabricHeaders

$updateResp = Invoke-WebRequest -Uri $updateUrl -Headers $updateHeaders -Method Post -Body $body -UseBasicParsing

if ($updateResp.StatusCode -eq 200) {
    Write-Host "  Definition updated successfully." -ForegroundColor Green
}
elseif ($updateResp.StatusCode -eq 202) {
    $opUrl = $updateResp.Headers["Location"]
    if ($opUrl) {
        Wait-FabricOperation -OperationUrl $opUrl | Out-Null
    }
    Write-Host "  Definition updated successfully (async)." -ForegroundColor Green
}
else {
    Write-Host "  Unexpected status: $($updateResp.StatusCode)" -ForegroundColor Red
    exit 1
}

Write-Host "`nNotebook connection switched successfully." -ForegroundColor Green
