param(
  [ValidateSet('share','classic')][string]$Mode = 'share',
  [int]$SlowMs = 20000,
  [int]$FastMs = 100,
  [int]$RemainingMessages = 19,
  [bool]$RenewLocks = $false
)
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$env:SLOW_MS = "$SlowMs"
$env:FAST_MS = "$FastMs"
$env:RENEW_LOCKS = "$RenewLocks".ToLowerInvariant()
$env:POC_MODE = $Mode
# WARNING: removes Kafka PoC data volumes/containers in this Compose project.
docker compose down --volumes --remove-orphans
if ($LASTEXITCODE -ne 0) { throw 'Docker compose down failed' }
docker compose up -d --wait kafka
if ($LASTEXITCODE -ne 0) { throw 'Kafka failed to start' }
docker compose --profile $Mode up -d --build "$Mode-slow"
if ($LASTEXITCODE -ne 0) { throw 'Slow worker failed to start' }
Write-Host 'Giving the slow worker time to join the group...'
Start-Sleep -Seconds 8
# Seed a SINGLE record to make the slow worker occupy partition 0.
docker compose run --rm --build -e POC_START=0 -e POC_COUNT=1 publisher
if ($LASTEXITCODE -ne 0) { throw 'First publish failed' }
Start-Sleep -Seconds 2
# The fast worker joins while the slow one is still busy (for SlowMs >= ~20000).
docker compose --profile $Mode up -d --build "$Mode-fast"
if ($LASTEXITCODE -ne 0) { throw 'Fast worker failed to start' }
Start-Sleep -Seconds 5
docker compose run --rm -e POC_START=1 -e POC_COUNT=$RemainingMessages publisher
if ($LASTEXITCODE -ne 0) { throw 'Bulk publish failed' }
Write-Host 'Producer done. Watch live:'
Write-Host "docker compose logs -f $Mode-slow $Mode-fast"
Write-Host "When processing finishes, capture: docker compose logs --no-color $Mode-slow $Mode-fast > $Mode.log"
Write-Host "Then analyze: python scripts/analyze.py $Mode.log"
