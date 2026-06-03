param(
    [switch]$RebuildOffline,
    [int]$ApiPort = 8000
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$productionRoot = Resolve-Path (Join-Path $scriptDir "..")
$repoRoot = Resolve-Path (Join-Path $productionRoot "..")
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$pythonExe = if (Test-Path $venvPython) { $venvPython } else { "python" }

function Wait-Http {
    param(
        [string]$Url,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 5
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
                return
            }
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    throw "Timed out waiting for $Url"
}

function Stop-ExistingApi {
    Get-CimInstance Win32_Process -Filter "name = 'python.exe'" |
        Where-Object {
            $_.CommandLine -like "*uvicorn*apps.api.main:app*" -and
            $_.CommandLine -like "*--port $ApiPort*"
        } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force }

    netstat -ano |
        Select-String ":$ApiPort\s+.*LISTENING\s+(\d+)" |
        ForEach-Object {
            $listenerPid = [int]$_.Matches[0].Groups[1].Value
            if ($listenerPid -gt 0) {
                Stop-Process -Id $listenerPid -Force -ErrorAction SilentlyContinue
            }
        }
    Start-Sleep -Seconds 2
}

Push-Location $productionRoot
try {
    Write-Host "Starting Docker infrastructure..."
    docker compose up -d

    Write-Host "Waiting for service endpoints..."
    Wait-Http "http://127.0.0.1:7474" 120
    Wait-Http "http://127.0.0.1:9000/minio/health/live" 120
    Wait-Http "http://127.0.0.1:8080/health" 120

    Write-Host "Ensuring Airflow schedules are enabled..."
    docker compose exec -T airflow airflow dags unpause monthly_restaurant_ingestion | Out-Null
    docker compose exec -T airflow airflow dags unpause weekly_rgcn_training | Out-Null

    if ($RebuildOffline) {
        Write-Host "Rebuilding offline data artifacts..."
        & $pythonExe scripts/run_offline_ingest.py --config configs/offline.yaml
        & $pythonExe scripts/build_kg.py --mode offline
        & $pythonExe scripts/build_indexes.py --mode offline
        & $pythonExe scripts/export_rgcn_snapshot.py --mode offline
    }

    Write-Host "Starting API on port $ApiPort..."
    Stop-ExistingApi
    Start-Process -FilePath $pythonExe `
        -ArgumentList @("-m", "uvicorn", "apps.api.main:app", "--host", "127.0.0.1", "--port", "$ApiPort") `
        -WorkingDirectory $productionRoot `
        -WindowStyle Hidden

    Wait-Http "http://127.0.0.1:$ApiPort/health" 90

    Write-Host ""
    Write-Host "System is running."
    Write-Host "User UI:       http://127.0.0.1:$ApiPort/"
    Write-Host "Monitoring UI: http://127.0.0.1:$ApiPort/monitoring"
    Write-Host "Airflow:       http://127.0.0.1:8080"
    Write-Host "MinIO:         http://127.0.0.1:9001"
    Write-Host "Neo4j:         http://127.0.0.1:7474"
} finally {
    Pop-Location
}
