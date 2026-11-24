<#
.SYNOPSIS
    Run test-fragment-1 (api + worker) locally, standalone, without Docker.

.DESCRIPTION
    Spins up the FastAPI `api` (uvicorn) and the `worker` process directly from
    source in an isolated Python venv. No Docker, no orchestrator, no DT instance.

    Defaults to a local SQLite database so there is zero external dependency:
    the api creates counter.db on first run via models.Base.metadata.create_all.
    Kafka is disabled by default (the worker just drives the api over HTTP).

    The pinned versions in requirements.txt predate Python 3.14, so this script
    installs a modern, 3.14-compatible set (fastapi / uvicorn / sqlalchemy / etc.)
    into .venv. The api/.env and any tracked file are never modified — the DB and
    worker settings are supplied purely via environment variables at launch.

.PARAMETER ApiPort
    Host port for the api. Default 8000.

.PARAMETER WorkerInterval
    Seconds between worker increments. Default 3.

.PARAMETER DatabaseUrl
    SQLAlchemy URL for the api. Default "sqlite:///./counter.db".
    For real Postgres parity, e.g.:
      -DatabaseUrl "postgresql://fragment-1-user:<password>@localhost:5433/fragment-1"
    (requires psycopg2 in the venv and that DB/role to exist).

.PARAMETER KafkaBootstrap
    Comma-separated Kafka bootstrap servers for the worker. Empty = Kafka disabled.
    e.g. -KafkaBootstrap "localhost:9092"

.PARAMETER NoWorker
    Start only the api (skip the worker).

.PARAMETER Recreate
    Delete and rebuild the .venv before starting.

.PARAMETER Stop
    Stop any api/worker started by a previous run of this script, then exit.

.EXAMPLE
    .\run-local.ps1
.EXAMPLE
    .\run-local.ps1 -ApiPort 8001 -WorkerInterval 1
.EXAMPLE
    .\run-local.ps1 -Stop
#>
[CmdletBinding()]
param(
    [int]$ApiPort = 8000,
    [int]$WorkerInterval = 3,
    [string]$DatabaseUrl = "sqlite:///./counter.db",
    [string]$KafkaBootstrap = "",
    [switch]$NoWorker,
    [switch]$Recreate,
    [switch]$Stop
)

$ErrorActionPreference = "Stop"
$root     = $PSScriptRoot
$venv     = Join-Path $root ".venv"
$py       = Join-Path $venv "Scripts\python.exe"
$runDir   = Join-Path $root ".run"
$pidFile  = Join-Path $runDir "pids.txt"
$apiLog   = Join-Path $runDir "api.log"
$workerLog= Join-Path $runDir "worker.log"

function Stop-Fragment {
    if (Test-Path $pidFile) {
        foreach ($line in Get-Content $pidFile) {
            $procId = ($line -split '=')[-1]
            if ($procId -match '^\d+$') {
                Get-Process -Id $procId -ErrorAction SilentlyContinue |
                    Stop-Process -Force -ErrorAction SilentlyContinue
            }
        }
        Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
    }
}

if ($Stop) {
    Write-Host "==> Stopping test-fragment-1 (api + worker)..." -ForegroundColor Yellow
    Stop-Fragment
    Write-Host "[+] Stopped." -ForegroundColor Green
    return
}

# --- venv -----------------------------------------------------------------
if ($Recreate -and (Test-Path $venv)) {
    Write-Host "==> Removing existing .venv..." -ForegroundColor Yellow
    Remove-Item $venv -Recurse -Force
}

if (-not (Test-Path $py)) {
    Write-Host "==> Creating Python venv..." -ForegroundColor Cyan
    $launcher = if (Get-Command py -ErrorAction SilentlyContinue) { "py" } else { "python" }
    if ($launcher -eq "py") { & py -3 -m venv $venv } else { & python -m venv $venv }

    Write-Host "==> Installing dependencies (3.14-compatible set)..." -ForegroundColor Cyan
    & $py -m pip install --upgrade pip -q
    # api needs: fastapi uvicorn[standard] sqlalchemy python-dotenv
    # worker needs: requests python-dotenv (kafka-python only if you enable Kafka)
    & $py -m pip install -q fastapi "uvicorn[standard]" sqlalchemy python-dotenv requests
    # Caller authentication library.
    & $py -m pip install -q "dt-fragment-auth @ git+https://github.com/DatiGroup/dt-fragment-auth.git@v0.3.2"
    if ($KafkaBootstrap) { & $py -m pip install -q kafka-python }
} else {
    Write-Host "==> Reusing existing .venv (use -Recreate to rebuild)." -ForegroundColor DarkGray
    if ($KafkaBootstrap) { & $py -m pip install -q kafka-python }
}

# --- clean slate ----------------------------------------------------------
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
Stop-Fragment                       # kill anything from a previous run
if (Test-Path $pidFile) { Remove-Item $pidFile -Force }

# --- start api ------------------------------------------------------------
Write-Host "==> Starting api on http://127.0.0.1:$ApiPort  (DB: $DatabaseUrl)" -ForegroundColor Cyan
$env:DATABASE_URL      = $DatabaseUrl
$env:COUNTER_WS_INTERVAL = "2"
# No platform (Keycloak, DT API) locally: run without the caller checks.
$env:FRAGMENT_AUTH_MODE = "off"
$apiProc = Start-Process -FilePath $py `
    -ArgumentList @("-m","uvicorn","main:app","--host","127.0.0.1","--port","$ApiPort") `
    -WorkingDirectory (Join-Path $root "api") `
    -RedirectStandardOutput $apiLog -RedirectStandardError "$apiLog.err" `
    -NoNewWindow -PassThru
"api=$($apiProc.Id)" | Out-File $pidFile -Encoding ascii

# wait for health
$healthy = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Milliseconds 500
    try {
        $r = Invoke-RestMethod "http://127.0.0.1:$ApiPort/healthcheck" -TimeoutSec 2
        if ($r.status -eq "ok") { $healthy = $true; break }
    } catch { }
    if ($apiProc.HasExited) { break }
}
if (-not $healthy) {
    Write-Host "[X] api failed to become healthy. Last log lines:" -ForegroundColor Red
    if (Test-Path "$apiLog.err") { Get-Content "$apiLog.err" -Tail 20 }
    if (Test-Path $apiLog)       { Get-Content $apiLog -Tail 20 }
    Stop-Fragment
    return
}
Write-Host "[+] api healthy." -ForegroundColor Green

# --- start worker ---------------------------------------------------------
if (-not $NoWorker) {
    Write-Host "==> Starting worker (interval=${WorkerInterval}s, Kafka: $(if($KafkaBootstrap){$KafkaBootstrap}else{'disabled'}))" -ForegroundColor Cyan
    $env:API_URL                 = "http://127.0.0.1:$ApiPort"
    $env:API_INTERVAL            = "$WorkerInterval"
    $env:KAFKA_BOOTSTRAP_SERVERS = $KafkaBootstrap
    $env:FRAGMENT_ID             = "test-fragment-1"
    $env:FRAGMENT_NAME           = "Test Fragment 1"
    $workerProc = Start-Process -FilePath $py `
        -ArgumentList @("main.py") `
        -WorkingDirectory (Join-Path $root "worker") `
        -RedirectStandardOutput $workerLog -RedirectStandardError "$workerLog.err" `
        -NoNewWindow -PassThru
    "worker=$($workerProc.Id)" | Out-File $pidFile -Append -Encoding ascii
    Write-Host "[+] worker started." -ForegroundColor Green
}

# --- info -----------------------------------------------------------------
Write-Host ""
Write-Host "======================================================================" -ForegroundColor Green
Write-Host " test-fragment-1 running locally (no Docker)"                          -ForegroundColor Green
Write-Host "----------------------------------------------------------------------"
Write-Host "  API        : http://127.0.0.1:$ApiPort"
Write-Host "  Docs       : http://127.0.0.1:$ApiPort/docs"
Write-Host "  Counter    : http://127.0.0.1:$ApiPort/counter"
Write-Host "  WebSocket  : ws://127.0.0.1:$ApiPort/ws/counter"
Write-Host "  Logs       : $runDir"
Write-Host "  Stop       : .\run-local.ps1 -Stop   (or Ctrl+C here)"
Write-Host "======================================================================" -ForegroundColor Green
Write-Host ""

# --- tail (Ctrl+C stops everything) --------------------------------------
try {
    if ($NoWorker) {
        Write-Host "Tailing api log. Press Ctrl+C to stop." -ForegroundColor DarkGray
        Get-Content $apiLog -Wait -Tail 20
    } else {
        Write-Host "Tailing worker log. Press Ctrl+C to stop." -ForegroundColor DarkGray
        Get-Content $workerLog -Wait -Tail 20
    }
} finally {
    Write-Host ""
    Write-Host "==> Shutting down..." -ForegroundColor Yellow
    Stop-Fragment
    Write-Host "[+] api + worker stopped." -ForegroundColor Green
}
