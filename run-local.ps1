# Incremental Data Pipeline - run the local pipeline (no Docker needed)
# Reads data\*.csv through checkpoints, writes data\behaviour_metrics.csv and
# the dashboard HTML, and records a run in data\warehouse.duckdb.
$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
Set-Location $root

if (-not (Test-Path ".\.venv\Scripts\python.exe")) {
    Write-Host "No .venv found. Run .\setup-local.ps1 first." -ForegroundColor Yellow
    exit 1
}

Write-Host "==> Running incremental pipeline" -ForegroundColor Cyan
& ".\.venv\Scripts\python.exe" pipeline\run_local.py

$dash = "$root\dags\scripts\dashboard\dashboard.html"
if (Test-Path $dash) {
    Write-Host "Opening dashboard: $dash" -ForegroundColor Green
    Start-Process -FilePath $dash
}

Write-Host ""
Write-Host "Start the API with:" -ForegroundColor Green
Write-Host "  .\.venv\Scripts\python.exe -m uvicorn pipeline.service:app --reload --port 8001"