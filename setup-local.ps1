# Incremental Data Pipeline - first-time local setup (Windows PowerShell)
# Creates a Python venv and installs the local pipeline + API dependencies.
$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
Set-Location $root

Write-Host "==> Creating virtualenv (.venv)" -ForegroundColor Cyan
if (-not (Test-Path ".\.venv")) { python -m venv .venv }
& ".\.venv\Scripts\python.exe" -m pip install --upgrade pip

Write-Host "==> Installing pipeline + API dependencies" -ForegroundColor Cyan
& ".\.venv\Scripts\python.exe" -m pip install -r requirements-local.txt

Write-Host ""
Write-Host "Setup complete." -ForegroundColor Green
Write-Host "Run the pipeline:  .\run-local.ps1"
Write-Host "Run the tests:     .\.venv\Scripts\python.exe -m pytest tests -q"
Write-Host "Start the API:     .\.venv\Scripts\python.exe -m uvicorn pipeline.service:app --reload --port 8001"