#Requires -Version 5.1
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
<#
.SYNOPSIS
    Deploy MindBody-Dahua Gate Sync locally on Windows.

.DESCRIPTION
    Sets up a Python virtual environment, installs dependencies,
    initialises the .env file, and starts the FastAPI server.

.PARAMETER Port
    Port to run the server on. Default: 8000

.PARAMETER Reload
    Enable auto-reload for development. Default: false

.EXAMPLE
    .\deploy-local.ps1
    .\deploy-local.ps1 -Port 9000 -Reload
#>

param(
    [int]$Port = 8080,
    [switch]$Reload
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  MindBody-Dahua Gate Sync - Local Deploy" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# --- Check prerequisites ---------------------------------------------------

function Test-Command($cmd) {
    return [bool](Get-Command $cmd -ErrorAction SilentlyContinue)
}

# Check Python
if (-not (Test-Command "python")) {
    Write-Host "[ERROR] Python not found. Install Python 3.12+ from https://python.org" -ForegroundColor Red
    exit 1
}

$pyVersion = python --version 2>&1
Write-Host "[OK] $pyVersion" -ForegroundColor Green

# Check uv
$useUv = Test-Command "uv"
if ($useUv) {
    Write-Host "[OK] uv found — using uv for package management" -ForegroundColor Green
} else {
    Write-Host "[INFO] uv not found — falling back to pip" -ForegroundColor Yellow
}

# --- .env setup ------------------------------------------------------------

$envFile = Join-Path $ProjectRoot ".env"
$envExample = Join-Path $ProjectRoot ".env.example"

if (-not (Test-Path $envFile)) {
    if (Test-Path $envExample) {
        Copy-Item $envExample $envFile
        Write-Host "[CREATED] .env copied from .env.example" -ForegroundColor Yellow
        Write-Host "          Edit .env with your MindBody and Dahua credentials before first sync." -ForegroundColor Yellow
    } else {
        Write-Host "[ERROR] .env.example not found" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "[OK] .env already exists" -ForegroundColor Green
}

# --- Virtual environment ---------------------------------------------------

$venvDir = Join-Path $ProjectRoot ".venv"
$venvPython = Join-Path $venvDir "Scripts\python.exe"
$venvActivate = Join-Path $venvDir "Scripts\Activate.ps1"

if (-not (Test-Path $venvPython)) {
    Write-Host ""
    Write-Host "[SETUP] Creating virtual environment..." -ForegroundColor Cyan
    if ($useUv) {
        uv venv $venvDir
    } else {
        python -m venv $venvDir
    }
    Write-Host "[OK] Virtual environment created at .venv\" -ForegroundColor Green
} else {
    Write-Host "[OK] Virtual environment exists" -ForegroundColor Green
}

# --- Install dependencies --------------------------------------------------

Write-Host ""
Write-Host "[SETUP] Installing dependencies..." -ForegroundColor Cyan

$reqFile = Join-Path $ProjectRoot "requirements.txt"

if ($useUv) {
    uv pip install -r $reqFile --python $venvPython
} else {
    & $venvPython -m pip install --quiet --upgrade pip
    & $venvPython -m pip install --quiet -r $reqFile
}

Write-Host "[OK] Dependencies installed" -ForegroundColor Green

# --- Create data directory -------------------------------------------------

$dataDir = Join-Path $ProjectRoot "data"
if (-not (Test-Path $dataDir)) {
    New-Item -ItemType Directory -Path $dataDir | Out-Null
    Write-Host "[OK] Created data\ directory for SQLite" -ForegroundColor Green
}

# --- Start server ----------------------------------------------------------

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Starting server on port $Port" -ForegroundColor Cyan
Write-Host "  Admin UI: http://localhost:$Port/admin/login" -ForegroundColor Cyan
Write-Host "  Health:   http://localhost:$Port/health" -ForegroundColor Cyan
Write-Host "  Press Ctrl+C to stop" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$uvicornArgs = @("app.main:app", "--host", "0.0.0.0", "--port", $Port.ToString())
if ($Reload) {
    $uvicornArgs += "--reload"
}

Set-Location $ProjectRoot
& $venvPython -m uvicorn @uvicornArgs
