<#
.SYNOPSIS
    One-click startup script for BedrockNexus — Minecraft Bedrock Multi-Server Manager & RakNet LAN Proxy.
#>
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "  BedrockNexus — Minecraft Bedrock Multi-Server Manager & Proxy   " -ForegroundColor Green
Write-Host "==================================================================" -ForegroundColor Cyan

# Use local virtual environment if present, otherwise system Python
$PythonExe = "python"
if (Test-Path "$PSScriptRoot\.venv\Scripts\python.exe") {
    $PythonExe = "$PSScriptRoot\.venv\Scripts\python.exe"
}

Write-Host "[1/2] Checking Python dependencies..." -ForegroundColor Yellow
& $PythonExe -m pip install -q -r requirements.txt

$Port = if ($env:PORT) { $env:PORT } else { "8000" }
Write-Host "[2/2] Starting FastAPI Web GUI on http://0.0.0.0:$Port and RakNet UDP Proxy on :19132..." -ForegroundColor Green
Write-Host "      Open http://localhost:$Port in your browser." -ForegroundColor Cyan

& $PythonExe -m uvicorn app.main:app --host 0.0.0.0 --port $Port

