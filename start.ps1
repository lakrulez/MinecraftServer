<#
.SYNOPSIS
    One-click self-bootstrapping startup script for BedrockNexus — Minecraft Bedrock Multi-Server Manager & LAN Proxy.
    Automatically installs Python (if missing), Python packages, Visual C++ Runtime DLLs,
    Mojang Bedrock Dedicated Server binary, and Windows Firewall LAN rules.
#>
$ErrorActionPreference = "Continue"
Set-Location -Path $PSScriptRoot

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "  BedrockNexus — Self-Healing Bedrock Multi-Server Manager & Proxy" -ForegroundColor Green
Write-Host "==================================================================" -ForegroundColor Cyan

function Find-PythonExe {
    if (Test-Path "$PSScriptRoot\.venv\Scripts\python.exe") {
        return "$PSScriptRoot\.venv\Scripts\python.exe"
    }
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd -and $cmd.Source -and ($cmd.Source -notmatch "WindowsApps\\python\.exe")) {
        return $cmd.Source
    }
    $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($pyLauncher) {
        return "py"
    }
    $commonPaths = @(
        "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
        "C:\Python312\python.exe",
        "C:\Python311\python.exe"
    )
    foreach ($p in $commonPaths) {
        if (Test-Path $p) { return $p }
    }
    return $null
}

$PythonExe = Find-PythonExe
if (-not $PythonExe) {
    Write-Host "[0/3] Python 3 not found. Automatically installing Python 3.12 via winget..." -ForegroundColor Yellow
    winget install -e --id Python.Python.3.12 --scope user --accept-source-agreements --accept-package-agreements
    $PythonExe = Find-PythonExe
    if (-not $PythonExe) {
        $PythonExe = "python"
    }
}

Write-Host "[1/3] Installing / verifying Python dependencies (FastAPI, Uvicorn, psutil, cryptography)..." -ForegroundColor Yellow
& $PythonExe -m pip install --upgrade pip -q
& $PythonExe -m pip install -q -r requirements.txt

Write-Host "[2/3] Running Auto-Bootstrapper (VC++ Runtime DLLs, Mojang BDS Binary, World Repair, Windows Firewall)..." -ForegroundColor Yellow
& $PythonExe -m app.bootstrap --elevate-firewall

$Port = if ($env:PORT) { $env:PORT } else { "8000" }
Write-Host "[3/3] Starting BedrockNexus Web GUI on http://0.0.0.0:$Port (LAN Discovery on UDP :7551 & :19132)..." -ForegroundColor Green
Write-Host "      Open http://localhost:$Port in your browser." -ForegroundColor Cyan

& $PythonExe -m uvicorn app.main:app --host 0.0.0.0 --port $Port
