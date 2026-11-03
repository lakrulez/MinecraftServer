@echo off
title BedrockNexus - Minecraft Bedrock Multi-Server Manager
cd /d "%~dp0"

echo ==================================================================
echo   BedrockNexus -- Minecraft Bedrock Multi-Server Manager ^& Proxy
echo ==================================================================

set "PYTHON_EXE=python"
if exist "%~dp0.venv\Scripts\python.exe" (
    set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"
)

echo [1/2] Checking Python dependencies...
"%PYTHON_EXE%" -m pip install -q -r requirements.txt
if errorlevel 1 (
    echo [ERROR] Failed to install requirements.
    pause
    exit /b 1
)

if "%PORT%"=="" set "PORT=8000"

echo [2/2] Starting FastAPI Web GUI on http://0.0.0.0:%PORT% and RakNet UDP Proxy on :19132...
echo       Open http://localhost:%PORT% in your browser.
echo.

"%PYTHON_EXE%" -m uvicorn app.main:app --host 0.0.0.0 --port %PORT%
if errorlevel 1 (
    pause
)

