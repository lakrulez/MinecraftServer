@echo off
title BedrockNexus - Self-Healing Minecraft Bedrock Multi-Server Manager
cd /d "%~dp0"

echo ==================================================================
echo   BedrockNexus -- Self-Healing Bedrock Multi-Server Manager
echo ==================================================================
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1"
if errorlevel 1 (
    echo.
    echo [WARN] PowerShell bootstrap exited with an error. Press any key to close...
    pause
)
