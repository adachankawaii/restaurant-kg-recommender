@echo off
setlocal

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0start_system.ps1" %*
exit /b %ERRORLEVEL%
