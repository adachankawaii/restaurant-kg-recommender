param(
    [switch]$RebuildOffline,
    [int]$ApiPort = 8000
)

$ErrorActionPreference = "Stop"
$scriptPath = Join-Path $PSScriptRoot "production\scripts\start_system.ps1"
& $scriptPath -RebuildOffline:$RebuildOffline -ApiPort $ApiPort
