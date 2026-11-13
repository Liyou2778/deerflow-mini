#requires -Version 5.1
<#
.SYNOPSIS
    Start deerflow-mini on http://127.0.0.1:8201 with the repository's shared venv.

.DESCRIPTION
    No absolute paths: the script resolves itself via $PSScriptRoot, prefers the
    shared venv at ..\.venv\Scripts\python.exe, and falls back to a python on PATH.
    All settings can be overridden with environment variables, e.g.
        $env:LLM_PROVIDER='openai'; $env:LLM_BASE_URL='https://...'; .\run.ps1
#>
[CmdletBinding()]
param(
    [int]$Port = $(if ($env:PORT) { [int]$env:PORT } else { 8201 }),
    [string]$HostAddress = $(if ($env:HOST) { $env:HOST } else { '127.0.0.1' }),
    [switch]$Reload
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$venvCandidates = @(
    (Join-Path $root '..\.venv\Scripts\python.exe'),
    (Join-Path $root '.venv\Scripts\python.exe')
)
$python = $venvCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $python) {
    $python = (Get-Command python -ErrorAction Stop).Source
}

Write-Host "deerflow-mini" -ForegroundColor Cyan
Write-Host "  python : $python"
Write-Host "  listen : http://${HostAddress}:${Port}/"
$providerLabel = if ($env:LLM_PROVIDER) { $env:LLM_PROVIDER } else { 'mock' }
Write-Host "  provider: $providerLabel (offline default)"
Write-Host ""

$uvicornArgs = @('-m', 'uvicorn', 'app.main:app', '--host', $HostAddress, '--port', "$Port")
if ($Reload) { $uvicornArgs += @('--reload') }

Push-Location $root
try {
    & $python @uvicornArgs
}
finally {
    Pop-Location
}
