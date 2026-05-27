param(
    [string]$Bind = "0.0.0.0",
    [int]$DiscoveryPort = 6111,
    [int]$StormPort = 6112,
    [string]$RoomName = "Challenger",
    [string]$MapPath = "maps/(2)Challenger.scm",
    [string]$MainHostName = "Sun",
    [string]$SubHostName = "SunX",
    [double]$AutoStartDelay = 3,
    [double]$GameStateDelay = 0.35,
    [double]$SeedDelay = 5.75,
    [double]$AdvertiseInterval = 2,
    [double]$StartStabilityDelay = 1,
    [string[]]$BroadcastAddress = @(),
    [string]$LogLevel = "DEBUG",
    [string]$LogFile = "logs/sc_host.log",
    [bool]$TraceGame = $true,
    [bool]$TraceNop = $false,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

$PrefixArgs = @()

if (-not $env:SC_HOST_PYTHON) {
    $env:SC_HOST_PYTHON = "C:\Users\sunziming\AppData\Local\Programs\Python\Python310\python.exe"
}

$PythonExe = $env:SC_HOST_PYTHON

$ArgsList = @(
    "-m", "sc_host",
    "--bind", $Bind,
    "--discovery-port", $DiscoveryPort,
    "--storm-port", $StormPort,
    "--room-name", $RoomName,
    "--map-path", $MapPath,
    "--main-host-name", $MainHostName,
    "--sub-host-name", $SubHostName,
    "--auto-start-delay", $AutoStartDelay,
    "--game-state-delay", $GameStateDelay,
    "--seed-delay", $SeedDelay,
    "--advertise-interval", $AdvertiseInterval,
    "--start-stability-delay", $StartStabilityDelay,
    "--log-level", $LogLevel,
    "--log-file", $LogFile
)

if ($TraceGame) {
    $ArgsList += "--trace-game"
}

if ($TraceNop) {
    $ArgsList += "--trace-nop"
}

foreach ($Address in $BroadcastAddress) {
    if ($Address) {
        $ArgsList += @("--broadcast-address", $Address)
    }
}

$ArgsList += $ExtraArgs

Write-Host "Starting StarCraft LAN host relay..."
Write-Host "Root: $Root"
Write-Host "Python: $PythonExe"

& $PythonExe @PrefixArgs @ArgsList
exit $LASTEXITCODE
