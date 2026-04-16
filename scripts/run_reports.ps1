Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectRoot = "D:\AI_Automation\amocrm_bot\project"
$debugPath = Join-Path $projectRoot "exports\debug"
$compiledPath = Join-Path $projectRoot "exports\compiled"

function Invoke-ReportCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$CommandLine
    )

    Push-Location $projectRoot
    try {
        $env:GOOGLE_API_AUTH_MODE = "cache_only"
        Write-Host ""
        Write-Host "GOOGLE_API_AUTH_MODE=$($env:GOOGLE_API_AUTH_MODE)"
        Write-Host "Running: $CommandLine"
        Invoke-Expression $CommandLine
        $exitCode = $LASTEXITCODE
        if ($null -eq $exitCode) {
            $exitCode = 0
        }
        Write-Host ""
        Write-Host "Command: $CommandLine"
        Write-Host "Exit code: $exitCode"
        Write-Host "Debug artifacts: $debugPath"
        Write-Host "Compiled artifacts: $compiledPath"
    }
    finally {
        Pop-Location
    }
}

Write-Host "amoCRM reports launcher"
Write-Host "1. Analytics dry-run batch from sheet DSL"
Write-Host "2. Analytics live write block A1"
Write-Host "3. Analytics live write block F1"
Write-Host "4. Weekly refusals dry-run 2m"
Write-Host "5. Weekly refusals live 2m"
Write-Host "6. Weekly refusals live cumulative long"
Write-Host ""
$choice = Read-Host "Select menu item (1-6)"

$command = switch ($choice) {
    "1" { "python -m src.run_profile_analytics --report-id analytics_utm_layout_example --writer-layout-api-batch-from-sheet-dsl-dry-run --browser-backend openclaw_cdp --tag-selection-mode script" }
    "2" { "python -m src.run_profile_analytics --report-id analytics_utm_layout_example --execution-from-sheet-dsl --writer-layout-api-target-dsl-cell A1 --writer-layout-api-write --browser-backend openclaw_cdp --tag-selection-mode script" }
    "3" { "python -m src.run_profile_analytics --report-id analytics_utm_layout_example --execution-from-sheet-dsl --writer-layout-api-target-dsl-cell F1 --writer-layout-api-write --browser-backend openclaw_cdp --tag-selection-mode script" }
    "4" { "python -m src.run_profile_analytics --report-id weekly_refusals_weekly_2m --writer-layout-api-dry-run --browser-backend openclaw_cdp" }
    "5" { "python -m src.run_profile_analytics --report-id weekly_refusals_weekly_2m --browser-backend openclaw_cdp" }
    "6" { "python -m src.run_profile_analytics --report-id weekly_refusals_cumulative_long --browser-backend openclaw_cdp" }
    default { $null }
}

if (-not $command) {
    Write-Host "Invalid selection: $choice"
    exit 1
}

Invoke-ReportCommand -CommandLine $command
