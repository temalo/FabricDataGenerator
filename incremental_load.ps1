param(
    [double]$Scale = 1.0,
    [string]$PythonCommand = "python",
    [switch]$DryRunPublish,
    [ValidateSet("blob", "dfs")]
    [string]$Api = "blob"
)

$ErrorActionPreference = "Stop"

function Invoke-Step {
    param(
        [string]$Title,
        [scriptblock]$Action
    )

    Write-Host ""
    Write-Host "=== $Title ==="
    & $Action
}

Invoke-Step -Title "Checking Python" -Action {
    & $PythonCommand --version
}

Invoke-Step -Title "Generating incremental changes" -Action {
    & $PythonCommand generate_gps_incremental_changes.py --scale $Scale
}

Invoke-Step -Title "Publishing incremental changes to Fabric landing zone" -Action {
    $publishArgs = @(
        "publish_to_fabric_open_mirroring.py",
        "--source-dir", "output/open_mirroring_incremental",
        "--api", $Api
    )

    if ($DryRunPublish) {
        $publishArgs += "--dry-run"
    }

    & $PythonCommand @publishArgs
}

Write-Host ""
Write-Host "Incremental load script completed."

