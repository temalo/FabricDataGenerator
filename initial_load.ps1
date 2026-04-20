param(
    [double]$Scale = 1.0,
    [string]$PythonCommand = "python",
    [switch]$InstallRequirements,
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

if ($InstallRequirements) {
    Invoke-Step -Title "Installing requirements" -Action {
        & $PythonCommand -m pip install -r requirements.txt
    }
}

Invoke-Step -Title "Generating initial dataset" -Action {
    & $PythonCommand generate_gps_initial_dataset.py --scale $Scale
}

Invoke-Step -Title "Publishing initial load to Fabric landing zone" -Action {
    $publishArgs = @(
        "publish_to_fabric_open_mirroring.py",
        "--source-dir", "output/open_mirroring_initial",
        "--api", $Api
    )

    if ($DryRunPublish) {
        $publishArgs += "--dry-run"
    }

    & $PythonCommand @publishArgs
}

Write-Host ""
Write-Host "Initial load script completed."

