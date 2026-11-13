# End to end demo: build, generate 100 files (plus one corrupt file), run the
# pipeline in batch mode, verify the output, then prove the rerun is idempotent.
#
#   .\scripts\demo.ps1 -Count 100

param(
    [int]$Count = 100,
    [switch]$KeepData
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    if (-not $KeepData -and (Test-Path ./data)) {
        Remove-Item -Recurse -Force ./data
    }

    Write-Host '==> building' -ForegroundColor Cyan
    New-Item -ItemType Directory -Force -Path ./bin | Out-Null
    go build -o ./bin/pipeline.exe ./cmd/pipeline
    if ($LASTEXITCODE -ne 0) { throw 'go build failed' }

    Write-Host "==> generating $Count files" -ForegroundColor Cyan
    & "$PSScriptRoot\make_testdata.ps1" -Count $Count -WithBadFile

    # Batch size = how many files the source will actually consider input. It is
    # recomputed before the rerun because the corrupt file moves to data/failed.
    function Get-InputCount {
        (Get-ChildItem ./data/inbox -File | Where-Object {
            $_.Name -notmatch '\.(tmp|part|partial|crdownload|swp|lock)$' -and
            -not $_.Name.StartsWith('.') -and
            -not $_.Name.EndsWith('~')
        }).Count
    }

    Write-Host '==> running the pipeline (batch mode: -limit)' -ForegroundColor Cyan
    $batch = Get-InputCount
    $sw = [Diagnostics.Stopwatch]::StartNew()
    ./bin/pipeline.exe -config ./configs/config.example.json -limit $batch
    $code = $LASTEXITCODE
    $sw.Stop()
    if ($code -ne 0) { throw "pipeline exited with $code" }

    function Read-GzipText([string]$Path) {
        $fs = [System.IO.File]::OpenRead($Path)
        try {
            $gz = [System.IO.Compression.GZipStream]::new($fs, [System.IO.Compression.CompressionMode]::Decompress)
            $sr = [System.IO.StreamReader]::new($gz, [System.Text.Encoding]::UTF8)
            try { return $sr.ReadToEnd() } finally { $sr.Dispose(); $gz.Dispose() }
        } finally { $fs.Dispose() }
    }

    Write-Host '==> verifying' -ForegroundColor Cyan
    $problems = New-Object System.Collections.Generic.List[string]
    $gz = Get-ChildItem -Path ./data/out -Recurse -Filter *.gz -File
    foreach ($f in $gz) {
        try {
            $text = Read-GzipText $f.FullName
        } catch {
            $problems.Add("unreadable gzip: $($f.FullName)")
            continue
        }
        if ($text.Contains("`r")) { $problems.Add("CR left in $($f.Name)") }
        foreach ($line in ($text -split "`n")) {
            if ($line -match '[ \t]+$') { $problems.Add("trailing whitespace in $($f.Name)") }
        }
    }

    $partial = Get-ChildItem -Path ./data/out -Recurse -File | Where-Object { $_.Name -like '*.part' -or $_.Name -like '*.tmp' }
    if ($partial) { $problems.Add("partial files in out/: $($partial.Name -join ', ')") }

    $staging = Get-ChildItem -Path ./data/out/.tmp -File -ErrorAction SilentlyContinue
    if ($staging) { $problems.Add("staging directory is not empty: $($staging.Count) files") }

    $manifests = Get-ChildItem -Path ./data/out -Recurse -Filter *.json -File
    $failed = Get-ChildItem -Path ./data/failed -Filter *.json -File -ErrorAction SilentlyContinue
    $journal = Get-ChildItem -Path ./data/work/jobs -Filter *.json -File
    $done = 0
    foreach ($j in $journal) {
        $doc = Get-Content $j.FullName -Raw | ConvertFrom-Json
        if ($doc.state -eq 'done') { $done++ }
    }

    $summary = [ordered]@{
        'input files'        = $Count + 1
        'artefacts (.gz)'    = $gz.Count
        'manifests (.json)'  = $manifests.Count
        'journal done'       = $done
        'dead letters'       = ($failed | Measure-Object).Count
        'elapsed seconds'    = [math]::Round($sw.Elapsed.TotalSeconds, 2)
    }
    $summary.GetEnumerator() | ForEach-Object { '{0,-20} {1}' -f $_.Key, $_.Value | Write-Host }

    if ($problems.Count -gt 0) {
        $problems | ForEach-Object { Write-Host "PROBLEM: $_" -ForegroundColor Red }
        throw ("verification failed with {0} problem(s)" -f $problems.Count)
    }
    Write-Host 'verification: all artefacts are complete, normalised gzip' -ForegroundColor Green

    Write-Host '==> rerunning the same inbox (idempotency)' -ForegroundColor Cyan
    $before = $gz.Count
    ./bin/pipeline.exe -config ./configs/config.example.json -limit (Get-InputCount) | Out-Null
    $after = (Get-ChildItem -Path ./data/out -Recurse -Filter *.gz -File).Count
    Write-Host ("artefacts before={0} after={1}" -f $before, $after)
    if ($after -ne $before) { throw 'the rerun created duplicate artefacts' }
    Write-Host 'idempotency: no duplicate output' -ForegroundColor Green

    Write-Host '==> sample artefact' -ForegroundColor Cyan
    $sample = $gz | Select-Object -First 1
    Get-Content (Join-Path (Split-Path $sample.FullName) ($sample.BaseName + '.json')) -Raw
    Write-Host '==> decompressed content of the sample' -ForegroundColor Cyan
    (Read-GzipText $sample.FullName) -split "`n" | Select-Object -First 3

    Write-Host '==> dead letter' -ForegroundColor Cyan
    $failed | Select-Object -First 1 | ForEach-Object { Get-Content $_.FullName -Raw }
}
finally {
    Pop-Location
}
