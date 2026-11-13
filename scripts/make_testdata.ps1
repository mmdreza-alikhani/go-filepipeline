# Creates a realistic inbox for the file pipeline demo.
#
#   .\scripts\make_testdata.ps1 -Count 100
#
# Every text file contains trailing spaces and CRLF line endings on purpose: the
# convert stage must normalise them, which is easy to verify in the output.

param(
    [int]$Count = 100,
    [string]$Inbox = (Join-Path $PSScriptRoot '..\data\inbox'),
    [switch]$WithBadFile
)

$ErrorActionPreference = 'Stop'

$Inbox = [System.IO.Path]::GetFullPath($Inbox)
New-Item -ItemType Directory -Force -Path $Inbox | Out-Null

$utf8 = [System.Text.UTF8Encoding]::new($false)

for ($i = 1; $i -le $Count; $i++) {
    $name = 'sample_{0:d4}.txt' -f $i
    $path = Join-Path $Inbox $name
    $lines = @()
    $lineCount = 5 + ($i % 7)
    for ($l = 0; $l -lt $lineCount; $l++) {
        $lines += ('record {0} line {1}: the quick brown fox jumps over the lazy dog' -f $i, $l)
    }
    # Trailing whitespace + CRLF are deliberate.
    $content = (($lines -join "   `r`n") + "  `r`n")
    [System.IO.File]::WriteAllText($path, $content, $utf8)
}

# Decoys that the source must ignore: temporary, partial and hidden names.
[System.IO.File]::WriteAllText((Join-Path $Inbox 'still-writing.tmp'), 'incomplete', $utf8)
[System.IO.File]::WriteAllText((Join-Path $Inbox 'half-download.part'), 'incomplete', $utf8)
[System.IO.File]::WriteAllText((Join-Path $Inbox '.hidden'), 'invisible', $utf8)

if ($WithBadFile) {
    # Invalid UTF-8: this one is expected to end up in data/failed.
    [System.IO.File]::WriteAllBytes((Join-Path $Inbox 'broken_0001.bin'), [byte[]](0xff, 0xfe, 0x00, 0x41, 0x42))
    Write-Host 'added broken_0001.bin (invalid UTF-8, will be dead lettered)'
}

Write-Host ("created {0} text files plus 3 decoys in {1}" -f $Count, $Inbox)
