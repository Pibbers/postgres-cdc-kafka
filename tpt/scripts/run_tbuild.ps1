# Preprocess a tbuild script (substituting $(VAR) from the current environment),
# write it to a temp file, and execute with the natively-installed tbuild.exe.
# Windows counterpart to run_tbuild.sh (same $(VAR) substitution convention).
#
# Usage: powershell -File run_tbuild.ps1 <path-to-job.tbuild> [extra tbuild args...]

param(
    [Parameter(Mandatory = $true)]
    [string]$ScriptPath,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

$TbuildExe = "C:\Program Files\Teradata\Client\20.00\bin\tbuild.exe"

$content = Get-Content -Raw -Path $ScriptPath
$substituted = [regex]::Replace($content, '\$\(([^)]+)\)', {
    param($m)
    $name = $m.Groups[1].Value
    $val = [Environment]::GetEnvironmentVariable($name)
    if ($null -eq $val) { "(UNDEF:$name)" } else { $val }
})

$tmpFile = [System.IO.Path]::GetTempFileName() + ".tbuild"
Set-Content -Path $tmpFile -Value $substituted -NoNewline

try {
    & $TbuildExe -f $tmpFile @ExtraArgs
    exit $LASTEXITCODE
} finally {
    Remove-Item -Path $tmpFile -Force -ErrorAction SilentlyContinue
}
