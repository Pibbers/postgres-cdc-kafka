# Preprocess a BTEQ script (substituting ${VAR} from the current environment) and
# pipe it into the natively-installed bteq.exe. Windows counterpart to run_bteq.sh
# (same ${VAR} substitution convention), for running BTEQ directly on this host
# instead of via the teradata/tpt Docker image (whose containerized network stack
# hangs on LOGON to this particular Teradata host - see build notes).
#
# Usage: powershell -File run_bteq.ps1 <path-to-script.bteq>

param(
    [Parameter(Mandatory = $true)]
    [string]$ScriptPath
)

$BteqExe = "C:\Program Files\Teradata\Client\20.00\bin\bteq.exe"

$content = Get-Content -Raw -Path $ScriptPath
$substituted = [regex]::Replace($content, '\$\{([^}]+)\}', {
    param($m)
    $name = $m.Groups[1].Value
    $val = [Environment]::GetEnvironmentVariable($name)
    if ($null -eq $val) { "(UNDEF:$name)" } else { $val }
})

$substituted | & $BteqExe
exit $LASTEXITCODE
