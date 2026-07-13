# Runs every merge script on a short loop (micro-batch cadence, spec Section 8.5).
# Usage: powershell -File run_merge_loop.ps1 [-IntervalSeconds 3]

param(
    [int]$IntervalSeconds = 3
)

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$env:TD_HOST     = if ($env:TD_HOST)     { $env:TD_HOST }     else { "192.168.1.205" }
$env:TD_USER     = if ($env:TD_USER)     { $env:TD_USER }     else { "dbc" }
$env:TD_PASSWORD = if ($env:TD_PASSWORD) { $env:TD_PASSWORD } else { "dbc" }

$scripts = @(
    "merge_a_customer.bteq", "merge_a_account.bteq", "merge_a_card.bteq", "merge_a_payment.bteq", "merge_a_transaction.bteq",
    "merge_b_fanout.bteq",
    "merge_c_transaction.bteq", "merge_c_warm.bteq"
)

Write-Output "Merge loop running every $IntervalSeconds s. Ctrl+C to stop."
while ($true) {
    foreach ($s in $scripts) {
        powershell -File "$Root\..\tpt\scripts\run_bteq.ps1" "$Root\merge\$s" | Out-Null
    }
    Start-Sleep -Seconds $IntervalSeconds
}
