# Launches all 5 Scenario A TPT jobs (1 topic -> 1 job -> 1 landing table each) as
# separate background processes. Each job runs until stopped (Ctrl+C the window,
# or Stop-Process on its PID) - container/process lifecycle = job lifecycle.
#
# Usage: powershell -File run_scenario_a.ps1
# Requires: TD_HOST / TPT_JOB_PASSWORD set in the environment (or edit below).
# Each job logs on as its own tpt_demo_a_<table> user (see
# teradata/ddl/08_tpt_users.bteq) rather than a shared admin login.

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

$env:TD_HOST          = if ($env:TD_HOST)          { $env:TD_HOST }          else { "192.168.1.205" }
$env:TPT_JOB_PASSWORD = if ($env:TPT_JOB_PASSWORD) { $env:TPT_JOB_PASSWORD } else { "TptDemo2026Pass" }
$env:TD_DATABASE  = "DEMO_A"
$env:KAFKA_BOOTSTRAP = "localhost:9092"
$env:KAFKA_DUMMY_DIR = "C:\Windows\Temp"
$env:KAFKA_AXSMOD_NAME = "libkafkaaxsmod.dll"
$env:KAFKA_IDLE_TIMEOUT = "30"

$tables = @("customer", "account", "card", "payment", "transaction")

foreach ($t in $tables) {
    $tptUser = "tpt_demo_a_$t"
    $env:KAFKA_TOPIC = "cdc.public.$t"
    $env:KAFKA_GROUP_ID = "tpt.demo_a.$t"
    Write-Output "Starting demo_a_$t (login: $tptUser) ..."
    Start-Process -FilePath "powershell" -ArgumentList @(
        "-NoExit", "-Command",
        "`$env:TD_HOST='$($env:TD_HOST)'; `$env:TD_USER='$tptUser'; `$env:TD_PASSWORD='$($env:TPT_JOB_PASSWORD)'; `$env:TD_DATABASE='DEMO_A'; `$env:KAFKA_BOOTSTRAP='localhost:9092'; `$env:KAFKA_DUMMY_DIR='C:\Windows\Temp'; `$env:KAFKA_AXSMOD_NAME='libkafkaaxsmod.dll'; `$env:KAFKA_IDLE_TIMEOUT='30'; `$env:KAFKA_TOPIC='cdc.public.$t'; `$env:KAFKA_GROUP_ID='tpt.demo_a.$t'; & '$Root\scripts\run_tbuild.ps1' '$Root\tbuild\scenario_a\load_$t.tbuild' -j demo_a_$t -l 5 -z 10"
    )
}
