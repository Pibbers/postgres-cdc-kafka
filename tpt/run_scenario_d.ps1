# Launches Scenario D's single TPT job (3 CSV topics -> 1 job -> 3 target tables,
# zero landing tables) as a background process in its own window. Logs on as
# tpt_demo_d_all (see teradata/ddl/08_tpt_users.bteq) rather than a shared admin
# login.
#
# Usage: powershell -File run_scenario_d.ps1

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

$TdHost      = if ($env:TD_HOST)          { $env:TD_HOST }          else { "192.168.1.205" }
$TptPassword = if ($env:TPT_JOB_PASSWORD) { $env:TPT_JOB_PASSWORD } else { "TptDemo2026Pass" }
$TptUser     = "tpt_demo_d_all"

Write-Output "Starting demo_d_all (login: $TptUser) ..."
Start-Process -FilePath "powershell" -ArgumentList @(
    "-NoExit", "-Command",
    "`$env:TD_HOST='$TdHost'; `$env:TD_USER='$TptUser'; `$env:TD_PASSWORD='$TptPassword'; `$env:TD_DATABASE='DEMO_D'; `$env:KAFKA_BOOTSTRAP='localhost:9092'; `$env:KAFKA_DUMMY_DIR='C:\Windows\Temp'; `$env:KAFKA_AXSMOD_NAME='libkafkaaxsmod.dll'; `$env:KAFKA_IDLE_TIMEOUT='30'; `$env:KAFKA_GROUP_ID='tpt.demo_d'; & '$Root\scripts\run_tbuild.ps1' '$Root\tbuild\scenario_d\load_all.tbuild' -j demo_d_all -l 5 -z 10"
)
