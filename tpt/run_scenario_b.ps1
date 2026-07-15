# Launches Scenario B's single TPT job (5 topics -> 1 job -> 1 landing table) as a
# background process in its own window. Logs on as tpt_demo_b_all (see
# teradata/ddl/08_tpt_users.bteq) rather than a shared admin login.
#
# Usage: powershell -File run_scenario_b.ps1

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

$TdHost      = if ($env:TD_HOST)          { $env:TD_HOST }          else { "192.168.1.205" }
$TptPassword = if ($env:TPT_JOB_PASSWORD) { $env:TPT_JOB_PASSWORD } else { "TptDemo2026Pass" }
$TptUser     = "tpt_demo_b_all"

Write-Output "Starting demo_b_all (login: $TptUser) ..."
Start-Process -FilePath "powershell" -ArgumentList @(
    "-NoExit", "-Command",
    "`$env:TD_HOST='$TdHost'; `$env:TD_USER='$TptUser'; `$env:TD_PASSWORD='$TptPassword'; `$env:TD_DATABASE='DEMO_B'; `$env:KAFKA_BOOTSTRAP='localhost:9092'; `$env:KAFKA_DUMMY_DIR='C:\Windows\Temp'; `$env:KAFKA_AXSMOD_NAME='libkafkaaxsmod.dll'; `$env:KAFKA_IDLE_TIMEOUT='30'; `$env:KAFKA_GROUP_ID='tpt.demo_b'; & '$Root\scripts\run_tbuild.ps1' '$Root\tbuild\scenario_b\load_all.tbuild' -j demo_b_all -l 5 -z 10"
)
