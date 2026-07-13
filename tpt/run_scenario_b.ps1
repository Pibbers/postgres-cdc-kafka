# Launches Scenario B's single TPT job (5 topics -> 1 job -> 1 landing table) as a
# background process in its own window.
#
# Usage: powershell -File run_scenario_b.ps1

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

$TdHost     = if ($env:TD_HOST)     { $env:TD_HOST }     else { "192.168.1.205" }
$TdUser     = if ($env:TD_USER)     { $env:TD_USER }     else { "dbc" }
$TdPassword = if ($env:TD_PASSWORD) { $env:TD_PASSWORD } else { "dbc" }

Write-Output "Starting demo_b_all ..."
Start-Process -FilePath "powershell" -ArgumentList @(
    "-NoExit", "-Command",
    "`$env:TD_HOST='$TdHost'; `$env:TD_USER='$TdUser'; `$env:TD_PASSWORD='$TdPassword'; `$env:TD_DATABASE='DEMO_B'; `$env:KAFKA_BOOTSTRAP='localhost:9092'; `$env:KAFKA_DUMMY_DIR='C:\Windows\Temp'; `$env:KAFKA_AXSMOD_NAME='libkafkaaxsmod.dll'; `$env:KAFKA_IDLE_TIMEOUT='30'; `$env:KAFKA_GROUP_ID='tpt.demo_b'; & '$Root\scripts\run_tbuild.ps1' '$Root\tbuild\scenario_b\load_all.tbuild' -j demo_b_all -l 5 -z 10"
)
