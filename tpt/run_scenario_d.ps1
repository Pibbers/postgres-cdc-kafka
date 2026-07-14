# Launches Scenario D's single TPT job (3 CSV topics -> 1 job -> 3 target tables,
# zero landing tables) as a background process in its own window.
#
# Usage: powershell -File run_scenario_d.ps1

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

$TdHost     = if ($env:TD_HOST)     { $env:TD_HOST }     else { "192.168.1.205" }
$TdUser     = if ($env:TD_USER)     { $env:TD_USER }     else { "dbc" }
$TdPassword = if ($env:TD_PASSWORD) { $env:TD_PASSWORD } else { "dbc" }

Write-Output "Starting demo_d_all ..."
Start-Process -FilePath "powershell" -ArgumentList @(
    "-NoExit", "-Command",
    "`$env:TD_HOST='$TdHost'; `$env:TD_USER='$TdUser'; `$env:TD_PASSWORD='$TdPassword'; `$env:TD_DATABASE='DEMO_D'; `$env:KAFKA_BOOTSTRAP='localhost:9092'; `$env:KAFKA_DUMMY_DIR='C:\Windows\Temp'; `$env:KAFKA_AXSMOD_NAME='libkafkaaxsmod.dll'; `$env:KAFKA_IDLE_TIMEOUT='30'; `$env:KAFKA_GROUP_ID='tpt.demo_d'; & '$Root\scripts\run_tbuild.ps1' '$Root\tbuild\scenario_d\load_all.tbuild' -j demo_d_all -l 5 -z 10"
)
