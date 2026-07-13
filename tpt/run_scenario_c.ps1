# Launches Scenario C's 2 tiered TPT jobs as background processes, each in its own
# window: 1 dedicated job for the hot transaction table, 1 consolidated job for the
# 4 warm/cold tables.
#
# Usage: powershell -File run_scenario_c.ps1

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

$TdHost     = if ($env:TD_HOST)     { $env:TD_HOST }     else { "192.168.1.205" }
$TdUser     = if ($env:TD_USER)     { $env:TD_USER }     else { "dbc" }
$TdPassword = if ($env:TD_PASSWORD) { $env:TD_PASSWORD } else { "dbc" }

Write-Output "Starting demo_c_transaction (tier 1: hot) ..."
Start-Process -FilePath "powershell" -ArgumentList @(
    "-NoExit", "-Command",
    "`$env:TD_HOST='$TdHost'; `$env:TD_USER='$TdUser'; `$env:TD_PASSWORD='$TdPassword'; `$env:TD_DATABASE='DEMO_C'; `$env:KAFKA_BOOTSTRAP='localhost:9092'; `$env:KAFKA_DUMMY_DIR='C:\Windows\Temp'; `$env:KAFKA_AXSMOD_NAME='libkafkaaxsmod.dll'; `$env:KAFKA_IDLE_TIMEOUT='30'; `$env:KAFKA_TOPIC='cdc.public.transaction'; `$env:KAFKA_GROUP_ID='tpt.demo_c.transaction'; & '$Root\scripts\run_tbuild.ps1' '$Root\tbuild\scenario_c\load_transaction.tbuild' -j demo_c_transaction -l 5 -z 10"
)

Write-Output "Starting demo_c_warm (tier 2: consolidated customer/account/card/payment) ..."
Start-Process -FilePath "powershell" -ArgumentList @(
    "-NoExit", "-Command",
    "`$env:TD_HOST='$TdHost'; `$env:TD_USER='$TdUser'; `$env:TD_PASSWORD='$TdPassword'; `$env:TD_DATABASE='DEMO_C'; `$env:KAFKA_BOOTSTRAP='localhost:9092'; `$env:KAFKA_DUMMY_DIR='C:\Windows\Temp'; `$env:KAFKA_AXSMOD_NAME='libkafkaaxsmod.dll'; `$env:KAFKA_IDLE_TIMEOUT='30'; `$env:KAFKA_GROUP_ID='tpt.demo_c.warm'; & '$Root\scripts\run_tbuild.ps1' '$Root\tbuild\scenario_c\load_warm.tbuild' -j demo_c_warm -l 5 -z 10"
)
