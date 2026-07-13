# Kafka CDC → TPT → Teradata Demo

One Postgres CDC source (5 tables) feeds three concurrent Kafka→TPT→Teradata topologies, so a room can watch "500 independent jobs doesn't scale, consolidate instead" play out live, side by side, against the same data:

| Scenario | Shape | What it shows |
|---|---|---|
| **A** | 5 topics → **5 independent TPT jobs** → 5 landing tables → 5 target tables | The "obvious" approach — full isolation, but 5 processes to run/monitor/restart here, 500 at real scale. |
| **B** | 5 topics → **1 consolidated TPT job** (multi-topic `UNION ALL`) → 1 landing table → fan-out merge → 5 target tables | The recommended production pattern: one job handles every topic, and handles 500 exactly the same way it handles 5. |
| **C** | 5 topics → **2 tiered TPT jobs** (1 dedicated job for the hot `transaction` table + 1 consolidated job for the 4 warm/cold tables) → 5 target tables | The middle ground: per-table isolation only where it matters, consolidation everywhere else — the same tiering the 500-table production recommendation uses. |

All three run continuously against the **same live CDC stream**, landing into three parallel Teradata schemas (`DEMO_A`, `DEMO_B`, `DEMO_C`).

---

## Architecture

- **PostgreSQL 16** (Docker) — 5 tables (`customer`, `account`, `card`, `payment`, `transaction`), logical replication, `REPLICA IDENTITY FULL`.
- **Kafka (KRaft) + Kafka Connect** (Docker) — one Debezium PostgreSQL connector across all 5 tables, producing `cdc.public.<table>` topics with the full Debezium JSON envelope (not the unwrapped form — the envelope's `before`/`after`/`op`/`source.ts_ms` fields are parsed downstream, in Teradata, at merge time).
- **Kafka UI** (Docker) — inspect topics, messages, consumer groups, the connector.
- **Load generator** (Docker) — seeds Postgres and then drives a continuous mix of INSERT/UPDATE/DELETE, weighted toward `transaction`; supports a burst mode and manual ad-hoc ops.
- **TPT (`tbuild`) jobs** — one Kafka Access Module (`libkafkaaxsmod`) reader per topic, streaming into Teradata via the `STREAM` operator. **Run as native Windows processes**
- **BTEQ DDL + merge routines** — Idempotent `MERGE` per table (dedup by business key + `source.ts_ms`, "latest wins"), soft-delete via `cdc_deleted_ind`/`cdc_operation_cd`.
- **Dashboard** (native Python/FastAPI + Chart.js, port 8091) — live row counts, per-table landing latency and landing-table backlog, per-scenario resource overhead (CPU/memory/process/session count), TPT job status, and buttons to burst-load, kill, and restart individual TPT jobs; burst triggers are annotated directly on the latency chart. Doubles as the metrics collector (no separate service).

Live Teradata target used during development: `192.168.1.205` (`dbc`/`dbc`). Point `.env`/your shell environment at a different target if needed — nothing is hardcoded beyond the defaults baked into the scripts below.

---

## Prerequisites

- Docker Desktop
- Native Teradata Tools and Utilities (TTU) 20.00 installed at `C:\Program Files\Teradata\Client\20.00\` (provides `bteq.exe`, `tbuild.exe`, `libkafkaaxsmod.dll`)
- Python 3.12+ on the host, with `pip install teradatasql` (used by `tpt/scripts/run_procedure.py` and the dashboard) — `dashboard/requirements.txt` and `load-generator/requirements.txt` cover the rest
- PowerShell (the native-side scripts are `.ps1`; POSIX equivalents `run_bteq.sh`/`run_tbuild.sh` exist in `tpt/scripts/` for a Linux/podman dev box, following the same `${VAR}`/`$(VAR)` substitution convention, but are unused on this machine)

---

## Repo layout

```
postgres/init.sql                 5-table source DDL, debezium role, publication
connectors/                       Debezium connector config + registration script
load-generator/                   seed / steady / burst / manual CDC load generator (Docker)
teradata/
  ddl/                            00_databases, 01/02/03_scenario_{a,b,c}, 04_admin, 06_scenario_c_tiered
  merge/                          idempotent MERGE routines, one per (scenario, table)
  run_merge_loop.ps1              loops every merge script on a short interval
  reset.bteq                      truncates all landing/target/admin tables for a clean rehearsal
tpt/
  tbuild/scenario_{a,b,c}/*.tbuild   the actual TPT job definitions
  scripts/run_bteq.ps1, run_tbuild.ps1, run_procedure.py   native-execution runners (env-var substitution)
  run_scenario_{a,b,c}.ps1        launches each scenario's job(s) in their own visible windows
dashboard/                        FastAPI + Chart.js dashboard / metrics collector (native)
docker-compose.yml                postgres, kafka, kafka-connect, kafka-ui, load-generator
```

---

## Quick start

Run these in order. Steps 1–4 are Docker-only and fully self-contained. Steps 5+ need the native TTU client and talk to the real Teradata target.

### 1. Configure environment

```bash
cp .env.example .env
```

`.env` is read by `docker-compose.yml` (Postgres creds, Kafka bootstrap for *container-to-container* use, load generator pacing). It is **not** read by the native PowerShell scripts below — those default to `TD_HOST=192.168.1.205`, `TD_USER=dbc`, `TD_PASSWORD=dbc`, `KAFKA_BOOTSTRAP=localhost:9092` (the host-mapped port, different from the container-internal `kafka:19092` in `.env`). Override by setting `$env:TD_HOST` etc. in your PowerShell session before running them if you need a different target.

### 2. Start the Docker stack

```powershell
bash ./scripts/start.sh postgres kafka kafka-connect kafka-ui load-generator
```

(`start.sh` uses Docker Desktop directly on this machine; it falls back to Podman's API socket automatically if `docker info` doesn't already succeed, e.g. on a Linux dev box. Run it via `bash` explicitly — PowerShell has no shebang support, so invoking `./scripts/start.sh` directly is a silent no-op.)

### 3. Register the Debezium connector

```powershell
bash ./connectors/register-connector.sh
```

Open http://localhost:8080 (Kafka UI) and confirm all 5 `cdc.public.*` topics appear with the initial snapshot.

### 4. Seed Postgres

The load-generator container defaults to continuous (`--mode run`); seeding is a separate one-off invocation:

```bash
docker compose run --rm load-generator --mode seed
```

This inserts ~500 customers, 800 accounts, 1000 cards, 1500 payments, 3000 transactions (FK-ordered). The already-running `load-generator` service then keeps generating live INSERT/UPDATE/DELETE traffic continuously (2/sec steady by default, weighted toward `transaction`).

### 5. Create the Teradata schema

From PowerShell, with the TTU client on PATH-equivalent (full path used explicitly by the scripts):

```powershell
$env:TD_HOST="192.168.1.205"; $env:TD_USER="dbc"; $env:TD_PASSWORD="dbc"
foreach ($f in "00_databases","01_scenario_a","02_scenario_b","03_scenario_c","04_admin","06_scenario_c_tiered") {
  powershell -File tpt\scripts\run_bteq.ps1 "teradata\ddl\$f.bteq"
}
```

This creates `DEMO_A`/`DEMO_B`/`DEMO_C`/`DEMO_ADMIN` (conservative `PERM` sizing — this is a shared lab system, not dedicated capacity) and every landing/target/admin table. Safe to re-run — DDL scripts tolerate "already exists" (informational, not fatal).

### 6. Start the TPT jobs

Each launcher opens its job(s) in their own visible PowerShell window (`-NoExit`) — deliberately, so a live audience can see "N processes running" and you can close a window to kill one:

```powershell
powershell -File tpt\run_scenario_a.ps1   # 5 windows: 1 per table
powershell -File tpt\run_scenario_b.ps1   # 1 window: the consolidated job
powershell -File tpt\run_scenario_c.ps1   # 2 windows: hot-tier + warm/cold-tier
```

These launcher scripts **do not clear TPT checkpoints** before starting — if a job name has stale checkpoint state from a prior run (see Known limitations below), start via the dashboard instead (its `/start` endpoint always clears first), or delete `<job_name>CPD1`/`CPD2`/`LVCP` under `C:\Program Files\Teradata\client\20.00\Teradata Parallel Transporter\checkpoint\` manually first.

### 7. Start the merge loop

```powershell
powershell -File teradata\run_merge_loop.ps1 -IntervalSeconds 3
```

Runs every merge script (all of A/B/C) back-to-back, sleeps, repeats. Landing → target latency is dominated by this interval plus the TPT `-l 5` flush.

### 8. Start the dashboard

```powershell
$env:TD_HOST="192.168.1.205"; $env:TD_USER="dbc"; $env:TD_PASSWORD="dbc"; $env:KAFKA_BOOTSTRAP="localhost:9092"
py dashboard\app.py
```

Open **http://localhost:8091** — live row counts, landing latency, and landing-table backlog per scenario/table, per-scenario resource overhead (CPU/memory/process count, and Teradata session count derived from job status), TPT job status, and buttons for burst load / kill / restart per job. Burst triggers are marked directly on the latency chart. The dashboard also owns job lifecycle for anything you start *through* it (its kill/restart act on the actual Windows process tree it spawned via `taskkill /T /F`); jobs started via the `run_scenario_*.ps1` scripts directly are visible in their own windows but not dashboard-controlled unless started through the dashboard's `/api/job/{name}/start`.

**"Start everything" / "Stop everything"** launch or kill all 8 TPT jobs plus the merge loop (`teradata/run_merge_loop.ps1`) in one click — same fresh-checkpoint-clearing behavior as per-job start. The merge loop is detected the same way as externally-launched TPT jobs (matched by process command line), so a loop started manually via step 7 above still shows up and can be stopped/sped up from the dashboard.

**Merge speed toggle** kills and relaunches the merge loop with a different `-IntervalSeconds` (3s normal / 1s fast) via `/api/merge/interval/{seconds}` — there's no way to change a running loop's sleep duration in place, so this is a restart under the hood. Landing tables are unaffected (merge is idempotent).

---

## Load generator controls

- **Burst button** (dashboard) or `curl -X POST http://localhost:8090/burst` — switches to 100/sec (weighted toward `transaction`) for 20s, then reverts to steady.
- **Manual one-off ops**, useful for a live "watch this specific row land" moment:
  ```bash
  docker compose run --rm load-generator --mode manual --table transaction --op insert
  docker compose run --rm load-generator --mode manual --table transaction --op update --id 4976
  docker compose run --rm load-generator --mode manual --table transaction --op delete --id 4976
  ```
- Status: `curl http://localhost:8090/status`

---

## Verifying things work

The dashboard (http://localhost:8091) is the easiest way — row counts, landing latency, and backlog per scenario/table update every ~3s.

To check directly, save a small script and run it with `py`, e.g. `check.py`:

```python
import teradatasql
con = teradatasql.connect(host="192.168.1.205", user="dbc", password="dbc")
cur = con.cursor()
cur.execute('SELECT COUNT(*), MAX(td_update_ts) FROM DEMO_A."TRANSACTION"')
print(cur.fetchall())
```

(Note `"TRANSACTION"` and `"ACCOUNT"` need double-quoting in any SQL you write by hand — they're Teradata reserved words.)

To confirm a specific end-to-end flow: insert or update a known row in Postgres, then check it landed in all three of `DEMO_A."TRANSACTION"`, `DEMO_B."TRANSACTION"`, `DEMO_C."TRANSACTION"` within a few seconds.

---

## Reset for a clean rehearsal run

1. Stop all TPT job windows (or kill via the dashboard).
2. Truncate every landing/target/admin table:
   ```powershell
   $env:TD_HOST="192.168.1.205"; $env:TD_USER="dbc"; $env:TD_PASSWORD="dbc"
   powershell -File tpt\scripts\run_bteq.ps1 teradata\reset.bteq
   ```
3. Clear TPT checkpoints so jobs start fresh rather than attempting to resume:
   ```powershell
   Remove-Item "C:\Program Files\Teradata\client\20.00\Teradata Parallel Transporter\checkpoint\demo_*" -Force
   ```
4. Re-seed Postgres (`docker compose run --rm load-generator --mode seed`) if you also want the source data reset — note this doesn't delete existing Postgres rows, only re-inserts seed rows with `ON CONFLICT DO NOTHING`; for a true from-zero Postgres reset, recreate the `postgres` container/volume instead.
5. Restart the TPT jobs (step 6 above) and the merge loop.

---

## Stopping

```powershell
bash ./scripts/stop.sh
```

Stops the Docker stack. As with `start.sh`, run it via `bash` explicitly — PowerShell won't execute `./scripts/stop.sh` directly. Native processes (TPT job windows, the merge loop, the dashboard) need to be stopped separately — close their windows, `Ctrl+C`, or use the dashboard's kill buttons.

---

## Known limitations

- **Restart doesn't do an exact Kafka-offset resume.** The Kafka Access Module's checkpoint mechanism proved unreliable on this setup (`Current Message offset ... is less than the last message offset read` even on a legitimate same-group restart) — the dashboard's restart clears checkpoints and re-reads from the topic start instead. Correctness is unaffected (the idempotent MERGE dedupes and upserts), just not maximally efficient. If demoing the failure/recovery moment, frame it as "resumes with no data loss or duplication," not "resumes from the exact last offset."
- **True consumer lag isn't shown.** TPT's Kafka Access Module doesn't join Kafka's standard consumer-group protocol (its jobs never appear in `kafka-consumer-groups.sh --list`), so lag isn't observable via the standard admin API. The dashboard shows job status (running/stopped) plus landing-table backlog (rows not yet merged+purged) as the closest available proxy for "is this pipeline falling behind."
- **Teradata session counts are derived, not queried live.** The dashboard's per-scenario session count is `running jobs × 2` (empirically confirmed sessions-per-job for a STREAM operator with no explicit `MaxSessions`), not a live `DBC.SessionInfoV` query — Teradata doesn't promptly release sessions after a killed job's `tbuild.exe` is `taskkill`'d, which would otherwise show stale/inflated numbers right at the kill/restart demo beat.
- Account/Payment tables in Scenario A were spot-verified but not exhaustively load-tested (Customer, Card, and Transaction were the tables put through the most iteration).
- No TLS/SASL, no PII masking — fine for a demo, caveat if presenting to a security-conscious audience.
