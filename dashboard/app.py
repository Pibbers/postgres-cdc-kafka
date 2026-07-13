import json
import os
import re
import subprocess
import threading
import time

import requests
import teradatasql
import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# Note: TPT's Kafka Access Module (-X group.id=) does not join the standard Kafka
# consumer-group protocol (JoinGroup/SyncGroup) - confirmed empirically, a running
# TPT job's group never appears in `kafka-consumer-groups.sh --list`. So consumer
# lag isn't observable through the standard admin API for these jobs; the
# dashboard relies on job status (running/stopped) plus Teradata-side row counts,
# latency, and landing-table backlog (rows not yet merged+purged, the closest
# available proxy for lag) instead, which are directly observable and are what
# Section 9's "most persuasive number in the room" (latency) actually needs.

TD_HOST = os.getenv("TD_HOST", "192.168.1.205")
TD_USER = os.getenv("TD_USER", "dbc")
TD_PASSWORD = os.getenv("TD_PASSWORD", "dbc")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
LOAD_GEN_URL = os.getenv("LOAD_GEN_URL", "http://localhost:8090")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "3"))

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TPT_ROOT = os.path.join(REPO_ROOT, "tpt")
RUN_TBUILD = os.path.join(TPT_ROOT, "scripts", "run_tbuild.ps1")
MERGE_LOOP_SCRIPT = os.path.join(REPO_ROOT, "teradata", "run_merge_loop.ps1")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

MERGE_NORMAL_INTERVAL = 3
MERGE_FAST_INTERVAL = 1

# Empirically confirmed (2026-07-13, against 192.168.1.205) across scenario
# A/B/C job types: a TPT STREAM operator with no explicit MaxSessions
# attribute opens exactly 2 Teradata sessions, regardless of how many
# DATACONNECTOR PRODUCER operators feed it (verified for scenario B's
# 5-producer job too). Session count is derived from job status rather than
# a live DBC.SessionInfoV query because Teradata doesn't promptly release
# sessions after a job's tbuild.exe is taskkill'd - a live query showed the
# same 2 sessions still "open" 20+ seconds after the process was gone, which
# would read as stale/inflated right at the "kill it, watch it recover" demo
# beat. If a .tbuild script ever sets an explicit MaxSessions, this constant
# needs revisiting.
TPT_SESSIONS_PER_JOB = 2

# --- Job registry: everything needed to launch/track/kill each TPT job ---
JOBS = {
    "demo_a_customer": dict(script="scenario_a/load_customer.tbuild", db="DEMO_A", topic="cdc.public.customer", group="tpt.demo_a.customer", scenario="A", tables=["customer"]),
    "demo_a_account": dict(script="scenario_a/load_account.tbuild", db="DEMO_A", topic="cdc.public.account", group="tpt.demo_a.account", scenario="A", tables=["account"]),
    "demo_a_card": dict(script="scenario_a/load_card.tbuild", db="DEMO_A", topic="cdc.public.card", group="tpt.demo_a.card", scenario="A", tables=["card"]),
    "demo_a_payment": dict(script="scenario_a/load_payment.tbuild", db="DEMO_A", topic="cdc.public.payment", group="tpt.demo_a.payment", scenario="A", tables=["payment"]),
    "demo_a_transaction": dict(script="scenario_a/load_transaction.tbuild", db="DEMO_A", topic="cdc.public.transaction", group="tpt.demo_a.transaction", scenario="A", tables=["transaction"]),
    "demo_b_all": dict(script="scenario_b/load_all.tbuild", db="DEMO_B", topic=None, group="tpt.demo_b", scenario="B", tables=["customer", "account", "card", "payment", "transaction"]),
    "demo_c_transaction": dict(script="scenario_c/load_transaction.tbuild", db="DEMO_C", topic="cdc.public.transaction", group="tpt.demo_c.transaction", scenario="C", tables=["transaction"]),
    "demo_c_warm": dict(script="scenario_c/load_warm.tbuild", db="DEMO_C", topic=None, group="tpt.demo_c.warm", scenario="C", tables=["customer", "account", "card", "payment"]),
}

LANDING_TABLES = {
    ("A", "customer"): "DEMO_A.CUSTOMER_LANDING", ("A", "account"): "DEMO_A.ACCOUNT_LANDING",
    ("A", "card"): "DEMO_A.CARD_LANDING", ("A", "payment"): "DEMO_A.PAYMENT_LANDING",
    ("A", "transaction"): "DEMO_A.TRANSACTION_LANDING",
    ("B", "*"): "DEMO_B.CDC_LANDING",
    ("C", "transaction"): "DEMO_C.TRANSACTION_LANDING", ("C", "warm"): "DEMO_C.WARM_LANDING",
}

TARGET_TABLES = {
    ("A", "customer"): "DEMO_A.CUSTOMER", ("A", "account"): 'DEMO_A."ACCOUNT"', ("A", "card"): "DEMO_A.CARD",
    ("A", "payment"): "DEMO_A.PAYMENT", ("A", "transaction"): 'DEMO_A."TRANSACTION"',
    ("B", "customer"): "DEMO_B.CUSTOMER", ("B", "account"): 'DEMO_B."ACCOUNT"', ("B", "card"): "DEMO_B.CARD",
    ("B", "payment"): "DEMO_B.PAYMENT", ("B", "transaction"): 'DEMO_B."TRANSACTION"',
    ("C", "customer"): "DEMO_C.CUSTOMER", ("C", "account"): 'DEMO_C."ACCOUNT"', ("C", "card"): "DEMO_C.CARD",
    ("C", "payment"): "DEMO_C.PAYMENT", ("C", "transaction"): 'DEMO_C."TRANSACTION"',
}

processes = {}  # job_name -> Popen, for jobs this dashboard process launched itself
external_pids = {}  # job_name -> pid, for jobs discovered running but launched elsewhere
merge_process = None  # Popen, if the merge loop was launched by this dashboard process
merge_external_pid = None  # pid, if the merge loop is running but was launched elsewhere
merge_interval = MERGE_NORMAL_INTERVAL  # best-known -IntervalSeconds of the running loop
metrics = {"scenarios": {}, "merge_loop": {}, "updated_at": None, "burst_events": []}
metrics_lock = threading.Lock()

_cpu_samples = {}  # pid -> (timestamp, cumulative_cpu_100ns), for CPU% deltas across polls
burst_events = []  # epoch-seconds of recent /api/burst triggers, for chart annotation

CHECKPOINT_DIR = r"C:\Program Files\Teradata\client\20.00\Teradata Parallel Transporter\checkpoint"


def clear_checkpoint(job_name):
    # A job's local checkpoint (<job_name>CPD1/CPD2/LVCP) records the last offset
    # TPT believes it read. If that ever diverges from Kafka's actual state for
    # the job's group.id (e.g. the group.id was reused across a different job
    # run), the Kafka Access Module refuses to resume ("Current Message offset
    # is less than the last message offset read ... Messages may have been
    # deleted") and the job aborts immediately. Clearing before a *fresh* start
    # avoids that; a *restart* after a kill deliberately skips this, since
    # resuming from checkpoint with no data loss is the point of that demo beat.
    for suffix in ("CPD1", "CPD2", "LVCP"):
        path = os.path.join(CHECKPOINT_DIR, f"{job_name}{suffix}")
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def launch_job(job_name, fresh=False):
    if fresh:
        clear_checkpoint(job_name)
    spec = JOBS[job_name]
    env = os.environ.copy()
    env["TD_HOST"] = TD_HOST
    env["TD_USER"] = TD_USER
    env["TD_PASSWORD"] = TD_PASSWORD
    env["TD_DATABASE"] = spec["db"]
    env["KAFKA_BOOTSTRAP"] = KAFKA_BOOTSTRAP
    env["KAFKA_DUMMY_DIR"] = "C:\\Windows\\Temp"
    env["KAFKA_AXSMOD_NAME"] = "libkafkaaxsmod.dll"
    env["KAFKA_IDLE_TIMEOUT"] = "30"
    env["KAFKA_GROUP_ID"] = spec["group"]
    if spec["topic"]:
        env["KAFKA_TOPIC"] = spec["topic"]

    script_path = os.path.join(TPT_ROOT, "tbuild", spec["script"])
    cmd = ["powershell", "-File", RUN_TBUILD, script_path, "-j", job_name, "-l", "5", "-z", "10"]
    p = subprocess.Popen(cmd, env=env, creationflags=subprocess.CREATE_NEW_CONSOLE)
    processes[job_name] = p
    return p.pid


def kill_job(job_name):
    p = processes.get(job_name)
    if p and p.poll() is None:
        subprocess.run(["taskkill", "/PID", str(p.pid), "/T", "/F"], capture_output=True)
        p.wait(timeout=5)
        return
    # keep the (now-terminated) handle so job_status() reports "stopped", not
    # "not_started" - the dashboard's kill/restart narrative depends on that
    # distinction being visible.
    pid = external_pids.pop(job_name, None)
    if pid:
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True)


def discover_external_processes():
    # Jobs/the merge loop are normally launched via tpt/run_scenario_{a,b,c}.ps1
    # and teradata/run_merge_loop.ps1 as independent detached processes, not
    # through this dashboard's own endpoints - so `processes`/`merge_process`
    # have no Popen handle for them. Find TPT jobs by the `-j <job_name>`
    # argument each tbuild.exe was started with, and the merge loop by matching
    # its script name among running powershell.exe processes.
    #
    # WorkingSetSize/KernelModeTime/UserModeTime are pulled in the same call so
    # the resource-overhead panel doesn't need a second round-trip; they're only
    # meaningful (and only collected) for the actual tbuild.exe worker, not the
    # powershell.exe wrapper that launched it, since that's what "the job costs
    # this much CPU/memory" should reflect.
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process -Filter \"Name='tbuild.exe' OR Name='powershell.exe'\" "
             "| Select-Object ProcessId,Name,CommandLine,WorkingSetSize,KernelModeTime,UserModeTime "
             "| ConvertTo-Json -Compress"],
            capture_output=True, text=True, timeout=10,
        )
        data = json.loads(result.stdout or "[]")
        if isinstance(data, dict):
            data = [data]
    except Exception:
        return {}, None, None, {}
    jobs_found = {}
    resource_by_job = {}
    merge_pid, merge_interval_found = None, None
    for proc in data:
        cmdline = proc.get("CommandLine") or ""
        m = re.search(r"-j\s+(\S+)", cmdline)
        if m and m.group(1) in JOBS:
            jobs_found[m.group(1)] = proc.get("ProcessId")
            if proc.get("Name") == "tbuild.exe":
                resource_by_job[m.group(1)] = {
                    "pid": proc.get("ProcessId"),
                    "memory_mb": (proc.get("WorkingSetSize") or 0) / (1024 * 1024),
                    "cpu_100ns": (proc.get("KernelModeTime") or 0) + (proc.get("UserModeTime") or 0),
                }
        if "run_merge_loop.ps1" in cmdline:
            merge_pid = proc.get("ProcessId")
            im = re.search(r"-IntervalSeconds\s+(\d+)", cmdline)
            merge_interval_found = int(im.group(1)) if im else MERGE_NORMAL_INTERVAL
    return jobs_found, merge_pid, merge_interval_found, resource_by_job


def cpu_percent(pid, cpu_100ns, now):
    # Win32_Process's Kernel/UserModeTime are cumulative since process start
    # (100ns units), not a live %, so CPU% has to be derived from the delta
    # between two polls. Normalized by logical core count to match Task
    # Manager's convention (100% = one fully-loaded core, not "all cores").
    ncpu = os.cpu_count() or 1
    prev = _cpu_samples.get(pid)
    _cpu_samples[pid] = (now, cpu_100ns)
    if not prev:
        return 0.0
    prev_t, prev_cpu = prev
    dt = now - prev_t
    dcpu = cpu_100ns - prev_cpu
    if dt <= 0 or dcpu < 0:
        return 0.0
    return (dcpu / 1e7) / dt / ncpu * 100


def job_status(job_name):
    p = processes.get(job_name)
    if p is not None:
        return "running" if p.poll() is None else "stopped"
    return "running" if job_name in external_pids else "not_started"


def launch_merge_loop(interval):
    global merge_process, merge_interval
    kill_merge_loop()
    env = os.environ.copy()
    env["TD_HOST"] = TD_HOST
    env["TD_USER"] = TD_USER
    env["TD_PASSWORD"] = TD_PASSWORD
    cmd = ["powershell", "-File", MERGE_LOOP_SCRIPT, "-IntervalSeconds", str(interval)]
    p = subprocess.Popen(cmd, env=env, creationflags=subprocess.CREATE_NEW_CONSOLE)
    merge_process = p
    merge_interval = interval
    return p.pid


def kill_merge_loop():
    global merge_process, merge_external_pid
    p = merge_process
    if p and p.poll() is None:
        subprocess.run(["taskkill", "/PID", str(p.pid), "/T", "/F"], capture_output=True)
        p.wait(timeout=5)
    merge_process = None
    if merge_external_pid:
        subprocess.run(["taskkill", "/PID", str(merge_external_pid), "/T", "/F"], capture_output=True)
        merge_external_pid = None


def merge_loop_status():
    if merge_process is not None:
        return "running" if merge_process.poll() is None else "stopped"
    return "running" if merge_external_pid else "not_started"


# --- Metrics polling ---

def td_query(cur, sql):
    cur.execute(sql)
    return cur.fetchall()


def parse_interval_seconds(s):
    # Teradata "D HH:MM:SS.ffffff" interval string
    if s is None:
        return None
    s = s.strip()
    try:
        day_part, time_part = s.split(" ", 1)
        h, m, sec = time_part.split(":")
        return int(day_part) * 86400 + int(h) * 3600 + int(m) * 60 + float(sec)
    except Exception:
        return None


def poll_metrics():
    global external_pids, merge_external_pid, merge_interval
    while True:
        try:
            jobs_found, found_merge_pid, found_merge_interval, resource_by_job = discover_external_processes()
            external_pids = jobs_found
            now = time.time()
            job_resources = {
                job_name: {
                    "cpu_percent": round(cpu_percent(r["pid"], r["cpu_100ns"], now), 1),
                    "memory_mb": round(r["memory_mb"], 1),
                }
                for job_name, r in resource_by_job.items()
            }
            if merge_process is None:
                merge_external_pid = found_merge_pid
                if found_merge_interval:
                    merge_interval = found_merge_interval
            else:
                merge_external_pid = None
            con = teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASSWORD)
            cur = con.cursor()
            scenarios = {"A": {"tables": {}}, "B": {"tables": {}}, "C": {"tables": {}}}

            for (scenario, table), target in TARGET_TABLES.items():
                try:
                    rows = td_query(cur, f"SELECT COUNT(*), MAX(td_update_ts) FROM {target}")
                    count, max_ts = rows[0]
                except Exception:
                    count, max_ts = None, None
                scenarios[scenario]["tables"].setdefault(table, {})["row_count"] = count
                scenarios[scenario]["tables"][table]["max_td_update_ts"] = str(max_ts) if max_ts else None

            # latency: latest row's landing latency per landing table
            for (scenario, table_key), landing in LANDING_TABLES.items():
                ts_field_map = {"transaction": "transaction_ts", "payment": "payment_ts"}
                try:
                    rows = td_query(cur, f"""
                        SELECT (INGEST_TS -
                            (CAST(DATE '1970-01-01' + (CAST(PAYLOAD.source.ts_ms AS BIGINT)/1000/86400) AS TIMESTAMP(6))
                              + (((CAST(PAYLOAD.source.ts_ms AS BIGINT)/1000) MOD 86400) / 3600) * INTERVAL '1' HOUR
                              + ((CAST(PAYLOAD.source.ts_ms AS BIGINT)/1000) MOD 3600) * INTERVAL '1' SECOND
                              + (CAST(PAYLOAD.source.ts_ms AS BIGINT) MOD 1000) * INTERVAL '0.001' SECOND)
                        ) DAY(4) TO SECOND(6)
                        FROM (SELECT TOP 1 PAYLOAD, INGEST_TS FROM {landing} ORDER BY INGEST_TS DESC) t
                    """)
                    lat = parse_interval_seconds(rows[0][0]) if rows else None
                except Exception:
                    lat = None
                # Backlog: rows still sitting in the landing table, not yet
                # merged+purged. TPT's Kafka Access Module doesn't join Kafka's
                # consumer-group protocol so real consumer lag isn't observable
                # (see README "Known limitations") - this is the closest
                # available proxy for "is this pipeline falling behind."
                try:
                    rows2 = td_query(cur, f"SELECT COUNT(*) FROM {landing}")
                    backlog = rows2[0][0] if rows2 else None
                except Exception:
                    backlog = None
                if table_key == "*":
                    for t in scenarios[scenario]["tables"]:
                        scenarios[scenario]["tables"][t]["latency_seconds"] = lat
                        scenarios[scenario]["tables"][t]["backlog"] = backlog
                elif table_key == "warm":
                    for t in ["customer", "account", "card", "payment"]:
                        scenarios[scenario]["tables"].setdefault(t, {})["latency_seconds"] = lat
                        scenarios[scenario]["tables"].setdefault(t, {})["backlog"] = backlog
                else:
                    scenarios[scenario]["tables"].setdefault(table_key, {})["latency_seconds"] = lat
                    scenarios[scenario]["tables"].setdefault(table_key, {})["backlog"] = backlog

            con.close()

            for job_name, spec in JOBS.items():
                res = job_resources.get(job_name)
                scenarios[spec["scenario"]].setdefault("jobs", {})[job_name] = {
                    "status": job_status(job_name),
                    "cpu_percent": res["cpu_percent"] if res else None,
                    "memory_mb": res["memory_mb"] if res else None,
                }

            for scenario, sc in scenarios.items():
                jobs = sc.get("jobs", {})
                cpu_vals = [j["cpu_percent"] for j in jobs.values() if j["cpu_percent"] is not None]
                mem_vals = [j["memory_mb"] for j in jobs.values() if j["memory_mb"] is not None]
                running_count = sum(1 for j in jobs.values() if j["status"] == "running")
                sc["resources"] = {
                    "cpu_percent": round(sum(cpu_vals), 1) if cpu_vals else None,
                    "memory_mb": round(sum(mem_vals), 1) if mem_vals else None,
                    "process_count": len(mem_vals),
                    "sessions": running_count * TPT_SESSIONS_PER_JOB,
                }

            with metrics_lock:
                metrics["scenarios"] = scenarios
                metrics["merge_loop"] = {"status": merge_loop_status(), "interval": merge_interval}
                metrics["updated_at"] = time.time()
                metrics["burst_events"] = burst_events[-20:]
        except Exception as e:
            print("poll_metrics error:", e)
        time.sleep(POLL_INTERVAL)


app = FastAPI()


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


# Explainer pages (overview / per-scenario / scaling narrative) linked from the
# dashboard nav - served as plain static files, not part of the metrics API.
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/api/metrics")
def api_metrics():
    with metrics_lock:
        return JSONResponse(metrics)


@app.post("/api/burst")
def api_burst():
    try:
        r = requests.post(f"{LOAD_GEN_URL}/burst", timeout=5)
        burst_events.append(time.time())
        del burst_events[:-20]
        return r.json()
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/job/{job_name}/start")
def api_start(job_name: str):
    if job_name not in JOBS:
        return JSONResponse({"error": "unknown job"}, status_code=404)
    if job_status(job_name) == "running":
        return JSONResponse({"error": "job already running"}, status_code=409)
    pid = launch_job(job_name, fresh=True)
    return {"status": "started", "pid": pid}


@app.post("/api/job/{job_name}/kill")
def api_kill(job_name: str):
    if job_name not in JOBS:
        return JSONResponse({"error": "unknown job"}, status_code=404)
    kill_job(job_name)
    return {"status": "killed"}


@app.post("/api/job/{job_name}/restart")
def api_restart(job_name: str):
    if job_name not in JOBS:
        return JSONResponse({"error": "unknown job"}, status_code=404)
    # True offset-based resume (checkpoint preserved) is unreliable against this
    # Kafka Access Module build - it errors ("Current Message offset ... is less
    # than the last message offset read") instead of seeking to the checkpointed
    # position. Clearing on restart too means each restart re-reads from the
    # topic's start rather than resuming exactly - correctness is unaffected
    # (the merge routines dedupe/upsert idempotently), so "kill -> other jobs
    # unaffected -> restart -> flowing again with no data loss or duplication"
    # still holds; it's just not an *exact* offset resume.
    kill_job(job_name)
    time.sleep(1)
    pid = launch_job(job_name, fresh=True)
    return {"status": "restarted", "pid": pid}


@app.post("/api/start_all")
def api_start_all():
    started = [name for name in JOBS if job_status(name) != "running"]
    for job_name in started:
        launch_job(job_name, fresh=True)
    if merge_loop_status() != "running":
        launch_merge_loop(merge_interval)
    return {"status": "started_all", "jobs": started}


@app.post("/api/stop_all")
def api_stop_all():
    for job_name in JOBS:
        kill_job(job_name)
    kill_merge_loop()
    return {"status": "stopped_all"}


@app.post("/api/merge/interval/{seconds}")
def api_merge_interval(seconds: int):
    if seconds < 1 or seconds > 60:
        return JSONResponse({"error": "interval must be between 1 and 60 seconds"}, status_code=400)
    pid = launch_merge_loop(seconds)
    return {"status": "restarted", "interval": seconds, "pid": pid}


if __name__ == "__main__":
    t = threading.Thread(target=poll_metrics, daemon=True)
    t.start()
    uvicorn.run(app, host="0.0.0.0", port=8091)
