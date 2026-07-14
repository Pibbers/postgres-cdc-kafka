import argparse
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from faker import Faker

fake = Faker()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")

TOPICS = {
    "merchant": "csv.merchant",
    "branch": "csv.branch",
    "fx_rate": "csv.fx_rate",
}

CATEGORIES = ["RETAIL", "GROCERY", "RESTAURANT", "FUEL", "TRAVEL", "ONLINE"]
STATUSES = ["ACTIVE", "SUSPENDED", "CLOSED"]
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
COUNTRIES = ["US", "GB", "DE", "FR", "CA", "AU", "JP"]
CURRENCY_PAIRS = ["USD/EUR", "USD/GBP", "USD/JPY", "EUR/GBP", "USD/CAD", "USD/CHF", "USD/AUD", "EUR/JPY"]

_producer = None


def producer():
    global _producer
    if _producer is None:
        _producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    return _producer


def csv_safe(s):
    # Bare delimited format, no quote-escaping - strip anything that would corrupt field boundaries.
    return str(s).replace(",", " ").replace("\n", " ").replace("\r", " ")


def now_ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def publish(table, op_code, event_ts, fields):
    row = ",".join([op_code, event_ts] + [csv_safe(f) for f in fields])
    producer().produce(TOPICS[table], value=row.encode("utf-8"))
    producer().poll(0)


class IdPool:
    """In-memory ID tracking - this generator writes straight to Kafka with no
    backing datastore to read state from, unlike load-generator/generator.py's
    Postgres-backed IdSequence. Starts empty; naturally fills via insert events."""

    def __init__(self, start=1):
        self.ids = []
        self._next = start
        self._lock = threading.Lock()

    def next_id(self):
        with self._lock:
            v = self._next
            self._next += 1
            return v

    def add(self, row_id):
        self.ids.append(row_id)

    def remove(self, row_id):
        if row_id in self.ids:
            self.ids.remove(row_id)

    def random(self):
        return random.choice(self.ids) if self.ids else None


pools = {"merchant": IdPool(), "branch": IdPool(), "fx_rate": IdPool()}


def merchant_fields():
    return [
        fake.company()[:100],
        random.choice(CATEGORIES),
        fake.city()[:50],
        random.choice(COUNTRIES),
        random.choice(STATUSES),
    ]


def branch_fields():
    return [
        f"{fake.city()} Branch"[:100],
        fake.city()[:50],
        random.choice(REGIONS),
        random.choice(STATUSES),
    ]


def fx_rate_fields():
    return [
        random.choice(CURRENCY_PAIRS),
        f"{round(random.uniform(0.5, 1.6), 6)}",
        datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    ]


FIELD_BUILDERS = {"merchant": merchant_fields, "branch": branch_fields, "fx_rate": fx_rate_fields}


def seed(counts=None):
    counts = counts or {"merchant": 50, "branch": 30, "fx_rate": 15}
    for table, n in counts.items():
        for _ in range(n):
            row_id = pools[table].next_id()
            publish(table, "I", now_ts(), [str(row_id)] + FIELD_BUILDERS[table]())
            pools[table].add(row_id)
        print(f"seeded {n} {table} rows")
    producer().flush()


class BurstControl:
    def __init__(self):
        self.until = 0.0
        self.rate = float(os.getenv("CSV_LOAD_GEN_STEADY_RATE", "2"))
        self.burst_rate = float(os.getenv("CSV_LOAD_GEN_BURST_RATE", "60"))
        self.burst_seconds = float(os.getenv("CSV_LOAD_GEN_BURST_SECONDS", "20"))

    def trigger(self):
        self.until = time.time() + self.burst_seconds

    def current_rate(self):
        return self.burst_rate if time.time() < self.until else self.rate


burst = BurstControl()


def random_event():
    # Weighted toward fx_rate - exchange rates update frequently in practice.
    table = random.choices(["fx_rate", "merchant", "branch"], weights=[60, 25, 15])[0]
    pool = pools[table]
    op = random.choices(["insert", "update", "delete"], weights=[50, 40, 10])[0]

    try:
        if op == "insert" or not pool.ids:
            row_id = pool.next_id()
            publish(table, "I", now_ts(), [str(row_id)] + FIELD_BUILDERS[table]())
            pool.add(row_id)
        elif op == "update":
            row_id = pool.random()
            publish(table, "U", now_ts(), [str(row_id)] + FIELD_BUILDERS[table]())
        else:
            row_id = pool.random()
            publish(table, "D", now_ts(), [str(row_id)] + FIELD_BUILDERS[table]())
            pool.remove(row_id)
    except Exception as e:
        print(f"event error ({table}/{op}): {e}")


def run_generation():
    print(f"csv generator running: steady={burst.rate}/s burst={burst.burst_rate}/s for {burst.burst_seconds}s")
    while True:
        rate = burst.current_rate()
        random_event()
        time.sleep(max(1.0 / rate, 0.01))


def run_control_api():
    from fastapi import FastAPI
    import uvicorn

    app = FastAPI()

    @app.post("/burst")
    def trigger_burst():
        burst.trigger()
        return {"status": "burst triggered", "until": burst.until, "rate": burst.burst_rate}

    @app.get("/status")
    def status():
        return {"current_rate": burst.current_rate(), "burst_active": time.time() < burst.until}

    uvicorn.run(app, host="0.0.0.0", port=8092, log_level="warning")


def manual_op(table, op, row_id):
    pool = pools[table]
    if op == "insert":
        new_id = pool.next_id()
        publish(table, "I", now_ts(), [str(new_id)] + FIELD_BUILDERS[table]())
        pool.add(new_id)
        producer().flush()
        print(f"manual insert into {table}.id={new_id} done at {datetime.now(timezone.utc).isoformat()}")
        return
    if op == "update":
        publish(table, "U", now_ts(), [str(row_id)] + FIELD_BUILDERS[table]())
    elif op == "delete":
        publish(table, "D", now_ts(), [str(row_id)] + FIELD_BUILDERS[table]())
        pool.remove(row_id)
    producer().flush()
    print(f"manual {op} on {table}.id={row_id} done at {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["seed", "run", "manual"], required=True)
    parser.add_argument("--table", choices=["merchant", "branch", "fx_rate"])
    parser.add_argument("--op", choices=["insert", "update", "delete"])
    parser.add_argument("--id", type=int)
    args = parser.parse_args()

    if args.mode == "seed":
        seed()
    elif args.mode == "manual":
        manual_op(args.table, args.op, args.id)
    else:
        t = threading.Thread(target=run_control_api, daemon=True)
        t.start()
        seed()
        run_generation()
