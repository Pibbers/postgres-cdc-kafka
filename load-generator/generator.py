import argparse
import os
import random
import threading
import time
from datetime import datetime, timezone

import psycopg2
from faker import Faker

fake = Faker()

DSN = dict(
    host=os.getenv("PGHOST", "postgres"),
    port=os.getenv("PGPORT", "5432"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", "postgres"),
    dbname=os.getenv("PGDATABASE", "mydb"),
)

ACCOUNT_TYPES = ["CHECKING", "SAVINGS", "CREDIT"]
ACCOUNT_STATUSES = ["ACTIVE", "SUSPENDED", "CLOSED"]
CARD_TYPES = ["DEBIT", "CREDIT"]
CARD_STATUSES = ["ACTIVE", "BLOCKED", "EXPIRED"]
PAYMENT_METHODS = ["ACH", "WIRE", "CARD"]
PAYMENT_STATUSES = ["PENDING", "SETTLED", "FAILED"]
TXN_STATUSES = ["AUTHORIZED", "SETTLED", "DECLINED", "REVERSED"]


def connect():
    return psycopg2.connect(**DSN)


class IdSequence:
    def __init__(self, conn, table, pk_col, start_hint):
        with conn.cursor() as cur:
            cur.execute(f"SELECT COALESCE(MAX({pk_col}), 0) FROM public.{table}")
            (current,) = cur.fetchone()
        self._next = max(current, start_hint) + 1
        self._lock = threading.Lock()

    def next(self):
        with self._lock:
            v = self._next
            self._next += 1
            return v


def seed(conn):
    cur = conn.cursor()

    customer_ids = []
    for i in range(500):
        cid = i + 1
        cur.execute(
            "INSERT INTO public.customer (customer_id, first_name, last_name, email, phone, address) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (cid, fake.first_name(), fake.last_name(), fake.unique.email(), fake.phone_number()[:30], fake.address().replace("\n", ", ")[:300]),
        )
        customer_ids.append(cid)
    conn.commit()
    print(f"seeded {len(customer_ids)} customers")

    account_ids = []
    for i in range(800):
        aid = i + 1
        cid = random.choice(customer_ids)
        cur.execute(
            "INSERT INTO public.account (account_id, customer_id, account_type, status) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (aid, cid, random.choice(ACCOUNT_TYPES), random.choice(ACCOUNT_STATUSES)),
        )
        account_ids.append(aid)
    conn.commit()
    print(f"seeded {len(account_ids)} accounts")

    card_ids = []
    for i in range(1000):
        cardid = i + 1
        aid = random.choice(account_ids)
        masked = f"**** **** **** {random.randint(1000, 9999)}"
        cur.execute(
            "INSERT INTO public.card (card_id, account_id, card_number_masked, card_type, status) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (cardid, aid, masked, random.choice(CARD_TYPES), random.choice(CARD_STATUSES)),
        )
        card_ids.append(cardid)
    conn.commit()
    print(f"seeded {len(card_ids)} cards")

    for i in range(1500):
        pid = i + 1
        aid = random.choice(account_ids)
        cur.execute(
            "INSERT INTO public.payment (payment_id, account_id, amount, payment_method, status) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (pid, aid, round(random.uniform(5, 5000), 2), random.choice(PAYMENT_METHODS), random.choice(PAYMENT_STATUSES)),
        )
    conn.commit()
    print("seeded 1500 payments")

    for i in range(3000):
        tid = i + 1
        aid = random.choice(account_ids)
        cardid = random.choice(card_ids)
        cur.execute(
            "INSERT INTO public.transaction (transaction_id, account_id, card_id, amount, merchant, status) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (tid, aid, cardid, round(random.uniform(1, 800), 2), fake.company(), random.choice(TXN_STATUSES)),
        )
    conn.commit()
    print("seeded 3000 transactions")

    cur.close()


class BurstControl:
    def __init__(self):
        self.until = 0.0
        self.rate = float(os.getenv("LOAD_GEN_STEADY_RATE", "2"))
        self.burst_rate = float(os.getenv("LOAD_GEN_BURST_RATE", "100"))
        self.burst_seconds = float(os.getenv("LOAD_GEN_BURST_SECONDS", "20"))

    def trigger(self):
        self.until = time.time() + self.burst_seconds

    def current_rate(self):
        return self.burst_rate if time.time() < self.until else self.rate


burst = BurstControl()


def random_event(conn, ids):
    cur = conn.cursor()
    # Weighted toward transaction (the "hot" table)
    table = random.choices(
        ["transaction", "payment", "card", "account", "customer"],
        weights=[55, 20, 10, 10, 5],
    )[0]
    op = random.choices(["insert", "update", "delete"], weights=[70, 25, 5])[0]

    try:
        if table == "transaction":
            if op == "insert" or not ids["transaction"]:
                tid = ids["transaction_seq"].next()
                aid = random.choice(ids["account"])
                cardid = random.choice(ids["card"])
                cur.execute(
                    "INSERT INTO public.transaction (transaction_id, account_id, card_id, amount, merchant, status) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (tid, aid, cardid, round(random.uniform(1, 800), 2), fake.company(), random.choice(TXN_STATUSES)),
                )
                ids["transaction"].append(tid)
            elif op == "update":
                tid = random.choice(ids["transaction"])
                cur.execute(
                    "UPDATE public.transaction SET status=%s, transaction_ts=now() WHERE transaction_id=%s",
                    (random.choice(TXN_STATUSES), tid),
                )
            else:
                tid = random.choice(ids["transaction"])
                cur.execute("DELETE FROM public.transaction WHERE transaction_id=%s", (tid,))
                ids["transaction"].remove(tid)

        elif table == "payment":
            if op == "insert" or not ids["payment"]:
                pid = ids["payment_seq"].next()
                aid = random.choice(ids["account"])
                cur.execute(
                    "INSERT INTO public.payment (payment_id, account_id, amount, payment_method, status) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (pid, aid, round(random.uniform(5, 5000), 2), random.choice(PAYMENT_METHODS), random.choice(PAYMENT_STATUSES)),
                )
                ids["payment"].append(pid)
            elif op == "update":
                pid = random.choice(ids["payment"])
                cur.execute("UPDATE public.payment SET status=%s WHERE payment_id=%s", (random.choice(PAYMENT_STATUSES), pid))
            else:
                pid = random.choice(ids["payment"])
                cur.execute("DELETE FROM public.payment WHERE payment_id=%s", (pid,))
                ids["payment"].remove(pid)

        elif table == "card":
            if op == "update" and ids["card"]:
                cardid = random.choice(ids["card"])
                cur.execute(
                    "UPDATE public.card SET status=%s, updated_ts=now() WHERE card_id=%s",
                    (random.choice(CARD_STATUSES), cardid),
                )
            else:
                cardid = ids["card_seq"].next()
                aid = random.choice(ids["account"])
                masked = f"**** **** **** {random.randint(1000, 9999)}"
                cur.execute(
                    "INSERT INTO public.card (card_id, account_id, card_number_masked, card_type, status) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (cardid, aid, masked, random.choice(CARD_TYPES), random.choice(CARD_STATUSES)),
                )
                ids["card"].append(cardid)

        elif table == "account":
            if op == "update" and ids["account"]:
                aid = random.choice(ids["account"])
                cur.execute(
                    "UPDATE public.account SET status=%s, updated_ts=now() WHERE account_id=%s",
                    (random.choice(ACCOUNT_STATUSES), aid),
                )
            else:
                aid = ids["account_seq"].next()
                cid = random.choice(ids["customer"])
                cur.execute(
                    "INSERT INTO public.account (account_id, customer_id, account_type, status) VALUES (%s,%s,%s,%s)",
                    (aid, cid, random.choice(ACCOUNT_TYPES), random.choice(ACCOUNT_STATUSES)),
                )
                ids["account"].append(aid)

        else:  # customer
            if op == "update" and ids["customer"]:
                cid = random.choice(ids["customer"])
                cur.execute(
                    "UPDATE public.customer SET phone=%s, updated_ts=now() WHERE customer_id=%s",
                    (fake.phone_number()[:30], cid),
                )
            else:
                cid = ids["customer_seq"].next()
                cur.execute(
                    "INSERT INTO public.customer (customer_id, first_name, last_name, email, phone, address) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (cid, fake.first_name(), fake.last_name(), fake.unique.email(), fake.phone_number()[:30], fake.address().replace("\n", ", ")[:300]),
                )
                ids["customer"].append(cid)

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"event error ({table}/{op}): {e}")
    finally:
        cur.close()


def load_ids(conn):
    ids = {}
    with conn.cursor() as cur:
        for table, col, key in [
            ("customer", "customer_id", "customer"),
            ("account", "account_id", "account"),
            ("card", "card_id", "card"),
            ("payment", "payment_id", "payment"),
            ("transaction", "transaction_id", "transaction"),
        ]:
            cur.execute(f"SELECT {col} FROM public.{table}")
            ids[key] = [r[0] for r in cur.fetchall()]
    ids["customer_seq"] = IdSequence(conn, "customer", "customer_id", 0)
    ids["account_seq"] = IdSequence(conn, "account", "account_id", 0)
    ids["card_seq"] = IdSequence(conn, "card", "card_id", 0)
    ids["payment_seq"] = IdSequence(conn, "payment", "payment_id", 0)
    ids["transaction_seq"] = IdSequence(conn, "transaction", "transaction_id", 0)
    return ids


def run_generation():
    conn = connect()
    ids = load_ids(conn)
    print(f"generator running: steady={burst.rate}/s burst={burst.burst_rate}/s for {burst.burst_seconds}s")
    while True:
        rate = burst.current_rate()
        random_event(conn, ids)
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

    uvicorn.run(app, host="0.0.0.0", port=8090, log_level="warning")


def manual_insert(conn, table, ids):
    cur = conn.cursor()
    if table == "customer":
        row_id = ids["customer_seq"].next()
        cur.execute(
            "INSERT INTO public.customer (customer_id, first_name, last_name, email, phone, address) VALUES (%s,%s,%s,%s,%s,%s)",
            (row_id, fake.first_name(), fake.last_name(), fake.unique.email(), fake.phone_number()[:30], fake.address().replace("\n", ", ")[:300]),
        )
    elif table == "account":
        row_id = ids["account_seq"].next()
        cur.execute(
            "INSERT INTO public.account (account_id, customer_id, account_type, status) VALUES (%s,%s,%s,%s)",
            (row_id, random.choice(ids["customer"]), random.choice(ACCOUNT_TYPES), random.choice(ACCOUNT_STATUSES)),
        )
    elif table == "card":
        row_id = ids["card_seq"].next()
        masked = f"**** **** **** {random.randint(1000, 9999)}"
        cur.execute(
            "INSERT INTO public.card (card_id, account_id, card_number_masked, card_type, status) VALUES (%s,%s,%s,%s,%s)",
            (row_id, random.choice(ids["account"]), masked, random.choice(CARD_TYPES), random.choice(CARD_STATUSES)),
        )
    elif table == "payment":
        row_id = ids["payment_seq"].next()
        cur.execute(
            "INSERT INTO public.payment (payment_id, account_id, amount, payment_method, status) VALUES (%s,%s,%s,%s,%s)",
            (row_id, random.choice(ids["account"]), round(random.uniform(5, 5000), 2), random.choice(PAYMENT_METHODS), random.choice(PAYMENT_STATUSES)),
        )
    else:  # transaction
        row_id = ids["transaction_seq"].next()
        cur.execute(
            "INSERT INTO public.transaction (transaction_id, account_id, card_id, amount, merchant, status) VALUES (%s,%s,%s,%s,%s,%s)",
            (row_id, random.choice(ids["account"]), random.choice(ids["card"]), round(random.uniform(1, 800), 2), fake.company(), random.choice(TXN_STATUSES)),
        )
    conn.commit()
    cur.close()
    return row_id


def manual_op(table, op, row_id):
    conn = connect()
    pk = {"customer": "customer_id", "account": "account_id", "card": "card_id", "payment": "payment_id", "transaction": "transaction_id"}[table]

    if op == "insert":
        ids = load_ids(conn)
        new_id = manual_insert(conn, table, ids)
        print(f"manual insert into {table}.{pk}={new_id} done at {datetime.now(timezone.utc).isoformat()}")
        conn.close()
        return

    cur = conn.cursor()
    if op == "delete":
        cur.execute(f"DELETE FROM public.{table} WHERE {pk}=%s", (row_id,))
    elif op == "update":
        status_col = "status" if table != "customer" else "phone"
        value = fake.phone_number()[:30] if table == "customer" else "UPDATED"
        cur.execute(f"UPDATE public.{table} SET {status_col}=%s WHERE {pk}=%s", (value, row_id))
    conn.commit()
    print(f"manual {op} on {table}.{pk}={row_id} done at {datetime.now(timezone.utc).isoformat()}")
    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["seed", "run", "manual"], required=True)
    parser.add_argument("--table")
    parser.add_argument("--op", choices=["insert", "update", "delete"])
    parser.add_argument("--id", type=int)
    args = parser.parse_args()

    if args.mode == "seed":
        c = connect()
        seed(c)
        c.close()
    elif args.mode == "manual":
        manual_op(args.table, args.op, args.id)
    else:
        t = threading.Thread(target=run_control_api, daemon=True)
        t.start()
        run_generation()
