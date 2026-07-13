# Demo Build Specification: Kafka CDC → TPT Stream → Teradata (5-Table Scenario Demo)

**Audience:** Coding agent / engineer building the demo
**Author context:** Paul Ibberson, Teradata — customer-facing architecture demo
**Status:** Draft v1.1 — ready to build, with open items flagged in Section 12
**Date:** 2026-07-09 (v1.1 revision, same day)

**v1.1 changelog (against v1.0):**
- Corrected the "1,024 global DBS session limit" claim throughout (Section 1, 10, 11) — that figure is actually Teradata's max-nodes-per-system-configuration limit, unrelated to sessions. Replaced with the verified per-PE (120) / per-gateway (1,200 certified max, default 600) session ceilings, per the Obsidian wiki's corrected `Teradata System Limits`, `Streaming Ingestion at Scale (500 Topics)`, and `Tiered Streaming Architecture` pages (all corrected 2026-07-09, same day as this revision). The illustrative concurrency table (192/96/38/8 max concurrent jobs) is unchanged — only the underlying mechanism explanation was wrong, not the headline numbers.
- Switched `tpt-runner` from a host-installed TTU process to the `teradata/tpt` Docker Hub image (Section 4), removing the "TTU must be installed somewhere" blocker from Open Item #2.
- Corrected all Kafka Access Module `AccessModuleInitStr` examples (Section 8.1–8.3) to use the actual documented flag set (`-BROKERS`, `-TOPIC`, `-PARTITION`, `-MODE`, `-OFFSETS`, `-CONFIG`, `-X group.id=`, etc.) sourced from the Obsidian wiki's TPT concept page, which cites the official Access Module Reference (B035-2425-103K) — replacing the earlier librdkafka-style `-X bootstrap.servers=...` guesswork that could not be verified against the JS-rendered docs.teradata.com pages.
- Added the `-l` (latency flush) and `-z` (checkpoint interval) `tbuild` flags, which are **mandatory** for a live demo — without `-l`, the Stream operator only flushes at end-of-source, which never arrives with a live Kafka producer, so **zero rows appear on screen for the entire demo**. This was sourced from a prior internal debrief (`teradata-streaming-integration-learnings.md`) on this exact stack and would otherwise have been discovered live, in front of the customer.
- Redesigned Scenario B's multi-topic ingestion (Section 8.2) around a documented TPT pattern (multiple DataConnector Producer instances combined via `UNION ALL`, one shared consumer group) instead of an unconfirmed comma-list/regex topic parameter — resolves former Open Item #3 with much higher confidence.

---

## 0. Purpose and Narrative

The customer's ambition is up to **500 Kafka topics from a CDC platform, mapped to 500 Teradata target tables**. Prior architecture research on this account (see Section 1) established that "500 topics = 500 independent TPT jobs" is an operational anti-pattern — Teradata's session ceiling and job-management overhead make it unworkable — and that the right production pattern is **consolidation**: fewer, larger TPT jobs handling many tables, with a landing → merge pattern underneath.

This demo exists to make that argument **visually and technically concrete** at a scale a room can watch (5 tables), by building and running **three topologies side by side against the same live CDC stream**, so the audience can directly compare them:

| Scenario | Topics : TPT Jobs : Landing Tables : Target Tables | What it proves |
|---|---|---|
| **A** | 5 : 5 : 5 : 5 (fully independent) | The "obvious" approach. Full isolation, simplest mental model — but shows *why* it doesn't scale to 500 (5 processes to run/monitor/restart here; 500 at full scale, blowing past Teradata's session ceiling). |
| **B** | 5 : 1 : 1 : 5 (consolidated ingest, staged fan-out) | The recommended production pattern. One job, one generic landing table, one merge routine fans out to 5 targets by a `source_table` discriminator. This is what "500 tables → 1-3 topics → 1 TPT job" looks like mechanically. |
| **C** | 5 : 1 : 5 : 5 (single job, multiple direct targets) | The middle ground: one `tbuild` process/job containing 5 producer→loader step-pairs, still writing to 5 separate landing/target tables. Fewer OS processes to manage than A, but no data consolidation like B. Useful when the customer needs per-table landing fidelity but still wants to shrink the job count (e.g. their "20 hot tables in one job" tiering idea). |

All three scenarios consume the **same 5 Debezium/Kafka topics**, produced by the **same Postgres source**, at the **same moment**, via three independent Kafka consumer groups, and land into **three parallel Teradata schemas** (`DEMO_A`, `DEMO_B`, `DEMO_C`). This means the demo needs no fragile "reset and replay" choreography — all three run continuously and concurrently, and the presenter just switches which dashboard panel / SQL terminal they're pointing at.

The demo should close by explicitly re-projecting the 5-table mechanics onto the 500-table numbers from the prior research (Section 11), so the audience sees the line from "what you just watched" to "what we'd build for you."

---

## 1. Inputs Already Available (Do Not Re-Derive)

This project already contains prior architecture research that this demo must stay consistent with. The coding agent should treat these as authoritative background, not re-litigate them:

- `01-Real-Time-Streaming-at-Scale-500-Topics-Tables-Into-Teradata.md` — the three source-topology scenarios (independent topics vs. CDC tables vs. 500 per-table jobs), and why "500 independent TPT jobs" is a broken pattern. **Note:** this doc's original session-ceiling figure has since been corrected — see the wiki sources below, which supersede it on that specific point (the 20-AMP → ~38-concurrent-jobs conclusion still holds, but the reasoning has changed; Section 11 has the corrected version).
- `02-Critical-Discovery-Questions.md` — the 5 discovery questions a Solutions Architect should ask before this demo is deployed against a live customer conversation.
- `03-Architecture-Decision-Tree.md` — decision trees mapping customer answers to recommended architecture tier.
- `Teradata CDC Ingestion Architecture.md` (710 lines) — the full production-grade design: Debezium connector grouping, domain topic routing, TPT Stream operator config, Kafka Access Module config, landing/stage/ODS table DDL, micro-batch MERGE pattern, TASM workload classes, DR, capacity model. **This demo is a scaled-down, runnable instantiation of that document.**
- **Obsidian wiki (`C:\Obsidian\Vault`), read 2026-07-09 for this revision** — the authoritative, actively-maintained technical reference, more current than the project docs above on TPT/Kafka mechanics:
  - `wiki/concepts/Teradata System Limits.md` — official platform limits from the Database Administration manual (R20.00, June 2025); the source of the session-ceiling correction in Section 11.
  - `wiki/concepts/Streaming Ingestion at Scale (500 Topics).md` and `wiki/concepts/Tiered Streaming Architecture.md` — corrected versions of the same 500-table architecture analysis as the project docs above, explicitly noting and fixing the same session-limit error.
  - `wiki/concepts/Teradata Parallel Transporter (TPT).md` — the operator/access-module reference this spec's TPT scripts are now built against, including the documented Kafka Access Module initialization-string parameter table (sourced from the official Access Module Reference, B035-2425-103K).
  - `dev/teradata-streaming-integration-learnings.md` — a debrief from a prior team build of a similar Kafka→TPT→Teradata demo stack, containing non-obvious operational gotchas (the `-l` latency-flush flag, checkpoint/restart behavior, `-W` flag ambiguity) that are not evident from product documentation and are folded into Section 8 below.

**Live environment already available in this session:** a Teradata Vantage system at version `20.00.30.76` is reachable via the connected Teradata MCP tools. Confirm with the user whether this is the intended demo target or a shared dev system before creating objects on it (see Section 12, Open Item #1).

---

## 2. Goals and Non-Goals

**Goals**
- Real Debezium CDC capture from a real Postgres source (per user decision — not a synthetic Kafka producer).
- Real TPT (`tbuild`, actual `.tpt` scripts, actual Stream Operator + Kafka Access Module) loading into a real Teradata Vantage target.
- Three topologies running concurrently against identical source data, so throughput/lag/row-count comparisons are apples-to-apples and visible live.
- A live dashboard so the audience watches rows land in near-real time rather than staring at terminal scrollback.
- A scripted failure/recovery moment (kill a job mid-stream, show Kafka-offset-based resume with no data loss).
- A clean, repeatable "reset to zero and go again" procedure for rehearsal and repeat demos.

**Non-goals** (explicitly out of scope for this build — call out if the customer asks)
- Active-active Kafka / cross-region DR (documented conceptually in Section 11, not built).
- Full TASM workload isolation (a lightweight version is a stretch goal, Section 8.4).
- Schema evolution live demo (mentioned as a talking point, not exercised live — flagged as a possible follow-up demo).
- Avro/Schema Registry (JSON envelope only, per the "simpler for TPT" recommendation in the prior research doc).
- True exactly-once guarantees — this demo implements **effectively-once** via idempotent merge keyed on source position, matching the prior research's recommendation.

---

## 3. High-Level Architecture

```
                         ┌────────────────────────┐
                         │   Postgres (source)     │
                         │   5 tables, wal_level=   │
                         │   logical, REPLICA        │
                         │   IDENTITY FULL            │
                         └────────────┬─────────────┘
                                      │ logical replication (pgoutput)
                                      ▼
                         ┌────────────────────────┐
                         │  Kafka Connect +          │
                         │  Debezium PostgreSQL       │
                         │  connector (1 connector,   │
                         │  5 tables in table.include │
                         │  .list)                    │
                         └────────────┬─────────────┘
                                      │ produces 5 topics (1 per table, Debezium default)
                                      ▼
        ┌────────────────────────────────────────────────────────┐
        │  Kafka (KRaft, single broker, dev-scale)                 │
        │  cdc.public.customer / account / card / transaction /    │
        │  payment  (3 partitions each, key = business PK)          │
        └───────┬───────────────────┬───────────────────┬─────────┘
                │                    │                    │
     consumer group          consumer group        consumer group
     tpt.demo_a.*             tpt.demo_b            tpt.demo_c
                │                    │                    │
                ▼                    ▼                    ▼
     ┌─────────────────┐  ┌────────────────────┐ ┌─────────────────────┐
     │ SCENARIO A         │  │ SCENARIO B           │ │ SCENARIO C             │
     │ 5 independent       │  │ 1 tbuild job,          │ │ 1 tbuild job,            │
     │ tbuild jobs,         │  │ 1 Kafka Access         │ │ 5 producer→loader        │
     │ 1 topic each         │  │ Module reading all      │ │ step-pairs in ONE         │
     │                     │  │ 5 topics, 1 generic     │ │ script                    │
     │                     │  │ landing table            │ │                          │
     └─────────┬───────────┘  └──────────┬─────────────┘ └───────────┬──────────────┘
               │                          │                            │
               ▼                          ▼                            ▼
     DEMO_A.<table>_LANDING     DEMO_B.CDC_LANDING (1 table)   DEMO_C.<table>_LANDING (x5)
     (x5)                        + source_table column
               │                          │                            │
               ▼                          ▼                            ▼
     MERGE per table (x5           Fan-out MERGE keyed on     MERGE per table (x5
     jobs/procs)                    source_table (1 proc,      procs, but 1 shared
                                     loops 5 targets)            job lifecycle)
               │                          │                            │
               ▼                          ▼                            ▼
     DEMO_A.<table> (x5)         DEMO_B.<table> (x5)          DEMO_C.<table> (x5)

                         ┌──────────────────────────┐
                         │   Load Generator            │
                         │   (Python, controllable     │
                         │   INSERT/UPDATE/DELETE      │
                         │   rate + burst mode)         │
                         └──────────────────────────┘

                         ┌──────────────────────────┐
                         │   Live Dashboard             │
                         │   consumer lag, rows/sec,     │
                         │   job count, side-by-side     │
                         │   latency per scenario         │
                         └──────────────────────────┘
```

---

## 4. Infrastructure (Docker Compose)

Build one `docker-compose.yml` with these services. Every component, including TPT itself, runs in containers — no host-installed Teradata Tools and Utilities (TTU) required.

| Service | Image / base | Purpose |
|---|---|---|
| `postgres` | `postgres:16` | Source DB. Must set `wal_level=logical`, `max_replication_slots=10`, `max_wal_senders=10`. |
| `kafka` | `apache/kafka:3.7` (KRaft mode, no Zookeeper needed) | Single-broker dev cluster. |
| `kafka-connect` | `debezium/connect:2.7` (bundles Debezium PostgreSQL connector) | Runs the Debezium connector via REST API on port 8083. |
| `load-generator` | custom Python image (`python:3.12-slim` + `psycopg2`, `faker`) | Drives INSERT/UPDATE/DELETE against Postgres at a controllable rate. |
| `dashboard` | custom Node/Python web app | Live metrics UI (Section 9). |
| `metrics-collector` | Python sidecar | Polls Kafka consumer-group lag (via `kafka-python` admin client) and Teradata row counts/latency (via `teradatasql` driver), writes to a small SQLite/JSON store the dashboard reads. |
| `tpt-runner` | **`teradata/tpt` (Docker Hub)** | Runs the actual `tbuild` jobs for Scenarios A/B/C. Mount the `.tpt` scripts, jobvars files, and a persistent volume for checkpoint files (`/opt/teradata/client/20.00/tbuild/checkpoint/`) into the container. Needs network reachability to both the `kafka` service (same Compose network) and the target Teradata system (Section 12, Open Item #1). |

**Before committing to this image:** confirm on Docker Hub that `teradata/tpt` bundles the Kafka Access Module (`libkafkaaxsmod.so`) and not just the core TPT/`tbuild` binaries — Access Modules are sometimes packaged separately from base TTU. A prior internal demo on this exact stack (`Kafka-TPT-demo-code/` in the Obsidian vault's SA-Reference archive) built its own Ubuntu-based image that explicitly installed "TPT + Kafka AM from the TeradataToolsAndUtilities package" as two distinct steps — suggesting the AM may need to be added on top of a base TPT image rather than assumed present. Verify with `docker run --rm teradata/tpt find / -name 'libkafkaaxsmod*'` (or equivalent) before building Section 8's scripts against it; if absent, extend the image with the AM package, following that prior Dockerfile as a reference.

Networking: run `tpt-runner` on the same Docker Compose network as `kafka` so it can reach it by service name (`kafka:9092`) without exposing ports to the host; expose Kafka Connect REST on `localhost:8083` and Postgres on `localhost:5432` for the presenter to run manual `INSERT`/`UPDATE` statements live if desired.

---

## 5. Source Data Model (Postgres → Debezium → Teradata)

Five tables, deliberately spanning low to high CDC volume so the throughput story is visible.

| Table | Approx. relative volume in demo | Role |
|---|---|---|
| `customer` | Lowest (seed 500 rows, rare updates) | Reference/dimension-like |
| `account` | Low (seed 800 rows, occasional status changes) | Reference-ish, some updates |
| `card` | Medium (seed 1,000 rows, moderate status updates) | Medium volume |
| `payment` | Medium-high (steady insert stream) | Transactional |
| `transaction` | **Highest — the "hot" table** (default 5–10 rows/sec, burst mode 100+ rows/sec) | Used to demonstrate throughput/Pack tuning differences |

### 5.1 Postgres DDL (source)

```sql
CREATE TABLE customer (
    customer_id     BIGINT PRIMARY KEY,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(200),
    phone           VARCHAR(30),
    address         VARCHAR(300),
    created_ts      TIMESTAMP NOT NULL DEFAULT now(),
    updated_ts      TIMESTAMP NOT NULL DEFAULT now()
);
ALTER TABLE customer REPLICA IDENTITY FULL;

CREATE TABLE account (
    account_id      BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES customer(customer_id),
    account_type    VARCHAR(20),        -- CHECKING / SAVINGS / CREDIT
    status          VARCHAR(20),        -- ACTIVE / SUSPENDED / CLOSED
    opened_ts       TIMESTAMP NOT NULL DEFAULT now(),
    updated_ts      TIMESTAMP NOT NULL DEFAULT now()
);
ALTER TABLE account REPLICA IDENTITY FULL;

CREATE TABLE card (
    card_id             BIGINT PRIMARY KEY,
    account_id          BIGINT NOT NULL REFERENCES account(account_id),
    card_number_masked  VARCHAR(20),    -- e.g. '**** **** **** 1234'
    card_type           VARCHAR(20),    -- DEBIT / CREDIT
    status              VARCHAR(20),    -- ACTIVE / BLOCKED / EXPIRED
    issued_ts           TIMESTAMP NOT NULL DEFAULT now(),
    updated_ts          TIMESTAMP NOT NULL DEFAULT now()
);
ALTER TABLE card REPLICA IDENTITY FULL;

CREATE TABLE payment (
    payment_id      BIGINT PRIMARY KEY,
    account_id      BIGINT NOT NULL REFERENCES account(account_id),
    amount          DECIMAL(12,2),
    currency        CHAR(3) DEFAULT 'USD',
    payment_method  VARCHAR(20),        -- ACH / WIRE / CARD
    status          VARCHAR(20),        -- PENDING / SETTLED / FAILED
    payment_ts      TIMESTAMP NOT NULL DEFAULT now()
);
ALTER TABLE payment REPLICA IDENTITY FULL;

CREATE TABLE transaction (
    transaction_id  BIGINT PRIMARY KEY,
    account_id      BIGINT NOT NULL REFERENCES account(account_id),
    card_id         BIGINT REFERENCES card(card_id),
    amount          DECIMAL(12,2),
    currency        CHAR(3) DEFAULT 'USD',
    merchant        VARCHAR(100),
    status          VARCHAR(20),        -- AUTHORIZED / SETTLED / DECLINED / REVERSED
    transaction_ts  TIMESTAMP NOT NULL DEFAULT now()
);
ALTER TABLE transaction REPLICA IDENTITY FULL;
```

`REPLICA IDENTITY FULL` is required on every table so Debezium's `before` image is populated on `UPDATE`/`DELETE` — without it, only the primary key is available in `before`, which weakens the "show a full before/after CDC event" part of the demo. This is confirmed current Debezium PostgreSQL connector behavior (see Section 12 sources).

### 5.2 Seed data

Seed each table with the row counts above using a Python/Faker script, respecting FK order (`customer` → `account` → `card`/`payment`/`transaction`). Keep referential integrity intentional — `account_id`/`card_id` foreign keys let the dashboard show a semi-realistic "customer 360" join later if desired, though that's a stretch goal, not required for the 3 core scenarios.

---

## 6. Debezium Connector (Single Connector, 5 Tables)

One Debezium PostgreSQL connector captures all 5 tables. Debezium's default table-routing behavior creates one topic per table (`<topic.prefix>.<schema>.<table>`), which is exactly the topic set all three scenarios need — **no separate topic-routing/consolidation layer is required for this demo**, because the differentiation between Scenario A/B/C lives entirely on the TPT/consumer side, not the produce side. (At true 500-table production scale, the prior research recommends *also* consolidating on the produce side into 50–100 domain topics — call this out explicitly to the audience as the next lever beyond what's demoed.)

```json
{
  "name": "debezium-demo-postgres",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "${file:/kafka/secrets/pg.properties:password}",
    "database.dbname": "demo",
    "topic.prefix": "cdc",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_demo_slot",
    "publication.autocreate.mode": "filtered",
    "table.include.list": "public.customer,public.account,public.card,public.payment,public.transaction",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "false",
    "decimal.handling.mode": "string",
    "heartbeat.interval.ms": "10000",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "dlq.cdc.demo"
  }
}
```

Notes:
- `plugin.name: pgoutput` is the native PostgreSQL 10+ logical decoding plugin, requires no extra Postgres extension install — confirmed current Debezium guidance (no `wal2json` needed).
- `publication.autocreate.mode: filtered` scopes the Postgres publication to just the 5 tables in `table.include.list`, avoiding accidental capture of future tables.
- Register via `POST http://localhost:8083/connectors` with this JSON body.
- Resulting topics: `cdc.public.customer`, `cdc.public.account`, `cdc.public.card`, `cdc.public.payment`, `cdc.public.transaction`.
- Set topic partition count to **3 per topic** for the demo (down from the 48–96 recommended for hot production domains — call this scaling factor out explicitly when presenting Section 11).

---

## 7. Teradata Target Design

### 7.1 Three parallel schemas

```sql
CREATE DATABASE DEMO_A FROM DBC AS PERM = 2e9, SPOOL = 2e9;
CREATE DATABASE DEMO_B FROM DBC AS PERM = 2e9, SPOOL = 2e9;
CREATE DATABASE DEMO_C FROM DBC AS PERM = 2e9, SPOOL = 2e9;
CREATE DATABASE DEMO_ADMIN FROM DBC AS PERM = 5e8, SPOOL = 5e8;
```

(Adjust PERM sizing to whatever the target system allows — this is dev/demo scale, not production capacity planning. Confirm space availability before running — see Open Item #1.)

### 7.2 Scenario A — 5 landing tables + 5 target tables (per table, x5)

Landing table pattern (one per source table, e.g. for `transaction`):

```sql
CREATE MULTISET TABLE DEMO_A.TRANSACTION_LANDING
(
    ingest_ts        TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    kafka_topic      VARCHAR(256),
    kafka_partition  INTEGER,
    kafka_offset     BIGINT,
    operation_code   CHAR(1),           -- c/u/d/r (Debezium op codes)
    event_ts         TIMESTAMP(6),
    payload_json     VARCHAR(16000)     -- raw Debezium envelope (before/after/source)
)
PRIMARY INDEX (kafka_partition);
```

Target table (typed, current-state):

```sql
CREATE MULTISET TABLE DEMO_A.TRANSACTION
(
    transaction_id      BIGINT NOT NULL,
    account_id          BIGINT,
    card_id             BIGINT,
    amount              DECIMAL(12,2),
    currency            CHAR(3),
    merchant             VARCHAR(100),
    status               VARCHAR(20),
    transaction_ts        TIMESTAMP(6),
    source_update_ts       TIMESTAMP(6),
    td_update_ts            TIMESTAMP(6),
    cdc_operation_cd         CHAR(1),
    cdc_deleted_ind           BYTEINT DEFAULT 0
)
UNIQUE PRIMARY INDEX (transaction_id);
```

Repeat this landing+target pair for `customer`, `account`, `card`, `payment` (adjust columns to match Section 5.1 DDL). **5 landing tables, 5 target tables, all in `DEMO_A`.**

### 7.3 Scenario B — 1 generic landing table + 5 target tables

```sql
CREATE MULTISET TABLE DEMO_B.CDC_LANDING
(
    ingest_ts        TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    kafka_topic      VARCHAR(256),
    kafka_partition  INTEGER,
    kafka_offset     BIGINT,
    source_table     VARCHAR(64),        -- 'customer' | 'account' | 'card' | 'payment' | 'transaction'
    operation_code   CHAR(1),
    event_ts         TIMESTAMP(6),
    payload_json     VARCHAR(16000),
    processed_ind    BYTEINT DEFAULT 0
)
PRIMARY INDEX (kafka_partition);
```

`source_table` is derived from the Kafka topic name at load time (strip the `cdc.public.` prefix) — either via a lightweight TPT Schema Mapping/expression, or (simpler, recommended for the demo) by running one Kafka Access Module reader **per topic but all inside the same job/step**, each hard-coding its own `source_table` literal into the row it emits, then all 5 streams feed the *same* Stream operator/landing table via a shared `APPLY` — see Section 8.2 for exact script shape.

Target tables: identical DDL to Section 7.2's target tables, just created under `DEMO_B` instead of `DEMO_A`.

### 7.4 Scenario C — 5 landing tables + 5 target tables (same DDL as A, under `DEMO_C`)

Identical table shapes to Section 7.2, created under `DEMO_C`. The difference from Scenario A is entirely in the **job orchestration** (Section 8.3), not the table design.

### 7.5 Shared admin/control tables (`DEMO_ADMIN`)

Reused across all three scenarios for monitoring and idempotency:

```sql
CREATE MULTISET TABLE DEMO_ADMIN.CDC_BATCH_CONTROL
(
    scenario_name    VARCHAR(10),
    batch_id         BIGINT,
    source_table     VARCHAR(64),
    batch_start_ts   TIMESTAMP(6),
    batch_end_ts     TIMESTAMP(6),
    row_count        BIGINT,
    insert_count     BIGINT,
    update_count     BIGINT,
    delete_count     BIGINT,
    status_cd        VARCHAR(20)
)
PRIMARY INDEX (scenario_name, batch_id);

CREATE MULTISET TABLE DEMO_ADMIN.APPLIED_EVENT_LOG
(
    scenario_name     VARCHAR(10),
    source_table      VARCHAR(64),
    primary_key_val   VARCHAR(128),
    kafka_topic       VARCHAR(256),
    kafka_partition   INTEGER,
    kafka_offset      BIGINT,
    applied_ts        TIMESTAMP(6)
)
PRIMARY INDEX (scenario_name, source_table, primary_key_val);
```

`APPLIED_EVENT_LOG` (or a dedup step against the landing table using `QUALIFY ROW_NUMBER() ... ORDER BY kafka_offset DESC`) is what makes the merge idempotent/effectively-once, consistent with the prior research doc's recommendation — this matters because Debezium is at-least-once by default, so duplicates on connector restart are expected, not exceptional.

---

## 8. Scenario Build Details

### 8.0 Critical operational flags — read before writing any script

These are non-obvious behaviors documented in a prior internal debrief on this exact stack (`dev/teradata-streaming-integration-learnings.md` in the Obsidian wiki), consumed real debugging time on that project, and are not evident from reading the Stream Operator or Kafka Access Module reference alone. Apply them to every `.tpt` script and `tbuild` invocation in Sections 8.1–8.3.

- **`tbuild -l <seconds>` is mandatory for a live demo.** Without it, the Stream operator only commits rows to Teradata when it flushes its internal buffers, which happens at end-of-source — i.e., when the Kafka idle timeout fires. With a live producer, that never happens, so **rows buffer forever and zero rows appear on screen for the entire demo.** Use `tbuild -f <script>.tpt -v <jobvars> -j <job_name> -l 5` (5-second flush) for all three scenarios. This is the single highest-risk gotcha in this whole build — it fails silently (the job looks like it's running fine) rather than erroring out.
- **`tbuild -z <seconds>` enables checkpointing**, required for the restart/resume behavior the Section 10 failure-recovery moment depends on. Use `-z 10` or similar.
- **Two different `-W` flags exist — do not confuse them:** `tbuild -W <seconds>` (command-line) is the subprocess spawn timeout (1–900s, default 120). A separate `-W <seconds>` *inside* `AccessModuleInitStr` is the Kafka Access Module's idle/drain timeout — how long the module waits after the last message before signalling end-of-stream (used for graceful shutdown when killing a producer, not during normal continuous operation). Keep these straight in the run scripts and comment which is which.
- **Checkpoint files** are written to `/opt/teradata/client/20.00/tbuild/checkpoint/` (per job/user). Run `twbrmcp <username>` to clear them before a clean-slate rehearsal run — do **not** clear them between the intentional kill/restart in Section 10, or the offset-resume demonstration won't have anything to resume from.
- **`ArraySupport`** should be enabled on the Stream operator (batches multiple rows per request — meaningful throughput gain for insert-heavy loads) and **`Buffers`** increased above its low default if sessions appear idle under load — both are called out as best practices in the wiki's TPT reference. Add `VARCHAR ArraySupport = 'Y'` to each Stream operator's `ATTRIBUTES` block below.

### 8.1 Scenario A — 5 independent TPT jobs

One `.tpt` script per table (or one parameterized script template invoked 5× with different job variables — recommended, to keep the demo repo DRY). Skeleton (shown for `transaction`; repeat for the other 4 with lighter session counts):

```sql
DEFINE JOB DEMO_A_LOAD_TRANSACTION
DESCRIPTION 'Scenario A: 1 topic -> 1 TPT job -> 1 target table'
(
  DEFINE SCHEMA CDC_RAW_SCHEMA
  (
      kafka_topic     VARCHAR(256),
      kafka_partition INTEGER,
      kafka_offset    BIGINT,
      op_code         VARCHAR(1),
      event_ts        VARCHAR(40),
      payload_json    VARCHAR(16000)
  );

  DEFINE OPERATOR Kafka_Reader
  TYPE DATACONNECTOR PRODUCER
  SCHEMA CDC_RAW_SCHEMA
  ATTRIBUTES
  (
      VARCHAR AccessModuleName    = 'libkafkaaxsmod.so',
      VARCHAR AccessModuleInitStr = '-BROKERS kafka:9092 -TOPIC cdc.public.transaction -PARTITION * -MODE C -OFFSETS stored -X group.id=tpt.demo_a.transaction'
  );

  DEFINE OPERATOR Stream_Loader
  TYPE STREAM
  SCHEMA *
  ATTRIBUTES
  (
      VARCHAR TdpId          = @TargetTdpId,
      VARCHAR UserName        = @TargetUserName,
      VARCHAR UserPassword     = @TargetUserPassword,
      VARCHAR TargetTable       = 'DEMO_A.TRANSACTION_LANDING',
      INTEGER MaxSessions        = 8,
      INTEGER MinSessions         = 8,
      INTEGER Pack                 = 100,
      VARCHAR PackMaximum           = 'N',
      VARCHAR ArraySupport           = 'Y',
      VARCHAR ErrorTable1            = 'DEMO_A.TRANSACTION_ET1',
      VARCHAR ErrorTable2             = 'DEMO_A.TRANSACTION_ET2',
      INTEGER ErrorLimit                = 1000,
      VARCHAR PrivateLogName              = 'demo_a_transaction_log'
  );

  APPLY TO OPERATOR (Stream_Loader)
  SELECT * FROM OPERATOR (Kafka_Reader);
);
```

`AccessModuleInitStr` parameter reference (per the documented Kafka Access Module flag set, not generic librdkafka syntax): `-BROKERS`/`-B` (comma-separated broker list — list all brokers of a replicated topic for automatic failover), `-TOPIC`/`-T` (single topic per Producer instance — see Section 8.2 for how multi-topic consumption is composed), `-PARTITION`/`-P` (`*` for all partitions, or a list/range), `-MODE`/`-M` (`C` for consumer/import), `-OFFSETS`/`-O` (`stored` resumes from broker-tracked offset — the default and correct choice for restart-safe demos; `beginning`/`end` for one-off resets), `-X group.id=<name>` (Kafka consumer group — coordinates partition assignment across multiple Producer instances declared as one group), `-CONFIG` (pass-through to librdkafka for anything not covered by a named flag, e.g. `compression.codec=gzip`). Add `-TRACELEVEL 1` temporarily while debugging a new script.

Session/Pack guidance for the other 4 (lower volume): `MaxSessions=2, MinSessions=2, Pack=20` for `customer`/`account`/`card`; `MaxSessions=4, Pack=40` for `payment`. Keep `transaction` at the higher setting — this is the pair of numbers the presenter tunes live to show the Pack/latency-throughput tradeoff described in the prior research (lower Pack = lower latency, higher Pack = higher throughput).

Run as 5 separate `tbuild` invocations (5 separate OS processes, 5 separate job names, 5 separate restart logs) — note the mandatory `-l` flush flag and `-z` checkpoint flag from Section 8.0:

```bash
tbuild -f demo_a_load_customer.tpt -v demo_a.jobvars -j demo_a_customer -l 5 -z 10
tbuild -f demo_a_load_account.tpt -v demo_a.jobvars -j demo_a_account -l 5 -z 10
tbuild -f demo_a_load_card.tpt -v demo_a.jobvars -j demo_a_card -l 5 -z 10
tbuild -f demo_a_load_payment.tpt -v demo_a.jobvars -j demo_a_payment -l 5 -z 10
tbuild -f demo_a_load_transaction.tpt -v demo_a.jobvars -j demo_a_transaction -l 5 -z 10
```

Downstream: 5 separate merge routines (BTEQ or stored procedure, one per table) run on a short loop (every 2–5 seconds) parsing `payload_json` and applying the idempotent MERGE pattern from Section 7.5/Section 8.5.

### 8.2 Scenario B — 1 job, 1 landing table, fan-out merge

The Kafka Access Module's `-TOPIC` parameter takes a single topic per Producer instance — there is no documented comma-list or regex form for subscribing one instance to multiple topics. The correct, documented way to consolidate multiple topics into one TPT job is the same mechanism TPT uses for multi-*partition* parallelism: **define one DataConnector Producer operator per topic, and combine them with `UNION ALL` feeding a single Stream operator**, all declared as one shared Kafka consumer group via `-X group.id=`. This is a documented pattern (the wiki's TPT reference describes exactly this for multiple `$FILE_READER`/Producer instances attaching their own copy of the access module and being coordinated as one consumer group) — extended here from "one instance per partition of one topic" to "one instance per topic," which is the same mechanism, just parameterized differently. Each Producer instance hard-codes its own `source_table` literal via a constant column in its `SCHEMA`, so the downstream fan-out merge has a reliable discriminator without needing to parse it out of the topic name at merge time.

```sql
DEFINE JOB DEMO_B_LOAD_ALL
DESCRIPTION 'Scenario B: 5 topics -> 1 TPT job -> 1 landing table -> fan-out merge'
(
  DEFINE SCHEMA CDC_RAW_SCHEMA
  (
      source_table    VARCHAR(64),      -- constant per producer instance, set below
      kafka_partition INTEGER,
      kafka_offset    BIGINT,
      op_code         VARCHAR(1),
      event_ts        VARCHAR(40),
      payload_json    VARCHAR(16000)
  );

  DEFINE OPERATOR Kafka_Customer TYPE DATACONNECTOR PRODUCER SCHEMA CDC_RAW_SCHEMA
  ATTRIBUTES ( VARCHAR AccessModuleName = 'libkafkaaxsmod.so',
               VARCHAR AccessModuleInitStr = '-BROKERS kafka:9092 -TOPIC cdc.public.customer -PARTITION * -MODE C -OFFSETS stored -X group.id=tpt.demo_b' );
  DEFINE OPERATOR Kafka_Account TYPE DATACONNECTOR PRODUCER SCHEMA CDC_RAW_SCHEMA
  ATTRIBUTES ( VARCHAR AccessModuleName = 'libkafkaaxsmod.so',
               VARCHAR AccessModuleInitStr = '-BROKERS kafka:9092 -TOPIC cdc.public.account -PARTITION * -MODE C -OFFSETS stored -X group.id=tpt.demo_b' );
  DEFINE OPERATOR Kafka_Card TYPE DATACONNECTOR PRODUCER SCHEMA CDC_RAW_SCHEMA
  ATTRIBUTES ( VARCHAR AccessModuleName = 'libkafkaaxsmod.so',
               VARCHAR AccessModuleInitStr = '-BROKERS kafka:9092 -TOPIC cdc.public.card -PARTITION * -MODE C -OFFSETS stored -X group.id=tpt.demo_b' );
  DEFINE OPERATOR Kafka_Payment TYPE DATACONNECTOR PRODUCER SCHEMA CDC_RAW_SCHEMA
  ATTRIBUTES ( VARCHAR AccessModuleName = 'libkafkaaxsmod.so',
               VARCHAR AccessModuleInitStr = '-BROKERS kafka:9092 -TOPIC cdc.public.payment -PARTITION * -MODE C -OFFSETS stored -X group.id=tpt.demo_b' );
  DEFINE OPERATOR Kafka_Transaction TYPE DATACONNECTOR PRODUCER SCHEMA CDC_RAW_SCHEMA
  ATTRIBUTES ( VARCHAR AccessModuleName = 'libkafkaaxsmod.so',
               VARCHAR AccessModuleInitStr = '-BROKERS kafka:9092 -TOPIC cdc.public.transaction -PARTITION * -MODE C -OFFSETS stored -X group.id=tpt.demo_b' );

  DEFINE OPERATOR Stream_Loader_All
  TYPE STREAM
  SCHEMA *
  ATTRIBUTES
  (
      VARCHAR TdpId          = @TargetTdpId,
      VARCHAR UserName        = @TargetUserName,
      VARCHAR UserPassword     = @TargetUserPassword,
      VARCHAR TargetTable       = 'DEMO_B.CDC_LANDING',
      INTEGER MaxSessions        = 16,
      INTEGER MinSessions         = 16,
      INTEGER Pack                 = 100,
      VARCHAR ArraySupport          = 'Y',
      VARCHAR ErrorTable1            = 'DEMO_B.CDC_LANDING_ET1',
      VARCHAR ErrorTable2             = 'DEMO_B.CDC_LANDING_ET2',
      INTEGER ErrorLimit                = 1000
  );

  APPLY TO OPERATOR (Stream_Loader_All)
  SELECT 'customer' AS source_table, * FROM OPERATOR (Kafka_Customer)
  UNION ALL
  SELECT 'account' AS source_table, * FROM OPERATOR (Kafka_Account)
  UNION ALL
  SELECT 'card' AS source_table, * FROM OPERATOR (Kafka_Card)
  UNION ALL
  SELECT 'payment' AS source_table, * FROM OPERATOR (Kafka_Payment)
  UNION ALL
  SELECT 'transaction' AS source_table, * FROM OPERATOR (Kafka_Transaction);
);
```

**Confirm before building** (this is a generalization of a documented pattern, not a verbatim documented example — verify empirically): that five Producer instances declared with the same `-X group.id=` but five *different* `-TOPIC` values behave as intended (each instance consumes its own topic; Kafka does not attempt to rebalance a topic's partitions across instances subscribed to a different topic) and that TPT's `UNION ALL` across five DataConnector Producer operators is accepted feeding one Stream operator the same way it accepts `UNION ALL` across multiple same-topic partition instances. If this exact multi-topic `UNION ALL` shape is rejected by the local TPT install, the fallback is running 5 separate single-topic Producer→landing-table pairs into `DEMO_B.CDC_LANDING` as 5 concurrent `APPLY` steps within the same job (structurally closer to Scenario C, but still 1 landing table since all 5 target the same `TargetTable`) — functionally equivalent for the demo's purposes, just a different script shape.

Single tbuild invocation:
```bash
tbuild -f demo_b_load_all.tpt -v demo_b.jobvars -j demo_b_all -l 5 -z 10
```

Downstream: **one** fan-out merge routine (stored procedure or BTEQ script) that:
1. Reads unprocessed rows from `DEMO_B.CDC_LANDING` (`processed_ind = 0`), deduplicated latest-per-key.
2. Branches on `source_table` (5-way `CASE`/dynamic SQL, or 5 sequential `MERGE` statements each filtered by `WHERE source_table = 'transaction'` etc. — functionally one routine, one execution, 5 target tables touched).
3. Marks landing rows processed, records batch stats to `DEMO_ADMIN.CDC_BATCH_CONTROL`.

This is the pattern the prior research calls the **Tier 1 / best-practice** production design, scaled down — call this out explicitly as "what we'd actually build for you at 500 tables" during the demo.

### 8.3 Scenario C — 1 job, 5 step-pairs, 5 direct targets

Same 5 Kafka-Reader→Stream-Loader operator pairs as Scenario A, but defined **inside one `.tpt` script** with 5 `APPLY` steps, executed as **one `tbuild` job/process**:

```sql
DEFINE JOB DEMO_C_LOAD_ALL
DESCRIPTION 'Scenario C: 5 topics -> 1 TPT job (5 step-pairs) -> 5 target tables, no shared landing'
(
  -- Step 1: customer
  DEFINE OPERATOR Kafka_Customer TYPE DATACONNECTOR PRODUCER SCHEMA CDC_RAW_SCHEMA
  ATTRIBUTES ( VARCHAR AccessModuleName = 'libkafkaaxsmod.so',
               VARCHAR AccessModuleInitStr = '-BROKERS kafka:9092 -TOPIC cdc.public.customer -PARTITION * -MODE C -OFFSETS stored -X group.id=tpt.demo_c.customer' );
  DEFINE OPERATOR Stream_Customer TYPE STREAM SCHEMA *
  ATTRIBUTES ( VARCHAR TdpId=@TargetTdpId, VARCHAR UserName=@TargetUserName, VARCHAR UserPassword=@TargetUserPassword,
               VARCHAR TargetTable='DEMO_C.CUSTOMER_LANDING', INTEGER MaxSessions=2, INTEGER MinSessions=2, INTEGER Pack=20, VARCHAR ArraySupport='Y',
               VARCHAR ErrorTable1='DEMO_C.CUSTOMER_ET1', VARCHAR ErrorTable2='DEMO_C.CUSTOMER_ET2', INTEGER ErrorLimit=1000 );

  -- Step 2: account   (same pattern, group.id=tpt.demo_c.account, -TOPIC cdc.public.account, TargetTable=DEMO_C.ACCOUNT_LANDING)
  -- Step 3: card       (same pattern, group.id=tpt.demo_c.card,    -TOPIC cdc.public.card,    TargetTable=DEMO_C.CARD_LANDING)
  -- Step 4: payment    (same pattern, group.id=tpt.demo_c.payment, -TOPIC cdc.public.payment, TargetTable=DEMO_C.PAYMENT_LANDING)
  -- Step 5: transaction (same pattern, group.id=tpt.demo_c.transaction, -TOPIC cdc.public.transaction, TargetTable=DEMO_C.TRANSACTION_LANDING, MaxSessions=8, Pack=100)
  -- Each step uses a DIFFERENT -X group.id (unlike Scenario B's shared group) since these are independent per-table consumers within the job, not a coordinated multi-topic group.

  APPLY TO OPERATOR (Stream_Customer)    SELECT * FROM OPERATOR (Kafka_Customer);
  APPLY TO OPERATOR (Stream_Account)     SELECT * FROM OPERATOR (Kafka_Account);
  APPLY TO OPERATOR (Stream_Card)        SELECT * FROM OPERATOR (Kafka_Card);
  APPLY TO OPERATOR (Stream_Payment)     SELECT * FROM OPERATOR (Kafka_Payment);
  APPLY TO OPERATOR (Stream_Transaction) SELECT * FROM OPERATOR (Kafka_Transaction);
);
```

Single tbuild invocation, one job name, one restart log — but **still 5 landing/target tables**, i.e. no data consolidation, only *process* consolidation:

```bash
tbuild -f demo_c_load_all.tpt -v demo_c.jobvars -j demo_c_all -l 5 -z 10
```

Downstream: 5 merge routines (same as Scenario A's, pointed at `DEMO_C` instead) — this is deliberate: it isolates the "one job vs. five jobs" variable from the "one landing table vs. five landing tables" variable, so the demo cleanly shows both axes of the tradeoff independently.

**Confirm before building:** whether TPT allows multiple independent `APPLY`/producer-consumer pairs within a single script/job to actually run and restart independently of one another, or whether a failure in one step pair aborts the whole job. This materially affects the "operational cost" story for Scenario C and must be verified empirically against the local TTU install (Open Item #4) — don't assert failure-isolation behavior in front of the customer without having tested it.

### 8.4 Stretch goal — lightweight TASM demonstration

Create two workload-management-visible groups even at this small scale, to preserve the production narrative:
- A "tactical ingest" classification for the TPT sessions (Scenario A/B/C `UserName`s), so their sessions are visibly tagged/prioritized.
- A simulated "BI query" load (a simple loop issuing `SELECT COUNT(*)` / aggregate queries against `DEMO_B.TRANSACTION` from a different session) running concurrently, so the presenter can show that ingest isn't starved by concurrent analytical query load. This directly demonstrates the `WD_CDC_TPT_STREAM_HIGH` vs `WD_BI_QUERY` isolation principle from the prior research (Section 19 of the architecture doc) without needing a full TASM ruleset built out — even just showing "ingest keeps flowing while queries run" is convincing at demo scale.

### 8.5 Merge/upsert SQL pattern (shared logic across A/B/C, parameterized by scenario schema)

```sql
-- Example for TRANSACTION, parameterized by schema (DEMO_A / DEMO_B / DEMO_C)
CREATE VOLATILE TABLE vt_transaction_latest AS
(
    SELECT *
    FROM <SCENARIO_SCHEMA>.CDC_LANDING   -- or <SCENARIO_SCHEMA>.TRANSACTION_LANDING for A/C
    WHERE source_table = 'transaction'   -- omit filter for A/C's dedicated landing tables
    QUALIFY ROW_NUMBER() OVER (PARTITION BY <business_key_from_payload_json>
                                ORDER BY kafka_offset DESC) = 1
) WITH DATA ON COMMIT PRESERVE ROWS;

MERGE INTO <SCENARIO_SCHEMA>.TRANSACTION AS t
USING vt_transaction_latest AS s
ON t.transaction_id = s.transaction_id
WHEN MATCHED AND s.operation_code = 'd' THEN UPDATE SET cdc_deleted_ind = 1, cdc_operation_cd = 'd', td_update_ts = CURRENT_TIMESTAMP(6)
WHEN MATCHED THEN UPDATE SET
    amount = s.amount, status = s.status, transaction_ts = s.transaction_ts,
    cdc_operation_cd = s.operation_code, td_update_ts = CURRENT_TIMESTAMP(6)
WHEN NOT MATCHED THEN INSERT (transaction_id, account_id, card_id, amount, currency, merchant, status, transaction_ts, td_update_ts, cdc_operation_cd, cdc_deleted_ind)
VALUES (s.transaction_id, s.account_id, s.card_id, s.amount, s.currency, s.merchant, s.status, s.transaction_ts, CURRENT_TIMESTAMP(6), s.operation_code, 0);
```

Run this loop every 2–5 seconds per table/scenario (a simple shell/Python scheduler invoking BTEQ, or a Teradata stored procedure on a driver loop) — this is the "micro-batch merge" cadence from the prior research, scaled to demo pacing.

---

## 9. Live Dashboard

Recommend a lightweight self-contained web app (Python/FastAPI or Node/Express + a single HTML page with Chart.js, polling every 1–2 seconds) rather than standing up a Prometheus/Grafana stack — faster to build, no extra infra dependency, and easy to run full-screen during a customer presentation. Grafana is a reasonable alternative if the customer's own environment already has it and the presenter wants a more "production monitoring" look — flag as a build choice for the coding agent, default to the lightweight option unless told otherwise.

**Required panels:**
1. **Side-by-side scenario cards (A / B / C)** — for each: active job/process count, total rows landed (last 60s), rows/sec (rolling), Kafka consumer-group lag (sum across the group's partitions).
2. **Per-table breakdown** within each scenario — rows landed per table, useful to show `transaction` dominating vs. `customer` being nearly idle.
3. **End-to-end latency** — timestamp delta from Postgres `commit` (via Debezium's `source.ts_ms`) to Teradata `td_update_ts`, per scenario, shown as a live line chart. This is the single most persuasive number in the room — make it prominent.
4. **Event log / ticker** — scrolling feed of recent CDC events (op type, table, key) so the audience can visually correlate "I just ran an UPDATE" with "there it is landing."
5. **Manual controls** (buttons, not just charts) — "burst transaction load," "kill Scenario A transaction job," "restart Scenario A transaction job," so the presenter doesn't need a second terminal window during the failure/recovery moment (Section 10).

Data sources for the metrics collector:
- Kafka consumer-group lag: `kafka-python` `AdminClient` / `describe_consumer_groups` + partition end-offsets.
- Teradata row counts / latest `td_update_ts`: periodic `SELECT COUNT(*), MAX(td_update_ts) FROM <schema>.<table>` via the `teradatasql` Python driver (reuse the same connection profile as the merge routines).
- Postgres source event issuance: the load generator publishes its own "I just wrote X" events to a small internal queue/log the dashboard also reads, so latency can be computed end-to-end from true source-commit time, not just from Kafka.

---

## 10. Demo Script / Run of Show

1. **Setup (before the room, not live):** `docker compose up -d`, seed Postgres, register the Debezium connector, verify all 5 topics exist and are receiving the initial snapshot. Start all 3 scenario TPT jobs (Section 8.1–8.3) and their merge loops. Confirm dashboard shows steady near-zero lag across A/B/C. Start load generator at a **low steady rate** (e.g. 1–2 events/sec across the 5 tables, weighted toward `transaction`).
2. **Open on the dashboard**, not a slide — the three scenario cards already show live movement.
3. **Narrate Scenario A** — point at the job-count metric ("5 processes running right now for 5 tables — at 500 tables that's 500 processes, and on a mid-size 20-AMP production system, Teradata's per-PE and per-gateway session ceilings mean only around 38 of those can actually run at once; the rest queue").
4. **Narrate Scenario B** — "same live data, one process, one landing table, fanning out." Show the merge routine's `source_table` branching briefly (have the SQL open in a second window).
5. **Narrate Scenario C** — "the middle ground — one process like B, but still five separate target tables, useful when a customer needs strict per-table landing but wants fewer jobs to babysit."
6. **Burst mode** — click "burst transaction load" on the dashboard, watch rows/sec spike across all three scenarios simultaneously, and watch the latency panel — this is where Scenario A's per-table Pack/session tuning advantage (if any) becomes visible vs. B's shared consumer competing for throughput across 5 topics.
7. **Failure/recovery moment** — click "kill Scenario A transaction job." Show the dashboard: Scenario A's `transaction` lag climbs while B and C (unaffected) keep flowing. Click "restart" — show it resumes from the last committed Kafka offset with zero data loss (verify via `APPLIED_EVENT_LOG` row counts matching Kafka high-water mark, or simply row counts continuing to climb with no gap). Explicitly contrast: "in Scenario A, only `transaction` was affected — that's the isolation benefit. In Scenario B, killing the one job would have stopped all 5 tables — that's the tradeoff."
8. **Close on Section 11's 500-table extrapolation** — return to a slide (not the dashboard) with the concurrency math table from the prior research.

---

## 11. Scaling Narrative — Explicitly Reconnect to the 500-Table Ask

**Corrected in this revision.** The original version of this section (and the project docs it was drawn from) stated "Teradata DBS session ceiling ≈ 1,024 (hard limit, non-negotiable)" as the mechanism behind the concurrency math. That is wrong: **1,024 is the maximum number of nodes per system configuration** (Appendix B, *Database Administration* manual, R20.00) — an unrelated platform limit. There is no single global 1,024-session ceiling. This was caught by re-checking the Obsidian wiki, whose `Teradata System Limits` page was corrected same-day after the same error was traced back to its source. Do not repeat the "1,024 sessions" line in front of a customer — a technically sharp customer (this is Teradata's own product) may know that number means something else, and it would undercut the demo's credibility at exactly the moment it's landing its strongest point.

**The corrected mechanism**, per `Teradata System Limits.md` and `Streaming Ingestion at Scale (500 Topics).md`:
- Real session ceilings are **per-PE (120 sessions per PE)** and **per-gateway (1,200 sessions certified maximum, 600 default, tunable)** — not a flat global number. Gateway count scales with node count (one gateway vproc per node by default), so total system-wide session capacity is a function of *node count × per-gateway limit*, not a fixed constant.
- 500 independent TPT jobs × ~4 sessions/job = ~2,000 sessions needed. Whether that fits depends on the gateway/PE topology of the specific target system, but the headline conclusion is unchanged from the original (uncorrected) framing:

| System | AMP Count | Max Concurrent Jobs | Queue Depth |
|---|---|---|---|
| POC | 4 | 192 | 308 |
| Small Prod | 8 | 96 | 404 |
| Medium Prod | 20 | 38 | 462 |
| Large Prod | 120 | 8 | 492 |

- Counter-intuitively, **larger systems run fewer concurrent jobs** — more AMPs/nodes means more sessions consumed per job under this model, not more jobs runnable. On a 20-AMP production system: 38 jobs run, 462 queue. Queue wait: 10 minutes to 31 hours. This is serialized batch with the appearance of streaming intent, not real-time.

Extrapolating the demo's three scenarios to 500 tables:
- **Scenario A** → the broken pattern above. 500 independent jobs is not viable on any but the smallest systems. **This is the demo's strongest visual argument against the "just give every table its own job" instinct**, and Scenario A in the room is deliberately built to make that instinct feel natural before the numbers undercut it.
- **Scenario B** → the recommended Tier 1 production pattern (`Streaming Ingestion at Scale (500 Topics).md`, Architecture 1): 10–30 Debezium connectors grouped by domain/criticality, 1–3 (or 50–100 for independent-topic sources) Kafka topics, 1 TPT job, 1 landing table, fan-out merge. Complexity LOW, latency 100–500ms, throughput 8K–40K rows/sec sustained. Independently corroborated benchmarks from the wiki's TPT reference: DHL (500M msg/day, 17 sources, 6,200 rows/sec at 0.2% additional CPU), a global car manufacturer (8,000 msg/sec, 500M msg/day via MQ), and an internal Telco reference (1 billion messages/hour).
- **Scenario C** → the middle ground, close to the `Tiered Streaming Architecture.md` pattern: top 10–20 hot tables get dedicated step-pairs (full isolation where it matters), remaining ~480 warm/cold tables go through Scenario B's fan-out pattern. Result: ~22 concurrent jobs instead of 500, comfortably within any system's session ceiling. This is the answer to a customer who says "we need per-table isolation for compliance" without conceding to 500 independent jobs.

Recommend closing with the **one-page summary table** already drafted in `03-Architecture-Decision-Tree.md` ("STREAMING AT SCALE: ARCHITECTURE SELECTION") as a leave-behind slide — its recommendations are unaffected by the session-limit correction, only the underlying mechanics text should be updated if that document is also revised.

---

## 12. Gaps, Assumptions, and Open Items (Resolve Before/During Build)

These were not fully specified by the original ask or could not be independently verified in this research pass — flagging rather than guessing, per the standing engineering practice of not shipping unverified assumptions into a customer-facing demo.

1. **Target Teradata system.** A live Vantage system (20.00.30.76) is reachable from this session via MCP tools. Confirm with Paul whether this is the intended demo target (and that creating `DEMO_A/B/C/ADMIN` databases on it is acceptable — check available PERM space and whether it's shared with other users/workloads) or whether a dedicated Vantage Express VM / sandbox instance should be provisioned instead. Recommend a dedicated instance for anything that will be run live in front of a customer, to avoid interference from other workloads on a shared dev system.
2. **TTU/TPT availability — largely resolved.** `tpt-runner` now uses the `teradata/tpt` Docker Hub image (Section 4) rather than requiring a host TTU install, removing the original blocking dependency. The one remaining check: confirm that image includes the Kafka Access Module specifically, not just core TPT/`tbuild` (see the verification command in Section 4) — Access Modules have historically been packaged as an add-on to base TTU, per a prior internal demo project on this exact stack that installed them as a separate step. If the module is missing from the image, extend it (Dockerfile pattern available in the Obsidian vault's `SA-Reference/Kafka-TPT-demo-code/Dockerfile`, referenced but not embedded in this spec — pull it from the vault if needed).
3. **Multi-topic ingestion for Scenario B — redesigned, moderate remaining risk.** Section 8.2 now uses a documented TPT mechanism (multiple DataConnector Producer instances, one per topic, combined via `UNION ALL`, coordinated as one consumer group via `-X group.id=`) rather than a guessed comma-list/regex topic parameter — this is a much better-grounded design than the v1.0 draft. However, the wiki's documentation of this pattern is explicitly for multiple instances reading *partitions of the same topic*; extending it to *different topics per instance* is a reasonable, consistent extrapolation but not a verbatim documented example. Test this shape against the actual TPT install early — it's the single highest-uncertainty piece of Scenario B — and fall back to the alternative script shape noted in Section 8.2 if `UNION ALL` across differently-topic'd Producer instances is rejected.
4. **Whether a single `.tpt` job with multiple independent `APPLY` step-pairs (Scenario C) fails/restarts as a unit or per-step.** This determines what "operational cost" claim can honestly be made about Scenario C's failure isolation vs. Scenario A's. Test this empirically (kill one step's target session mid-run, observe whether the other 4 steps continue) before using it as a talking point in Section 10 step 7.
5. **Data realism / volumes.** Seed row counts (500–1,000 per table) and load-generator rates (1–2/sec steady, burst to 100+/sec on `transaction`) are demo-pacing choices, not derived from a specific customer profile. If this demo is for a specific named customer, consider re-deriving these from their actual table row counts / peak TPS if known, so the "this scales to your numbers" close in Section 11 lands more specifically.
6. **Avro/Schema Registry.** Out of scope per Section 2, but the customer's original ask (per the imported project prompt) explicitly mentions "AVRO Support" as a topic of interest from a prior meeting. If schema evolution/Avro is likely to come up in Q&A, consider a small follow-up demo or at least a prepared slide — don't let this be the first time it's addressed live.
7. **Security.** No TLS/SASL, no PII masking, and plaintext-ish handling of `card_number_masked` are fine for a demo but should be explicitly caveated as "hardened separately for production" if a security-conscious stakeholder is in the room — Section 15 of the architecture doc's hardening checklist is the reference to point to.
8. **Dashboard hosting during the live demo.** Confirm whether the presenter needs this reachable over the customer's network/screen-share (browser tab) or purely local — affects whether the dashboard needs auth or can stay open/unauthenticated for the demo's lifespan.
9. **Vault self-inconsistency, worth fixing at the source.** `wiki/concepts/Teradata Parallel Transporter (TPT).md` §"Concurrency Constraints at Scale — The 500-Job Problem" (around line 403) still states the old "1,024 global, non-negotiable" session-limit claim, even though `Teradata System Limits.md`, `Streaming Ingestion at Scale (500 Topics).md`, and `Tiered Streaming Architecture.md` were all corrected same-day (2026-07-09) and explicitly cross-reference the fix. This spec used the corrected figures throughout, but the TPT concept page itself is now the one stale page in an otherwise-corrected set — worth a quick edit in the vault so the next person reading that page in isolation doesn't pick up the wrong number again.

---

## 13. Build Checklist for the Coding Agent

- [ ] `docker-compose.yml` with postgres, kafka (KRaft), kafka-connect (Debezium), load-generator, metrics-collector, dashboard services.
- [ ] Postgres DDL + seed script (Section 5).
- [ ] Debezium connector JSON + registration script (Section 6); verify 5 topics appear with `kafka-topics.sh --list`.
- [ ] Teradata DDL for `DEMO_A`, `DEMO_B`, `DEMO_C`, `DEMO_ADMIN` (Section 7) — run against the confirmed target system (Open Item #1).
- [ ] 5 Scenario-A `.tpt` scripts + run script (Section 8.1), all `tbuild` invocations including `-l 5 -z 10`.
- [ ] 1 Scenario-B `.tpt` script + run script (Section 8.2) — pending Open Item #3 confirmation (multi-topic `UNION ALL` shape).
- [ ] 1 Scenario-C `.tpt` script (5 step-pairs) + run script (Section 8.3) — pending Open Item #4 confirmation (step failure isolation).
- [ ] Confirm `teradata/tpt` image includes the Kafka Access Module (Section 4 verification command) before writing any script against it.
- [ ] Merge routines for A/B/C (Section 8.5), scheduled on a 2–5s loop.
- [ ] Load generator script with steady-rate and burst modes, plus manual INSERT/UPDATE/DELETE convenience CLI for live ad-hoc use.
- [ ] Metrics collector (Kafka lag + Teradata row counts/latency).
- [ ] Dashboard web app (Section 9), all 5 panel types, manual kill/restart/burst controls.
- [ ] End-to-end test per Section 14 before calling this demo-ready.
- [ ] One-page "reset to zero" runbook (truncate all Teradata target/landing tables, reset Kafka consumer group offsets or recreate topics, restart TPT jobs, restart load generator) for rehearsal.

---

## 14. Acceptance / Verification Requirements (Do Not Skip)

Per standing practice: an implementation is only done when it has been run and observed, not when the code "looks right." Before this is called demo-ready, the coding agent must actually execute and observe each of the following, not just implement them:

0. **`-l` flush behavior (test this first, before anything else):** start a Scenario A job with the load generator already running continuously, and confirm rows actually appear in the landing table within `-l`'s flush interval (5s) — not just at job shutdown. This is the gotcha from Section 8.0 that produces a job which looks healthy but shows zero rows for the entire demo if `-l` is missing or misconfigured. Do this check before investing time debugging anything else in Scenarios A/B/C.
1. **End-to-end latency check:** with the load generator running at steady rate, issue a manual `UPDATE` on a known `transaction_id` in Postgres, and confirm the new value appears in all three of `DEMO_A.TRANSACTION`, `DEMO_B.TRANSACTION`, `DEMO_C.TRANSACTION` within a few seconds — query them directly, don't infer from the dashboard alone.
2. **Delete handling:** issue a `DELETE` in Postgres on a seeded row, confirm `cdc_deleted_ind = 1` (not a hard delete, per the soft-delete pattern in Section 8.5) lands in all three targets.
3. **Fan-out correctness (Scenario B specifically):** with all 5 tables receiving concurrent changes, confirm `DEMO_B.CDC_LANDING` rows are correctly routed to all 5 target tables and no rows are silently dropped or misrouted to the wrong table — spot-check row counts against Scenario A/C for the same time window as a cross-check.
4. **Isolation check (Scenario A):** kill only the `demo_a_transaction` `tbuild` process; confirm the other 4 Scenario-A jobs and both Scenario B and C continue landing rows uninterrupted; confirm `demo_a_transaction` resumes from its last committed offset on restart with no gap and no duplicate-driven double counting (verify via `APPLIED_EVENT_LOG` or a business-level row count reconciliation against Postgres).
5. **Scenario C step-failure behavior:** actually test Open Item #4 empirically — kill/interrupt one step-pair's target table access (e.g. revoke access or drop the error table) and observe whether the job aborts entirely or only that step fails, and document the actual observed behavior in the final demo runbook rather than the assumed behavior in Section 8.3.
6. **Burst load:** confirm the dashboard's rows/sec and latency panels visibly respond within the demo's live-presentation timeframe (a few seconds) when burst mode is triggered — if the dashboard polling interval makes this feel laggy, tighten it.
7. **Rehearsal run:** do at least one full run-through of the Section 10 script end-to-end, timed, before it's presented to a customer.

---

**End of specification.**
