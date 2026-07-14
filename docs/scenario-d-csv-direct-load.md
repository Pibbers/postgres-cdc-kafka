# Scenario D — CSV-Native Direct-to-Target TPT Load (No Landing Table)

**Audience:** Coding agent / engineer extending the demo
**Status:** Built and verified end-to-end against the live Teradata target (192.168.1.205) on 2026-07-14
**Companion to:** `docs/kafka-tpt-teradata-demo-spec.md` (Scenarios A/B/C)

---

## 0. Purpose

Scenarios A/B/C all consume Debezium's JSON CDC envelope, which forces a landing-table-plus-downstream-merge pattern (spec Section 8.5) because the JSON's nested `before`/`after`/`source` shape doesn't map onto flat TPT `SCHEMA` columns — the raw payload gets landed as opaque text and shredded later by a BTEQ `MERGE`. Scenario D asks: what if the source data is *already* flat and typed? It adds three new Kafka topics carrying plain CSV rows from a standalone synthetic feed (**not** Postgres/Debezium CDC), and shows that TPT can `APPLY`-upsert straight into the real target tables from one consolidated job — no landing table, no merge loop, no BTEQ script at all.

This directly extends the "consolidate ingest" thesis Scenario B argues for (spec Section 11): when the data shape allows it, the landing-and-shred tier isn't just consolidatable, it's *removable*.

**Data model** (3 new tables, distinct from A/B/C's 5, reference-style so the CSV framing feels natural): `merchant` (merchant_id, merchant_name, category, city, country, status), `branch` (branch_id, branch_name, city, region, status), `fx_rate` (fx_rate_id, currency_pair, rate, rate_date). Each CSV row carries the business columns plus an explicit `op_code` (I/U/D) and `event_ts`, since there's no Debezium envelope to source CDC metadata from — the synthetic producer manufactures it directly.

---

## 1. The Core Mechanism (Empirically Verified)

A single TPT `APPLY` **statement** can hold multiple comma-joined `APPLY` **clauses**, each with its own `TO OPERATOR`, all fed by **one** shared `SELECT ... FROM OPERATOR (...)` at the end:

```sql
APPLY (...) TO OPERATOR (op1)
,
APPLY (...) TO OPERATOR (op2)
,
APPLY (...) TO OPERATOR (op3)
SELECT ... FROM OPERATOR (producer1)
UNION ALL SELECT ... FROM OPERATOR (producer2)
UNION ALL SELECT ... FROM OPERATOR (producer3);
```

This is the same grammar classic MultiLoad has used for decades to populate up to 5 target tables from one job — not a new invention. It is **different** from Scenario C's earlier rejected multi-`APPLY` attempt (spec Section 8.3's "Confirm before building" note): that attempt used fully independent `APPLY ... SELECT ...;` **statements**, each with its own `SELECT`, which the grammar genuinely forbids (one job step = one shared `SELECT`). The shape above shares one `SELECT` across multiple clauses instead.

### 1.1 Two constraints not documented anywhere, found only by hitting them

Spiked live against 192.168.1.205 on 2026-07-14 (throwaway 2-table script, scratch tables under `DEMO_ADMIN`, dropped after) before the real 3-table script was written:

1. **Consumer operators used this way must declare `SCHEMA *` (deferred), not a named schema.** An explicit schema throws:
   ```
   TPT_INFRA: TPT03107: Operator 'X' has explicit input schema. Restricted APPLY
   statement allows only deferred schema for consumer operators.
   ```
2. **No modifiers are accepted between `TO OPERATOR (...)` and the next clause/the final `SELECT`** in this "Restricted APPLY Statement" grammar — not `WHERE`, not `IGNORE DUPLICATE ROWS`. Both were tried and both produced the identical syntax error:
   ```
   TPT_INFRA: TPT02954: Error: Syntax error ... At "WHERE" missing SEMICOL_ in Rule: STEP
   TPT_INFRA: TPT02954: Error: Syntax error ... At "IGNORE" missing SEMICOL_ in Rule: STEP
   ```

**Consequence:** per-table row routing has to be embedded **inside the DML statement text itself**, not as an `APPLY`-level modifier:

- The `UPDATE`'s `WHERE` guards both the row match *and* the type-unsafe `CAST`, via `CASE WHEN :RECORD_TYPE='merchant' THEN CAST(:MERCHANT_ID AS BIGINT) END`. A mismatched-type row (e.g. a `branch` row flowing through the `merchant` clause) never gets its `CAST` evaluated, so there's no data-conversion error on the 0-row-match path.
- The `INSERT` uses a guarded `SELECT ... WHERE :RECORD_TYPE='merchant'` form instead of `VALUES (...)`. When the guard is false, the `SELECT` yields zero rows and the `INSERT` cleanly inserts nothing — no `NOT NULL` violation on a mismatched row.

### 1.2 A third constraint found while building the real (non-spike) script

`ArraySupport` is incompatible with this DML shape:
```
STREAM_MERCHANT: TPT16160: Error: DML group 1 must contain a single SQL statement
or a pair of UPDATE and INSERT statements (for an UPSERT) when using the Array
Support feature.
```
The guarded `SELECT ... WHERE` form of `INSERT` isn't a "pair" TPT recognizes as array-batchable (it wants a plain `VALUES (...)` `INSERT` for that). Set `ArraySupport = 'Off'` on these `STREAM` operators — correctness over batching throughput, and at this reference-data scale it doesn't matter.

Also: `ArraySupport`'s valid values are `'On'`/`'Off'`, not `'Y'`/`'N'` like several other TPT attributes in this project use — a straight copy-paste from A/B/C's attribute blocks will throw `TPT10310: Invalid 'ArraySupport' attribute value`.

### 1.3 A fourth constraint: derived columns need a schema home

The `SELECT`'s `'merchant' AS RECORD_TYPE` literal (the discriminator used by the DML's `:RECORD_TYPE` guards) isn't a real wire field from any producer — it's injected in the projection. Referencing it as `:RECORD_TYPE` in the `APPLY` DML fails to compile unless *some* `DEFINE SCHEMA` in the job — not necessarily attached to any operator — declares a matching `RECORD_TYPE` column:
```
TPT_INFRA: Semantic error ...
TPT_INFRA: TPT03278: Undefined derived column name: RECORD_TYPE must be defined in a job schema.
```
Fix: declare an unattached `CSV_WIDE_SCHEMA` (all 18 columns across all 3 tables' union-compatible projection, including `RECORD_TYPE`) purely so the parser has somewhere to resolve the name from. See `tpt/tbuild/scenario_d/load_all.tbuild` for the full definition.

### 1.4 Known, accepted noise

Because the `INSERT` fires unconditionally (guarded only by `RECORD_TYPE`, not by whether the row already exists), **every successful `UPDATE` is followed by a same-row `INSERT` attempt that hits a duplicate-key violation** and lands in that `STREAM` operator's own `ErrorTable`. This is expected and harmless — per-row, not job-fatal — and can't be silenced via `IGNORE DUPLICATE ROWS` since that modifier isn't available in this grammar position (§1.1). Size `ErrorLimit` generously; non-zero `ErrorTable` row counts are normal for this design, not a failure signal.

### 1.5 Out-of-order handling

There's no landing-table batch to `QUALIFY ROW_NUMBER() ... ORDER BY source.ts_ms DESC` over (that's how A/B/C get "latest wins"). Instead, each row is applied independently with a per-row guard: `WHERE ... AND :EVENT_TS > source_updated_ts`. A stale/out-of-order event's `UPDATE` matches 0 rows (guard fails), then its unconditional `INSERT` attempt hits the duplicate-key case from §1.4 and is silently dropped. Net effect: correct, verified empirically (a synthetic out-of-order update in the Phase 0 spike did not clobber a newer value).

---

## 2. Build

| Piece | Path |
|---|---|
| Teradata DDL (3 target tables, `DEMO_D`, no landing tables) | `teradata/ddl/07_scenario_d.bteq` |
| Reset hook | `teradata/reset.bteq` |
| CSV producer (Kafka, not Postgres — port 8092) | `csv-load-generator/generator.py` |
| Docker service | `docker-compose.yml` → `csv-load-generator` |
| TPT job script | `tpt/tbuild/scenario_d/load_all.tbuild` |
| Launch wrapper | `tpt/run_scenario_d.ps1` |
| Dashboard wiring | `dashboard/app.py` (`JOBS`, `TARGET_TABLES`, D-specific latency block), `dashboard/static/index.html`, `dashboard/static/pages/scenario-d.html` |

Kafka topics (created manually, no Kafka Connect/Debezium involved): `csv.merchant`, `csv.branch`, `csv.fx_rate`.

**Latency**, unlike A/B/C, is computed directly off each target table's own bookkeeping columns (`td_update_ts - source_updated_ts` on the latest-touched row) rather than a landing-table join — there's no landing table to join against. **Backlog is always 0** by design, which is itself part of the dashboard story: Scenario D's card shows the same shape as B/C's but with a permanently-empty backlog column.

---

## 3. Demo Script Beat (extends spec Section 10)

Narrate Scenario D immediately after Scenario B, while B's mechanics are fresh: *"Same idea as B — one job, one consolidated stream from multiple topics. But this source is CSV, not Debezium JSON, so there's no landing table and no separate merge process running on a 2-5 second loop. Watch the latency panel next to B's during a burst — D's number reflects only the TPT flush interval."* Then explicitly name the tradeoff: Scenario B's landing table doubles as a raw-payload audit/replay trail; Scenario D has no equivalent staging tier, so that safety net doesn't exist here. Not a strictly-better replacement for B — the same "consolidate ingest" thesis, one step further, for sources where the data shape allows it.

---

## 4. Verification Performed

1. Phase 0 spike (2 scratch tables, `DEMO_ADMIN.SPIKE_T1`/`SPIKE_T2`, dropped after): confirmed the core mechanism, both grammar constraints in §1.1, and correct upsert/soft-delete/out-of-order-rejection semantics via direct table query (not just "job completed successfully").
2. Real 3-table script (`DEMO_D.MERCHANT`/`BRANCH`/`FX_RATE`): seeded 50/30/15 rows via `csv-load-generator --mode seed`, ran the job, confirmed exact row counts landed with correct content (spot-checked business columns, `cdc_operation_cd`, `cdc_deleted_ind`).
3. Manual update path: `csv-load-generator --mode manual --table merchant --op update --id 1` published a real change; a subsequent job run correctly updated `merchant_id=1`'s `merchant_name` in place (confirmed via direct query, before/after).
4. Dashboard: `/api/metrics` confirmed to return a populated `"D"` scenario block with correct row counts, `backlog: 0`, and computed latency; `index.html`, `scenario-d.html`, and the other explainer pages confirmed to load (HTTP 200) with the new nav wiring intact.

**Not yet done** (rehearsal-stage items, consistent with spec Section 14's "run it, don't just implement it" discipline): a live burst-mode comparison against Scenario B's latency panel during an actual presentation rehearsal; a full concurrent-load test with all 4 scenarios running simultaneously.
