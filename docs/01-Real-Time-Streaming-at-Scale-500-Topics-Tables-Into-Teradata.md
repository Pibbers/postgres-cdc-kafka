---
title: Real-Time Streaming at Scale - 500 Topics/Tables Into Teradata
type: concept
tags: [kafka, streaming, teradata, tpt, cdc, architecture, scalability, customer-discovery, requirements-gathering]
sources: []
created: 2026-06-30
updated: 2026-06-30
---

# Real-Time Streaming at Scale into Teradata

## Comprehensive Guide: 500+ Topics/Tables Architectures

When a customer specifies ingesting **500 Kafka topics OR 500 source tables** into Teradata at real-time scale, the architectural challenge is fundamentally different depending on the source topology. This document consolidates the analysis, design patterns, and critical discovery questions needed to architect the right solution.

---

## Executive Summary: The Three Scenarios

| Scenario | Source | Kafka Topology | Architecture | Complexity | Recommended |
|---|---|---|---|---|---|
| **500 Independent Kafka Topics** | External event streams | 500 topics × 3–6 partitions = 1,500–3,000 partitions | 3–5 domain-segmented TPT jobs | High | ✓ If truly independent sources |
| **500 Source Tables (CDC)** | Relational database | 1–3 consolidated topics | 1 TPT job + CDC tool | Low | ✓ **PRIMARY RECOMMENDATION** |
| **500 Per-Table TPT Jobs** | Either | 500 independent topics OR 500 CDC topics | 500 independent Stream jobs | Very High (Operational Nightmare) | ✗ Avoid unless <100 rows/sec per table |

---

# Part 1: Critical Customer Discovery Questions

## Tier 1: Clarify the Source (Non-Negotiable)

### Question 1.1: What is the actual source system?

**Ask:** "Are these 500 topics coming from **independent Kafka producers** (microservices publishing independently), or are they **change events captured from database tables** via CDC?"

**Why it matters:** 
- Independent topics = manage topology complexity in Kafka
- CDC tables = manage source volume complexity in CDC tool; Kafka becomes simple

**Red flag:** If customer says "we have 500 Kafka topics" without describing the upstream producers, they may not understand their own architecture. Dig deeper.

---

### Question 1.2: Are these truly 500 or is this a request for "data from 500 systems"?

**Ask:** "If I told you these 500 sources could be consolidated into 80–150 Kafka topics without losing functionality, would that change your requirements?"

**Why it matters:**
- "500 sources" ≠ "500 topics"
- Schema consolidation can reduce topic count by 70–80%
- Example: 120 payment sources (transfers, checks, ACH) → 1 `payments` topic with `source_system` field

**Listen for:** "Well, we never actually thought about consolidation. Our architects just assumed one topic per source."

---

### Question 1.3: Do all 500 topics have the same schema or dramatically different schemas?

**Ask:** "Can you describe the key schema differences? Are they all variations of a common event (with optional/required fields), or are they completely unrelated?"

**Why it matters:**
- Same schema family → consolidate into 1–3 topics
- Completely different schemas → may need separate topics, but can still consolidate by domain

**Example answers:**
- ✓ "All payments events with optional fields (ACH has extra 'routing_number', wire has 'swift_code')" → Consolidate
- ✗ "Customer master updates, payment transactions, inventory movements, employee records" → Keep separate, but group by domain

---

## Tier 2: Understand the Topology and Ingestion Patterns

### Question 2.1: Is the data **push** (producers send to Kafka) or **pull** (you extract from source systems)?

**Ask:** "Who is responsible for getting data into Kafka? Do your upstream systems publish events themselves, or does a CDC/ETL tool extract data from databases?"

**Why it matters:**
- **Push:** Topics are already created; you're reading them. Focus on consumer group strategy.
- **Pull (CDC):** You choose the consolidation topology. Opportunity to reduce topics.

**If CDC:** Follow up with Question 1.4 below.

---

### Question 2.2: If CDC, what is the source database system?

**Ask:** "If these come from database tables, what database? Oracle, SQL Server, PostgreSQL, Teradata, something else?"

**Why it matters:**
- Oracle → HVR (log-based CDC, zero source load)
- PostgreSQL → Debezium (open source, good tooling)
- SQL Server → Debezium or Informatica CDC
- Mixed sources → Multi-source CDC orchestration

**Each CDC tool has different operational characteristics and latency profiles.**

---

### Question 2.3: What is the peak transaction rate across all 500 sources?

**Ask:** "At peak load (peak business hour), how many messages per second are produced across all 500 topics? For example, 10,000 msg/sec? 100,000 msg/sec?"

**Why it matters:**
- Determines Kafka partition count strategy
- Determines Teradata loader scaling and TASM allocation
- 10,000 msg/sec → 1–2 TPT instances sufficient
- 100,000 msg/sec → 4–8 TPT instances, multiple consumer groups

**Listen for:** "We don't know. We're building this new." → This is normal; estimate based on comparable systems.

---

## Tier 3: Understand SLA and Criticality

### Question 3.1: What are the required latencies per topic/table?

**Ask:** "Do all 500 topics need the same latency SLA? Or do some have real-time requirements (≤200ms) while others are fine with 5s or 5 minutes?"

**Why it matters:**
- Determines consumer group strategy and TASM workload tiers
- Real-time topics → separate consumer group, tactical tier, dedicated TPT job
- Batch-like topics → shared consumer group, off-peak batch

**Typical segmentation:**
- Fraud/Risk: ≤200ms (tactical)
- Operational/Analytics: ≤5s (standard)
- Reporting/Batch: ≤5m (off-peak)

**If customer says "all real-time," follow up:**

---

### Question 3.1b: If all 500 claim real-time, which 10–20 are actually critical?

**Ask:** "If you had to pick the top 10–20 topics that would cause business impact if they lagged by 5 seconds, which are they?"

**Why it matters:**
- Customers conflate "possible to ingest" with "needs low latency"
- Typical finding: 10–20 hot topics, 80 warm, 400 cold
- Allows **tiered architecture:** hot topics get per-topic attention, cold topics batch at night

**Listen for:** "All 500 are critical." → Probe more. Ask about specific business impact.

---

### Question 3.2: What are the failure tolerance requirements?

**Ask:** "If a topic ingestion job fails, how long can that data be delayed or missed before it impacts the business? Can you re-run overnight?"

**Why it matters:**
- Determines recovery strategy and complexity
- High tolerance (can batch hourly or nightly) → Simpler architecture
- Low tolerance (must never lose) → Exactly-once semantics, deduplication, more complex

**Related:**
- "Do you need exactly-once semantics or at-least-once?"
- "Can you deduplicate at the Teradata side?"

---

## Tier 4: Operational and Governance Constraints

### Question 4.1: Do you need per-table/per-topic isolation for compliance or operational reasons?

**Ask:** "For audit, compliance, or operational purposes, do you need each table to have its own ingestion job, with separate monitoring and failure handling? Or can they be consolidated?"

**Why it matters:**
- Per-table isolation = 500 independent jobs, queuing bottleneck (max ~50–200 active)
- Consolidated = 1–5 jobs, no queuing, but less granular control

**Clarify the "why":**
- Compliance/regulatory → Consolidate with tagging/lineage (cheaper than 500 jobs)
- Operational audit → Consolidate with domain labeling
- Strict failure isolation → Only needed for top 10–20 critical tables

---

### Question 4.2: Do you have existing CDC tooling or should we recommend one?

**Ask:** "If the source is databases, do you already own HVR, Debezium, GoldenGate, or Informatica? Or are we choosing new tools?"

**Why it matters:**
- Using existing tools → Cost lower, operational knowledge available
- Buying new → Licensing cost, learning curve, but best-fit tool

**If they need guidance:**

| Source | Recommendation | Rationale |
|---|---|---|
| Oracle (1000s of tables) | HVR | Log-based, zero source load, 100–500ms latency |
| PostgreSQL at scale | Debezium | Open source, well-maintained, good Kafka integration |
| SQL Server | Debezium or Informatica | Both solid; Informatica if cloud-first |
| Mixed (Oracle + PG + SQL Server) | Informatica or Qlik Replicate | Multi-source CDC orchestration |

---

### Question 4.3: What monitoring and alerting do you expect?

**Ask:** "For monitoring, do you want one dashboard for the entire streaming pipeline, or per-topic/per-table dashboards? What metrics matter most?"

**Why it matters:**
- Affects operational tooling and complexity
- 500 per-topic dashboards = very high noise
- 1–5 consolidated dashboards = manageable

**Metrics that matter:**
- Consumer lag (per domain, not per topic)
- Insert rate (Teradata rows/sec)
- Failure rate (errors, retries)
- TASM tactical tier allocation (is streaming starving reporting?)

---

## Tier 5: Current State and Migration Path

### Question 5.1: Is this greenfield or are you migrating existing pipelines?

**Ask:** "Is this a new project, or are you replacing existing pipelines (Informatica, Spark, Sqoop, etc.)?"

**Why it matters:**
- Greenfield → Design optimally from scratch
- Migration → Likely need interim dual-run, gradual cutover

**If migration:**
- "What is the current ingestion tool?"
- "What are the failure modes you want to eliminate?"
- "Can we run parallel for validation (X weeks)?"

---

### Question 5.2: What is your implementation timeline and risk tolerance?

**Ask:** "Can you pilot with 10–20 topics/tables first, or do all 500 need to be live by [date]?"

**Why it matters:**
- Pilot → Domain-segmented design, proven first, then scale
- Big bang → Higher risk, needs more upfront architecture certainty

**Recommendation:** Always pilot 10–20 topics/tables first (representative sample). Learn, tune TASM, validate monitoring. Then scale to full 500.

---

# Part 2: Architecture Decision Trees

## Decision Tree 1: Determine Source Topology

```
Customer says: "500 Kafka topics into Teradata"

├─ Are these topics already in Kafka?
│  │
│  ├─ YES → Independent Topics architecture (see Section 2.1)
│  │
│  └─ NO, we're building the producer side
│     │
│     └─ Is the upstream data in databases?
│        │
│        ├─ YES → CDC architecture (see Section 2.2) ← RECOMMENDED
│        │
│        └─ NO, data comes from APIs, logs, event streams
│           └─ Independent Topics architecture (see Section 2.1)
```

---

## Decision Tree 2: Concurrency Reality Check

```
Consumer Group Strategy:

Start: 500 topics / tables

├─ All have same latency SLA?
│  │
│  ├─ YES, all real-time (≤200ms)
│  │  └─ Are all 500 truly critical, or top 10–20?
│  │     ├─ Top 10–20 critical → Tiered: hot=1 job, rest=1 job (2 jobs)
│  │     └─ Actually all critical (rare) → 1 job, high DataConnector count
│  │
│  └─ NO, mixed SLAs (fraud=200ms, ops=5s, batch=nightly)
│     └─ Segment by criticality tier → 3–5 domain jobs

├─ Customer insists on per-table isolation?
│  │
│  ├─ YES, strict compliance/audit needs
│  │  └─ Tiered compromise: hot=per-table, cold=consolidated (see Section 2.4)
│  │  └─ Warn: 500 jobs → massive queuing, operational nightmare
│  │
│  └─ NO → Consolidate (recommended)
│     └─ If CDC source: 1 job
│     └─ If independent topics: 3–5 domain jobs

└─ What's the max concurrent active jobs?
   (See Teradata constraints: ~50–200 depending on system size)
```

---

# Critical Summary Tables

## Architecture Recommendation At A Glance

| Scenario | Recommended | Complexity | Throughput | Latency | Operational Toil |
|---|---|---|---|---|---|
| **Database source (CDC)** | 1 CDC tool + 1 TPT job | LOW | 8–40K rows/sec | 100–500ms | LOW ✓✓✓ |
| **Independent topics** | 3–5 domain TPT jobs | MEDIUM | 40–80K rows/sec | 200ms–5s | MEDIUM ✓✓ |
| **All hot + isolation** | Tiered (20 hot + 1 cold) | MEDIUM-HIGH | 20–50K rows/sec | tiered | MEDIUM-HIGH ✓ |
| **500 per-table jobs** | ✗ DO NOT DO | EXTREME | 8–50K (queued) | 10 min–31 hrs | EXTREME ✗✗✗ |

---

## Concurrency Limits By System Size

| System | AMP Count | Max Sessions | Max Concurrent Jobs | Queue Depth |
|---|---|---|---|---|
| POC | 4 | 1,024 | 192 | 308 |
| Small Prod | 8 | 1,024 | 96 | 404 |
| Medium Prod | 20 | 1,024 | 38 | 462 |
| Large Prod | 120 | 1,024 | 8 | 492 |

**Key insight:** Larger systems have FEWER concurrent jobs (DBS session limit is global, not per-AMP).

---

## Critical Discovery Questions Summary

**Ask these 5 in the first 15 minutes:**

1. **Q1.1:** Are these independent Kafka topics or database tables via CDC?
2. **Q2.3:** What's the peak throughput (messages/second)?
3. **Q3.1:** Do all 500 need same latency, or tiered (fraud ≤200ms, ops ≤5s, batch ≤24h)?
4. **Q3.1b:** Which 10–20 are actually critical (if they claim all)?
5. **Q4.1:** Do you need per-table isolation, or is consolidated OK?

---

## Recommended Architecture (Based on Common Answers)

**If: Database source + consolidatable**
→ **CDC Architecture (BEST)**
- HVR or Debezium consolidates 500 tables → 1–3 topics
- Single TPT job
- Complexity: LOW | Latency: 100–500ms | Operational Toil: LOW

**If: Independent Kafka topics + mixed SLAs**
→ **Domain Segmentation (GOOD)**
- 3–5 consumer groups (payments, customers, ops, etc.)
- 3–5 domain TPT jobs
- Complexity: MEDIUM | Latency: 200ms–5s | Operational Toil: MEDIUM

**If: All hot + strict isolation required**
→ **Tiered Compromise (ACCEPTABLE)**
- Top 20 tables: per-table TPT job (20 jobs)
- Remaining 480: consolidated CDC job (1 job)
- Total: ~22 concurrent jobs (manageable)
- Complexity: MEDIUM-HIGH | Operational Toil: MEDIUM-HIGH

**If: Customer insists on 500 independent TPT jobs**
→ **WARN about reality, then propose Tiered**
- Teradata DBS limit = 1,024 sessions
- 500 jobs × 4 sessions = 2,000 needed
- Only 50–200 can run; 300–450 queued indefinitely
- This is NOT real-time; it's serialized batch

---

## Final Checklist: Before Your Next Customer Call

- [ ] **Determine source topology** — Topics? Tables? Both?
- [ ] **Understand criticality** — All hot or tiered?
- [ ] **Identify consolidation opportunities** — Can 500 → 150 topics?
- [ ] **Probe SLAs** — Real-time vs. batch?
- [ ] **Clarify isolation requirements** — Compliance-driven or preference?
- [ ] **Assess operational readiness** — Can they manage 500 jobs?
- [ ] **Recommend architecture** — CDC / Segmented / Tiered
- [ ] **Propose pilot scope** — 10–20 topics/tables
- [ ] **Define success criteria** — Latency, rebalancing, lag thresholds
- [ ] **Establish timeline** — 16–20 weeks to full production

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-30  
**Audience:** Solutions Architects, Sales Engineers, Customer Success
