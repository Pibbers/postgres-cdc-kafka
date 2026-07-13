# Streaming at Scale: Architecture Decision Tree & Visual Guide

## Quick Visual Guide for 500+ Topics/Tables Selection

Use this decision tree during customer conversations to navigate toward the right architecture.

---

## Decision Flow 1: Source Topology

```
┌─ Customer says: "500 Kafka topics into Teradata"
│
├─ Are these topics ALREADY in Kafka?
│  │
│  ├─ YES → Go to Decision Flow 2: Topic Strategy
│  │
│  └─ NO: We're building the producer side
│     │
│     └─ Is the upstream data in DATABASES?
│        │
│        ├─ YES → CDC ARCHITECTURE (Recommended) ✓
│        │         └─ Deploy CDC tool (HVR/Debezium)
│        │         └─ Consolidate to 1–3 Kafka topics
│        │         └─ Single TPT job
│        │         └─ Complexity: LOW
│        │
│        └─ NO: Data from APIs, event streams, logs
│           └─ INDEPENDENT TOPICS ARCHITECTURE
│              └─ Go to Decision Flow 2: Topic Strategy
```

---

## Decision Flow 2: Topic Consolidation Opportunity

```
┌─ Start: 500 sources / topics
│
├─ Do all 500 have FUNDAMENTALLY DIFFERENT SCHEMAS?
│  │
│  ├─ YES (e.g., customer master, payments, inventory all unrelated)
│  │   └─ Keep separate
│  │   └─ Organize by DOMAIN (see Decision Flow 3)
│  │
│  └─ NO (e.g., 120 payment sources, same event structure)
│      └─ CONSOLIDATE!
│      └─ 500 sources → 80–150 topics
│      └─ Use 'source_system' field for lineage
│      └─ Major win: reduces partition count by 70%
```

---

## Decision Flow 3: Domain Segmentation

```
┌─ Independent Kafka topics (or after source consolidation)
│
├─ Organize topics into BUSINESS DOMAINS
│  │
│  └─ Example: 500 topics → 4 domains
│     │
│     ├─ DOMAIN 1: Payments (120 topics, 380 partitions)
│     │  └─ TPT Job 1, 4 DataConnector instances
│     │  └─ SLA: ≤200ms (tactical tier)
│     │
│     ├─ DOMAIN 2: Customers (80 topics, 240 partitions)
│     │  └─ TPT Job 2, 3 DataConnector instances
│     │  └─ SLA: ≤5s (standard tier)
│     │
│     ├─ DOMAIN 3: Operations (100 topics, 300 partitions)
│     │  └─ TPT Job 3, 3 DataConnector instances
│     │
│     └─ DOMAIN 4+: Others
│
└─ Result: 3–5 TPT jobs, not 500
```

---

## Decision Flow 4: Criticality Tiering

```
┌─ Customer asks: "Do we need per-table isolation?"
│
├─ Are ALL 500 tables equally critical?
│  │
│  ├─ YES → ALL real-time, ALL hot
│  │   └─ Unlikely. Probe harder (Q3.1b)
│  │
│  └─ NO → Some hot, some cold
│      │
│      └─ TIERED COMPROMISE (Recommended) ✓
│         │
│         ├─ TIER 1: Hot Tables (10–20)
│         │  └─ Real-time SLA (≤200ms)
│         │  └─ Per-table job or dedicated CDC topic
│         │  └─ 20 concurrent jobs (manageable)
│         │
│         ├─ TIER 2: Warm Tables (80)
│         │  └─ SLA: ≤5s
│         │  └─ Shared CDC job (1 job)
│         │
│         └─ TIER 3: Cold Tables (400)
│            └─ SLA: ≤24h
│            └─ Batch consolidation (nightly)
│
└─ Result: ~22 concurrent jobs (not 500)
```

---

## Decision Flow 5: Concurrency Reality Check

```
┌─ Customer pushes for "500 independent TPT jobs"
│
├─ Reality Check:
│  │
│  └─ Teradata DBS Session Limit = 1,024 (HARD CEILING, non-negotiable)
│     │
│     └─ 500 jobs × 4 sessions/job = 2,000 sessions needed
│        │
│        └─ Only ~256 jobs can run; 244 jobs QUEUED INDEFINITELY
│
├─ By System Size:
│  │
│  ├─ 4-AMP POC:      192 concurrent max (308 queued)
│  ├─ 8-AMP small:    96 concurrent max (404 queued)
│  ├─ 20-AMP prod:    38 concurrent max (462 queued)
│  └─ 120-AMP xlarge: 8 concurrent max (492 queued)
│
└─ Recommendation:
   │
   ├─ Do NOT attempt 500 independent jobs
   │
   └─ INSTEAD: Tiered compromise (see Decision Flow 4)
      └─ Top 20 hot tables = per-table jobs
      └─ Remaining 480 = consolidated job
      └─ Total: ~20 concurrent (acceptable)
```

---

## Architecture Recommendation Summary

```
Based on answers to discovery questions, recommend:

┌─ IF: Database source + consolidatable
│   RECOMMEND: CDC Architecture (Tier 1) ✓✓✓ BEST
│   └─ HVR/Debezium → 1–3 consolidated topics
│   └─ Single TPT job
│   └─ Complexity: LOW
│   └─ Latency: 100–500ms
│   └─ Throughput: 8,000–40,000 rows/sec

├─ ELIF: Independent topics + mixed SLAs
│   RECOMMEND: Domain Segmentation (Tier 2) ✓✓ GOOD
│   └─ 3–5 domain consumer groups
│   └─ 3–5 TPT jobs
│   └─ Complexity: MEDIUM
│   └─ Latency: 200ms–5s (per domain)
│   └─ Throughput: 40,000–80,000 rows/sec

├─ ELIF: All hot + strict isolation + < 100 rows/sec per table
│   RECOMMEND: Tiered (20 hot + 1 cold) (Tier 3) ✓ ACCEPTABLE
│   └─ Top 20 tables: per-table jobs
│   └─ Remaining 480: consolidated job
│   └─ Complexity: MEDIUM-HIGH
│   └─ Latency: 100–5,000ms (tiered)
│   └─ Throughput: 20,000–50,000 rows/sec

└─ DO NOT RECOMMEND: 500 independent TPT jobs ✗✗✗ BROKEN
    └─ Max concurrent: 50–200 (system-dependent)
    └─ Queue depth: 300–450 jobs waiting indefinitely
    └─ Latency: 10 min – 31 hours average per job
    └─ Complexity: EXTREMELY HIGH
    └─ Operational toil: Unsustainable
    
    → If customer insists, explain realities and propose Tiered instead.
```

---

## SLA Mapping to Consumer Groups

```
┌─ Based on latency SLA, determine consumer group:
│
├─ ≤200ms   → Real-time (fraud, risk, detection)
│           → TASM tactical tier
│           → Dedicated job or separate consumer group
│           → Max lag alarm: 5s
│
├─ ≤5s      → Operational (dashboard, reporting queries)
│           → TASM standard tier
│           → Shared consumer group (by domain)
│           → Max lag alarm: 30s
│
├─ ≤5min    → Near-real-time analytics
│           → Standard tier
│           → Can batch within domain job
│           → Max lag alarm: 120s
│
└─ ≤24h     → Batch (ETL, reference data, legacy)
            → Off-peak tier
            → Consolidate, run nightly
            → Max lag alarm: 48h
```

---

## Consumer Group Sizing Rule

```
┌─ Calculate concurrent active jobs:
│
├─ Step 1: Determine partition count per domain
│          = sum of (topics × partitions)
│          Example: 120 topics × 3 partitions = 360 partitions
│
├─ Step 2: Determine DataConnector instances
│          = partitions / 100 (rough estimate)
│          Example: 360 / 100 ≈ 4 instances
│
├─ Step 3: Check Teradata session limit
│          Max concurrent jobs = 1,024 DBS sessions / (instances × 4–20)
│          
│          4-AMP system:  1,024 / 4 sessions = 256 jobs max
│          20-AMP system: 1,024 / 20 sessions = 51 jobs max
│
└─ If customer wants > max concurrent jobs
   └─ Wave batching required (run jobs in waves)
   └─ Or accept queuing
```

---

## Migration Timeline

```
Greenfield (New Project)
├─ Phase 1: Assessment (weeks 1–2)
├─ Phase 2: Pilot 10–20 topics (weeks 3–6)
├─ Phase 3: Domain rollout (weeks 7–14)
└─ Phase 4: Optimization (weeks 15–20)
   Total: 16–20 weeks to production

Migration (Replacing Existing)
├─ Phase 0: Existing pipeline baseline (weeks 0–2)
├─ Phase 1: New architecture pilot (parallel) (weeks 3–6)
├─ Phase 2: Validation period (weeks 7–10)
├─ Phase 3: Cutover to new (weeks 11–12)
└─ Phase 4: Decommission old (weeks 13–14)
   Total: 20–28 weeks (longer due to validation)
```

---

## Red Flags Checklist

| Red Flag | Status | Action |
|----------|--------|--------|
| "We have 500 Kafka topics" (no context) | ⚠️ | Ask: Where do they come from? |
| "All 500 are real-time critical" | ⚠️ | Ask: Top 10–20 only? (Be skeptical) |
| "We want 500 independent TPT jobs" | 🚨 | Show concurrency table, recommend Tiered |
| "We've never measured throughput" | ⚠️ | Offer to estimate; plan pilot |
| "We have no SLA requirements yet" | ⚠️ | Push back: "What business impact?" |

---

## One-Page Summary (For Handing to Customer)

```
STREAMING AT SCALE: ARCHITECTURE SELECTION

Your architecture depends on 5 answers:

1. DATABASE SOURCE or independent topics?
   → Database: CDC Architecture (simplest)
   → Topics: Domain Segmentation (3–5 jobs)

2. Peak throughput?
   → <10K msg/sec: 1–2 TPT jobs
   → 10–50K msg/sec: 3–5 domain jobs
   → >50K msg/sec: 5+ jobs + preprocessing

3. Same latency SLA or tiered?
   → Same: Single consumer group
   → Tiered: Separate groups per tier

4. Top 10–20 critical or all 500?
   → Critical few: Tiered architecture
   → All 500: Unusual; consolidate if possible

5. Per-table isolation required?
   → Yes: Tiered (20 hot + 1 cold)
   → No: Consolidated (recommended)

HARD LIMIT: Teradata DBS has ~1,024 sessions globally.
500 jobs × 4 sessions = 2,000 needed.
Maximum concurrent: 50–200 jobs (system-dependent).
Remaining 300–450 jobs queue indefinitely.

RECOMMENDATION:
☑ CDC (if database source) OR Domain Segmentation (if topics)
☑ Tiered if strict isolation needed (20 hot + 1 cold)
☑ Pilot 10–20 representative topics first
☑ Implement per-domain SLAs and monitoring
☑ 16–20 weeks to full production

DO NOT:
✗ Attempt 500 independent TPT jobs
✗ Over-engineer for "just in case"
✗ Delay pilot waiting for perfect architecture
```

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-30  
**For:** Solutions Architects, Sales Engineers  
**Review Before:** Customer call on "500 topics/tables" streaming projects
