# Critical Customer Discovery Questions — 500 Topics/Tables Streaming

## Quick Reference: Ask These Questions First

Use this checklist in your initial customer conversation to understand their architecture and recommend the right solution.

---

## The 5 Critical Questions (First 15 Minutes)

### Q1: What is the actual source system?

**Ask:** "Are these 500 topics coming from **independent Kafka producers** (microservices), or are they **database tables** being captured via CDC?"

**Why:** Independent topics = topology complexity; database tables = source volume complexity (simpler with CDC)

**Red Flag:** "We have 500 Kafka topics" without knowing where they come from

---

### Q2: What's the peak transaction rate?

**Ask:** "At peak load, how many messages per second across all 500? 10,000? 50,000? 100,000?"

**Why:** Determines partition count, consumer groups, Teradata loader sizing

**If they don't know:** Estimate based on comparable systems or pilot measurement

---

### Q3: Do all 500 need the same latency SLA?

**Ask:** "Do all 500 need ≤200ms? Or do fraud/risk need fast, operations can tolerate 5s, and batch is nightly?"

**Why:** Determines consumer group segmentation and TASM tier allocation

**Typical:** Fraud ≤200ms (tactical), ops ≤5s (standard), reporting ≤5m (batch)

---

### Q4: If all claim "real-time," which 10–20 are actually critical?

**Ask:** "Top 10–20 that would cause immediate impact if delayed 5 seconds?"

**Why:** Customers conflate "possible" with "necessary"; typical finding: 10–20 hot, 80 warm, 400 cold

**Tiered architecture:** Hot gets real-time, cold batches nightly

---

### Q5: Do you need per-table isolation?

**Ask:** "For compliance/audit, do you need each table as its own job? Or can tables be consolidated?"

**Why:** Per-table = queuing nightmare (max 50–200 concurrent); consolidated = simpler

**Clarify:** Is this compliance-driven or just a preference?

---

## Tier-by-Tier Discovery Questions

### Tier 1: Source Topology (Q1–3)

**Q1.1:** "Are these 500 topics coming from **independent Kafka producers** (microservices publishing independently), or are they **change events captured from database tables** via CDC?"

**Q1.2:** "If I said these 500 sources could become 80–150 Kafka topics without losing functionality (consolidate by domain), would that help?"

**Q1.3:** "Do all 500 have fundamentally different schemas, or are they variations of common events?"

---

### Tier 2: Topology & Load (Q4–6)

**Q2.1:** "Is the data **push** (producers send to Kafka) or **pull** (you extract from databases)?"

**Q2.2:** "If CDC, what source database? Oracle, SQL Server, PostgreSQL, Teradata?"

**Q2.3:** "Peak transaction rate across all 500? 10K msg/sec? 50K? 100K?"

---

### Tier 3: SLA & Criticality (Q7–8)

**Q3.1:** "Do all 500 need same latency SLA, or tiered?"

**Q3.1b:** "Top 10–20 that need ≤5 second latency? Rest okay with hourly/nightly?"

**Q3.2:** "If one ingestion job fails, how long can data be delayed before impact?"

---

### Tier 4: Operations & Governance (Q9–11)

**Q4.1:** "Do you need per-table isolation for compliance, or is consolidated OK?"

**Q4.2:** "Do you already own HVR, Debezium, GoldenGate, or Informatica? Or choosing new tools?"

**Q4.3:** "Want 500 per-topic dashboards, or 1–5 consolidated?"

---

### Tier 5: Current State & Timeline (Q12–13)

**Q5.1:** "Greenfield or migrating existing pipelines?"

**Q5.2:** "Can you pilot 10–20 topics first, or all 500 by [date]?"

---

## One-Minute Assessment (After 15 Minutes)

Fill this in to determine the right architecture:

```
Customer: ________________________     Date: ______________

SOURCE TOPOLOGY
  [ ] Independent Kafka topics
  [ ] Database CDC (Oracle / PostgreSQL / SQL Server / Other: ___)
  [ ] Mixed (topics + tables)

PEAK THROUGHPUT
  [ ] <10K msg/sec
  [ ] 10–50K msg/sec
  [ ] >50K msg/sec

LATENCY SLA
  [ ] All ≤200ms (real-time)
  [ ] Mixed (fraud ≤200ms, ops ≤5s, batch ≤24h)
  [ ] Mostly batch (≤5m or nightly)

CRITICALITY TIER
  [ ] All 500 equally hot
  [ ] Top 10–20 hot + rest cold

ISOLATION REQUIREMENT
  [ ] Per-table strict isolation
  [ ] Consolidated is fine
  [ ] Tiered acceptable (hot isolated, cold consolidated)

RECOMMENDED ARCHITECTURE:
  [ ] CDC + Single Job (Tier 1 - BEST)
  [ ] Domain Segmentation (Tier 2 - GOOD)
  [ ] Tiered: 20 hot + 1 cold (Tier 3 - ACCEPTABLE)
  [ ] Warn: Not 500 independent jobs (BROKEN)

NEXT STEPS:
  [ ] Pilot 10–20 representative topics
  [ ] Validate latency SLAs
  [ ] Configure TASM workload tiers
```

---

## Conversation Starters (Copy-Paste Ready)

### Opener 1: Clarify Scope
> "Before we talk architecture, help me understand: Are these **500 independent Kafka producers** publishing independently, or are these **500 database tables** being captured via CDC? That changes everything."

### Opener 2: Probe Source
> "If these 500 are **database tables**, we can consolidate them into 1–3 Kafka topics with a CDC tool, making Teradata much simpler. Is the source data in a database?"

### Opener 3: Triage Criticality
> "I'm guessing not all 500 are equally urgent. Top 10–20 that need ≤5 second latency? The rest probably fine with hourly or nightly."

### Opener 4: Assess Operational Appetite
> "Running 500 independent ingestion jobs sounds appealing, but operationally it means 500 dashboards, 500 restart procedures, and queuing bottlenecks. Would simpler be better?"

### Opener 5: Set Expectations
> "The hard constraint at this scale isn't Kafka or Teradata — it's managing concurrent job slots. Teradata has a ~1,024 DBS session limit. We'll design so we're not queuing jobs. Usually means batching or waves. Acceptable?"

---

## Red Flags Checklist

| Red Flag | Status | Action |
|----------|--------|--------|
| "We have 500 Kafka topics" (no context) | ⚠️ | Ask Q1.1: Where do they come from? |
| "All 500 are real-time critical" | ⚠️ | Ask Q3.1b: Top 10–20 only? (Be skeptical) |
| "We want 500 independent TPT jobs" | 🚨 | Show concurrency limits table, recommend Tiered |
| "We've never measured throughput" | ⚠️ | Plan pilot; estimate based on comparable systems |
| "We have no SLA requirements yet" | ⚠️ | Push back: "What business impact if 5s latency?" |

---

## Architecture Recommendation Summary

**Based on answers to discovery questions:**

| If... | Recommend... | Why |
|------|-------------|-----|
| Database source + consolidatable | CDC + 1 TPT job (Tier 1) | Complexity LOW; latency 100–500ms ✓✓✓ |
| Independent topics + mixed SLAs | 3–5 domain jobs (Tier 2) | Complexity MEDIUM; latency 200ms–5s ✓✓ |
| All hot + strict isolation | Tiered: 20 hot + 1 cold (Tier 3) | Complexity MEDIUM-HIGH; manageable ✓ |
| 500 independent TPT jobs | ✗ DO NOT (Tier 4 - Broken) | Session limit forces queuing; not real-time ✗✗✗ |

---

## Concurrency Reality Check

**Teradata DBS Session Limit = 1,024 (HARD CEILING)**

If customer wants 500 independent TPT jobs:
- 500 jobs × 4 sessions/job = 2,000 sessions needed
- Only ~256 jobs can run; 244 queued indefinitely
- Average wait: 10 min – 31 hours per job
- Larger systems are WORSE (20-AMP: only 38 concurrent)

**Show this table to reset expectations:**

| System | AMP | Max Concurrent | Queue Depth | Acceptable? |
|--------|-----|----------------|-------------|-------------|
| POC | 4 | 192 | 308 | Maybe |
| Prod | 20 | 38 | 462 | NO |
| XLarge | 120 | 8 | 492 | NO |

---

## Decision Flow (Visual)

```
Customer says: "500 Kafka topics into Teradata"

├─ Q1.1: Independent topics or database CDC?
│  ├─ Database → CDC ARCHITECTURE (Best)
│  └─ Topics → Independent Topics (Next)
│
├─ Q2.3: Peak throughput?
│  ├─ <10K msg/sec → 1–2 TPT jobs
│  ├─ 10–50K msg/sec → 3–5 domain jobs
│  └─ >50K msg/sec → 5+ jobs + Flink preprocessing
│
├─ Q3.1: Same latency SLA or tiered?
│  ├─ Same → 1 consumer group
│  └─ Tiered → Separate consumer groups per tier
│
├─ Q3.1b: Which 10–20 are critical?
│  ├─ All 500 → Probe harder (unlikely)
│  └─ Top 10–20 → Tiered architecture (typical)
│
└─ Q4.1: Per-table isolation required?
   ├─ YES → Tiered compromise (20 hot + 1 cold)
   └─ NO → Consolidated (recommended)
```

---

## Before Your Next Call: Prep Checklist

- [ ] Review this document (5 minutes)
- [ ] Prepare the one-minute assessment template
- [ ] Have concurrency limits table ready
- [ ] Have conversation starters memorized
- [ ] Know your recommendation criteria
- [ ] Have red flags list ready

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-30  
**Audience:** Solutions Architects, Sales Engineers, Customer Success  
**Review Before:** Customer call on streaming at scale
