I found several internal Teradata architecture assets and a recent meeting discussing this exact pattern. The strongest match is CIB/TD - Real-Time Capabilities and AVRO Support Session, where streaming architectures, TPT Stream, Kafka, schema evolution, workload management, and operational SLAs were discussed. The meeting specifically covered direct Kafka → TPT Stream → Teradata ingestion, partitioning strategies, workload management, and handling Avro schema evolution.

The internal architecture material also recommends:

Hot path: Kafka → TPT → Teradata
Cold path: Kafka → Object Store/Iceberg → OTF/NOS
Dedicated TASM workload for streaming ingest
MaxSessions tuned toward AMP count
Partitioning and workload isolation for concurrency
Mixed hot/cold architecture for production deployments
Recommended Tier-1 Design

Based on your stated requirements:

Requirement	ValueLatency	100–500 ms
Throughput	8K–40K rows/sec
Complexity	Low
Sources	CDC
Target	Teradata
Recommended Pattern	HVR / Debezium → Kafka → TPT Stream
High-Level Architecture
┌─────────────────────┐
│ Source Databases    │
│ Oracle / SQLServer  │
│ PostgreSQL etc      │
└──────────┬──────────┘
           │ CDC
           ▼
┌─────────────────────┐
│ Debezium / HVR      │
│ Change Capture      │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Kafka Cluster       │
│ 1-3 Topics Only     │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ TPT Stream Job      │
│ Kafka Access Module │
│ Single Job          │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Landing Tables      │
│ ODS / Raw Layer     │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Consumer Views      │
│ APIs                │
│ ODS                 │
│ Real-time Reports   │
└─────────────────────┘

Why Consolidate Into 1-3 Topics?

Many architects initially create:

550 tables
=
550 Kafka topics
=
550 TPT jobs


This becomes operationally expensive.

Instead:

CDC Sources
      ↓
topic_customer
topic_finance
topic_reference


or

topic_cdc


with:

{
  "source_table":"CUSTOMER",
  "op":"U",
  "payload":{ ... }
}


Benefits:

One TPT process
One monitoring surface
Easier recovery
Easier schema governance
Better Kafka partition utilization

This aligns with the pattern discussed in your June architecture sessions around evaluating latency requirements per workload instead of per table.

Landing Model

I would not stream directly into business tables.

Use:

CDC
  ↓
RAW_CDC_EVENT
  ↓
ODS_CURRENT
  ↓
Business Models


Example:

CREATE TABLE RAW_CDC_EVENT
(
    EVENT_TS         TIMESTAMP(6),
    SOURCE_TABLE     VARCHAR(100),
    OPERATION_TYPE   CHAR(1),
    PRIMARY_KEY      VARCHAR(500),
    PAYLOAD          JSON
);


Benefits:

Replay capability
Auditability
Schema evolution
Easier troubleshooting
TPT Design

Single continuously running TPT Stream Job.

Kafka Access Module
Kafka
    ↓
Kafka Access Module
    ↓
TPT Stream Operator
    ↓
RAW_CDC_EVENT


Internal streaming guidance also notes:

Increase BufferCapacity
Use dedicated TASM workload
Use higher session counts for throughput
Partition target tables for concurrency
Suggested Initial Settings
MaxSessions      = 32-64
BufferCapacity   = 1MB-4MB
ErrorLimit       = 1000
Checkpoint       = enabled


Then tune from there.

Table Design

For high ingest rates:

Option A (Recommended)
PARTITION BY RANGE_N
(
   EVENT_TS
   BETWEEN DATE '2026-01-01'
   AND DATE '2030-12-31'
   EACH INTERVAL '1' DAY
);

Option B

Hash distribution on business key.

Avoid:

MERGE


for every CDC row.

Instead:

Stream into landing table
Micro-batch merge every 5-30 seconds

This typically scales much better.

Throughput Expectations

Your targets are realistic.

Pattern	Typical ThroughputTPT Stream + Kafka	8K–40K rows/sec
Tuned system	50K–100K+ rows/sec
Micro-batched	much higher

The main bottleneck is usually:

Kafka
Network
Serialization

not Teradata itself. This is called out in internal streaming guidance, which notes that most latency issues occur outside the database.

Operational Design
TASM

Create:

REALTIME_INGEST


workload

Priority:

High


Dedicated to TPT sessions so analytical workloads cannot starve ingestion. This is specifically recommended in internal streaming architecture material.

DR / Recovery

Enable:

Kafka Retention = 7 days


TPT restartable checkpoints.

Recovery process:

TPT failure
    ↓
Restart
    ↓
Read from Kafka offset
    ↓
Continue loading


No source reload required.

My Recommended Production Pattern

For a bank-scale deployment with 500+ source tables:

Oracle / SQL Server
        ↓
Debezium or HVR
        ↓
Kafka
        ↓
3 logical CDC topics
        ↓
Single TPT Stream Job
        ↓
RAW_CDC_EVENT
        ↓
5-second micro-batch MERGE
        ↓
ODS_CURRENT
        ↓
Gold / Reporting / APIs


This gives:

Low complexity
Near real-time (<500ms ingestion)
Replay capability
Schema evolution support
Easy operational support
Good scaling beyond your 40K rows/sec target
Deep Research Prompt

If you want to take this into a detailed architecture study, use:

Act as a Teradata real-time architecture specialist.

Design a production-grade CDC ingestion platform using:

- Debezium or HVR
- Apache Kafka
- Teradata Parallel Transporter Stream Operator
- Teradata Vantage 20.x

Requirements:

- 500+ source tables
- 400GB/day CDC volume
- 100–500ms ingestion latency
- 8,000–40,000 rows/sec sustained throughput
- Exactly-once or effectively-once processing
- Schema evolution support
- Active-active Kafka
- Disaster recovery
- TASM workload isolation
- Partitioned target tables
- Operational monitoring

Provide:

1. End-to-end architecture
2. Kafka topic strategy (1, 3, 50, 500 topic comparison)
3. TPT Stream operator configuration
4. Kafka access module configuration
5. Teradata physical table design
6. PI and partitioning strategy
7. CDC landing model
8. Merge/upsert strategy
9. Performance benchmarks
10. Failure recovery workflow
11. Capacity planning model
12. Cost model
13. Comparison against:
    - Kafka Connect Teradata Sink
    - StreamSets
    - Informatica CDC
    - Flink + Iceberg + OTF
14. Reference architecture diagrams
15. Production hardening checklist


That prompt should produce a 20–30 page architecture-level analysis suitable for a customer design review.