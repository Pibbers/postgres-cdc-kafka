Below is the architecture I would deploy for the workload discussed in CIB/TD - Real-Time Capabilities and AVRO Support Session (≈550 tables, ~400 GB/day, sub-second SLA, Kafka + TPT Stream). The meeting notes align very closely with your requirements.

Executive Summary

Recommended Production Architecture

                 ACTIVE REGION A
 ┌───────────────────────────────────────────┐
 │ Source DBs (500+ tables)                  │
 │ Oracle / SQL Server / DB2 / Postgres      │
 └───────────────────────────────────────────┘
                     │ CDC
                     ▼
          Debezium OR HVR Replication
                     │
                     ▼
        Confluent Kafka Cluster A
     (30-60 partitions per CDC domain)
                     │
      ┌──────────────┴───────────────┐
      │                              │
      ▼                              ▼
 TPT Stream Farm                Object Store
(Kafka Access Module)           (DR / Replay)
      │                              │
      ▼                              ▼
 Teradata CDC Landing          Retained CDC Log
      │
      ▼
 Merge Services
      │
      ▼
 ODS Tables (Low Latency)
      │
      ▼
 Silver / Gold / EDW


Parallel replication:

Kafka Cluster A  <----MirrorMaker2/Cluster Linking----> Kafka Cluster B


This provides:

100-500ms ingestion latency
8k-40k rows/sec sustained
replay capability
active-active Kafka
DR
schema evolution
workload isolation

The architecture aligns with demonstrated Teradata streaming patterns using Kafka + TPT Stream. Production examples inside your enterprise material show 6,200 rows/sec sustained on DHL workloads, 500M messages/day, and low Teradata resource utilization.

1. End-to-End Architecture
Capture Layer
Preferred
HVR


Advantages:

Enterprise CDC
DDL propagation
Restart capability
Monitoring
Alternative
Debezium


Advantages:

Open source
Excellent Kafka integration
Lower licensing cost

For 500+ tables:

5-20 CDC connectors


grouped by source application.

2. Kafka Topic Strategy
Option 1 — Single Topic
cdc_all


Pros:

Easy administration

Cons:

Hot partitions
Difficult replay
Difficult tuning

Not recommended.

Option 2 — 3 Topics
cdc_high
cdc_medium
cdc_low


Pros:

Simple

Cons:

Operational coupling

Acceptable.

Option 3 — 50 Topics

Recommended.

cdc_customer
cdc_account
cdc_payment
cdc_card
cdc_transaction
...


Benefits:

Independent scaling
Easier replay
Better partition management
Option 4 — 500 Topics

One topic per table.

Pros:

Maximum isolation

Cons:

Operational nightmare

Not recommended.

Recommended
50-100 business-domain topics


each containing:

{
 "table":"CUSTOMER",
 "op":"U",
 "ts":"...",
 "before":{},
 "after":{}
}

Kafka Partition Count

Throughput target:

40,000 rows/sec


Assume:

16 consumers


Use:

48-96 partitions


per high-volume topic.

Consumer-group scalability is supported through Kafka partition assignment implemented by the Kafka Access Module.

3. TPT Stream Operator Configuration

Use multiple TPT instances.

TPT Cluster Nodes: 4-8


Each node:

8-16 TPT jobs


Target:

32-64 parallel streams


Example:

DEFINE OPERATOR STREAM_LOAD
TYPE STREAM
ATTRIBUTES
(
 MaxSessions = 32,
 MinSessions = 32,
 Pack = 200,
 ArraySupport = 'Y'
);


TPT guidance identifies PACK and session count as the primary performance levers. Higher PACK increases throughput while lower PACK reduces latency.

4. Kafka Access Module Configuration

Example:

-X bootstrap.servers=kafka1:9092,kafka2:9092
-X group.id=tpt_cdc_loaders
-X enable.auto.commit=false
-X auto.offset.reset=earliest
-X fetch.min.bytes=1048576
-X session.timeout.ms=30000


Use:

One TPT consumer group


per load domain.

5. Physical Table Design

Separate:

CDC_LANDING
ODS
CORE


Schemas.

Landing Table
CREATE MULTISET TABLE CDC_LANDING
(
  Kafka_Offset BIGINT,
  Kafka_Partition INTEGER,
  Source_Table VARCHAR(128),
  Op_Type CHAR(1),
  Event_TS TIMESTAMP(6),
  Payload JSON
);


Never merge directly from Kafka.

Always land first.

6. PI and Partition Strategy
CDC Landing

PI:

PI (Kafka_Partition)


Spreads load evenly.

ODS

PI:

PI (Business_Key)


Example:

PI(Customer_ID)


Allows single-AMP updates.

TPT documentation recommends carrying the full PI during updates to preserve efficient single-AMP operations.

Partitioning
PARTITION BY RANGE_N
(
 Event_TS
 BETWEEN DATE '2026-01-01'
 AND DATE '2035-12-31'
 EACH INTERVAL '1' DAY
);


For transaction tables:

Daily partitions


For history:

Monthly partitions

7. CDC Landing Model

Recommended:

Raw CDC Landing
      ->
Validated CDC
      ->
ODS Current
      ->
Historical Vault


Landing retains:

90 days


for replay.

8. Merge / Upsert Strategy

Never merge row-by-row.

Instead:

100ms
250ms
500ms
micro-batches


Workflow:

Kafka
 -> Landing
 -> Staging
 -> MERGE


Example:

MERGE INTO ODS_CUSTOMER T
USING CDC_BATCH S
ON T.Customer_ID=S.Customer_ID
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT;

9. Expected Benchmarks

Based on internal TPT Stream customer references:

Configuration	ThroughputSmall	5k rows/sec
Medium	15k rows/sec
Large	40k rows/sec
Aggressive	80k+ rows/sec

Enterprise references show:

6,200 rows/sec at 0.2% additional CPU in DHL streaming deployments.
8,000 msg/sec peak in documented streaming use cases.

Expected latency:

Stage	LatencyCDC	20-100ms
Kafka	5-20ms
TPT Flush	100-250ms
Merge	50-200ms
Total	175-500ms
10. Failure Recovery
Kafka Broker Failure
Replication Factor = 3
min.insync.replicas = 2
acks=all


No data loss.

TPT Failure

Restart job:

Resume from committed offset

Teradata Outage

Kafka retains messages.

Example:

Retention = 7 days


TPT resumes.

Regional Disaster
Cluster Linking
or
MirrorMaker2


to second Kafka cluster.

Failover:

Kafka B
   ->
Standby TPT
   ->
Standby Vantage

11. Capacity Planning

400 GB/day.

Assume:

2 KB average CDC event


≈

200 million events/day


Average:

2300 rows/sec


Peak x10:

23,000 rows/sec


Target design:

40,000 rows/sec sustained


provides growth margin.

12. Cost Model
Low
Debezium
Kafka OSS
TPT


Cost = mostly infrastructure.

Medium
Confluent Platform
Debezium


Higher operational simplicity.

Enterprise
HVR
Confluent
TPT


Highest cost.

Best operational maturity.

13. Competitive Comparison
Solution	Latency	Scale	Schema Evolution	NotesTPT Stream	Excellent	Excellent	Good	Best Teradata throughput
Kafka Connect Sink	Good	Medium	Very Good	Easier deployment
StreamSets	Good	Medium	Good	GUI-centric
Informatica CDC	Good	Good	Good	Expensive
Flink + Iceberg + OTF	Seconds	Excellent	Excellent	Best lakehouse pattern

Internal Teradata references position Kafka Connect as schema-aware and easier to operate but not optimized for the very highest throughput, whereas TPT is the preferred ingest workhorse for extreme sustained volume.

14. Reference Production Diagram
   Oracle/SQL/Postgres
            |
      Debezium/HVR
            |
     +--------------+
     | Kafka RegionA |
     +--------------+
            |
     +--------------+
     | Kafka RegionB |
     +--------------+
            |
       MirrorMaker2
            |
      TPT Stream Farm
        (32-64 jobs)
            |
     CDC Landing Tables
            |
       Batch Merge
            |
        ODS Tables
            |
      Data Vault/EDW

15. Production Hardening Checklist
Kafka

✅ RF=3
 ✅ min.insync.replicas=2
 ✅ rack awareness
 ✅ TLS
 ✅ SCRAM authentication
 ✅ topic quotas

TPT

✅ dedicated nodes
 ✅ PACK tuning
 ✅ fixed session counts
 ✅ restart scripts
 ✅ offset tracking

Teradata

✅ TASM ingestion workload
 ✅ Resource partitioning
 ✅ daily partitioned tables
 ✅ fallback on critical ODS tables
 ✅ workload throttles

Monitoring

✅ Debezium/HVR lag
 ✅ Kafka consumer lag
 ✅ TPT rows/sec
 ✅ TPT rejects
 ✅ AMP CPU
 ✅ skew
 ✅ merge duration
 ✅ partition growth

DR

✅ Active-active Kafka
 ✅ standby Vantage
 ✅ 7-30 day Kafka retention
 ✅ replay procedures tested quarterly

Final Recommendation

For your stated workload (500+ tables, 400GB/day, sub-second SLA), I would recommend:

HVR CDC
      →
50–100 Kafka domain topics
      →
48–96 partitions/topic
      →
TPT Kafka Access Module
      →
CDC Landing Tables
      →
Micro-batch MERGE (100–500ms)
      →
Partitioned ODS
      →
Silver/Gold


This is the lowest-risk architecture for achieving 100–500ms latency, 40k rows/sec sustained throughput, and Teradata-native operational support while retaining replay, schema evolution, TASM governance, and DR capabilities. Sources: 202509 - Teradata Ecosystem Checkpoint.pptx, SA_Streaming_Deck.pptx, lakehouse-detail.pptx, and the transcribed CIB/TD - Real-Time Capabilities and AVRO Support Session.