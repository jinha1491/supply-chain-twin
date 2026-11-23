# Results

Measured numbers from this project, with the step that produced each one.

## Simulation (steps 3 and 4)
- Full run: 19.9M events in 66.5 s (about 300K events/s), 140 MB Parquet.
- Engine matches Palm's theorem with unlimited DC stock: |z| < 0.7 across 4 regions,
  overall fill 0.981 simulated vs 0.981 predicted.
- Fill rate shortfall of 6.4 points (98.1% predicted to 91.7% realized), split into
  demand clustering (3.6 points) and upstream DC delays (2.8 points).

## Streaming (steps 5 to 7)
- Kafka producer: 3.26M Avro events at about 77K events/s, zero loss
  (sent count equals topic count).
- Kafka to Iceberg: 3,259,030 rows, zero duplicate offsets, no offset gaps.
- Daily service windows (2-day watermark): 6,185 written, 0 mismatches against batch
  recomputation, 12 left open by the watermark at the end of the stream.
- Stockout episodes (stateful streaming): 68,486 episodes, identical to batch
  recomputation at every site.
- Regional stockouts for Asia-sourced parts last 5.0 days on average vs 3.0 for
  domestic (67% longer), with the same frequency, because of upstream DC delays.

## Late data and watermarks (step 7c)
- 2% of demand and backorder events delayed 1 to 30 days; 1,365 late events measured
  from Kafka lineage columns (offset order vs event time), roughly flat by 5-day bin.
- Kafka-source windowed aggregation, ~163 batches: 2-day watermark dropped 895 late
  events (66%), 7-day dropped 488 (36%), 30-day dropped 0. The 30-day watermark left
  about 28 more days of windows unfinished at the end of the stream.
- Since Spark 3.4, late rows are filtered with the previous batch's watermark
  (SPARK-40925), so effective tolerance is the watermark plus about one batch of event
  time. Replays with batches spanning about 32 days dropped nothing at a 2-day
  watermark.
- Splitting micro-batches by file inside an Iceberg commit reordered data enough to
  drop 939 events at a 2-day watermark with no extra injected lateness.

## Exactly-once under crashes (step 8)
- Killed ingestion and stateful aggregation 3 times each with SIGKILL at random
  times; all 6 kills landed with a batch in flight, so every restart replayed a
  partial batch.
- Raw table after recovery: 3,259,030 rows = Kafka messages, 0 duplicate offsets,
  0 missing offsets, 327 Iceberg commits with no batch committed twice.
- Inventory positions: all 11,647 site-parts identical to batch recomputation.
- Daily windows (30-day watermark): no window over-counted; 10 of 6,074 short by one
  event each. Every short window held an event 25 to 30 days late, the edge of the
  injected 1-to-30-day lateness, so a watermark equal to maximum source lateness
  leaves no margin for cross-partition skew.

## Schema evolution mid-stream (step 9)
- Schema Registry (BACKWARD) accepted v2 (optional carrier_id with null default) and
  rejected a required-field variant with HTTP 409 before any producer could use it.
- Producer switched from v1 to v2 at 2000-02-15: 1,648,105 v1 and 1,610,925 v2
  messages in one topic.
- Spark decodes each message with its writer schema (schema ID from the 5-byte
  header) and resolves to the latest; Iceberg columns added with metadata-only
  ALTER TABLE, no data files rewritten.
- A consumer that only knew v1 stopped at the first v2 message with an explicit error
  instead of misreading or skipping data; after restart: 3,259,030 rows = Kafka,
  0 duplicates, 0 gaps, 7 commits with none repeated.
- v1 shipments read carrier_id as null (0 of 64,538); all 52,813 v2 shipments carry a
  carrier matching their lane.
- Lesson: decoding versions in separate unioned branches changed the query's source
  count and made the checkpoint unusable; decoding in one CASE expression keeps a
  single source for any number of schema versions.

## Change data capture (step 10a)
- Postgres master data (suppliers, sites, parts, policy) streamed into Kafka by
  Debezium from the write-ahead log (pgoutput, replication slot, initial snapshot).
- Spark foreachBatch MERGE keeps current-state Iceberg tables in sync and appends
  every change to history tables; both are idempotent under replay (MERGE by key,
  history keyed by key plus Kafka offset).
- Snapshot: 15,062 rows across 4 tables, 0 missing and 0 extra vs Postgres (JDBC
  exceptAll both ways).
- Changes: 2 supplier updates, 1 site update, 1 part delete with 5 policy deletes;
  lakehouse still exact. Two updates to the same supplier in one batch resolved to
  the newest by WAL position, with both versions kept in history.

## Scenario: port congestion via CDC (step 10b)
- Simulator applied Asia lead-time changes on days 600 (45 to 70) and 1300 (back to
  45) and wrote them to Postgres during the run; Debezium and MERGE kept the
  lakehouse exact (15,064 change messages, 0 missing, 0 extra).
- As-of join of 51,824 DC purchase orders to the supplier version in effect at order
  time: Asia 45-day version realized 44.8 days, 70-day version 69.9, domestic 7.0,
  Mexico 18.0.
- Regional stockouts for Asia parts rose from 5.25 to 7.60 days during congestion
  (+2.34) while other parts moved -0.15; difference-in-differences +2.50 days.
  Asia stockouts stayed elevated after recovery (5.82) while in-flight 70-day orders
  cleared.
