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
