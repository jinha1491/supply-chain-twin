from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq

from twin.run import horizon_days
from twin.sim.events import Emitter, EventType
from twin.sim.sinks import EVENT_SCHEMA, ParquetSink


def test_parquet_sink_writes_batches_in_order(tmp_path):
    path = tmp_path / "events.parquet"
    with ParquetSink(path, batch_size=2) as sink:
        emitter = Emitter("t", datetime(2000, 1, 1), sink)
        for i in range(5):
            emitter.emit(i * 0.5, EventType.DEMAND, f"p{i % 2}", "west", 1, ref_id=f"o{i}")
        emitter.emit(3.0, EventType.INVENTORY_SNAPSHOT, "p0", "west", 4)

    assert sink.count == 6
    assert pq.ParquetFile(path).num_row_groups == 3

    table = pq.read_table(path)
    assert table.schema.field("event_time").type == EVENT_SCHEMA.field("event_time").type

    df = table.to_pandas()
    assert df["event_id"].tolist() == [f"t-{i:09d}" for i in range(1, 7)]
    assert df["event_type"].astype(str).tolist() == ["demand"] * 5 + ["inventory_snapshot"]
    assert pd.isna(df["ref_id"].iloc[-1])


def test_horizon_covers_whole_months():
    months = pd.date_range("2000-01-01", periods=3, freq="MS")
    demand = pd.DataFrame({"part_id": "p", "month": months, "demand": 1})
    assert horizon_days(demand) == 91
