from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from twin.sim.events import Event

EVENT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_time", pa.timestamp("us", tz="UTC")),
        ("event_type", pa.dictionary(pa.int32(), pa.string())),
        ("part_id", pa.dictionary(pa.int32(), pa.string())),
        ("site", pa.dictionary(pa.int32(), pa.string())),
        ("quantity", pa.int32()),
        ("ref_id", pa.string()),
        ("carrier_id", pa.string()),
    ]
)
DICTIONARY_COLUMNS = {"event_type", "part_id", "site"}


class ParquetSink:
    def __init__(self, path: str | Path, batch_size: int = 250_000) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self.count = 0
        self._writer = pq.ParquetWriter(self.path, EVENT_SCHEMA)
        self._columns: dict[str, list] = {name: [] for name in EVENT_SCHEMA.names}

    def emit(self, event: Event) -> None:
        columns = self._columns
        columns["event_id"].append(event.event_id)
        columns["event_time"].append(event.event_time)
        columns["event_type"].append(event.event_type.value)
        columns["part_id"].append(event.part_id)
        columns["site"].append(event.site)
        columns["quantity"].append(event.quantity)
        columns["ref_id"].append(event.ref_id)
        columns["carrier_id"].append(event.carrier_id)
        self.count += 1
        if len(columns["event_id"]) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._columns["event_id"]:
            return
        arrays = []
        for field in EVENT_SCHEMA:
            values = self._columns[field.name]
            if field.name in DICTIONARY_COLUMNS:
                arrays.append(pa.array(values, type=pa.string()).dictionary_encode())
            else:
                arrays.append(pa.array(values, type=field.type))
        self._writer.write_batch(pa.record_batch(arrays, schema=EVENT_SCHEMA))
        for values in self._columns.values():
            values.clear()

    def close(self) -> None:
        self.flush()
        self._writer.close()

    def __enter__(self) -> "ParquetSink":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
