from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol


class EventType(StrEnum):
    DEMAND = "demand"
    FULFILLED = "fulfilled"
    BACKORDER = "backorder"
    REPLENISHMENT_ORDER = "replenishment_order"
    SHIPMENT_DISPATCHED = "shipment_dispatched"
    RECEIPT = "receipt"
    INVENTORY_SNAPSHOT = "inventory_snapshot"


@dataclass(frozen=True)
class Event:
    event_id: str
    event_time: datetime
    event_type: EventType
    part_id: str
    site: str
    quantity: int
    ref_id: str | None = None

    def to_dict(self) -> dict:
        record = asdict(self)
        record["event_type"] = self.event_type.value
        record["event_time"] = self.event_time.isoformat()
        return record


class EventSink(Protocol):
    def emit(self, event: Event) -> None: ...


class ListSink:
    def __init__(self) -> None:
        self.events: list[Event] = []

    def emit(self, event: Event) -> None:
        self.events.append(event)


class Emitter:
    def __init__(self, run_id: str, start: datetime, sink: EventSink) -> None:
        self.run_id = run_id
        self.start = start if start.tzinfo else start.replace(tzinfo=UTC)
        self.sink = sink
        self._seq = 0

    def emit(
        self,
        sim_days: float,
        event_type: EventType,
        part_id: str,
        site: str,
        quantity: int,
        ref_id: str | None = None,
    ) -> Event:
        self._seq += 1
        event = Event(
            event_id=f"{self.run_id}-{self._seq:09d}",
            event_time=self.start + timedelta(days=sim_days),
            event_type=event_type,
            part_id=part_id,
            site=site,
            quantity=quantity,
            ref_id=ref_id,
        )
        self.sink.emit(event)
        return event
