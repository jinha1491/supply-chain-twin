import heapq
from datetime import timedelta

import numpy as np

from twin.sim.events import Event


class LateSink:
    def __init__(
        self,
        inner,
        fraction: float,
        min_days: float = 1.0,
        max_days: float = 5.0,
        event_types: tuple[str, ...] = ("demand", "backorder"),
        seed: int = 0,
    ) -> None:
        self.inner = inner
        self.fraction = fraction
        self.min_days = min_days
        self.max_days = max_days
        self.event_types = set(event_types)
        self.rng = np.random.default_rng(seed)
        self.count = 0
        self.delayed = 0
        self._held: list[tuple] = []
        self._seq = 0

    def _send(self, event: Event) -> None:
        self.inner.emit(event)
        self.count += 1

    def _release_until(self, now) -> None:
        while self._held and self._held[0][0] <= now:
            self._send(heapq.heappop(self._held)[2])

    def emit(self, event: Event) -> None:
        self._release_until(event.event_time)
        if event.event_type.value in self.event_types and self.rng.random() < self.fraction:
            delay = timedelta(days=float(self.rng.uniform(self.min_days, self.max_days)))
            heapq.heappush(self._held, (event.event_time + delay, self._seq, event))
            self._seq += 1
            self.delayed += 1
            return
        self._send(event)

    def close(self) -> None:
        while self._held:
            self._send(heapq.heappop(self._held)[2])
        close = getattr(self.inner, "close", None)
        if close:
            close()

    def __enter__(self) -> "LateSink":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
