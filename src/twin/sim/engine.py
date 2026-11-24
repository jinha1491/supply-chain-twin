import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, replace

import numpy as np
import pandas as pd
import simpy

from twin.network import NetworkConfig, Supplier, load_config
from twin.sim.demand import generate_orders
from twin.sim.events import Emitter, EventType, ListSink


@dataclass(frozen=True)
class Policy:
    region_base_stock: dict[tuple[str, str], int]
    dc_reorder_point: dict[str, int]
    dc_order_up_to: dict[str, int]


class NetworkSim:
    def __init__(
        self,
        env: simpy.Environment,
        config: NetworkConfig,
        part_supplier: dict[str, str],
        policy: Policy,
        emitter: Emitter,
        rng: np.random.Generator,
    ) -> None:
        self.env = env
        self.dc = config.dc_name
        self.suppliers = {s.name: s for s in config.suppliers}
        self.transit_days = {r.name: r.transit_days for r in config.regions}
        self.region_carrier = {r.name: r.carrier for r in config.regions}
        self.part_supplier = part_supplier
        self.policy = policy
        self.emitter = emitter
        self.rng = rng

        self.on_hand: dict[tuple[str, str], int] = defaultdict(int)
        for key, level in policy.region_base_stock.items():
            self.on_hand[key] = level
        for part, level in policy.dc_order_up_to.items():
            self.on_hand[(self.dc, part)] = level

        self.region_backorders: dict[tuple[str, str], deque] = defaultdict(deque)
        self.dc_queue: dict[str, deque] = defaultdict(deque)
        self.dc_on_order: dict[str, int] = defaultdict(int)
        self.dc_reorder_point = dict(policy.dc_reorder_point)
        self.dc_order_up_to = dict(policy.dc_order_up_to)
        self.region_level = dict(policy.region_base_stock)
        self.region_skip: dict[tuple[str, str], int] = defaultdict(int)
        self._id_seq = 0

    def _new_id(self, prefix: str) -> str:
        self._id_seq += 1
        return f"{prefix}-{self._id_seq:08d}"

    def _emit(
        self, event_type: EventType, part: str, site: str, qty: int, ref_id, carrier_id=None
    ) -> None:
        self.emitter.emit(self.env.now, event_type, part, site, qty, ref_id, carrier_id)

    def run_orders(self, orders: pd.DataFrame):
        for order in orders.itertuples(index=False):
            yield self.env.timeout(order.time_days - self.env.now)
            self.handle_demand(order.order_id, order.part_id, order.region)

    def handle_demand(self, order_id: str, part: str, region: str) -> None:
        self._emit(EventType.DEMAND, part, region, 1, order_id)
        key = (region, part)
        if self.on_hand[key] > 0:
            self.on_hand[key] -= 1
            self._emit(EventType.FULFILLED, part, region, 1, order_id)
        else:
            self.region_backorders[key].append(order_id)
            self._emit(EventType.BACKORDER, part, region, 1, order_id)

        if self.region_skip[key] > 0:
            self.region_skip[key] -= 1
            return
        self._request_from_dc(part, region)

    def _request_from_dc(self, part: str, region: str) -> None:
        request_id = self._new_id("rr")
        self._emit(EventType.REPLENISHMENT_ORDER, part, region, 1, request_id)
        self.dc_request(part, region, request_id)

    def dc_request(self, part: str, region: str, request_id: str) -> None:
        key = (self.dc, part)
        if self.on_hand[key] > 0:
            self.on_hand[key] -= 1
            self._ship_to_region(part, region, request_id)
        else:
            self.dc_queue[part].append((region, request_id))
        self._check_dc_reorder(part)

    def _ship_to_region(self, part: str, region: str, request_id: str) -> None:
        carrier = self.region_carrier[region]
        self._emit(EventType.SHIPMENT_DISPATCHED, part, self.dc, 1, request_id, carrier)
        self.env.process(self._deliver_to_region(part, region, request_id))

    def _deliver_to_region(self, part: str, region: str, request_id: str):
        yield self.env.timeout(self.transit_days[region])
        self._emit(EventType.RECEIPT, part, region, 1, request_id)
        key = (region, part)
        if self.region_backorders[key]:
            order_id = self.region_backorders[key].popleft()
            self._emit(EventType.FULFILLED, part, region, 1, order_id)
        else:
            self.on_hand[key] += 1

    def dc_position(self, part: str) -> int:
        return self.on_hand[(self.dc, part)] + self.dc_on_order[part] - len(self.dc_queue[part])

    def _check_dc_reorder(self, part: str) -> None:
        position = self.dc_position(part)
        if position > self.dc_reorder_point.get(part, 0):
            return
        quantity = self.dc_order_up_to.get(part, 0) - position
        if quantity <= 0:
            return

        po_id = self._new_id("po")
        supplier = self.suppliers[self.part_supplier[part]]
        self.dc_on_order[part] += quantity
        self._emit(EventType.REPLENISHMENT_ORDER, part, self.dc, quantity, po_id)
        self._emit(
            EventType.SHIPMENT_DISPATCHED, part, supplier.name, quantity, po_id, supplier.carrier
        )
        lead_time = self.lead_time(supplier)
        self.env.process(self._deliver_to_dc(part, quantity, po_id, lead_time))

    def lead_time(self, supplier: Supplier) -> float:
        if supplier.lead_time_std == 0:
            return supplier.lead_time_mean
        shape = (supplier.lead_time_mean / supplier.lead_time_std) ** 2
        scale = supplier.lead_time_std**2 / supplier.lead_time_mean
        return float(self.rng.gamma(shape, scale))

    def _deliver_to_dc(self, part: str, quantity: int, po_id: str, lead_time: float):
        yield self.env.timeout(lead_time)
        self._emit(EventType.RECEIPT, part, self.dc, quantity, po_id)
        self.dc_on_order[part] -= quantity
        key = (self.dc, part)
        self.on_hand[key] += quantity
        queue = self.dc_queue[part]
        while queue and self.on_hand[key] > 0:
            region, request_id = queue.popleft()
            self.on_hand[key] -= 1
            self._ship_to_region(part, region, request_id)

    def set_supplier_lead_time(self, name: str, mean: float, std: float) -> None:
        self.suppliers[name] = replace(self.suppliers[name], lead_time_mean=mean, lead_time_std=std)

    def run_changes(self, changes: list[dict], on_change=None):
        for change in sorted(changes, key=lambda c: c["day"]):
            yield self.env.timeout(change["day"] - self.env.now)
            self.set_supplier_lead_time(
                change["name"], change["lead_time_mean"], change["lead_time_std"]
            )
            if on_change:
                on_change(change)

    def update_policy(self, region_levels: dict, dc_reorder: dict, dc_order_up_to: dict) -> None:
        for key, level in region_levels.items():
            change = level - self.region_level.get(key, 0)
            self.region_level[key] = level
            if change > 0:
                cancelled = min(change, self.region_skip[key])
                self.region_skip[key] -= cancelled
                region, part = key
                for _ in range(change - cancelled):
                    self._request_from_dc(part, region)
            elif change < 0:
                self.region_skip[key] -= change
        self.dc_reorder_point.update(dc_reorder)
        self.dc_order_up_to.update(dc_order_up_to)
        for part in dc_reorder:
            self._check_dc_reorder(part)

    def run_policy_updates(self, updates: list[tuple]):
        for day, region_levels, dc_reorder, dc_order_up_to in sorted(updates, key=lambda u: u[0]):
            yield self.env.timeout(day - self.env.now)
            self.update_policy(region_levels, dc_reorder, dc_order_up_to)

    def run_snapshots(self, every_days: float, until: float):
        while self.env.now + every_days <= until:
            yield self.env.timeout(every_days)
            for (site, part), units in self.on_hand.items():
                self._emit(EventType.INVENTORY_SNAPSHOT, part, site, units, None)


def simulate(
    config: NetworkConfig,
    part_supplier: dict[str, str],
    policy: Policy,
    orders: pd.DataFrame,
    emitter: Emitter,
    rng: np.random.Generator,
    until: float | None = None,
    snapshot_every_days: float | None = None,
    changes: list[dict] | None = None,
    on_change=None,
    policy_updates: list[tuple] | None = None,
    observers: list | None = None,
) -> NetworkSim:
    if snapshot_every_days and until is None:
        raise ValueError("snapshots need an end time")
    env = simpy.Environment()
    sim = NetworkSim(env, config, part_supplier, policy, emitter, rng)
    env.process(sim.run_orders(orders))
    if changes:
        env.process(sim.run_changes(changes, on_change))
    if policy_updates:
        env.process(sim.run_policy_updates(policy_updates))
    for observer in observers or []:
        env.process(observer(sim))
    if snapshot_every_days:
        env.process(sim.run_snapshots(snapshot_every_days, until))
    env.run(until=until)
    return sim


if __name__ == "__main__":
    config = load_config("config/network.yaml")
    demand = pd.read_parquet(config.demand_file)
    parts = pd.read_parquet("data/processed/parts.parquet")
    rng = np.random.default_rng(config.seed)
    orders = generate_orders(demand, parts.index, config.regions, rng)

    flat_policy = Policy(
        region_base_stock={(r.name, p): 1 for r in config.regions for p in parts.index},
        dc_reorder_point={p: 1 for p in parts.index},
        dc_order_up_to={p: 3 for p in parts.index},
    )
    sink = ListSink()
    emitter = Emitter("demo", demand["month"].min().to_pydatetime(), sink)

    started = time.perf_counter()
    simulate(config, parts["supplier"].to_dict(), flat_policy, orders, emitter, rng)
    elapsed = time.perf_counter() - started

    counts = Counter(e.event_type.value for e in sink.events)
    for event_type, n in sorted(counts.items()):
        print(f"{event_type}: {n}")
    print(f"fill rate: {1 - counts['backorder'] / counts['demand']:.3f}")
    print(f"{len(sink.events)} events in {elapsed:.1f}s")
