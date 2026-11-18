import numpy as np
import pandas as pd

from twin.network import Region, load_config


def generate_orders(
    demand: pd.DataFrame,
    part_ids,
    regions: tuple[Region, ...],
    rng: np.random.Generator,
) -> pd.DataFrame:
    start = demand["month"].min()
    rows = demand[demand["part_id"].isin(part_ids) & (demand["demand"] > 0)]

    units = rows["demand"].to_numpy()
    if not np.array_equal(units, np.round(units)):
        raise ValueError("demand must be whole units")

    counts = rng.multinomial(units.astype(int), [r.demand_share for r in regions])

    n_rows, n_regions = counts.shape
    flat = counts.ravel()
    row_idx = np.repeat(np.repeat(np.arange(n_rows), n_regions), flat)
    region_idx = np.repeat(np.tile(np.arange(n_regions), n_rows), flat)

    month_offset = (rows["month"] - start).dt.days.to_numpy()
    month_length = rows["month"].dt.days_in_month.to_numpy()
    time_days = month_offset[row_idx] + rng.uniform(size=len(row_idx)) * month_length[row_idx]

    orders = pd.DataFrame(
        {
            "part_id": rows["part_id"].to_numpy()[row_idx],
            "region": np.array([r.name for r in regions])[region_idx],
            "time_days": time_days,
        }
    )
    orders = orders.sort_values("time_days", kind="mergesort").reset_index(drop=True)
    orders.insert(0, "order_id", [f"ord-{i:08d}" for i in range(len(orders))])
    return orders


if __name__ == "__main__":
    config = load_config("config/network.yaml")
    demand = pd.read_parquet(config.demand_file)
    parts = pd.read_parquet("data/processed/parts.parquet")

    orders = generate_orders(
        demand, parts.index, config.regions, np.random.default_rng(config.seed)
    )

    expected = demand.loc[demand["part_id"].isin(parts.index), "demand"].sum()
    print(f"orders: {len(orders)} (expected units: {int(expected)})")
    print(orders["region"].value_counts(normalize=True).round(3).to_string())
    print(f"time range: {orders['time_days'].min():.2f} to {orders['time_days'].max():.2f} days")
