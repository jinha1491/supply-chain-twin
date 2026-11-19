import pandas as pd

EPISODE_COLUMNS = ["site", "part_id", "out_start", "out_end", "duration_days"]


def to_micros(time) -> int:
    return pd.Timestamp(time).value // 1000


def track_episodes(
    site: str,
    part_id: str,
    events: pd.DataFrame,
    on_hand: int,
    out_since_us: int | None,
) -> tuple[pd.DataFrame, int, int | None]:
    episodes = []
    net_by_time = events.groupby("event_time", sort=True)["delta"].sum()

    for time, change in net_by_time.items():
        if change == 0:
            continue
        before = on_hand
        on_hand += int(change)

        if before > 0 and on_hand <= 0:
            out_since_us = to_micros(time)
        elif before <= 0 and on_hand > 0 and out_since_us is not None:
            start = pd.Timestamp(out_since_us, unit="us")
            duration = (pd.Timestamp(time) - start).total_seconds() / 86400
            episodes.append((site, part_id, start, pd.Timestamp(time), duration))
            out_since_us = None

    return pd.DataFrame(episodes, columns=EPISODE_COLUMNS), on_hand, out_since_us
