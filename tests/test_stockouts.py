import pandas as pd
import pytest

from twin.stockouts import track_episodes


def changes(*pairs):
    return pd.DataFrame(
        {"event_time": [pd.Timestamp(t) for t, _ in pairs], "delta": [d for _, d in pairs]}
    )


def test_stockout_and_recovery_make_one_episode():
    events = changes(("2000-01-01", -1), ("2000-01-04", 1))
    episodes, on_hand, out_since = track_episodes("west", "p", events, 1, None)

    assert len(episodes) == 1
    assert episodes.iloc[0]["out_start"] == pd.Timestamp("2000-01-01")
    assert episodes.iloc[0]["duration_days"] == pytest.approx(3.0)
    assert on_hand == 1
    assert out_since is None


def test_backorder_fill_at_same_instant_is_not_a_recovery():
    events = changes(
        ("2000-01-01", -1),
        ("2000-01-02", 1),
        ("2000-01-02", -1),
        ("2000-01-05", 1),
    )
    episodes, _, _ = track_episodes("west", "p", events, 1, None)

    assert len(episodes) == 1
    assert episodes.iloc[0]["duration_days"] == pytest.approx(4.0)


def test_state_carries_a_stockout_across_batches():
    _, on_hand, out_since = track_episodes("west", "p", changes(("2000-01-01", -1)), 1, None)
    assert on_hand == 0 and out_since is not None

    episodes, on_hand, _ = track_episodes(
        "west", "p", changes(("2000-01-03", 1)), on_hand, out_since
    )
    assert len(episodes) == 1
    assert episodes.iloc[0]["out_start"] == pd.Timestamp("2000-01-01")
    assert on_hand == 1


def test_no_episode_when_stock_never_hits_zero():
    events = changes(("2000-01-01", -1), ("2000-01-02", 1))
    episodes, on_hand, _ = track_episodes("west", "p", events, 2, None)
    assert episodes.empty
    assert on_hand == 2
