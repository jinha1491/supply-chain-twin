import numpy as np
import pandas as pd

from twin.data.profile import classify_demand, missing_positions


def make_df(series: dict[str, list[float]]) -> pd.DataFrame:
    frames = []
    for part_id, values in series.items():
        months = pd.date_range("1998-01-01", periods=len(values), freq="MS")
        frames.append(pd.DataFrame({"part_id": part_id, "month": months, "demand": values}))
    return pd.concat(frames, ignore_index=True)


def test_classify_demand_patterns():
    nan = np.nan
    df = make_df(
        {
            "smooth": [1, 1, 1, 1],
            "erratic": [9, 1, 9, 1],
            "intermittent": [0, 2, 0, 2],
            "lumpy": [0, 9, 0, 1],
            "no_demand": [0, 0, 0, 0],
            "no_data": [nan, nan, nan, nan],
        }
    )
    profile = classify_demand(df)
    for part_id in profile.index:
        assert profile.loc[part_id, "pattern"] == part_id


def test_adi_ignores_missing_months():
    df = make_df({"p": [np.nan, 0, 3, 0, 3]})
    profile = classify_demand(df)
    assert profile.loc["p", "adi"] == 2.0


def test_missing_positions():
    df = make_df({"p": [np.nan, 1, np.nan, 2, np.nan], "q": [np.nan, np.nan]})
    counts = missing_positions(df)
    assert counts == {"leading": 1, "interior": 1, "trailing": 1, "all_missing_parts": 1}
