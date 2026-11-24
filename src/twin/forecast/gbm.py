import lightgbm as lgb
import numpy as np
import pandas as pd

from twin.forecast.methods import croston

LAGS = 12
PARAMS = dict(
    objective="tweedie",
    tweedie_variance_power=1.2,
    learning_rate=0.05,
    n_estimators=300,
    num_leaves=31,
    min_child_samples=50,
    subsample=0.8,
    subsample_freq=1,
    colsample_bytree=0.8,
    random_state=0,
    deterministic=True,
    force_row_wise=True,
    n_jobs=4,
    verbose=-1,
)


def features_at(demand: np.ndarray, month: int) -> pd.DataFrame:
    history = demand[:, :month]
    nonzero = history > 0
    count = nonzero.sum(axis=1)
    last_index = month - 1 - np.argmax(nonzero[:, ::-1], axis=1)
    has_demand = nonzero.any(axis=1)

    columns = {f"lag_{k}": history[:, -k] for k in range(1, LAGS + 1)}
    for window in (3, 6, 12):
        columns[f"mean_{window}"] = history[:, -window:].mean(axis=1)
    columns["mean_all"] = history.mean(axis=1)
    columns["nonzero_share_12"] = nonzero[:, -12:].mean(axis=1)
    columns["months_since_demand"] = np.where(has_demand, month - last_index, month + 1)
    columns["mean_size"] = np.where(count > 0, history.sum(axis=1) / np.maximum(count, 1), 0.0)
    columns["adi"] = month / np.maximum(count, 1)
    columns["sba"] = croston(history, variant="sba")
    columns["month_of_year"] = np.full(demand.shape[0], month % 12)
    return pd.DataFrame(columns)


def rolling_gbm(demand: np.ndarray, origins: list[int]) -> np.ndarray:
    frames = []
    for month in range(LAGS, demand.shape[1]):
        frame = features_at(demand, month)
        frame["target_month"] = month
        frame["target"] = demand[:, month]
        frame["row"] = np.arange(demand.shape[0])
        frames.append(frame)
    table = pd.concat(frames, ignore_index=True)
    features = [c for c in table.columns if c not in ("target_month", "target", "row")]

    forecasts = np.zeros((demand.shape[0], len(origins)))
    for i, origin in enumerate(origins):
        train = table[table["target_month"] < origin]
        test = table[table["target_month"] == origin].sort_values("row")
        model = lgb.LGBMRegressor(**PARAMS).fit(train[features], train["target"])
        forecasts[:, i] = model.predict(test[features])
    return forecasts
