import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from twin.forecast import methods

FIRST_ORIGIN = 24
OUTPUT = Path("data/processed/forecasts.parquet")

METHODS = {
    "zero": lambda history: np.zeros(history.shape[0]),
    "naive": methods.naive,
    "moving_average_12": methods.moving_average,
    "ses": methods.ses,
    "croston": methods.croston,
    "sba": lambda history: methods.croston(history, variant="sba"),
    "tsb": methods.tsb,
}


def demand_matrix(demand: pd.DataFrame, part_ids) -> tuple[np.ndarray, list, list]:
    wide = (
        demand[demand["part_id"].isin(part_ids)]
        .pivot(index="part_id", columns="month", values="demand")
        .sort_index()
    )
    return wide.to_numpy(dtype=float), list(wide.index), list(wide.columns)


def rolling(history: np.ndarray, forecast, origins: list[int]) -> np.ndarray:
    return np.column_stack([forecast(history[:, :origin]) for origin in origins])


def score(forecast: np.ndarray, actual: np.ndarray) -> dict:
    error = forecast - actual
    return {
        "mae": float(np.abs(error).mean()),
        "rmse": float(np.sqrt((error**2).mean())),
        "bias": float(error.mean()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Rolling-origin evaluation of demand forecasts.")
    parser.add_argument("--no-gbm", action="store_true")
    parser.add_argument("--tracking-uri", default="sqlite:///mlflow.db")
    args = parser.parse_args()

    demand = pd.read_parquet("data/processed/carparts.parquet")
    parts = pd.read_parquet("data/processed/parts.parquet")
    matrix, part_ids, months = demand_matrix(demand, parts.index)
    patterns = parts.loc[part_ids, "pattern"].to_numpy()

    origins = list(range(FIRST_ORIGIN, matrix.shape[1]))
    actual = matrix[:, origins]
    forecasts = {name: rolling(matrix, fn, origins) for name, fn in METHODS.items()}
    if not args.no_gbm:
        from twin.forecast.gbm import rolling_gbm

        forecasts["lightgbm"] = rolling_gbm(matrix, origins)

    rows = []
    for name, forecast in forecasts.items():
        row = {"method": name, **score(forecast, actual)}
        for pattern in ("intermittent", "lumpy"):
            mask = patterns == pattern
            row[f"rmse_{pattern}"] = score(forecast[mask], actual[mask])["rmse"]
        rows.append(row)
    results = pd.DataFrame(rows).set_index("method")
    print(f"{len(part_ids):,} parts x {len(origins)} months = {actual.size:,} forecasts per method")
    print(results.round(4).to_string())

    long = pd.concat(
        pd.DataFrame(
            {
                "part_id": np.repeat(part_ids, len(origins)),
                "month": np.tile([months[o] for o in origins], len(part_ids)),
                "method": name,
                "forecast": forecast.ravel(),
                "actual": actual.ravel(),
            }
        )
        for name, forecast in forecasts.items()
    )
    long.to_parquet(OUTPUT, index=False)

    import mlflow

    mlflow.set_tracking_uri(args.tracking_uri)
    mlflow.set_experiment("demand-forecasting")
    with mlflow.start_run(run_name="rolling-origin"):
        mlflow.log_params(
            {"parts": len(part_ids), "first_origin": FIRST_ORIGIN, "forecast_months": len(origins)}
        )
        for name, row in results.iterrows():
            with mlflow.start_run(run_name=name, nested=True):
                mlflow.log_metrics(row.to_dict())
        mlflow.log_artifact(str(OUTPUT))
    print(f"saved {len(long):,} forecasts to {OUTPUT} and logged to MLflow")


if __name__ == "__main__":
    main()
