from pathlib import Path

import yaml

REQUIRED = {"day", "kind", "name", "lead_time_mean", "lead_time_std"}


def load_scenario(path: str | Path) -> dict:
    scenario = yaml.safe_load(Path(path).read_text())
    for change in scenario["changes"]:
        missing = REQUIRED - change.keys()
        if missing:
            raise ValueError(f"change {change} is missing {sorted(missing)}")
        if change["kind"] != "supplier":
            raise ValueError(f"unsupported change kind {change['kind']!r}")
        if change["day"] < 0 or change["lead_time_mean"] <= 0 or change["lead_time_std"] < 0:
            raise ValueError(f"change {change} has an invalid day or lead time")
    return scenario
