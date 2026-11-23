import argparse
import subprocess
import time
from pathlib import Path

DBT = ["--entrypoint", "dbt", "spark"]
DBT_DIRS = ["--project-dir", "/app/dbt", "--profiles-dir", "/app/dbt"]

STEPS = [
    ("ingest_raw", ["spark", "/app/spark/jobs/ingest_raw.py", "--once"]),
    ("cdc_merge", ["spark", "/app/spark/jobs/cdc_merge.py", "--once"]),
    ("aggregate", ["spark", "/app/spark/jobs/aggregate.py", "--once"]),
    ("stockouts", ["spark", "/app/spark/jobs/stockouts.py", "--once"]),
    ("dbt_freshness", [*DBT, "source", "freshness", *DBT_DIRS]),
    ("dbt_build", [*DBT, "build", *DBT_DIRS]),
]


def run_step(name: str, args: list[str], log_dir: Path) -> tuple[int, float]:
    log_path = log_dir / f"{name}.log"
    started = time.perf_counter()
    with open(log_path, "w") as log:
        code = subprocess.run(
            ["docker", "compose", "run", "--rm", *args], stdout=log, stderr=subprocess.STDOUT
        ).returncode
    return code, time.perf_counter() - started


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the batch side of the pipeline in order.")
    parser.add_argument("--from-step", default=STEPS[0][0], choices=[name for name, _ in STEPS])
    parser.add_argument("--log-dir", default="logs")
    args = parser.parse_args()

    log_dir = Path(args.log_dir)
    log_dir.mkdir(exist_ok=True)
    names = [name for name, _ in STEPS]
    for name, step_args in STEPS[names.index(args.from_step) :]:
        code, seconds = run_step(name, step_args, log_dir)
        print(f"{name:<14} {'ok' if code == 0 else 'FAILED'}  {seconds:6.1f}s")
        if code != 0:
            tail = (log_dir / f"{name}.log").read_text().splitlines()[-15:]
            print("\n".join(tail))
            raise SystemExit(code)


if __name__ == "__main__":
    main()
