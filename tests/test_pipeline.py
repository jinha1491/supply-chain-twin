from pathlib import Path

from twin.pipeline import STEPS


def test_every_spark_job_exists():
    for _, args in STEPS:
        if args[0] == "spark":
            assert Path(args[1].replace("/app/", "")).exists(), args[1]


def test_dbt_build_runs_last():
    assert STEPS[-1][0] == "dbt_build"
