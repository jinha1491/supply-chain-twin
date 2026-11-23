import re
from datetime import UTC, datetime

from twin.masterdata import DDL, TABLES, sim_time
from twin.stream.connector import CONFIG


def test_sim_time_counts_from_1998():
    assert sim_time(0) == datetime(1998, 1, 1, tzinfo=UTC)
    assert sim_time(775) == datetime(2000, 2, 15, tzinfo=UTC)


def test_connector_captures_every_master_data_table():
    created = set(re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", DDL))
    captured = {t.removeprefix("public.") for t in CONFIG["table.include.list"].split(",")}
    assert created == captured == set(TABLES)


def test_every_table_has_a_primary_key_and_effective_time():
    for block in DDL.split("CREATE TABLE")[1:]:
        assert "PRIMARY KEY" in block
        assert "effective_at timestamptz NOT NULL" in block
