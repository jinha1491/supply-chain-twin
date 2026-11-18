import numpy as np
import pandas as pd
import pytest

from twin.data.carparts import read_tsf, summarize

SAMPLE = """# comment line
@relation test
@attribute series_name string
@attribute start_timestamp date
@frequency monthly
@missing true
@equallength true
@data
T1:1998-01-01 00-00-00:0,2,?
T2:1998-01-01 00-00-00:1,0,0
"""


@pytest.fixture
def sample_file(tmp_path):
    path = tmp_path / "sample.tsf"
    path.write_text(SAMPLE)
    return path


def test_read_tsf_columns_and_rows(sample_file):
    df = read_tsf(sample_file)
    assert list(df.columns) == ["part_id", "month", "demand"]
    assert len(df) == 6


def test_months_are_consecutive(sample_file):
    df = read_tsf(sample_file)
    t1_months = df[df["part_id"] == "T1"]["month"].tolist()
    assert t1_months == list(pd.date_range("1998-01-01", periods=3, freq="MS"))


def test_missing_values_become_nan(sample_file):
    df = read_tsf(sample_file)
    assert np.isnan(df.iloc[2]["demand"])


def test_summarize_counts(sample_file):
    stats = summarize(read_tsf(sample_file))
    assert stats["parts"] == 2
    assert stats["missing_share"] == round(1 / 6, 3)
    assert stats["zero_share"] == 0.6


def test_file_without_data_section_raises(tmp_path):
    path = tmp_path / "bad.tsf"
    path.write_text("@relation bad\n")
    with pytest.raises(ValueError):
        read_tsf(path)
