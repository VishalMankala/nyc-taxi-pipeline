from datetime import date, datetime

import pytest

from taxi_pipeline.config import Month, Settings


def test_month_parse_and_str():
    assert str(Month.parse("2024-03")) == "2024-03"
    assert Month.parse(" 2024-12 ") == Month(2024, 12)


@pytest.mark.parametrize("bad", ["2024-13", "2024-00", "24-01", "2024/01", ""])
def test_month_parse_rejects_bad_input(bad):
    with pytest.raises(ValueError):
        Month.parse(bad)


def test_month_shift_crosses_years():
    assert Month(2024, 1).shift(-1) == Month(2023, 12)
    assert Month(2024, 11).shift(3) == Month(2025, 2)
    assert Month(2024, 5).shift(-14) == Month(2023, 3)


def test_month_bounds():
    m = Month(2024, 2)
    assert m.start == datetime(2024, 2, 1)
    assert m.end == datetime(2024, 3, 1)
    assert Month.from_date(date(2024, 2, 29)) == m


def test_local_layout(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_URI", str(tmp_path))
    s = Settings()
    assert not s.is_s3
    assert s.raw_key(Month(2024, 1)) == (
        "raw/yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet"
    )
    assert s.curated_path() == f"{tmp_path}/curated/yellow_trips"


def test_s3_layout(monkeypatch):
    monkeypatch.setenv("LAKE_URI", "s3://my-lake/prod")
    s = Settings()
    assert s.is_s3
    assert s.lake_root == "s3://my-lake/prod"
    assert s.curated_path() == "s3a://my-lake/prod/curated/yellow_trips"
