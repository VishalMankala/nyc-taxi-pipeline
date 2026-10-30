import pandas as pd
import pytest
from helpers import rows, to_spark, zones_df

from taxi_pipeline.transform import CURATED_COLUMNS, standardize, tag_rejects, transform


def _reasons(spark, tmp_path, month, *overrides):
    df, _ = standardize(to_spark(spark, rows(*overrides), tmp_path))
    return [r["reject_reason"] for r in tag_rejects(df, month).collect()]


def test_standardize_handles_mixed_case_and_missing_optional(spark, tmp_path):
    df, report = standardize(to_spark(spark, rows({}), tmp_path))
    assert "airport_fee" in df.columns and "vendor_id" in df.columns
    assert report.missing_optional == ["cbd_congestion_fee"]  # added by TLC in 2025
    assert dict(df.dtypes)["passenger_count"] == "int"
    assert dict(df.dtypes)["pickup_ts"] == "timestamp"


def test_standardize_reports_unexpected_columns(spark, tmp_path):
    pdf = rows({}).assign(brand_new_fee=1.0)
    _, report = standardize(to_spark(spark, pdf, tmp_path))
    assert report.unexpected == ["brand_new_fee"]


def test_standardize_fails_without_required_column(spark, tmp_path):
    pdf = rows({}).drop(columns=["total_amount"])
    with pytest.raises(ValueError, match="total_amount"):
        standardize(to_spark(spark, pdf, tmp_path))


def test_valid_row_has_no_reject_reason(spark, tmp_path, month):
    assert _reasons(spark, tmp_path, month, {}) == [None]


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"tpep_pickup_datetime": pd.Timestamp("2023-12-31 23:50:00"),
          "tpep_dropoff_datetime": pd.Timestamp("2024-01-01 00:10:00")}, "pickup_outside_month"),
        ({"tpep_pickup_datetime": pd.Timestamp("2024-02-01 00:00:00"),
          "tpep_dropoff_datetime": pd.Timestamp("2024-02-01 00:10:00")}, "pickup_outside_month"),
        ({"tpep_dropoff_datetime": pd.Timestamp("2024-01-15 07:59:00")}, "dropoff_not_after_pickup"),
        ({"tpep_dropoff_datetime": pd.Timestamp("2024-01-15 08:00:30")}, "duration_too_short"),
        ({"tpep_dropoff_datetime": pd.Timestamp("2024-01-15 15:00:00")}, "duration_too_long"),
        ({"trip_distance": 0.0}, "invalid_distance"),
        ({"trip_distance": 250.0}, "invalid_distance"),
        ({"trip_distance": 60.0}, "implausible_speed"),  # 60 mi in 20 min = 180 mph
        ({"fare_amount": -5.0}, "negative_or_null_amount"),
        ({"PULocationID": 0}, "invalid_location"),
        ({"DOLocationID": 300}, "invalid_location"),
        ({"passenger_count": 12.0}, "invalid_passenger_count"),
    ],
)
def test_reject_rules(spark, tmp_path, month, override, reason):
    assert _reasons(spark, tmp_path, month, override) == [reason]


def test_month_boundaries_are_inclusive_exclusive(spark, tmp_path, month):
    first = {"tpep_pickup_datetime": pd.Timestamp("2024-01-01 00:00:00"),
             "tpep_dropoff_datetime": pd.Timestamp("2024-01-01 00:15:00")}
    last = {"tpep_pickup_datetime": pd.Timestamp("2024-01-31 23:59:59"),
            "tpep_dropoff_datetime": pd.Timestamp("2024-02-01 00:15:00")}
    assert _reasons(spark, tmp_path, month, first, last) == [None, None]


def test_null_passenger_count_is_allowed(spark, tmp_path, month):
    assert _reasons(spark, tmp_path, month, {"passenger_count": None}) == [None]


def test_transform_derives_dedups_and_enriches(spark, tmp_path, month):
    pdf = rows(
        {},  # Monday 2024-01-15, card
        {},  # exact duplicate -> removed
        {"payment_type": 2, "tip_amount": 0.0, "PULocationID": 132},  # cash, from JFK
        {"trip_distance": 0.0},  # rejected
    )
    valid, rejected, _ = transform(to_spark(spark, pdf, tmp_path), zones_df(spark), month, "src")
    out = valid.orderBy("payment_type").collect()

    assert valid.columns == CURATED_COLUMNS
    assert len(out) == 2 and rejected.count() == 1

    card, cash = out
    assert card.pickup_dow == 1  # ISO Monday
    assert card.pickup_hour == 8
    assert card.trip_duration_min == 20.0
    assert card.avg_speed_mph == 12.0
    assert card.tip_pct == 20.0
    assert card.payment_type_desc == "Credit card"
    assert card.is_airport_trip is False
    assert (card.pickup_year, card.pickup_month) == (2024, 1)
    assert card.pickup_borough is not None

    assert cash.tip_pct is None  # cash tips are not recorded by TLC
    assert cash.is_airport_trip is True
    assert cash.pickup_zone == "JFK Airport"
    assert len(card.trip_id) == 64
