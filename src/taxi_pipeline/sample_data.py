"""Synthetic TLC-shaped data for tests, CI and offline demos.

Mirrors the real raw schema (original column names and types, including the
mixed-case `VendorID` / `Airport_fee`) and plants known-bad rows so every quality
rule is exercised. Real results come from real TLC data (see README).
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from taxi_pipeline.config import Month, Settings
from taxi_pipeline.storage import Storage

BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
SPECIAL_ZONES = {
    1: ("EWR", "Newark Airport", "EWR"),
    132: ("Queens", "JFK Airport", "Airports"),
    138: ("Queens", "LaGuardia Airport", "Airports"),
    264: ("Unknown", "N/A", "N/A"),
    265: ("N/A", "Outside of NYC", "N/A"),
}

# Planted defects: (name, count) -> each maps to one reject_reason in transform.py
DEFECTS = (
    "pickup_outside_month",
    "dropoff_not_after_pickup",
    "duration_too_long",
    "invalid_distance",
    "negative_or_null_amount",
    "invalid_location",
    "invalid_passenger_count",
)


def zone_lookup() -> pd.DataFrame:
    rows = []
    for loc in range(1, 266):
        if loc in SPECIAL_ZONES:
            borough, zone, service = SPECIAL_ZONES[loc]
        else:
            borough = BOROUGHS[loc % len(BOROUGHS)]
            zone = f"Zone {loc}"
            service = "Yellow Zone" if borough == "Manhattan" else "Boro Zone"
        rows.append({"LocationID": loc, "Borough": borough, "Zone": zone, "service_zone": service})
    return pd.DataFrame(rows)


def generate_month(month: Month, n_rows: int = 5_000, seed: int = 7,
                   defects_per_kind: int = 10, duplicates: int = 10) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    seconds_in_month = int((month.end - month.start).total_seconds())

    pickup = pd.to_datetime(month.start) + pd.to_timedelta(
        rng.integers(0, seconds_in_month - 7200, n_rows), unit="s"
    )
    duration = rng.gamma(shape=2.0, scale=420.0, size=n_rows).clip(90, 3 * 3600)
    distance = np.round((duration / 3600) * rng.uniform(6, 22, n_rows), 2).clip(0.1, 60)
    fare = np.round(3.0 + 2.5 * distance + 0.6 * duration / 60, 2)
    payment = rng.choice([1, 2, 3, 4], size=n_rows, p=[0.78, 0.18, 0.02, 0.02])
    tip = np.where(payment == 1, np.round(fare * rng.uniform(0.0, 0.3, n_rows), 2), 0.0)
    pu = rng.integers(1, 264, n_rows)
    do = rng.integers(1, 264, n_rows)
    airport_fee = np.where(np.isin(pu, [132, 138]), 1.75, 0.0)
    extra, mta, improvement, congestion = 1.0, 0.5, 1.0, 2.5
    total = np.round(fare + extra + mta + tip + improvement + congestion + airport_fee, 2)

    df = pd.DataFrame({
        "VendorID": rng.choice([1, 2], size=n_rows).astype("int32"),
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": pickup + pd.to_timedelta(duration, unit="s"),
        "passenger_count": rng.choice([1.0, 1.0, 1.0, 2.0, 3.0, np.nan], size=n_rows),
        "trip_distance": distance,
        "RatecodeID": 1.0,
        "store_and_fwd_flag": "N",
        "PULocationID": pu.astype("int32"),
        "DOLocationID": do.astype("int32"),
        "payment_type": payment.astype("int64"),
        "fare_amount": fare,
        "extra": extra,
        "mta_tax": mta,
        "tip_amount": tip,
        "tolls_amount": 0.0,
        "improvement_surcharge": improvement,
        "total_amount": total,
        "congestion_surcharge": congestion,
        "Airport_fee": airport_fee,
    })

    # --- plant defects in disjoint row ranges ---------------------------------------
    k = defects_per_kind
    for i, kind in enumerate(DEFECTS):
        idx = df.index[i * k:(i + 1) * k]
        if kind == "pickup_outside_month":
            df.loc[idx, "tpep_pickup_datetime"] = pd.Timestamp("2002-12-31 23:00:00")
            df.loc[idx, "tpep_dropoff_datetime"] = pd.Timestamp("2002-12-31 23:20:00")
        elif kind == "dropoff_not_after_pickup":
            df.loc[idx, "tpep_dropoff_datetime"] = df.loc[idx, "tpep_pickup_datetime"]
        elif kind == "duration_too_long":
            df.loc[idx, "tpep_dropoff_datetime"] = (
                df.loc[idx, "tpep_pickup_datetime"] + pd.Timedelta(hours=9)
            )
            df.loc[idx, "trip_distance"] = 50.0
        elif kind == "invalid_distance":
            df.loc[idx, "trip_distance"] = 0.0
        elif kind == "negative_or_null_amount":
            df.loc[idx, "fare_amount"] = -12.5
            df.loc[idx, "total_amount"] = -17.0
        elif kind == "invalid_location":
            df.loc[idx, "PULocationID"] = 999
        elif kind == "invalid_passenger_count":
            df.loc[idx, "passenger_count"] = 42.0

    # Exact duplicate rows, as seen when a vendor re-sends records.
    clean_tail = df.iloc[len(DEFECTS) * k:]
    dupes = clean_tail.sample(n=duplicates, random_state=seed)
    df = pd.concat([df, dupes], ignore_index=True)
    return df.sample(frac=1.0, random_state=seed).reset_index(drop=True)


def seed_lake(month: Month, settings: Settings | None = None, n_rows: int = 5_000) -> dict:
    """Write a synthetic raw month + zone lookup into the lake (no internet needed)."""
    settings = settings or Settings()
    storage = Storage(settings)
    df = generate_month(month, n_rows=n_rows)
    with tempfile.TemporaryDirectory() as tmp:
        trips = Path(tmp) / "trips.parquet"
        df.to_parquet(trips, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
        storage.put_file(trips, settings.raw_key(month))
        zones = Path(tmp) / "zones.csv"
        zone_lookup().to_csv(zones, index=False)
        storage.put_file(zones, settings.zone_lookup_key())
    return {"month": str(month), "rows": len(df)}
