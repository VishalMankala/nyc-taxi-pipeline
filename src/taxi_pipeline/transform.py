"""Clean, validate, enrich: raw TLC trips -> curated trips + quarantine.

Every function here is pure (DataFrame in, DataFrame out) so it is unit-testable
with a tiny local SparkSession and no storage.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DataType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)

from taxi_pipeline.config import Month

log = logging.getLogger(__name__)

# Canonical column -> (accepted raw names after lower-casing, type)
# TLC renamed and re-typed columns over the years (e.g. Airport_fee vs airport_fee,
# passenger_count int64 vs double), so we map every month onto one schema.
CANONICAL_SCHEMA: dict[str, tuple[tuple[str, ...], DataType]] = {
    "vendor_id": (("vendorid",), IntegerType()),
    "pickup_ts": (("tpep_pickup_datetime",), TimestampType()),
    "dropoff_ts": (("tpep_dropoff_datetime",), TimestampType()),
    "passenger_count": (("passenger_count",), IntegerType()),
    "trip_distance": (("trip_distance",), DoubleType()),
    "rate_code_id": (("ratecodeid",), IntegerType()),
    "store_and_fwd_flag": (("store_and_fwd_flag",), StringType()),
    "pu_location_id": (("pulocationid",), IntegerType()),
    "do_location_id": (("dolocationid",), IntegerType()),
    "payment_type": (("payment_type",), IntegerType()),
    "fare_amount": (("fare_amount",), DoubleType()),
    "extra": (("extra",), DoubleType()),
    "mta_tax": (("mta_tax",), DoubleType()),
    "tip_amount": (("tip_amount",), DoubleType()),
    "tolls_amount": (("tolls_amount",), DoubleType()),
    "improvement_surcharge": (("improvement_surcharge",), DoubleType()),
    "total_amount": (("total_amount",), DoubleType()),
    "congestion_surcharge": (("congestion_surcharge",), DoubleType()),
    "airport_fee": (("airport_fee",), DoubleType()),
    "cbd_congestion_fee": (("cbd_congestion_fee",), DoubleType()),
}

# Columns a raw file must have. Without these a trip is meaningless.
REQUIRED = ("pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id", "total_amount")

PAYMENT_TYPES = {
    0: "Flex fare",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}
AIRPORT_ZONES = (1, 132, 138)  # EWR, JFK, LaGuardia
MAX_LOCATION_ID = 265

# Business rules. Tuned against real 2024 data; documented in docs/DESIGN.md.
MIN_DURATION_S = 60
MAX_DURATION_S = 6 * 3600
MAX_DISTANCE_MI = 200
MAX_SPEED_MPH = 100
MAX_PASSENGERS = 9

NATURAL_KEY = ("vendor_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id",
               "total_amount")


@dataclass
class SchemaReport:
    missing_optional: list[str] = field(default_factory=list)
    unexpected: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {"missing_optional": self.missing_optional, "unexpected": self.unexpected}


def standardize(raw: DataFrame) -> tuple[DataFrame, SchemaReport]:
    """Map any month's raw schema onto CANONICAL_SCHEMA and report schema drift."""
    by_lower = {c.lower(): c for c in raw.columns}
    known = {alias for aliases, _ in CANONICAL_SCHEMA.values() for alias in aliases}
    report = SchemaReport(unexpected=sorted(set(by_lower) - known))

    cols: list[Column] = []
    for name, (aliases, dtype) in CANONICAL_SCHEMA.items():
        source = next((by_lower[a] for a in aliases if a in by_lower), None)
        if source is None:
            if name in REQUIRED:
                raise ValueError(f"Raw data is missing required column {name!r} {aliases}")
            report.missing_optional.append(name)
            cols.append(F.lit(None).cast(dtype).alias(name))
        else:
            cols.append(F.col(f"`{source}`").cast(dtype).alias(name))

    if report.unexpected or report.missing_optional:
        log.warning("schema drift: %s", report.as_dict())
    return raw.select(*cols), report


def ts_lit(dt) -> Column:
    """Timestamp literal parsed in the Spark session time zone (UTC).

    F.lit(datetime) would be converted using the *driver's* OS time zone, which shifts
    month boundaries by hours on a laptop outside UTC. Parsing a string avoids that.
    """
    return F.to_timestamp(F.lit(dt.strftime("%Y-%m-%d %H:%M:%S")))


def _duration_s() -> Column:
    return F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")


def tag_rejects(df: DataFrame, month: Month) -> DataFrame:
    """Add reject_reason (null = valid). First failing rule wins, so reasons are exclusive."""
    duration = _duration_s()
    speed = F.col("trip_distance") / (duration / F.lit(3600.0))
    loc_ok = lambda c: F.col(c).between(1, MAX_LOCATION_ID)  # noqa: E731

    reason = (
        F.when(F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(), "null_timestamp")
        .when(
            (F.col("pickup_ts") < ts_lit(month.start)) | (F.col("pickup_ts") >= ts_lit(month.end)),
            "pickup_outside_month",
        )
        .when(F.col("dropoff_ts") <= F.col("pickup_ts"), "dropoff_not_after_pickup")
        .when(duration < MIN_DURATION_S, "duration_too_short")
        .when(duration > MAX_DURATION_S, "duration_too_long")
        .when(
            F.col("trip_distance").isNull()
            | (F.col("trip_distance") <= 0)
            | (F.col("trip_distance") > MAX_DISTANCE_MI),
            "invalid_distance",
        )
        .when(speed > MAX_SPEED_MPH, "implausible_speed")
        .when(
            F.col("fare_amount").isNull()
            | (F.col("fare_amount") < 0)
            | F.col("total_amount").isNull()
            | (F.col("total_amount") < 0),
            "negative_or_null_amount",
        )
        .when(~(loc_ok("pu_location_id") & loc_ok("do_location_id")), "invalid_location")
        .when(
            F.col("passenger_count").isNotNull()
            & ~F.col("passenger_count").between(0, MAX_PASSENGERS),
            "invalid_passenger_count",
        )
    )
    return df.withColumn("reject_reason", reason)


def derive(df: DataFrame) -> DataFrame:
    """Add analysis-ready columns."""
    duration = _duration_s()
    payment_map = F.create_map(*[x for k, v in PAYMENT_TYPES.items() for x in (F.lit(k), F.lit(v))])
    return (
        df.withColumn(
            "trip_id",
            F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in NATURAL_KEY]), 256),
        )
        .withColumn("trip_duration_min", F.round(duration / 60.0, 2))
        .withColumn("avg_speed_mph", F.round(F.col("trip_distance") / (duration / 3600.0), 2))
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        # ISO day of week: 1 = Monday ... 7 = Sunday
        .withColumn("pickup_dow", ((F.dayofweek("pickup_ts") + 5) % 7) + 1)
        .withColumn("pickup_year", F.year("pickup_ts"))
        .withColumn("pickup_month", F.month("pickup_ts"))
        .withColumn(
            "payment_type_desc", F.coalesce(payment_map[F.col("payment_type")], F.lit("Unknown"))
        )
        # Cash tips are not recorded by TLC, so tip % is only meaningful for card trips.
        .withColumn(
            "tip_pct",
            F.when(
                (F.col("payment_type") == 1) & (F.col("fare_amount") > 0),
                F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2),
            ),
        )
        .withColumn(
            "is_airport_trip",
            F.col("pu_location_id").isin(*AIRPORT_ZONES) | F.col("do_location_id").isin(*AIRPORT_ZONES),
        )
    )


def enrich(df: DataFrame, zones: DataFrame) -> DataFrame:
    """Attach borough / zone names for pickup and dropoff (zone table is 265 rows: broadcast)."""
    z = zones.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone"),
    )
    pu = F.broadcast(z.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    ))
    do = F.broadcast(z.select(
        F.col("location_id").alias("do_location_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
    ))
    return df.join(pu, "pu_location_id", "left").join(do, "do_location_id", "left")


CURATED_COLUMNS = [
    "trip_id", "vendor_id", "pickup_ts", "dropoff_ts", "pickup_date", "pickup_hour",
    "pickup_dow", "passenger_count", "trip_distance", "trip_duration_min", "avg_speed_mph",
    "rate_code_id", "store_and_fwd_flag",
    "pu_location_id", "pickup_borough", "pickup_zone", "pickup_service_zone",
    "do_location_id", "dropoff_borough", "dropoff_zone", "is_airport_trip",
    "payment_type", "payment_type_desc", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tip_pct", "tolls_amount", "improvement_surcharge", "congestion_surcharge", "airport_fee",
    "cbd_congestion_fee", "total_amount",
    "_source_file", "_run_month", "_processed_at", "pickup_year", "pickup_month",
]


def transform(
    raw: DataFrame, zones: DataFrame, month: Month, source_file: str
) -> tuple[DataFrame, DataFrame, SchemaReport]:
    """Full raw -> (curated, quarantine) transformation for one month."""
    std, report = standardize(raw)
    tagged = tag_rejects(std, month).withColumns({
        "_source_file": F.lit(source_file),
        "_run_month": F.lit(str(month)),
        "_processed_at": F.current_timestamp(),
    })

    valid = (
        tagged.filter(F.col("reject_reason").isNull())
        .drop("reject_reason")
        .transform(derive)
        .dropDuplicates(["trip_id"])
        .transform(lambda d: enrich(d, zones))
        .select(*CURATED_COLUMNS)
    )
    rejected = tagged.filter(F.col("reject_reason").isNotNull()).withColumns({
        "quarantine_year": F.lit(month.year),
        "quarantine_month": F.lit(month.month),
    })
    return valid, rejected, report
