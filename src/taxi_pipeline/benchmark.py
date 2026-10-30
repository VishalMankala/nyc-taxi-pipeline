"""Reproducible benchmark: the same analytical queries on CSV vs Parquet.

Uses DuckDB so the comparison measures the file format, not cluster tuning.
Run it on a real month:  taxi-pipeline benchmark --parquet lake/raw/.../yellow_tripdata_2024-01.parquet
"""

from __future__ import annotations

import json
import statistics
import time
from pathlib import Path

import duckdb

QUERIES = {
    "daily_revenue": """
        SELECT CAST(tpep_pickup_datetime AS DATE) AS day, COUNT(*) AS trips,
               SUM(total_amount) AS revenue
        FROM {src} GROUP BY 1 ORDER BY 1""",
    "top_pickup_zones": """
        SELECT PULocationID, COUNT(*) AS trips FROM {src}
        GROUP BY 1 ORDER BY trips DESC LIMIT 10""",
    "card_tip_rate_by_hour": """
        SELECT HOUR(tpep_pickup_datetime) AS hr, AVG(tip_amount / NULLIF(fare_amount, 0)) AS tip_rate
        FROM {src} WHERE payment_type = 1 GROUP BY 1 ORDER BY 1""",
    "long_trips_count": "SELECT COUNT(*) FROM {src} WHERE trip_distance > 20",
}


def _time(con: duckdb.DuckDBPyConnection, sql: str, repeats: int) -> float:
    con.execute(sql).fetchall()  # warm-up (OS page cache, parsing)
    runs = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        con.execute(sql).fetchall()
        runs.append(time.perf_counter() - t0)
    return statistics.median(runs)


def run_benchmark(parquet_path: str, out_dir: str = "benchmarks", repeats: int = 5) -> dict:
    parquet = Path(parquet_path).resolve()
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    csv = out / f"{parquet.stem}.csv"

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    if not csv.exists():
        con.execute(f"COPY (SELECT * FROM read_parquet('{parquet}')) TO '{csv}' (HEADER)")

    rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet}')").fetchone()[0]
    results = []
    for name, template in QUERIES.items():
        t_csv = _time(con, template.format(src=f"read_csv_auto('{csv}')"), repeats)
        t_pq = _time(con, template.format(src=f"read_parquet('{parquet}')"), repeats)
        results.append({"query": name, "csv_s": round(t_csv, 4), "parquet_s": round(t_pq, 4),
                        "speedup": round(t_csv / max(t_pq, 1e-9), 1)})

    summary = {
        "source": parquet.name,
        "rows": rows,
        "csv_mb": round(csv.stat().st_size / 1e6, 1),
        "parquet_mb": round(parquet.stat().st_size / 1e6, 1),
        "size_ratio": round(csv.stat().st_size / parquet.stat().st_size, 1),
        "median_speedup": round(statistics.median(r["speedup"] for r in results), 1),
        "duckdb_version": duckdb.__version__,
        "repeats": repeats,
        "queries": results,
    }
    (out / "results.json").write_text(json.dumps(summary, indent=2))
    (out / "results.md").write_text(to_markdown(summary))
    return summary


def to_markdown(s: dict) -> str:
    lines = [
        f"# CSV vs Parquet benchmark ({s['source']}, {s['rows']:,} rows)",
        "",
        f"Storage: CSV {s['csv_mb']} MB vs Parquet {s['parquet_mb']} MB "
        f"(Parquet is {s['size_ratio']}x smaller).  ",
        f"Median query speedup: **{s['median_speedup']}x**. "
        f"DuckDB {s['duckdb_version']}, median of {s['repeats']} warm runs.",
        "",
        "| Query | CSV (s) | Parquet (s) | Speedup |",
        "|---|---:|---:|---:|",
    ]
    lines += [f"| {r['query']} | {r['csv_s']} | {r['parquet_s']} | {r['speedup']}x |"
              for r in s["queries"]]
    return "\n".join(lines) + "\n"
