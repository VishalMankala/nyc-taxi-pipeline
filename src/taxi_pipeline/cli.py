"""Command line entry point.

    taxi-pipeline run --month 2024-01              # ingest -> stage -> audit -> publish
    taxi-pipeline backfill --start 2024-01 --end 2024-12
    taxi-pipeline seed-sample --month 2024-01      # synthetic data, no internet needed
    taxi-pipeline benchmark --parquet <file>
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from taxi_pipeline.config import Month, Settings


def _months(start: Month, end: Month) -> list[Month]:
    out, m = [], start
    while (m.year, m.month) <= (end.year, end.month):
        out.append(m)
        m = m.shift(1)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="taxi-pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="Run the full pipeline for one month")
    run.add_argument("--month", required=True, type=Month.parse)
    run.add_argument("--force-download", action="store_true")
    run.add_argument("--skip-ingest", action="store_true", help="Use raw data already in the lake")

    backfill = sub.add_parser("backfill", help="Run a range of months, oldest first")
    backfill.add_argument("--start", required=True, type=Month.parse)
    backfill.add_argument("--end", required=True, type=Month.parse)

    seed = sub.add_parser("seed-sample", help="Write synthetic raw data into the lake")
    seed.add_argument("--month", required=True, type=Month.parse)
    seed.add_argument("--rows", type=int, default=50_000)

    bench = sub.add_parser("benchmark", help="CSV vs Parquet query benchmark")
    bench.add_argument("--parquet", required=True)
    bench.add_argument("--out", default="benchmarks")
    bench.add_argument("--repeats", type=int, default=5)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings()

    if args.cmd == "run":
        from taxi_pipeline import pipeline
        from taxi_pipeline.spark_session import get_spark

        if args.skip_ingest:
            spark = get_spark(settings)
            pipeline.stage(args.month, settings, spark)
            pipeline.audit(args.month, settings, spark)
            result = pipeline.publish(args.month, settings, spark)
        else:
            result = pipeline.run_month(args.month, settings, force_download=args.force_download)
        print(json.dumps({k: v for k, v in result.items() if k != "quality"}, indent=2))
    elif args.cmd == "backfill":
        from taxi_pipeline.pipeline import run_month

        for m in _months(args.start, args.end):
            result = run_month(m, settings)
            print(f"{m}: {result['staged_rows']:,} rows published in {result['total_seconds']}s")
    elif args.cmd == "seed-sample":
        from taxi_pipeline.sample_data import seed_lake

        print(seed_lake(args.month, settings, n_rows=args.rows))
    elif args.cmd == "benchmark":
        from taxi_pipeline.benchmark import run_benchmark, to_markdown

        print(to_markdown(run_benchmark(args.parquet, args.out, args.repeats)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
