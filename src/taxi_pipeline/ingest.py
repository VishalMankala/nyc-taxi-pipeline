"""Land raw TLC files in the lake, unchanged.

The raw zone is an exact copy of the source file. Every downstream table can be
rebuilt from it, so we never edit raw data, only add to it.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import requests

from taxi_pipeline.config import Month, Settings
from taxi_pipeline.storage import Storage

log = logging.getLogger(__name__)

CHUNK = 1 << 20  # 1 MiB
TIMEOUT = (10, 120)  # connect, read seconds


class SourceNotPublishedError(RuntimeError):
    """TLC publishes each month with a lag of about two months."""


def _download(url: str, dest: Path) -> int:
    with requests.get(url, stream=True, timeout=TIMEOUT) as resp:
        if resp.status_code in (403, 404):
            raise SourceNotPublishedError(f"{url} returned {resp.status_code}; not published yet?")
        resp.raise_for_status()
        written = 0
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(CHUNK):
                fh.write(chunk)
                written += len(chunk)
    expected = resp.headers.get("Content-Length")
    if expected is not None and int(expected) != written:
        raise OSError(f"Truncated download from {url}: {written} of {expected} bytes")
    return written


def _validate_parquet(path: Path) -> int:
    """Fail fast on HTML error pages or truncated files. Returns the row count."""
    with open(path, "rb") as fh:
        head = fh.read(4)
        fh.seek(-4, 2)
        tail = fh.read(4)
    if head != b"PAR1" or tail != b"PAR1":
        raise ValueError(f"{path} is not a valid Parquet file")
    return pq.ParquetFile(path).metadata.num_rows


def ingest_month(month: Month, settings: Settings | None = None, force: bool = False) -> dict:
    """Download one month of yellow taxi trips into the raw zone. Idempotent."""
    settings = settings or Settings()
    storage = Storage(settings)
    key = settings.raw_key(month)

    if storage.exists(key) and not force:
        log.info("raw file already landed, skipping: %s", key)
        return {"month": str(month), "key": key, "skipped": True, "bytes": storage.size(key)}

    url = f"{settings.tlc_base_url}/yellow_tripdata_{month}.parquet"
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / f"yellow_tripdata_{month}.parquet"
        size = _download(url, local)
        rows = _validate_parquet(local)
        storage.put_file(local, key)

    log.info("landed %s (%s rows, %.1f MB)", key, f"{rows:,}", size / 1e6)
    return {"month": str(month), "key": key, "skipped": False, "bytes": size, "rows": rows}


def ingest_zone_lookup(settings: Settings | None = None, force: bool = False) -> dict:
    """Download the 265-row TLC taxi zone lookup used to enrich trips."""
    settings = settings or Settings()
    storage = Storage(settings)
    key = settings.zone_lookup_key()
    if storage.exists(key) and not force:
        return {"key": key, "skipped": True}

    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / "taxi_zone_lookup.csv"
        _download(settings.zone_lookup_url, local)
        header = local.read_text().splitlines()[0]
        if "LocationID" not in header:
            raise ValueError(f"Unexpected zone lookup header: {header!r}")
        storage.put_file(local, key)
    return {"key": key, "skipped": False}
