import io

import pytest
from helpers import rows

from taxi_pipeline import ingest
from taxi_pipeline.benchmark import run_benchmark
from taxi_pipeline.storage import Storage


class FakeResponse:
    def __init__(self, body: bytes, status: int = 200):
        self.body, self.status_code = body, status
        self.headers = {"Content-Length": str(len(body))}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)

    def iter_content(self, chunk):
        for i in range(0, len(self.body), chunk):
            yield self.body[i:i + chunk]


def _parquet_bytes(n=3) -> bytes:
    buf = io.BytesIO()
    rows(*[{}] * n).to_parquet(buf, index=False)
    return buf.getvalue()


def test_ingest_lands_file_then_skips(settings, month, monkeypatch):
    calls = []

    def fake_get(url, **kw):
        calls.append(url)
        return FakeResponse(_parquet_bytes())

    monkeypatch.setattr(ingest.requests, "get", fake_get)
    first = ingest.ingest_month(month, settings)
    second = ingest.ingest_month(month, settings)

    assert first["rows"] == 3 and not first["skipped"]
    assert second["skipped"] and len(calls) == 1
    assert calls[0].endswith("yellow_tripdata_2024-01.parquet")
    assert Storage(settings).exists(settings.raw_key(month))


def test_ingest_rejects_html_error_page(settings, month, monkeypatch):
    monkeypatch.setattr(ingest.requests, "get", lambda url, **kw: FakeResponse(b"<html>nope</html>"))
    with pytest.raises(ValueError, match="not a valid Parquet"):
        ingest.ingest_month(month, settings)
    assert not Storage(settings).exists(settings.raw_key(month))


def test_ingest_unpublished_month(settings, month, monkeypatch):
    monkeypatch.setattr(ingest.requests, "get", lambda url, **kw: FakeResponse(b"", status=403))
    with pytest.raises(ingest.SourceNotPublishedError):
        ingest.ingest_month(month, settings)


def test_benchmark_produces_report(tmp_path):
    src = tmp_path / "trips.parquet"
    rows(*[{}] * 200).to_parquet(src, index=False)
    summary = run_benchmark(str(src), out_dir=str(tmp_path / "bench"), repeats=1)
    assert summary["rows"] == 200
    assert len(summary["queries"]) == 4
    assert (tmp_path / "bench" / "results.md").exists()
