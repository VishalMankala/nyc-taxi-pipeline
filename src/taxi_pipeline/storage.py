"""Tiny object-store abstraction over a local folder or an S3 bucket.

Spark reads and writes the lake directly. This module covers the plain-Python
side: landing raw files, writing run manifests, and cleaning up staging.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from urllib.parse import urlparse

from taxi_pipeline.config import Settings


class Storage:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._s3 = None
        if settings.is_s3:
            parsed = urlparse(settings.lake_root)
            self.bucket = parsed.netloc
            self.prefix = parsed.path.strip("/")
        else:
            self.root = Path(settings.lake_root)

    # ---- helpers ---------------------------------------------------------------
    @property
    def s3(self):
        if self._s3 is None:
            import boto3

            self._s3 = boto3.client("s3", endpoint_url=self.settings.s3_endpoint_url)
            if self.settings.s3_endpoint_url:  # local S3 endpoint: create the bucket on first use
                self.ensure_bucket(self._s3)
        return self._s3

    def ensure_bucket(self, client) -> None:
        """Create the bucket if it is missing. Idempotent; used for local S3 endpoints only."""
        from botocore.exceptions import ClientError

        try:
            client.head_bucket(Bucket=self.bucket)
        except ClientError as err:
            if err.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
                raise
            try:
                client.create_bucket(Bucket=self.bucket)
            except ClientError as create_err:  # another task created it first
                if create_err.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou",
                                                                "BucketAlreadyExists"):
                    raise

    def _s3_key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def _local(self, key: str) -> Path:
        return self.root / key

    # ---- API -------------------------------------------------------------------
    def exists(self, key: str) -> bool:
        if self.settings.is_s3:
            from botocore.exceptions import ClientError

            try:
                self.s3.head_object(Bucket=self.bucket, Key=self._s3_key(key))
                return True
            except ClientError as err:
                if err.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                    return False
                raise
        return self._local(key).exists()

    def size(self, key: str) -> int:
        if self.settings.is_s3:
            head = self.s3.head_object(Bucket=self.bucket, Key=self._s3_key(key))
            return int(head["ContentLength"])
        return self._local(key).stat().st_size

    def put_file(self, local_path: str | os.PathLike, key: str) -> None:
        if self.settings.is_s3:
            self.s3.upload_file(str(local_path), self.bucket, self._s3_key(key))
            return
        dest = self._local(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        shutil.copyfile(local_path, tmp)
        os.replace(tmp, dest)  # atomic rename: readers never see a half-written file

    def put_bytes(self, data: bytes, key: str) -> None:
        if self.settings.is_s3:
            self.s3.put_object(Bucket=self.bucket, Key=self._s3_key(key), Body=data)
            return
        dest = self._local(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, dest)

    def get_bytes(self, key: str) -> bytes:
        if self.settings.is_s3:
            obj = self.s3.get_object(Bucket=self.bucket, Key=self._s3_key(key))
            return obj["Body"].read()
        return self._local(key).read_bytes()

    def delete_prefix(self, key_prefix: str) -> None:
        if self.settings.is_s3:
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=self._s3_key(key_prefix)):
                keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if keys:
                    self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": keys})
            return
        shutil.rmtree(self._local(key_prefix), ignore_errors=True)
