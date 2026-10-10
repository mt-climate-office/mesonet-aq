"""Object storage: S3 in production, a local directory for tests and dry runs.

Writes are always whole-object overwrites (never delete-then-write), so a
reader on the CDN sees the old file or the new one, never a gap.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Protocol

log = logging.getLogger(__name__)


class Store(Protocol):
    def get(self, key: str) -> bytes | None: ...

    def put(
        self, key: str, body: bytes, *, content_type: str, cache_control: str
    ) -> None: ...

    def list(self, prefix: str) -> Iterator[str]: ...

    def delete(self, key: str) -> None: ...


class S3Store:
    def __init__(self, bucket: str, client=None):
        import boto3

        self.bucket = bucket
        self.s3 = client or boto3.client("s3")

    def get(self, key: str) -> bytes | None:
        try:
            return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except self.s3.exceptions.NoSuchKey:
            return None

    def put(
        self, key: str, body: bytes, *, content_type: str, cache_control: str
    ) -> None:
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
            CacheControl=cache_control,
        )
        log.info(
            "put s3://%s/%s (%d bytes, %s)", self.bucket, key, len(body), cache_control
        )

    def list(self, prefix: str) -> Iterator[str]:
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield obj["Key"]

    def delete(self, key: str) -> None:
        self.s3.delete_object(Bucket=self.bucket, Key=key)


class LocalStore:
    """Directory-backed store. Metadata (content type, cache control) is not kept."""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def get(self, key: str) -> bytes | None:
        p = self.root / key
        return p.read_bytes() if p.is_file() else None

    def put(
        self, key: str, body: bytes, *, content_type: str, cache_control: str
    ) -> None:
        p = self.root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + ".tmp")
        tmp.write_bytes(body)
        tmp.replace(p)

    def list(self, prefix: str) -> Iterator[str]:
        base = self.root
        for p in sorted(base.rglob("*")):
            if p.is_file():
                key = p.relative_to(base).as_posix()
                if key.startswith(prefix):
                    yield key

    def delete(self, key: str) -> None:
        (self.root / key).unlink(missing_ok=True)


def open_store(spec: str) -> Store:
    if spec.startswith("s3://"):
        return S3Store(spec.removeprefix("s3://").strip("/"))
    return LocalStore(spec)
