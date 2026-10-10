"""Object keys and their cache policy -- the published contract.

    <prefix>/README.md
    <prefix>/manifest.json
    <prefix>/stations.parquet | stations.geojson
    <prefix>/latest/latest.json | latest.parquet
    <prefix>/raw/station=<id>/year=YYYY/<id>_YYYY-MM.parquet
    <prefix>/hourly/station=<id>/year=YYYY/<id>_YYYY.parquet

Keys carry a literal `=` so DuckDB and Arrow detect Hive partitions. HTTPS has
no listing, so readers enumerate files from manifest.json.
"""

from __future__ import annotations

import re
from datetime import date

# Closed periods change only on a re-fetch, which invalidates them; a day of
# browser/CDN cache is plenty. Open periods change nightly.
CACHE_CLOSED = "public, max-age=86400"
CACHE_OPEN = "public, max-age=300"
CACHE_INDEX = "public, max-age=300"
CACHE_DOC = "public, max-age=3600"

PARQUET = "application/vnd.apache.parquet"
JSON = "application/json"
GEOJSON = "application/geo+json"
MARKDOWN = "text/markdown; charset=utf-8"

MANIFEST = "manifest.json"
STATIONS_PARQUET = "stations.parquet"
STATIONS_GEOJSON = "stations.geojson"
LATEST_JSON = "latest/latest.json"
LATEST_PARQUET = "latest/latest.parquet"
README = "README.md"

# Pre-2026 layout, read only by `migrate`.
LEGACY_DAILY = re.compile(r"station=([^/]+)/date=(\d{4}-\d{2}-\d{2})/[^/]+\.parquet$")


def raw_key(station: str, year: int, month: int) -> str:
    return f"raw/station={station}/year={year:04d}/{station}_{year:04d}-{month:02d}.parquet"


def hourly_key(station: str, year: int) -> str:
    return f"hourly/station={station}/year={year:04d}/{station}_{year:04d}.parquet"


def raw_cache(year: int, month: int, today: date) -> str:
    return CACHE_OPEN if (year, month) >= (today.year, today.month) else CACHE_CLOSED


def hourly_cache(year: int, today: date) -> str:
    return CACHE_OPEN if year >= today.year else CACHE_CLOSED
