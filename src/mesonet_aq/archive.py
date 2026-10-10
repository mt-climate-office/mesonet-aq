"""Read and write the published archive through a Store, tracking what changed."""

from __future__ import annotations

import json
import logging
from datetime import date

import pandas as pd

from . import layout
from .derive import hourly
from .schema import (
    HOURLY_SCHEMA,
    RAW_SCHEMA,
    conform,
    read_parquet_bytes,
    to_parquet_bytes,
)
from .store import Store

log = logging.getLogger(__name__)


class Archive:
    def __init__(self, store: Store, prefix: str, today: date):
        self.store = store
        self.prefix = prefix.strip("/")
        self.today = today
        self.changed: list[str] = []  # keys relative to prefix, for CDN invalidation
        # path -> {"rows", "bytes"}; seeded from the prior manifest by the caller.
        self.files: dict[str, dict] = {}

    def _k(self, rel: str) -> str:
        return f"{self.prefix}/{rel}"

    # ── generic ────────────────────────────────────────────────────────────
    def get_json(self, rel: str) -> dict | None:
        body = self.store.get(self._k(rel))
        return json.loads(body) if body else None

    def put(self, rel: str, body: bytes, content_type: str, cache_control: str) -> None:
        self.store.put(
            self._k(rel), body, content_type=content_type, cache_control=cache_control
        )
        self.changed.append(rel)

    def put_json(
        self, rel: str, obj, cache_control: str, content_type: str = layout.JSON
    ) -> None:
        body = (
            json.dumps(obj, indent=None, separators=(",", ":"), default=str) + "\n"
        ).encode()
        self.put(rel, body, content_type, cache_control)

    def _put_table(self, rel: str, table, cache_control: str) -> None:
        body = to_parquet_bytes(table)
        self.put(rel, body, layout.PARQUET, cache_control)
        self.files[rel] = {"rows": table.num_rows, "bytes": len(body)}

    # ── raw station-months ───────────────────────────────────────────────
    def read_raw(self, station: str, year: int, month: int) -> pd.DataFrame:
        body = self.store.get(self._k(layout.raw_key(station, year, month)))
        return read_parquet_bytes(body) if body else pd.DataFrame()

    def write_raw(self, station: str, year: int, month: int, df: pd.DataFrame) -> None:
        df = (
            df.drop_duplicates(subset=["time_stamp"], keep="last")
            .sort_values("time_stamp")
            .reset_index(drop=True)
        )
        self._put_table(
            layout.raw_key(station, year, month),
            conform(df, RAW_SCHEMA),
            layout.raw_cache(year, month, self.today),
        )

    def merge_raw(self, station: str, new: pd.DataFrame) -> set[int]:
        """Upsert rows into their station-month files. Returns the years touched."""
        if new.empty:
            return set()
        years = set()
        ts = new["time_stamp"]
        for (y, m), chunk in new.groupby([ts.dt.year, ts.dt.month]):
            existing = self.read_raw(station, y, m)
            merged = (
                pd.concat([existing, chunk], ignore_index=True)
                if not existing.empty
                else chunk
            )
            self.write_raw(station, y, m, merged)
            years.add(int(y))
        return years

    # ── hourly station-years ─────────────────────────────────────────────
    def rebuild_hourly(self, station: str, year: int) -> pd.DataFrame:
        """Recompute one station-year from raw. December of the prior year is
        read too so NowCast is continuous across New Year."""
        periods = [(year - 1, 12)] + [
            (year, m)
            for m in range(1, 13)
            if (year, m) <= (self.today.year, self.today.month)
        ]
        frames = [
            f for f in (self.read_raw(station, y, m) for y, m in periods) if not f.empty
        ]
        if not frames:
            return pd.DataFrame()
        h = hourly(pd.concat(frames, ignore_index=True).sort_values("time_stamp"))
        h = h[h["time_stamp"].dt.year == year].reset_index(drop=True)
        if not h.empty:
            self._put_table(
                layout.hourly_key(station, year),
                conform(h, HOURLY_SCHEMA),
                layout.hourly_cache(year, self.today),
            )
        return h

    def read_hourly(self, station: str, year: int) -> pd.DataFrame:
        body = self.store.get(self._k(layout.hourly_key(station, year)))
        return read_parquet_bytes(body) if body else pd.DataFrame()
