"""One-shot migration: legacy station-day files -> station-month layout.

Reads `<prefix>/station=<id>/date=<YYYY-MM-DD>/<id>_<date>.parquet` (left in
place, read-only), writes raw/ and hourly/ plus the index, then verifies every
station-day against its source and writes `_migration/report-<date>.json`.
Idempotent: re-running rewrites the same outputs. No PurpleAir calls.
"""

from __future__ import annotations

import io
import json
import logging
from collections import defaultdict
from datetime import UTC, date, datetime

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from . import layout
from .airtable import fetch_deployments
from .archive import Archive
from .config import Settings
from .meta import prior_files
from .pipeline import _publish
from .store import Store, open_store

log = logging.getLogger(__name__)

CHECK_COLS = ["pm2.5_atm_a", "pm2.5_atm_b", "humidity_a", "temperature_a"]


def read_legacy(body: bytes) -> pd.DataFrame:
    df = pq.read_table(io.BytesIO(body)).to_pandas()
    df = df.rename(columns={"sensor": "sensor_index"}).drop(
        columns=["date"], errors="ignore"
    )
    df["time_stamp"] = pd.to_datetime(df["time_stamp"], utc=True).dt.floor("s")
    return df


def day_stats(df: pd.DataFrame) -> dict[str, dict]:
    """Per-UTC-day fingerprint: row count, time bounds, and float32 column sums."""
    out = {}
    if df.empty:
        return out
    for day, g in df.groupby(df["time_stamp"].dt.strftime("%Y-%m-%d")):
        s = {
            "rows": len(g),
            "min": g["time_stamp"].min().isoformat(),
            "max": g["time_stamp"].max().isoformat(),
        }
        for c in CHECK_COLS:
            # A column absent from a legacy file is all-null after conform(): sum 0.
            s[c] = 0.0
            if c in g:
                vals = pd.to_numeric(g[c], errors="coerce").to_numpy(dtype="float32")
                s[c] = float(np.nansum(vals, dtype="float64"))
        out[day] = s
    return out


def _same(a: dict, b: dict) -> bool:
    if a.keys() != b.keys():
        return False
    for k in a:
        if isinstance(a[k], float):
            if not np.isclose(a[k], b[k], rtol=1e-6, atol=1e-3):
                return False
        elif a[k] != b[k]:
            return False
    return True


def migrate(
    settings: Settings,
    *,
    stations: list[str] | None = None,
    today: date | None = None,
    store: Store | None = None,
) -> dict:
    today = today or datetime.now(UTC).date()
    arc = Archive(store or open_store(settings.store), settings.prefix, today)

    groups: dict[tuple[str, int, int], list[str]] = defaultdict(list)
    last_day: dict[str, str] = {}
    for key in arc.store.list(f"{arc.prefix}/station="):
        m = layout.LEGACY_DAILY.search(key)
        if not m or (stations and m[1] not in stations):
            continue
        sid, d = m[1], m[2]
        groups[(sid, int(d[:4]), int(d[5:7]))].append(key)
        last_day[sid] = max(last_day.get(sid, ""), d)
    log.info(
        "migrate: %d legacy file(s) in %d station-month(s)",
        sum(map(len, groups.values())),
        len(groups),
    )

    report = {"started_at": datetime.now(UTC).isoformat(), "station_months": len(groups), "days": 0,
              "duplicates_dropped": 0, "mismatches": [], "empty_files": []}  # fmt: skip
    years: set[tuple[str, int]] = set()
    for (sid, y, mo), keys in sorted(groups.items()):
        frames = []
        for k in sorted(keys):
            df = read_legacy(arc.store.get(k))
            if df.empty:
                report["empty_files"].append(k)
            frames.append(df)
        src = pd.concat(frames, ignore_index=True)
        before = len(src)
        src = src.drop_duplicates(subset=["time_stamp"], keep="last")
        report["duplicates_dropped"] += before - len(src)
        arc.write_raw(sid, y, mo, src)
        years.add((sid, y))

        # Verify by reading back what was actually written.
        expected = day_stats(src)
        got = day_stats(arc.read_raw(sid, y, mo))
        report["days"] += len(expected)
        for day in sorted(set(expected) | set(got)):
            if not _same(expected.get(day, {}), got.get(day, {})):
                report["mismatches"].append(
                    {
                        "station": sid,
                        "day": day,
                        "expected": expected.get(day),
                        "got": got.get(day),
                    }
                )

    for sid, y in sorted(years):
        arc.rebuild_hourly(sid, y)

    prior = arc.get_json(layout.MANIFEST) or {}
    prior_stations = {s["id"]: s for s in prior.get("stations", [])}
    fetched = {sid: s.get("fetched_through") for sid, s in prior_stations.items()}
    for sid, d in last_day.items():
        fetched[sid] = max(fetched.get(sid) or "", d)
    arc.files.update(
        {k: v for k, v in prior_files(prior).items() if k not in arc.files}
    )

    deployments = (
        fetch_deployments(settings.airtable_token, settings.airtable_base_id)
        if settings.airtable_token
        else []
    )
    _publish(arc, settings, deployments, prior_stations, fetched)

    report["finished_at"] = datetime.now(UTC).isoformat()
    arc.put(
        f"_migration/report-{today.isoformat()}.json",
        json.dumps(report, indent=2).encode(),
        layout.JSON,
        "no-store",
    )
    log.info(
        "migrate: %d day(s) verified, %d mismatch(es)",
        report["days"],
        len(report["mismatches"]),
    )
    return report
