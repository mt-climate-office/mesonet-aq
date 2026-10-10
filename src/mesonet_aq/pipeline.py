"""The nightly run and the derived-product rebuild."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import pandas as pd

from . import cdn, layout
from .airtable import Deployment, fetch_deployments, sensor_on
from .archive import Archive
from .config import Settings
from .http import session
from .meta import (
    RAW_FILE,
    fetch_mesonet_stations,
    prior_files,
    publish_index,
    station_records,
)
from .purpleair import fetch_day
from .store import Store, open_store

log = logging.getLogger(__name__)

# Re-fetch the most recent day once more: the run starts ~7.5 h after the
# UTC day closes, and a sensor that was briefly offline can fill in late.
TRAILING_DAYS = 1
# Courtesy pause between PurpleAir calls.
REQUEST_PAUSE_S = 1.0

_RAW_KEY = re.compile(r"raw/station=([^/]+)/year=(\d{4})/")


@dataclass
class RunResult:
    fetched_days: int = 0
    rows: int = 0
    errors: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)


def _days(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def plan_start(
    deps: list[Deployment], fetched_through: str | None, since: date | None
) -> date:
    first = min(d.deployed for d in deps)
    if since:
        return max(since, first)
    if fetched_through:
        return max(
            date.fromisoformat(fetched_through) - timedelta(days=TRAILING_DAYS - 1),
            first,
        )
    return first


def run(
    settings: Settings,
    *,
    since: date | None = None,
    stations: list[str] | None = None,
    today: date | None = None,
    store: Store | None = None,
) -> RunResult:
    if not (
        settings.purpleair_api_key
        and settings.airtable_token
        and settings.airtable_base_id
    ):
        raise OSError(
            "PURPLEAIR_API_KEY, AIRTABLE_TOKEN and AIRTABLE_BASE_ID must be set"
        )
    today = today or datetime.now(UTC).date()
    yesterday = today - timedelta(days=1)
    arc = Archive(store or open_store(settings.store), settings.prefix, today)
    prior = arc.get_json(layout.MANIFEST)
    if (
        prior is None
        and since is None
        and next(arc.store.list(f"{arc.prefix}/station="), None)
    ):
        # Without a manifest every station would restart at its deployment
        # date and re-download the whole history. The legacy tree means the
        # migration has not run yet.
        raise RuntimeError(
            "legacy station=/date= files present but no manifest.json: run `mesonet-aq migrate` first"
        )
    arc.files = prior_files(prior)
    prior_stations = {s["id"]: s for s in (prior or {}).get("stations", [])}

    http = session()
    deployments = fetch_deployments(
        settings.airtable_token, settings.airtable_base_id, http
    )
    by_station: dict[str, list[Deployment]] = {}
    for d in deployments:
        by_station.setdefault(d.station, []).append(d)

    result = RunResult()
    fetched_through: dict[str, str | None] = {
        sid: s.get("fetched_through") for sid, s in prior_stations.items()
    }
    for sid, deps in sorted(by_station.items()):
        if stations and sid not in stations:
            continue
        start = plan_start(deps, fetched_through.get(sid), since)
        frames, last_ok = [], None
        for day in _days(start, yesterday):
            dep = sensor_on(deps, sid, day)
            if dep is None:
                continue
            try:
                df = fetch_day(
                    settings.purpleair_api_key, dep.sensor_index, sid, day, http
                )
            except Exception as e:
                # Stop this station here so fetched_through never skips a day;
                # the next run resumes from the gap.
                log.error("%s %s: %s", sid, day, e)
                result.errors.append(f"{sid} {day}: {e}")
                break
            if not df.empty:
                frames.append(df)
            last_ok = day
            result.fetched_days += 1
            time.sleep(REQUEST_PAUSE_S)
        if frames:
            new = pd.concat(frames, ignore_index=True)
            result.rows += len(new)
            for year in sorted(arc.merge_raw(sid, new)):
                arc.rebuild_hourly(sid, year)
        if last_ok and (fetched_through.get(sid) or "") < last_ok.isoformat():
            fetched_through[sid] = last_ok.isoformat()
        log.info(
            "%s: fetched %s..%s, %d rows",
            sid,
            start,
            last_ok,
            sum(len(f) for f in frames),
        )

    _publish(arc, settings, deployments, prior_stations, fetched_through)
    result.changed = arc.changed
    return result


def rebuild_derived(
    settings: Settings, *, today: date | None = None, store: Store | None = None
) -> RunResult:
    """Recompute every hourly file and the index from the raw tree (no API calls).

    Uses a prefix-scoped listing: the source of truth here is what is in S3,
    not what the manifest believes.
    """
    today = today or datetime.now(UTC).date()
    arc = Archive(store or open_store(settings.store), settings.prefix, today)
    prior = arc.get_json(layout.MANIFEST) or {}
    prior_stations = {s["id"]: s for s in prior.get("stations", [])}
    arc.files = {}
    pairs: set[tuple[str, int]] = set()
    for key in arc.store.list(f"{arc.prefix}/raw/"):
        rel = key.removeprefix(f"{arc.prefix}/")
        if m := RAW_FILE.search(rel):
            sid, year, month = m[1], int(m[3]), int(m[4])
            pairs.add((sid, year))
            arc.files[rel] = {
                "rows": len(arc.read_raw(sid, year, month)),
                "bytes": None,
            }
    for sid, year in sorted(pairs):
        arc.rebuild_hourly(sid, year)
    deployments = (
        fetch_deployments(settings.airtable_token, settings.airtable_base_id)
        if settings.airtable_token
        else []
    )
    fetched = {sid: s.get("fetched_through") for sid, s in prior_stations.items()}
    _publish(arc, settings, deployments, prior_stations, fetched)
    return RunResult(changed=arc.changed)


def _publish(
    arc: Archive, settings: Settings, deployments, prior_stations, fetched_through
) -> None:
    file_stations = {m[1] for p in arc.files if (m := _RAW_KEY.match(p))}
    stations = station_records(
        deployments, fetch_mesonet_stations(), prior_stations, file_stations
    )
    for s in stations:
        s["fetched_through"] = fetched_through.get(s["id"])
    publish_index(arc, stations, settings.public_base)
    if settings.distribution_id:
        # Short-TTL files (open periods, indexes) expire on their own; only
        # long-TTL objects that changed need flushing.
        long_lived = [k for k in arc.changed if _is_long_lived(k, arc.today)]
        cdn.invalidate(
            settings.distribution_id,
            cdn.invalidation_paths(settings.prefix, long_lived),
        )


def _is_long_lived(rel: str, today: date) -> bool:
    if rel in (layout.STATIONS_PARQUET, layout.STATIONS_GEOJSON, layout.README):
        return True
    if m := re.match(
        r"raw/station=[^/]+/year=(\d{4})/.+_(\d{4})-(\d{2})\.parquet$", rel
    ):
        return layout.raw_cache(int(m[2]), int(m[3]), today) == layout.CACHE_CLOSED
    if m := re.match(r"hourly/station=[^/]+/year=(\d{4})/", rel):
        return layout.hourly_cache(int(m[1]), today) == layout.CACHE_CLOSED
    return False
