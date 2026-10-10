"""manifest.json, stations.*, latest/* -- the small, frequently-read index files."""

from __future__ import annotations

import logging
import math
import re
from datetime import UTC, datetime, timedelta
from importlib.resources import files

import pandas as pd
import pyarrow as pa

from . import layout
from .airtable import Deployment
from .archive import Archive
from .config import MESONET_STATIONS_API
from .http import session
from .schema import to_parquet_bytes

log = logging.getLogger(__name__)

SCHEMA_VERSION = 1
STALE_AFTER = timedelta(hours=3)

RAW_FILE = re.compile(
    r"raw/station=([^/]+)/year=(\d{4})/[^/]+_(\d{4})-(\d{2})\.parquet$"
)
_HOURLY = re.compile(r"hourly/station=([^/]+)/year=(\d{4})/[^/]+\.parquet$")


def fetch_mesonet_stations() -> dict[str, dict]:
    """{station: {name, lat, lon, elevation}} from the Mesonet API; {} if it is down."""
    try:
        r = session().get(MESONET_STATIONS_API, timeout=30)
        r.raise_for_status()
        return {
            s["station"]: {
                "name": s.get("name"),
                "lat": s.get("latitude"),
                "lon": s.get("longitude"),
                "elevation": s.get("elevation"),
            }
            for s in r.json()
            if s.get("station")
        }
    except Exception as e:  # metadata is decoration; never fail the run on it
        log.warning("mesonet station API unavailable: %s", e)
        return {}


def _clean(v):
    if v is None or (isinstance(v, float) and math.isnan(v)) or v is pd.NA:
        return None
    if isinstance(v, pd.Timestamp):
        return v.isoformat().replace("+00:00", "Z")
    if hasattr(v, "item"):
        return v.item()
    return v


def station_records(
    deployments: list[Deployment],
    mesonet: dict[str, dict],
    prior: dict[str, dict],
    file_stations: set[str],
) -> list[dict]:
    """One record per station: Airtable stations first, then any with data only."""
    by_station: dict[str, list[Deployment]] = {}
    for d in deployments:
        by_station.setdefault(d.station, []).append(d)
    out = []
    for sid in sorted(set(by_station) | file_stations | set(prior)):
        deps = sorted(by_station.get(sid, []), key=lambda d: d.deployed)
        meta = mesonet.get(sid) or {
            k: prior.get(sid, {}).get(k) for k in ("name", "lat", "lon", "elevation")
        }
        out.append(
            {
                "id": sid,
                "name": meta.get("name") or sid,
                "lat": meta.get("lat"),
                "lon": meta.get("lon"),
                "elevation": meta.get("elevation"),
                "sensor_index": deps[-1].sensor_index
                if deps
                else prior.get(sid, {}).get("sensor_index"),
                "deployments": [
                    {"sensor_index": d.sensor_index, "deployed": d.deployed.isoformat()}
                    for d in deps
                ]
                or prior.get(sid, {}).get("deployments", []),
                "fetched_through": prior.get(sid, {}).get("fetched_through"),
            }
        )
    return out


def file_index(files: dict[str, dict]) -> dict[str, list[dict]]:
    raw, hourly = [], []
    for path, info in sorted(files.items()):
        if m := RAW_FILE.search(path):
            raw.append(
                {
                    "station": m[1],
                    "year": int(m[3]),
                    "month": int(m[4]),
                    "path": path,
                    **info,
                }
            )
        elif m := _HOURLY.search(path):
            hourly.append({"station": m[1], "year": int(m[2]), "path": path, **info})
    return {"raw": raw, "hourly": hourly}


def latest_rows(arc: Archive, stations: list[dict], now: datetime) -> list[dict]:
    data_end = now.replace(hour=0, minute=0, second=0, microsecond=0)
    rows = []
    for s in stations:
        h = pd.DataFrame()
        for year in (now.year, now.year - 1):
            h = arc.read_hourly(s["id"], year)
            if not h.empty:
                break
        row = {"station": s["id"], "name": s["name"], "lat": s["lat"], "lon": s["lon"]}
        if h.empty:
            rows.append({**row, "time_stamp": None, "stale": True})
            continue
        last = h.iloc[-1]
        # Only an AQI from near the end of the record speaks for "latest".
        with_aqi = h[
            h["aqi"].notna() & (h["time_stamp"] >= last["time_stamp"] - STALE_AFTER)
        ]
        best = with_aqi.iloc[-1] if not with_aqi.empty else last
        row.update(
            {
                "time_stamp": _clean(last["time_stamp"]),
                # Data is published through the end of yesterday (UTC), so
                # "stale" is measured from there, not from the wall clock.
                "stale": bool(
                    data_end - last["time_stamp"].to_pydatetime() > STALE_AFTER
                ),
                "pm2.5_atm": _clean(last["pm2.5_atm"]),
                "pm2.5_epa": _clean(last["pm2.5_epa"]),
                "aqi_time_stamp": _clean(best["time_stamp"])
                if not with_aqi.empty
                else None,
                "pm2.5_nowcast": _clean(best["pm2.5_nowcast"]),
                "aqi": _clean(best["aqi"]),
                "aqi_category": _clean(best["aqi_category"]),
                "temperature": _clean(last["temperature"]),
                "humidity": _clean(last["humidity"]),
            }
        )
        rows.append(row)
    return rows


def publish_index(
    arc: Archive, stations: list[dict], public_base: str, now: datetime | None = None
) -> dict:
    """Write stations.*, latest/*, then manifest.json LAST (it is the commit point)."""
    now = now or datetime.now(UTC)
    readme = files("mesonet_aq").joinpath("PUBLIC_README.md").read_bytes()
    if arc.store.get(f"{arc.prefix}/{layout.README}") != readme:
        arc.put(layout.README, readme, layout.MARKDOWN, layout.CACHE_DOC)
    idx = file_index(arc.files)
    for s in stations:
        months = [f for f in idx["raw"] if f["station"] == s["id"]]
        s["data_start"] = (
            f"{months[0]['year']:04d}-{months[0]['month']:02d}" if months else None
        )

    arc.put(layout.STATIONS_PARQUET, to_parquet_bytes(pa.Table.from_pylist(
        [{k: v for k, v in s.items() if k != "deployments"} for s in stations]
    )), layout.PARQUET, layout.CACHE_DOC)  # fmt: skip
    arc.put_json(
        layout.STATIONS_GEOJSON,
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [s["lon"], s["lat"]]}
                    if s["lat"] is not None
                    else None,
                    "properties": {
                        k: v for k, v in s.items() if k not in ("lat", "lon")
                    },
                }
                for s in stations
            ],
        },
        layout.CACHE_DOC,
        content_type=layout.GEOJSON,
    )

    latest = latest_rows(arc, stations, now)
    stamp = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    arc.put_json(
        layout.LATEST_JSON,
        {"generated_at": stamp, "stations": latest},
        layout.CACHE_INDEX,
    )
    arc.put(
        layout.LATEST_PARQUET,
        to_parquet_bytes(pa.Table.from_pylist(latest)),
        layout.PARQUET,
        layout.CACHE_INDEX,
    )

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": stamp,
        "base_url": public_base,
        "stations": stations,
        "files": idx,
    }
    arc.put_json(layout.MANIFEST, manifest, layout.CACHE_INDEX)
    return manifest


def prior_files(manifest: dict | None) -> dict[str, dict]:
    if not manifest:
        return {}
    out = {}
    for kind in ("raw", "hourly"):
        for f in manifest.get("files", {}).get(kind, []):
            out[f["path"]] = {"rows": f.get("rows"), "bytes": f.get("bytes")}
    return out
