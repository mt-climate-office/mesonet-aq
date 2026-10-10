"""PurpleAir history API: one UTC day of real-time (~2 min) readings per call."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta

import pandas as pd
import requests

from .http import session
from .schema import PURPLEAIR_FIELDS

log = logging.getLogger(__name__)

API = "https://api.purpleair.com/v1/sensors/{sensor_index}/history"


def fetch_day(
    api_key: str,
    sensor_index: int,
    station: str,
    day: date,
    http: requests.Session | None = None,
) -> pd.DataFrame:
    """Rows for [day 00:00 UTC, day+1 00:00 UTC). Empty frame if the sensor was silent."""
    http = http or session()
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    end = start + timedelta(days=1)
    r = http.get(
        API.format(sensor_index=sensor_index),
        params={
            "start_timestamp": int(start.timestamp()),
            "end_timestamp": int(end.timestamp()),
            "average": 0,
            "fields": ",".join(PURPLEAIR_FIELDS),
        },
        headers={"X-API-Key": api_key},
        timeout=60,
    )
    if not r.ok:
        raise RuntimeError(
            f"PurpleAir {r.status_code} for sensor {sensor_index} {day}: {r.text[:300]}"
        )
    return to_frame(
        r.json(), station=station, sensor_index=sensor_index, start=start, end=end
    )


def to_frame(
    body: dict, *, station: str, sensor_index: int, start: datetime, end: datetime
) -> pd.DataFrame:
    df = pd.DataFrame(body.get("data", []), columns=body.get("fields", []))
    if df.empty:
        return df
    df["time_stamp"] = pd.to_datetime(df["time_stamp"], unit="s", utc=True)
    # The API's end bound is inclusive; keep each reading in exactly one day.
    df = df[(df["time_stamp"] >= start) & (df["time_stamp"] < end)].copy()
    df["station"] = station
    df["sensor_index"] = sensor_index
    return df.sort_values("time_stamp").reset_index(drop=True)
