"""Sensor deployments from Airtable (the canonical list of AQ-equipped stations)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import requests

from .config import AIRTABLE_TABLE_NAME, AIRTABLE_VIEW_NAME
from .http import session

log = logging.getLogger(__name__)

FIELDS = ["Deployment ID", "Registration ID", "Station ID", "Deployment Date"]


@dataclass(frozen=True)
class Deployment:
    deployment_id: str
    sensor_index: int
    station: str
    deployed: date


def fetch_deployments(
    token: str, base_id: str, http: requests.Session | None = None
) -> list[Deployment]:
    http = http or session()
    url = f"https://api.airtable.com/v0/{base_id}/{AIRTABLE_TABLE_NAME}"
    params: list[tuple[str, str]] = [
        ("view", AIRTABLE_VIEW_NAME),
        *[("fields[]", f) for f in FIELDS],
    ]
    out: list[Deployment] = []
    offset = None
    while True:
        page_params = params + ([("offset", offset)] if offset else [])
        r = http.get(
            url,
            params=page_params,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        r.raise_for_status()
        body = r.json()
        for rec in body.get("records", []):
            d = parse_record(rec.get("fields", {}))
            if d:
                out.append(d)
        offset = body.get("offset")
        if not offset:
            break
    log.info("airtable: %d deployment(s)", len(out))
    return out


def parse_record(f: dict) -> Deployment | None:
    station = (f.get("Station ID") or [None])[0]
    sensor = f.get("Registration ID")
    deployed = f.get("Deployment Date")
    if not (station and sensor and deployed):
        log.warning("airtable: skipping incomplete record %s", f.get("Deployment ID"))
        return None
    return Deployment(
        deployment_id=str(f.get("Deployment ID", "")),
        sensor_index=int(sensor),
        station=str(station),
        deployed=date.fromisoformat(str(deployed)[:10]),
    )


def sensor_on(
    deployments: list[Deployment], station: str, day: date
) -> Deployment | None:
    """The deployment active at `station` on `day`: the latest one deployed on or before it."""
    active = [d for d in deployments if d.station == station and d.deployed <= day]
    return max(active, key=lambda d: d.deployed) if active else None
