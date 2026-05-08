import os
import re
import json
import time
import requests
import pandas as pd
import boto3
import s3fs
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode, quote
from pathlib import Path

# PurpleAir configuration
API_KEY = os.getenv("PURPLEAIR_API_KEY")
FIELDS_TO_REQUEST = [
    "humidity_a", "humidity_b", "temperature_a", "temperature_b",
    "pressure_a", "pressure_b", "voc_a", "voc_b",
    "pm1.0_atm_a", "pm1.0_atm_b", "pm2.5_atm_a", "pm2.5_atm_b",
    "pm10.0_atm_a", "pm10.0_atm_b", "scattering_coefficient_a", "scattering_coefficient_b",
    "deciviews_a", "deciviews_b", "visual_range_a", "visual_range_b",
    "0.3_um_count_a", "0.3_um_count_b", "0.5_um_count_a", "0.5_um_count_b",
    "1.0_um_count_a", "1.0_um_count_b", "2.5_um_count_a", "2.5_um_count_b",
    "5.0_um_count_a", "5.0_um_count_b", "10.0_um_count_a", "10.0_um_count_b"
]

# AirTable configuration
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = "appM3B8YPnUdZ6z7D"
AIRTABLE_TABLE_NAME = "Deployments"
AIRTABLE_VIEW_NAME = "Public"

# S3 configuration
S3_BUCKET = "mco-mesonet"
S3_PREFIX = "air-quality"
S3_REGION = "us-west-2"
S3_BUCKET_BASE_URL = f"https://{S3_BUCKET}.s3.amazonaws.com/{S3_PREFIX}"

# Mesonet station-metadata API (used to attach lat/lon to the explorer manifest)
MESONET_API = "https://mesonet.climate.umt.edu/api/v2/stations/?type=json"

# Output manifest for the static web explorer (committed by the workflow)
MANIFEST_PATH = Path("docs/manifest.json")


def get_sensors_from_airtable():
    fields = ["Deployment ID", "Registration ID", "Station ID", "Deployment Date"]
    params = [
        ("view", AIRTABLE_VIEW_NAME),
        *[("fields[]", f) for f in fields]
    ]
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{quote(AIRTABLE_TABLE_NAME)}?{urlencode(params)}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

    response = requests.get(url, headers=headers)
    if not response.ok:
        print("Failed to fetch Airtable data:", response.text)
        raise Exception("Failed to fetch Airtable data")

    data = response.json()
    return [record.get("fields", {}) for record in data.get("records", [])]


def fetch_sensor_data(sensor, date):
    url = f"https://api.purpleair.com/v1/sensors/{sensor['Registration ID']}/history"

    start_ts = int(pd.to_datetime(date).timestamp())
    end_ts = start_ts + 86400

    params = {
        "start_timestamp": start_ts,
        "end_timestamp": end_ts,
        "average": 0,
        "fields": ",".join(FIELDS_TO_REQUEST)
    }
    headers = {"X-API-Key": API_KEY}

    response = requests.get(url, params=params, headers=headers)
    if not response.ok:
        print("Failed to fetch PurpleAir data:", response.text)
        raise Exception("Failed to fetch PurpleAir data")

    data = response.json()
    df = pd.DataFrame(data.get("data", []), columns=data.get("fields", {}))

    df["time_stamp"] = (
        pd.to_datetime(df["time_stamp"], unit="s")
          .dt.tz_localize("UTC")
    )

    df["station"] = sensor["Station ID"][0]
    df["sensor"] = sensor["Registration ID"]
    df["date"] = pd.to_datetime(date).date().isoformat()

    leading = ["station", "date", "sensor"]
    df = (
        df[leading + [col for col in df.columns if col not in leading]]
            .sort_values(by="time_stamp")
            .reset_index(drop=True)
    )
    return df


def upload_to_s3(df, s3_bucket, base_prefix, s3fs_filesystem, partition_cols=("station", "date")):
    if df.empty:
        print("DataFrame is empty — skipping upload.")
        return
    if not all(col in df.columns for col in partition_cols):
        raise ValueError(f"Missing partition columns: {partition_cols}")

    for keys, group in df.groupby(list(partition_cols)):
        partition_path = "/".join([f"{k}={v}" for k, v in zip(partition_cols, keys)])
        s3_partition_path = f"s3://{s3_bucket}/{base_prefix.rstrip('/')}/{partition_path}/"

        if s3fs_filesystem.exists(s3_partition_path):
            print(f"🔁 Cleaning {s3_partition_path}")
            s3fs_filesystem.rm(s3_partition_path, recursive=True)

        filename = f"{keys[0]}_{keys[1]}.parquet"
        s3_file_path = s3_partition_path + filename

        group.to_parquet(
            s3_file_path,
            engine="pyarrow",
            index=False,
            filesystem=s3fs_filesystem
        )
        print(f"✅ Wrote {s3_file_path}")


def load_parquet_manifest(s3_client, bucket: str, prefix: str) -> set[tuple[str, str]]:
    """List existing parquet partitions using signed S3 calls."""
    paginator = s3_client.get_paginator("list_objects_v2")
    pat = re.compile(r"station=([^/]+)/date=(\d{4}-\d{2}-\d{2})/")
    partitions = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            m = pat.search(key)
            if m:
                partitions.add((m.group(1), m.group(2)))
    return partitions


def fetch_mesonet_metadata() -> dict:
    """Fetch lat/lon/name for all Mesonet stations. Returns {} on failure."""
    try:
        r = requests.get(MESONET_API, timeout=20)
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
    except Exception as e:
        print(f"⚠️  Mesonet metadata API unavailable: {e}")
        return {}


def write_manifest(partitions, sensors, mesonet_meta, path: Path):
    """Write docs/manifest.json — input for the static web explorer."""
    dates_by_station: dict[str, set[str]] = {}
    for station, date in partitions:
        dates_by_station.setdefault(station, set()).add(date)

    stations = []
    seen = set()

    # Airtable is the canonical source of AQ-equipped stations; emit those first
    # so stations without data yet still appear in the explorer.
    for s in sensors:
        sid_field = s.get("Station ID") or []
        sid = sid_field[0] if sid_field else None
        if not sid or sid in seen:
            continue
        seen.add(sid)
        meta = mesonet_meta.get(sid, {})
        stations.append({
            "id": sid,
            "name": meta.get("name") or sid,
            "lat": meta.get("lat"),
            "lon": meta.get("lon"),
            "elevation": meta.get("elevation"),
            "deployment_date": s.get("Deployment Date"),
            "dates": sorted(dates_by_station.get(sid, [])),
        })

    # Include any S3 partitions we have data for that aren't in Airtable.
    for sid in sorted(set(dates_by_station) - seen):
        meta = mesonet_meta.get(sid, {})
        stations.append({
            "id": sid,
            "name": meta.get("name") or sid,
            "lat": meta.get("lat"),
            "lon": meta.get("lon"),
            "elevation": meta.get("elevation"),
            "deployment_date": None,
            "dates": sorted(dates_by_station.get(sid, [])),
        })

    stations.sort(key=lambda x: x["id"])

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "bucket_base": S3_BUCKET_BASE_URL,
        "stations": stations,
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"📝 Wrote {path} with {len(stations)} station(s)")


def main():
    if not API_KEY:
        raise EnvironmentError("PURPLEAIR_API_KEY environment variable not set.")

    today = datetime.now(timezone.utc).date()

    sensors = get_sensors_from_airtable()

    s3_client = boto3.client("s3", region_name=S3_REGION)
    s3fs_filesystem = s3fs.S3FileSystem(client_kwargs={"region_name": S3_REGION})

    existing_partitions = load_parquet_manifest(s3_client, S3_BUCKET, S3_PREFIX)

    for sensor in sensors:
        station = sensor["Station ID"][0]
        deployment_date = pd.to_datetime(sensor["Deployment Date"]).date()
        yesterday = today - timedelta(days=1)

        for single_date in pd.date_range(deployment_date, yesterday):
            target_date = single_date.date().isoformat()
            partition_key = (station, target_date)

            if partition_key in existing_partitions:
                print(f"⏩ Skipping {partition_key} — already uploaded.")
                continue

            print(f"\n📥 Fetching data for sensor {sensor['Deployment ID']} on {target_date}...")
            data = fetch_sensor_data(sensor=sensor, date=target_date)
            upload_to_s3(
                data,
                s3_bucket=S3_BUCKET,
                base_prefix=S3_PREFIX,
                s3fs_filesystem=s3fs_filesystem,
            )
            existing_partitions.add(partition_key)
            time.sleep(1)

    mesonet_meta = fetch_mesonet_metadata()
    write_manifest(existing_partitions, sensors, mesonet_meta, MANIFEST_PATH)


if __name__ == "__main__":
    main()
