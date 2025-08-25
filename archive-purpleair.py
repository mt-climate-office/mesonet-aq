import os
import requests
import time
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode, quote
import boto3
import io
import s3fs
import hashlib
import subprocess
import botocore
    
# PurpleAir configuration
API_KEY = os.getenv("PURPLEAIR_API_KEY")  # Must be set in your environment
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
AIRTABLE_BASE_ID = 'appM3B8YPnUdZ6z7D'
AIRTABLE_TABLE_NAME = 'Deployments'
AIRTABLE_VIEW_NAME = 'Public'



def get_sensors_from_airtable():
    
    fields = ["Deployment ID", "Registration ID", "Station ID", "Deployment Date"]
    params = [
        ("view", AIRTABLE_VIEW_NAME),
        *[( "fields[]", f ) for f in fields]
    ]

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{quote(AIRTABLE_TABLE_NAME)}?{urlencode(params)}"

    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}"
    }

    response = requests.get(url, headers=headers)
    if not response.ok:
        print("Failed to fetch Airtable data:", response.text)
        raise Exception("Failed to fetch Airtable data")

    data = response.json()
    
    return [
        record.get("fields", {})
        for record in data.get("records", [])
    ]

def fetch_sensor_data(sensor, date):
    url = f"https://api.purpleair.com/v1/sensors/{sensor["Registration ID"]}/history"
    
    start_ts = int(pd.to_datetime(date).timestamp())
    end_ts = start_ts + 86400
    
    params = {
        "start_timestamp": start_ts,
        "end_timestamp": end_ts,
        "average": 0,
        "fields": ",".join(FIELDS_TO_REQUEST)
    }
    headers = {
        "X-API-Key": API_KEY
    }
    
    response = requests.get(url, params=params, headers=headers)
    
    if not response.ok:
        print("Failed to fetch Airtable data:", response.text)
        raise Exception("Failed to fetch Airtable data")

    data = response.json()
    df = pd.DataFrame(data.get("data", []), columns=data.get("fields", {}))
    
    df["time_stamp"] = (
            pd
        .to_datetime(df["time_stamp"], unit="s")
        .dt.tz_localize("UTC")
        #.dt.tz_convert("America/Denver")
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

def boto3_session_with_sso(profile_name):
    session = boto3.Session(profile_name=profile_name)
    sts = session.client("sts")

    try:
        sts.get_caller_identity()
        return session
    except (botocore.exceptions.UnauthorizedSSOTokenError,
            botocore.exceptions.SSOTokenLoadError):
        print(f"🔐 SSO session missing or expired for profile '{profile_name}' — attempting login...")
        result = subprocess.run(
            ["aws", "sso", "login", "--profile", profile_name],
            capture_output=False
        )

        if result.returncode != 0:
            raise RuntimeError(f"❌ SSO login failed for profile '{profile_name}'.")

        # Recreate session after login
        return boto3.Session(profile_name=profile_name)
    
def boto3_session_with_github_actions():
    sts = boto3.client("s3")

    sts.get_caller_identity()
    return session

def upload_to_s3(df, s3_bucket, base_prefix, s3fs_filesystem, partition_cols=["station", "date"]):
    if df.empty:
        print("DataFrame is empty — skipping upload.")
        return
    if not all(col in df.columns for col in partition_cols):
        raise ValueError(f"Missing partition columns: {partition_cols}")

    for keys, group in df.groupby(partition_cols):
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

def load_parquet_manifest(bucket: str, prefix: str) -> set[tuple[str, str]]:
    import requests
    import re
    from xml.etree import ElementTree

    def get_namespace(element):
        m = re.match(r"\{(.*)\}", element.tag)
        return m.group(1) if m else ""

    url = f"https://{bucket}.s3.amazonaws.com"
    params = {"list-type": "2", "prefix": prefix}
    partitions = set()

    while True:
        r = requests.get(url, params=params)
        r.raise_for_status()
        root = ElementTree.fromstring(r.content)

        ns_uri = get_namespace(root)
        ns = {"ns": ns_uri} if ns_uri else {}

        for contents in root.findall(".//ns:Contents", namespaces=ns):
            key = contents.find("ns:Key", namespaces=ns).text
            #print("🔍", key)
            if key.endswith(".parquet"):
                match = re.search(r"station=(.+?)/date=(\d{4}-\d{2}-\d{2})/", key)
                if match:
                    partitions.add((match.group(1), match.group(2)))

        token = root.find(".//ns:NextContinuationToken", namespaces=ns)
        if token is not None:
            params["continuation-token"] = token.text
        else:
            break

    return partitions
    
def main():
    if not API_KEY:
        raise EnvironmentError("PURPLEAIR_API_KEY environment variable not set.")
    
    today = datetime.now(timezone.utc).date()
    
    sensors = get_sensors_from_airtable()
    
    existing_partitions = load_parquet_manifest(bucket = "mco-mesonet", prefix = "air-quality")

    #session = boto3_session_with_sso("umt-sso")
    session = boto3_session_with_github_actions()
    
    creds = session.get_credentials().get_frozen_credentials()
    s3fs_filesystem = s3fs.S3FileSystem(
        key=creds.access_key,
        secret=creds.secret_key,
        token=creds.token
    )
    
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
                s3_bucket="mco-mesonet",
                base_prefix="air-quality",
                s3fs_filesystem=s3fs_filesystem
            )

            time.sleep(1)

if __name__ == "__main__":
    main()
