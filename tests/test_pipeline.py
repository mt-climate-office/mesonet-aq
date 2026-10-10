import io
import json
from datetime import date

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mesonet_aq import cdn, meta, migrate, pipeline
from mesonet_aq.layout import hourly_key, raw_key
from mesonet_aq.schema import RAW_SCHEMA


def fake_day(api_key, sensor_index, station, day, http=None):
    ts = pd.date_range(pd.Timestamp(day, tz="UTC"), periods=720, freq="2min")
    return pd.DataFrame(
        {
            "time_stamp": ts,
            "station": station,
            "sensor_index": sensor_index,
            "pm2.5_atm_a": 8.0,
            "pm2.5_atm_b": 8.5,
            "pm2.5_cf_1_a": 8.0,
            "pm2.5_cf_1_b": 8.5,
            "humidity_a": 30,
        }
    )


def manifest(store):
    return json.loads(store.get("air-quality/manifest.json"))


def test_run_spans_months_and_resumes(settings, store, monkeypatch):
    calls = []
    monkeypatch.setattr(
        pipeline, "fetch_day", lambda *a, **k: calls.append(a[3]) or fake_day(*a, **k)
    )

    res = pipeline.run(settings, today=date(2026, 10, 3), store=store)
    assert not res.errors
    assert calls[0] == date(2026, 9, 28) and calls[-1] == date(2026, 10, 2)
    sept = pq.read_table(
        io.BytesIO(store.get("air-quality/" + raw_key("teststa", 2026, 9)))
    )
    assert sept.schema.equals(RAW_SCHEMA)
    assert sept.num_rows == 3 * 720
    assert store.get("air-quality/" + hourly_key("teststa", 2026))

    m = manifest(store)
    assert m["stations"][0]["fetched_through"] == "2026-10-02"
    assert m["stations"][0]["name"] == "Test Station"
    assert {f["month"] for f in m["files"]["raw"]} == {9, 10}

    # Next night: re-fetch the trailing day, add the new one, no duplicates.
    calls.clear()
    pipeline.run(settings, today=date(2026, 10, 4), store=store)
    assert calls == [date(2026, 10, 2), date(2026, 10, 3)]
    octo = pq.read_table(
        io.BytesIO(store.get("air-quality/" + raw_key("teststa", 2026, 10)))
    )
    assert octo.num_rows == 3 * 720  # Oct 1-3, the re-fetched Oct 2 deduplicated

    latest = json.loads(store.get("air-quality/latest/latest.json"))
    assert latest["stations"][0]["aqi_category"] == "Good"
    assert isinstance(latest["stations"][0]["aqi"], int)


def test_latest_json_is_strict_json_when_values_are_missing(
    settings, store, monkeypatch
):
    # No humidity means no EPA correction: float32 NaN in the hourly table,
    # which must publish as null, never as bare NaN (browsers reject the file).
    def no_humidity(*a, **k):
        return fake_day(*a, **k).drop(columns="humidity_a")

    monkeypatch.setattr(pipeline, "fetch_day", no_humidity)
    pipeline.run(settings, today=date(2026, 10, 3), store=store)

    def reject(token):
        raise ValueError(f"non-JSON constant {token}")

    body = store.get("air-quality/latest/latest.json").decode()
    row = json.loads(body, parse_constant=reject)["stations"][0]
    assert row["pm2.5_epa"] is None and row["humidity"] is None


def test_clean_maps_numpy_nan_to_none():
    assert meta._clean(np.float32("nan")) is None
    assert meta._clean(np.float64("nan")) is None
    assert meta._clean(pd.NaT) is None
    assert meta._clean(np.float32(1.5)) == 1.5


def test_run_stops_station_at_first_failure(settings, store, monkeypatch):
    def flaky(api_key, sensor_index, station, day, http=None):
        if day == date(2026, 9, 30):
            raise RuntimeError("boom")
        return fake_day(api_key, sensor_index, station, day)

    monkeypatch.setattr(pipeline, "fetch_day", flaky)
    res = pipeline.run(settings, today=date(2026, 10, 3), store=store)
    assert res.errors
    assert manifest(store)["stations"][0]["fetched_through"] == "2026-09-29"


def _legacy(store, sid, day, rows=5, null_b=True):
    ts = pd.date_range(pd.Timestamp(day, tz="UTC"), periods=rows, freq="2min").astype(
        "datetime64[ms, UTC]"
    )
    cols = {
        "station": pa.array([sid] * rows, pa.large_string()),
        "date": pa.array([day] * rows, pa.large_string()),
        "sensor": pa.array([111] * rows, pa.int64()),
        "time_stamp": pa.array(ts),
        "humidity_a": pa.array([20] * rows, pa.int64()),
        "humidity_b": pa.nulls(rows) if null_b else pa.array([21] * rows, pa.int64()),
        "pm2.5_atm_a": pa.array([5.3 + i for i in range(rows)], pa.float64()),
        "pm2.5_atm_b": pa.array([4.2] * rows, pa.float64()),
    }
    buf = io.BytesIO()
    pq.write_table(pa.table(cols), buf)
    store.put(
        f"air-quality/station={sid}/date={day}/{sid}_{day}.parquet",
        buf.getvalue(),
        content_type="",
        cache_control="",
    )


def test_migrate_verifies_and_sets_resume_point(settings, store):
    for day in ("2025-08-30", "2025-08-31", "2025-09-01"):
        _legacy(store, "teststa", day, null_b=(day != "2025-08-31"))
    report = migrate.migrate(settings, today=date(2026, 10, 10), store=store)

    assert report["mismatches"] == []
    assert report["days"] == 3
    aug = pq.read_table(
        io.BytesIO(store.get("air-quality/" + raw_key("teststa", 2025, 8)))
    )
    assert aug.num_rows == 10 and aug.schema.equals(RAW_SCHEMA)
    m = manifest(store)
    assert m["stations"][0]["fetched_through"] == "2025-09-01"
    assert store.get("air-quality/_migration/report-2026-10-10.json")
    # Legacy files are left in place.
    assert store.get(
        "air-quality/station=teststa/date=2025-08-30/teststa_2025-08-30.parquet"
    )

    # Idempotent.
    assert (
        migrate.migrate(settings, today=date(2026, 10, 10), store=store)["mismatches"]
        == []
    )


def test_run_refuses_before_migration(settings, store, monkeypatch):
    _legacy(store, "teststa", "2025-08-30")
    monkeypatch.setattr(pipeline, "fetch_day", fake_day)
    with pytest.raises(RuntimeError, match="migrate"):
        pipeline.run(settings, today=date(2026, 10, 3), store=store)


def test_invalidation_paths_are_cache_keys():
    assert cdn.invalidation_paths("air-quality", ["stations.parquet"]) == [
        "/air-quality/stations.parquet"
    ]
    assert cdn.invalidation_paths("air-quality", [f"k{i}" for i in range(40)]) == [
        "/air-quality/*"
    ]
    assert cdn.invalidation_paths("air-quality", []) == []


def test_long_lived_classification():
    today = date(2026, 10, 10)
    assert pipeline._is_long_lived(raw_key("x", 2026, 9), today)
    assert not pipeline._is_long_lived(raw_key("x", 2026, 10), today)
    assert not pipeline._is_long_lived(hourly_key("x", 2026), today)
    assert pipeline._is_long_lived(hourly_key("x", 2025), today)
    assert not pipeline._is_long_lived("manifest.json", today)
