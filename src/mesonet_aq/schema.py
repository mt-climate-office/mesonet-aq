"""Fixed Arrow schemas for the published Parquet.

The pre-2026 daily files let pandas infer types per file, so a sensor with a
dead B channel produced `null`-typed columns and broke multi-file reads. Every
file written now carries exactly these schemas.
"""

from __future__ import annotations

import io

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# PurpleAir history fields, in request order. `cf_1` PM2.5 was added in v1 of
# this layout because the EPA correction is defined on it; older rows have it
# null (see derive.epa_basis).
PURPLEAIR_FIELDS = [
    "humidity_a", "humidity_b", "temperature_a", "temperature_b",
    "pressure_a", "pressure_b", "voc_a", "voc_b",
    "pm1.0_atm_a", "pm1.0_atm_b", "pm2.5_atm_a", "pm2.5_atm_b",
    "pm10.0_atm_a", "pm10.0_atm_b",
    "pm2.5_cf_1_a", "pm2.5_cf_1_b",
    "scattering_coefficient_a", "scattering_coefficient_b",
    "deciviews_a", "deciviews_b", "visual_range_a", "visual_range_b",
    "0.3_um_count_a", "0.3_um_count_b", "0.5_um_count_a", "0.5_um_count_b",
    "1.0_um_count_a", "1.0_um_count_b", "2.5_um_count_a", "2.5_um_count_b",
    "5.0_um_count_a", "5.0_um_count_b", "10.0_um_count_a", "10.0_um_count_b",
]  # fmt: skip

RAW_SCHEMA = pa.schema(
    [
        pa.field("station", pa.string(), nullable=False),
        pa.field("sensor_index", pa.int32(), nullable=False),
        pa.field("time_stamp", pa.timestamp("ms", tz="UTC"), nullable=False),
        *[pa.field(f, pa.float32()) for f in PURPLEAIR_FIELDS],
    ]
)

HOURLY_SCHEMA = pa.schema(
    [
        pa.field("station", pa.string(), nullable=False),
        pa.field(
            "time_stamp", pa.timestamp("ms", tz="UTC"), nullable=False
        ),  # hour start
        pa.field("n_obs", pa.int16(), nullable=False),
        pa.field("completeness", pa.float32()),
        pa.field("pm2.5_atm_a", pa.float32()),
        pa.field("pm2.5_atm_b", pa.float32()),
        pa.field("pm2.5_atm", pa.float32()),
        pa.field("pm2.5_cf_1", pa.float32()),
        pa.field("pm1.0_atm", pa.float32()),
        pa.field("pm10.0_atm", pa.float32()),
        pa.field("humidity", pa.float32()),
        pa.field("temperature", pa.float32()),
        pa.field("pressure", pa.float32()),
        pa.field("ab_disagree", pa.bool_()),
        pa.field("pm2.5_epa", pa.float32()),
        pa.field("pm2.5_epa_basis", pa.string()),
        pa.field("pm2.5_nowcast", pa.float32()),
        pa.field("aqi", pa.int16()),
        pa.field("aqi_category", pa.string()),
    ]
)


def conform(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """Cast a frame to `schema`: add missing columns as null, drop extras."""
    cols = {}
    for field in schema:
        if field.name in df.columns:
            cols[field.name] = pa.array(
                df[field.name], type=field.type, from_pandas=True, safe=False
            )
        else:
            cols[field.name] = pa.nulls(len(df), type=field.type)
    return pa.table(cols, schema=schema)


def to_parquet_bytes(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    # Small row groups keep HTTP range reads cheap for browser readers.
    pq.write_table(table, buf, compression="zstd", row_group_size=50_000)
    return buf.getvalue()


def read_parquet_bytes(body: bytes) -> pd.DataFrame:
    return pq.read_table(io.BytesIO(body)).to_pandas()
