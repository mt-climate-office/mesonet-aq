# Montana Mesonet air quality (PurpleAir) archive

PurpleAir PA-II sensors deployed at Montana Mesonet stations, archived nightly by the
Montana Climate Office. Source code: <https://github.com/mt-climate-office/mesonet-aq>.
Explorer: <https://mesonet.climate.umt.edu/air/>.

Base URL: `https://data2.climate.umt.edu/mesonet/air-quality/`

HTTPS has no directory listing. Start from `manifest.json`, which lists every
station and file.

## Files

| Path | Contents | Refreshed |
|---|---|---|
| `manifest.json` | schema version, stations, every raw/hourly file with row counts | nightly |
| `stations.parquet`, `stations.geojson` | station id, name, location, PurpleAir sensor index, deployments | nightly |
| `latest/latest.json`, `latest/latest.parquet` | most recent hour + NowCast AQI per station | nightly |
| `raw/station=<id>/year=YYYY/<id>_YYYY-MM.parquet` | ~2-minute A/B channel readings, one file per station-month | nightly (current month) |
| `hourly/station=<id>/year=YYYY/<id>_YYYY.parquet` | hourly means, EPA-corrected PM2.5, NowCast, AQI, one file per station-year | nightly (current year) |

All times are UTC. Partitions follow the UTC day.

## Raw columns

`station`, `sensor_index`, `time_stamp` (UTC), then the PurpleAir history fields as
float32, each with `_a` and `_b` laser channels: `pm1.0_atm`, `pm2.5_atm`,
`pm10.0_atm`, `pm2.5_cf_1` (from October 2026 on), particle counts `0.3_um_count` through
`10.0_um_count` (per dl), `scattering_coefficient` (Mm⁻¹), `deciviews`, `visual_range` (km),
`humidity` (%), `temperature` (°F), `pressure` (hPa) and `voc`. Mass concentrations are in µg/m³.
The `humidity`, `temperature` and `pressure` B channels are usually empty because
the sensor has a single BME280.

## Hourly columns

`n_obs` and `completeness` are the readings that hour, out of 30. The table also has
A/B-mean `pm2.5_atm`, `pm2.5_cf_1`, `pm1.0_atm`, `pm10.0_atm`, `humidity`, `temperature`
and `pressure`, plus these derived columns:

- `ab_disagree`: the EPA channel-agreement screen. It is true when |A−B| ≥ 5 µg/m³ and the relative difference is ≥ 70%.
- `pm2.5_epa`: PM2.5 corrected with the US-wide EPA correction (Barkjohn et al. 2021), using the
  smoke extension from the AirNow Fire and Smoke Map (Barkjohn et al. 2023).
  It is null when a channel is missing, the channels disagree, humidity is
  missing, or completeness is below 75%.
- `pm2.5_epa_basis`: `cf_1` when the correction used the cf_1 channels, which is the intended input.
  It is `atm` for data before cf_1 was collected. The `atm` channels match cf_1 below
  about 25 µg/m³ and read low above that, so `atm`-based corrections underestimate heavy smoke.
- `pm2.5_nowcast`, `aqi`, `aqi_category`: the 12-hour EPA NowCast of `pm2.5_epa`, and the AQI it maps to.
  AQI uses the EPA breakpoints revised in May 2024.

These are low-cost sensor data, corrected but not regulatory measurements.

## Reading

```python
import duckdb, json, urllib.request

base = "https://data2.climate.umt.edu/mesonet/air-quality"
m = json.load(urllib.request.urlopen(f"{base}/manifest.json"))
urls = [
    f"{base}/{f['path']}" for f in m["files"]["hourly"] if f["station"] == "acebirne"
]
df = duckdb.sql(f"SELECT * FROM read_parquet({urls})").df()
```

There is an R example in `read-archive.R` in the source repository.
