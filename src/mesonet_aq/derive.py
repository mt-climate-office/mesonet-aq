"""Hourly summaries, EPA-corrected PM2.5, NowCast and AQI.

References
----------
- Barkjohn, Gantt & Clements (2021), AMT 14:4617 -- US-wide correction,
  extended for smoke by Barkjohn et al. (2023) as used on the AirNow Fire and
  Smoke Map. Defined on the A/B mean of `pm2.5_cf_1` and PurpleAir's own RH.
- EPA PM2.5 NowCast (12-hour weighted, w = max(min/max, 0.5)).
- EPA AQI breakpoints for PM2.5 as revised 2024-05-06 (Good tops out at 9.0).
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

# The real-time history cadence is 2 minutes.
EXPECTED_PER_HOUR = 30
# Hours below this fraction of expected readings get no corrected value.
MIN_COMPLETENESS = 0.75
# EPA channel-agreement screen: drop when BOTH |A-B| >= 5 ug/m3 AND RPD >= 70%.
AB_ABS_DIFF = 5.0
AB_REL_DIFF = 0.70

AQI_BREAKPOINTS = [
    # (C_lo, C_hi, I_lo, I_hi, category)
    (0.0, 9.0, 0, 50, "Good"),
    (9.1, 35.4, 51, 100, "Moderate"),
    (35.5, 55.4, 101, 150, "Unhealthy for Sensitive Groups"),
    (55.5, 125.4, 151, 200, "Unhealthy"),
    (125.5, 225.4, 201, 300, "Very Unhealthy"),
    (225.5, 325.4, 301, 500, "Hazardous"),
]


def _pair_mean(df: pd.DataFrame, base: str) -> pd.Series:
    cols = [c for c in (f"{base}_a", f"{base}_b") if c in df.columns]
    return (
        df[cols].mean(axis=1, skipna=True)
        if cols
        else pd.Series(np.nan, index=df.index)
    )


def epa_correct(pa: np.ndarray | float, rh: np.ndarray | float) -> np.ndarray:
    """Barkjohn 2021/2023 piecewise correction. `pa` is the A/B mean PM2.5 (cf_1)."""
    pa = np.asarray(pa, dtype="float64")
    rh = np.asarray(rh, dtype="float64")
    low = 0.524 * pa - 0.0862 * rh + 5.75
    mid_w = pa / 20 - 3 / 2
    mid = (0.786 * mid_w + 0.524 * (1 - mid_w)) * pa - 0.0862 * rh + 5.75
    high = 0.786 * pa - 0.0862 * rh + 5.75
    sm_w = pa / 50 - 21 / 5
    smoke_mid = (
        (0.69 * sm_w + 0.786 * (1 - sm_w)) * pa
        - 0.0862 * rh * (1 - sm_w)
        + 2.966 * sm_w
        + 5.75 * (1 - sm_w)
        + 8.84e-4 * pa**2 * sm_w
    )
    smoke = 2.966 + 0.69 * pa + 8.84e-4 * pa**2
    out = np.select(
        [pa < 30, pa < 50, pa < 210, pa < 260],
        [low, mid, high, smoke_mid],
        default=smoke,
    )
    out = np.where(np.isnan(pa) | np.isnan(rh), np.nan, out)
    return np.clip(out, 0, None)


def ab_disagree(a: pd.Series, b: pd.Series) -> pd.Series:
    diff = (a - b).abs()
    rpd = diff * 2 / (a + b).where((a + b) > 0)
    return ((diff >= AB_ABS_DIFF) & (rpd >= AB_REL_DIFF)).fillna(False)


def hourly(raw: pd.DataFrame) -> pd.DataFrame:
    """Hourly means for ONE station's raw rows (any span). Index-free frame."""
    if raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    df["time_stamp"] = df["time_stamp"].dt.floor("h")
    g = df.groupby("time_stamp")
    h = pd.DataFrame(index=g.size().index)
    h["n_obs"] = g.size()
    h["completeness"] = (h["n_obs"] / EXPECTED_PER_HOUR).clip(upper=1.0)
    means = g.mean(numeric_only=True)
    for col in ("pm2.5_atm_a", "pm2.5_atm_b", "pm2.5_cf_1_a", "pm2.5_cf_1_b"):
        h[col] = means[col] if col in means else np.nan
    h["pm2.5_atm"] = _pair_mean(h, "pm2.5_atm")
    h["pm2.5_cf_1"] = _pair_mean(h, "pm2.5_cf_1")
    h["pm1.0_atm"] = _pair_mean(means, "pm1.0_atm")
    h["pm10.0_atm"] = _pair_mean(means, "pm10.0_atm")
    h["humidity"] = _pair_mean(means, "humidity")
    h["temperature"] = _pair_mean(means, "temperature")
    h["pressure"] = _pair_mean(means, "pressure")

    # EPA basis: cf_1 when both channels report it, otherwise the atm channels
    # (identical to cf_1 below ~25 ug/m3, biased low above it -- flagged).
    use_cf1 = h["pm2.5_cf_1_a"].notna() & h["pm2.5_cf_1_b"].notna()
    a = h["pm2.5_cf_1_a"].where(use_cf1, h["pm2.5_atm_a"])
    b = h["pm2.5_cf_1_b"].where(use_cf1, h["pm2.5_atm_b"])
    h["ab_disagree"] = ab_disagree(a, b)
    usable = (
        a.notna() & b.notna() & h["humidity"].notna()
        & ~h["ab_disagree"] & (h["completeness"] >= MIN_COMPLETENESS)
    )  # fmt: skip
    h["pm2.5_epa"] = np.where(usable, epa_correct((a + b) / 2, h["humidity"]), np.nan)
    h["pm2.5_epa_basis"] = np.where(usable, np.where(use_cf1, "cf_1", "atm"), None)
    h = h.drop(columns=["pm2.5_cf_1_a", "pm2.5_cf_1_b"])
    h["station"] = raw["station"].iloc[0]
    h = h.reset_index()
    return add_nowcast(h)


def nowcast(conc: np.ndarray) -> np.ndarray:
    """NowCast over a CONTIGUOUS hourly series (NaN = missing hour)."""
    n = len(conc)
    out = np.full(n, np.nan)
    for t in range(n):
        win = conc[max(0, t - 11) : t + 1][::-1]  # most recent first
        if np.count_nonzero(~np.isnan(win[:3])) < 2:
            continue
        valid = ~np.isnan(win)
        cmax = np.nanmax(win)
        cmin = np.nanmin(win)
        w = 1.0 if cmax <= 0 else max(cmin / cmax, 0.5)
        weights = w ** np.arange(len(win))
        out[t] = np.sum(weights[valid] * win[valid]) / np.sum(weights[valid])
    # Truncate to 0.1 (EPA), after rounding away float noise like 19.9999999.
    return np.floor(np.round(out * 10, 6)) / 10


def add_nowcast(h: pd.DataFrame) -> pd.DataFrame:
    full = pd.date_range(h["time_stamp"].min(), h["time_stamp"].max(), freq="h")
    series = h.set_index("time_stamp")["pm2.5_epa"].reindex(full)
    nc = pd.Series(nowcast(series.to_numpy(dtype="float64")), index=full)
    h["pm2.5_nowcast"] = h["time_stamp"].map(nc)
    aqis = [aqi(c) for c in h["pm2.5_nowcast"]]
    h["aqi"] = pd.array([a for a, _ in aqis], dtype="Int16")
    h["aqi_category"] = [c for _, c in aqis]
    return h


def aqi(conc: float | None) -> tuple[int | None, str | None]:
    if conc is None or (isinstance(conc, float) and math.isnan(conc)):
        return None, None
    c = math.floor(round(float(conc) * 10, 6)) / 10
    for c_lo, c_hi, i_lo, i_hi, cat in AQI_BREAKPOINTS:
        if c <= c_hi:
            # Round half up (EPA), not Python's round-half-to-even.
            i = (i_hi - i_lo) / (c_hi - c_lo) * (max(c, c_lo) - c_lo) + i_lo
            return math.floor(i + 0.5), cat
    return 500, "Hazardous"
