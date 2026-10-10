import math

import numpy as np
import pandas as pd
import pytest

from mesonet_aq.derive import ab_disagree, aqi, epa_correct, hourly, nowcast


def test_epa_low_range_matches_barkjohn_2021():
    assert epa_correct(10.0, 50.0) == pytest.approx(0.524 * 10 - 0.0862 * 50 + 5.75)


@pytest.mark.parametrize("pa", [30.0, 50.0, 210.0, 260.0])
def test_epa_piecewise_is_continuous(pa):
    rh = 40.0
    lo, hi = epa_correct([pa - 1e-9, pa + 1e-9], [rh, rh])
    assert lo == pytest.approx(hi, abs=1e-5)


def test_epa_nan_and_floor():
    out = epa_correct([np.nan, 0.0], [50.0, 100.0])
    assert math.isnan(out[0])
    assert out[1] == 0.0  # 5.75 - 8.62 clipped


@pytest.mark.parametrize(
    "conc,expected",
    [
        (0.0, (0, "Good")),
        (9.0, (50, "Good")),
        (9.1, (51, "Moderate")),
        (12.0, (56, "Moderate")),
        (35.4, (100, "Moderate")),
        (35.5, (101, "Unhealthy for Sensitive Groups")),
        (55.5, (151, "Unhealthy")),
        (225.5, (301, "Hazardous")),
        (900.0, (500, "Hazardous")),
        (float("nan"), (None, None)),
    ],
)
def test_aqi_2024_breakpoints(conc, expected):
    assert aqi(conc) == expected


def test_aqi_truncates_not_rounds():
    assert aqi(9.09) == (50, "Good")


def test_nowcast_constant_series():
    out = nowcast(np.full(12, 20.0))
    assert math.isnan(out[0])  # one hour is not 2 of the last 3
    assert np.all(out[1:] == 20.0)


def test_nowcast_needs_two_of_three_recent_hours():
    c = np.array([10.0] * 10 + [np.nan, np.nan])
    assert math.isnan(nowcast(c)[-1])
    c[-1] = 10.0
    assert nowcast(c)[-1] == 10.0


def test_nowcast_weights_recent_hours_when_rising():
    c = np.array([5.0] * 11 + [100.0])
    v = nowcast(c)[-1]
    assert 50 < v < 100  # w = 0.5 floor puts ~half the weight on the latest hour


def test_ab_disagree_needs_both_thresholds():
    a = pd.Series([10.0, 10.0, 100.0])
    b = pd.Series([4.0, 1.0, 90.0])
    # |diff| 6 & RPD 86% -> flag; 9 & 164% -> flag; 10 & 10.5% -> keep
    assert ab_disagree(a, b).tolist() == [True, True, False]


def _raw(hours=3, per_hour=30, a=10.0, b=10.0, rh=40.0):
    ts = pd.date_range("2026-07-01", periods=hours * per_hour, freq="2min", tz="UTC")
    return pd.DataFrame(
        {
            "station": "teststa",
            "sensor_index": 1,
            "time_stamp": ts,
            "pm2.5_atm_a": a,
            "pm2.5_atm_b": b,
            "humidity_a": rh,
            "temperature_a": 70.0,
        }
    )


def test_hourly_atm_basis_and_completeness():
    h = hourly(_raw())
    assert len(h) == 3
    assert (h["n_obs"] == 30).all()
    assert (h["pm2.5_epa_basis"] == "atm").all()
    assert h["pm2.5_epa"].iloc[0] == pytest.approx(epa_correct(10.0, 40.0))
    assert h["aqi"].iloc[1:].notna().all()  # NowCast needs 2 of the last 3 hours


def test_hourly_incomplete_hour_has_no_epa():
    h = hourly(_raw(hours=1, per_hour=10).assign(time_stamp=lambda d: d["time_stamp"]))
    assert h["completeness"].iloc[0] < 0.75
    assert math.isnan(h["pm2.5_epa"].iloc[0])


def test_hourly_prefers_cf1():
    raw = _raw().assign(**{"pm2.5_cf_1_a": 12.0, "pm2.5_cf_1_b": 12.0})
    h = hourly(raw)
    assert (h["pm2.5_epa_basis"] == "cf_1").all()
    assert h["pm2.5_epa"].iloc[0] == pytest.approx(epa_correct(12.0, 40.0))
