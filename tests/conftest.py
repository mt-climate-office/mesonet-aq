from datetime import date

import pytest

from mesonet_aq import meta, migrate, pipeline
from mesonet_aq.airtable import Deployment
from mesonet_aq.config import Settings
from mesonet_aq.store import LocalStore

STATIONS = {
    "teststa": {"name": "Test Station", "lat": 46.9, "lon": -114.0, "elevation": 975.0}
}


@pytest.fixture
def store(tmp_path):
    return LocalStore(tmp_path)


@pytest.fixture
def settings(tmp_path):
    return Settings(
        store=str(tmp_path),
        prefix="air-quality",
        cdn_base="https://data2.climate.umt.edu/mesonet",
        distribution_id=None,
        purpleair_api_key="pa-test",
        airtable_token="at-test",
        airtable_base_id="base-test",
        metric_namespace=None,
    )


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    """No network in tests: stub the station API and Airtable."""
    monkeypatch.setattr(meta, "fetch_mesonet_stations", lambda: STATIONS)
    monkeypatch.setattr(pipeline, "fetch_mesonet_stations", lambda: STATIONS)
    deps = [Deployment("D1", 111, "teststa", date(2026, 9, 28))]
    monkeypatch.setattr(pipeline, "fetch_deployments", lambda *a, **k: deps)
    monkeypatch.setattr(migrate, "fetch_deployments", lambda *a, **k: deps)
    monkeypatch.setattr(pipeline.time, "sleep", lambda s: None)
    return deps
