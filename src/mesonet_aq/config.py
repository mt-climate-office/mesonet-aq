"""Runtime configuration, read once from the environment.

Credentials arrive as environment variables: in ECS they are resolved from
Secrets Manager by the task definition (`secrets`), locally from `.env`.
Nothing here ever holds an AWS credential -- boto3 finds those itself.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# The data CDN (mco-data-cdn, distribution E24I4W0YAJ2A27) maps
# https://data2.climate.umt.edu/mesonet/<key> to s3://mco-mesonet/<key>.
DEFAULT_CDN_BASE = "https://data2.climate.umt.edu/mesonet"
DEFAULT_DISTRIBUTION_ID = "E24I4W0YAJ2A27"

MESONET_STATIONS_API = "https://mesonet.climate.umt.edu/api/v2/stations/?type=json"

AIRTABLE_TABLE_NAME = "Deployments"
AIRTABLE_VIEW_NAME = "Public"


@dataclass(frozen=True)
class Settings:
    # Where objects go: "s3://bucket" or a local directory (tests, dry runs).
    store: str
    prefix: str
    cdn_base: str
    distribution_id: str | None
    purpleair_api_key: str | None
    airtable_token: str | None
    airtable_base_id: str | None
    metric_namespace: str | None

    @property
    def public_base(self) -> str:
        """Public HTTPS URL of the prefix root, as readers see it."""
        return f"{self.cdn_base.rstrip('/')}/{self.prefix}"

    @classmethod
    def from_env(cls) -> Settings:
        bucket = os.getenv("S3_BUCKET", "mco-mesonet")
        return cls(
            store=os.getenv("AQ_STORE") or f"s3://{bucket}",
            prefix=os.getenv("AQ_PREFIX", "air-quality").strip("/"),
            cdn_base=os.getenv("CDN_BASE_URL", DEFAULT_CDN_BASE),
            # Empty string disables invalidation (local runs, scratch prefixes).
            distribution_id=os.getenv("CDN_DISTRIBUTION_ID", DEFAULT_DISTRIBUTION_ID)
            or None,
            purpleair_api_key=os.getenv("PURPLEAIR_API_KEY"),
            airtable_token=os.getenv("AIRTABLE_TOKEN"),
            airtable_base_id=os.getenv("AIRTABLE_BASE_ID"),
            metric_namespace=os.getenv("METRIC_NAMESPACE") or None,
        )
