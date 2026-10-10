"""CloudFront invalidation and the run-success heartbeat metric."""

from __future__ import annotations

import logging
import time

log = logging.getLogger(__name__)

# Past this many paths one wildcard is cheaper (a wildcard bills as one path).
MAX_EXPLICIT_PATHS = 15


def invalidation_paths(prefix: str, rel_keys: list[str]) -> list[str]:
    """CloudFront cache keys for changed objects.

    ⚠ Cache keys are the S3 keys, NOT the public URL: the CDN strips its
    /mesonet origin prefix before caching, so `/air-quality/x` is right and
    `/mesonet/air-quality/x` silently flushes nothing (mesonet-db-rds #167).
    """
    keys = sorted(set(rel_keys))
    if not keys:
        return []
    if len(keys) > MAX_EXPLICIT_PATHS:
        return [f"/{prefix}/*"]
    return [f"/{prefix}/{k}" for k in keys]


def invalidate(distribution_id: str, paths: list[str]) -> None:
    if not paths:
        return
    import boto3

    cf = boto3.client("cloudfront")
    resp = cf.create_invalidation(
        DistributionId=distribution_id,
        InvalidationBatch={
            "Paths": {"Quantity": len(paths), "Items": paths},
            "CallerReference": f"mesonet-aq-{time.time_ns()}",
        },
    )
    log.info("invalidation %s: %s", resp["Invalidation"]["Id"], paths)


def heartbeat(namespace: str, metric: str = "RunSucceeded", value: float = 1.0) -> None:
    """Publish a datapoint the dead-man alarm watches (terraform/alarms.tf)."""
    import boto3

    boto3.client("cloudwatch").put_metric_data(
        Namespace=namespace,
        MetricData=[{"MetricName": metric, "Value": value, "Unit": "Count"}],
    )
    log.info("heartbeat %s/%s=%s", namespace, metric, value)
