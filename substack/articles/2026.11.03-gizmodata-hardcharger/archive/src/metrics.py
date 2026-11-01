"""CloudWatch custom metric publishing."""

from __future__ import annotations

from typing import Any


def publish_metric(
    name: str,
    value: float,
    *,
    region: str,
    namespace: str = "HardchargerDemo",
    dimensions: dict[str, str] | None = None,
    client: Any | None = None,
) -> None:
    if client is None:
        import boto3

        client = boto3.client("cloudwatch", region_name=region)
    client.put_metric_data(
        Namespace=namespace,
        MetricData=[
            {
                "MetricName": name,
                "Value": value,
                "Unit": "Seconds" if name == "ETLDuration" else "Count",
                "Dimensions": [
                    {"Name": key, "Value": val} for key, val in (dimensions or {}).items()
                ],
            }
        ],
    )
