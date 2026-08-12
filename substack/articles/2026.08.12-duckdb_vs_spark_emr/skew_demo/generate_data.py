from __future__ import annotations

import argparse
import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3

from skew_demo.config import (
    DEFAULT_BUCKET,
    DEFAULT_DATASET_PREFIX,
    DEFAULT_ROOT_PREFIX,
    DemoPaths,
    default_run_date,
    ensure_local_artifact_dirs,
)

SPEAKERS = ["customer", "agent", "bot"]
INTENTS = [
    "greeting",
    "return_request",
    "reason",
    "policy_question",
    "escalation",
    "resolution",
]
RETURN_REASONS = ["damaged", "wrong_item", "missing_parts", "changed_mind", "other"]
TEXT_FRAGMENTS = [
    "The box arrived with cracks on the side.",
    "I bought this drill but it does not power on.",
    "Can I return this if the receipt is missing?",
    "The color does not match what I ordered.",
    "I need to return this because a part is missing.",
    "Can I get a refund to the original payment method?",
]


@dataclass
class FileSpec:
    order_id: str
    object_key: str
    target_size_bytes: int
    skew_class: str


def random_message(rng: random.Random, index: int, padding_chars: int = 0) -> dict[str, Any]:
    text = f"{rng.choice(TEXT_FRAGMENTS)} Message index {index}."
    if padding_chars > 0:
        text += " " + ("x" * padding_chars)
    return {
        "ts": (datetime.now(timezone.utc) + timedelta(seconds=index)).isoformat(),
        "speaker": rng.choice(SPEAKERS),
        "message_id": str(uuid.uuid4()),
        "text": text,
        "intent": rng.choice(INTENTS),
        "sentiment_score": round(rng.uniform(-1.0, 1.0), 3),
        "return_reason_code": rng.choice(RETURN_REASONS),
        "refund_requested": rng.random() < 0.4,
    }


def build_transcript(rng: random.Random, order_id: str, target_size_bytes: int, large: bool) -> dict[str, Any]:
    base_payload: dict[str, Any] = {
        "transcript_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": f"cust-{rng.randint(100000, 999999)}",
        "store_id": f"store-{rng.randint(1, 50):03d}",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "return_intent": True,
        "product_sku": f"HW-{rng.randint(1000, 9999)}",
        "messages": [],
    }

    avg_message_bytes = 460 if large else 220
    min_messages = 40 if not large else 2000
    estimate = max(min_messages, target_size_bytes // avg_message_bytes)

    for i in range(int(estimate)):
        padding = rng.randint(100, 400) if large else rng.randint(0, 40)
        base_payload["messages"].append(random_message(rng, i, padding_chars=padding))

    blob = json.dumps(base_payload, indent=2)
    current_size = len(blob.encode("utf-8"))

    if current_size < target_size_bytes:
        gap = target_size_bytes - current_size
        if base_payload["messages"]:
            base_payload["messages"][-1]["text"] += " " + ("z" * max(0, gap - 32))

    return base_payload


def build_file_plan(
    paths: DemoPaths,
    order_count: int,
    files_per_order: int,
    large_file_count: int,
    small_min_kb: int,
    small_max_kb: int,
    large_mb: int,
    rng: random.Random,
) -> list[FileSpec]:
    orders = [f"order-{i:06d}" for i in range(1, order_count + 1)]
    large_slots = set(rng.sample(range(order_count * files_per_order), k=large_file_count))

    file_specs: list[FileSpec] = []
    slot = 0
    for order_id in orders:
        for file_idx in range(files_per_order):
            is_large = slot in large_slots
            if is_large:
                target_size = large_mb * 1024 * 1024
                skew = "large"
            else:
                target_size = rng.randint(small_min_kb * 1024, small_max_kb * 1024)
                skew = "small"

            key = (
                f"{paths.raw_prefix}/order_id={order_id}/"
                f"chat_{file_idx + 1:03d}_{skew}.json"
            )
            file_specs.append(
                FileSpec(
                    order_id=order_id,
                    object_key=key,
                    target_size_bytes=target_size,
                    skew_class=skew,
                )
            )
            slot += 1

    return file_specs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate skewed JSON chat logs in S3")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--root-prefix", default=DEFAULT_ROOT_PREFIX)
    parser.add_argument("--dataset-prefix", default=DEFAULT_DATASET_PREFIX)
    parser.add_argument("--run-date", default=default_run_date())
    parser.add_argument("--orders", type=int, default=250)
    parser.add_argument("--files-per-order", type=int, default=4)
    parser.add_argument("--large-files", type=int, default=2)
    parser.add_argument("--small-min-kb", type=int, default=3)
    parser.add_argument("--small-max-kb", type=int, default=8)
    parser.add_argument("--large-mb", type=int, default=20)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    s3 = boto3.client("s3")

    paths = DemoPaths(
        bucket=args.bucket,
        root_prefix=args.root_prefix,
        dataset_prefix=args.dataset_prefix,
        run_date=args.run_date,
    )

    if args.large_files > args.orders * args.files_per_order:
        raise ValueError("large-files cannot exceed total file count")

    file_specs = build_file_plan(
        paths=paths,
        order_count=args.orders,
        files_per_order=args.files_per_order,
        large_file_count=args.large_files,
        small_min_kb=args.small_min_kb,
        small_max_kb=args.small_max_kb,
        large_mb=args.large_mb,
        rng=rng,
    )

    manifest_rows: list[dict[str, Any]] = []
    for idx, spec in enumerate(file_specs, start=1):
        payload = build_transcript(
            rng=rng,
            order_id=spec.order_id,
            target_size_bytes=spec.target_size_bytes,
            large=(spec.skew_class == "large"),
        )
        blob = json.dumps(payload, indent=2)
        body = blob.encode("utf-8")

        s3.put_object(
            Bucket=paths.bucket,
            Key=spec.object_key,
            Body=body,
            ContentType="application/json",
        )

        manifest_rows.append(
            {
                "bucket": paths.bucket,
                "key": spec.object_key,
                "order_id": spec.order_id,
                "size_bytes": len(body),
                "target_size_bytes": spec.target_size_bytes,
                "skew_class": spec.skew_class,
            }
        )

        if idx % 50 == 0:
            print(f"Uploaded {idx}/{len(file_specs)} objects...")

    summary = {
        "bucket": paths.bucket,
        "raw_prefix": paths.raw_prefix,
        "run_date": paths.run_date,
        "file_count": len(manifest_rows),
        "small_files": sum(1 for row in manifest_rows if row["skew_class"] == "small"),
        "large_files": sum(1 for row in manifest_rows if row["skew_class"] == "large"),
        "bytes_total": sum(row["size_bytes"] for row in manifest_rows),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    local_artifacts = ensure_local_artifact_dirs()
    local_manifest_path = local_artifacts / "manifest.json"
    local_manifest_path.write_text(
        json.dumps({"summary": summary, "objects": manifest_rows}, indent=2),
        encoding="utf-8",
    )

    manifest_key = f"{paths.manifest_prefix}/manifest.json"
    s3.put_object(
        Bucket=paths.bucket,
        Key=manifest_key,
        Body=local_manifest_path.read_bytes(),
        ContentType="application/json",
    )

    print("Generation complete")
    print(json.dumps(summary, indent=2))
    print(f"Manifest: s3://{paths.bucket}/{manifest_key}")


if __name__ == "__main__":
    main()