#!/usr/bin/env python3
"""Generate bike-shop themed nested JSON data and upload to S3 for Athena demos.

This script writes newline-delimited JSON (NDJSON) files into day partitions under
s3://<bucket>/<prefix>/dt=YYYY-MM-DD/. It intentionally creates many small files so
Athena scans over raw JSON are slower than compacted Parquet.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import boto3


def money(value: float) -> float:
    return float(Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


FIRST_NAMES = [
    "Avery",
    "Jordan",
    "Casey",
    "Riley",
    "Skyler",
    "Morgan",
    "Taylor",
    "Parker",
    "Reese",
    "Cameron",
    "Kai",
    "Quinn",
]

LAST_NAMES = [
    "Brooks",
    "Diaz",
    "Nguyen",
    "Patel",
    "Kim",
    "Thompson",
    "Miller",
    "Garcia",
    "Wilson",
    "Lee",
    "Cooper",
    "Evans",
]

CITIES = [
    ("Boston", "MA"),
    ("Denver", "CO"),
    ("Seattle", "WA"),
    ("Austin", "TX"),
    ("Portland", "OR"),
    ("San Diego", "CA"),
]

BIKE_CATALOG = [
    {"sku": "RB-AL-105", "name": "Road Bike Alloy 105", "category": "road", "base_price": 1699.00},
    {"sku": "MT-TR-29", "name": "Trail Bike 29er", "category": "mountain", "base_price": 2199.00},
    {"sku": "GR-CF-1", "name": "Gravel Carbon One", "category": "gravel", "base_price": 2999.00},
    {"sku": "EB-CT-500", "name": "City eBike 500", "category": "electric", "base_price": 2599.00},
    {"sku": "BK-KD-20", "name": "Kids Bike 20 inch", "category": "kids", "base_price": 399.00},
]

ACCESSORY_CATALOG = [
    {"sku": "AC-HL-900", "name": "Lumen 900 Headlight", "category": "accessory", "base_price": 89.00},
    {"sku": "AC-LK-UL", "name": "Ultra Lock", "category": "accessory", "base_price": 59.00},
    {"sku": "AC-PM-CM", "name": "Clipless Pedals", "category": "accessory", "base_price": 149.00},
    {"sku": "AC-BG-18", "name": "Bikepacking Saddle Bag 18L", "category": "accessory", "base_price": 129.00},
    {"sku": "AC-HM-GR", "name": "Gel Grips", "category": "accessory", "base_price": 39.00},
]


@dataclass
class Config:
    bucket: str
    prefix: str
    days: int
    orders_per_day: int
    customers_per_day: int
    order_files_per_day: int
    customer_files_per_day: int
    start_date: date
    seed: int


def normalize_prefix(prefix: str) -> str:
    return prefix.strip("/")


def s3_key(prefix: str, relative: str) -> str:
    return f"{normalize_prefix(prefix)}/{relative}"


def make_customer(customer_id: str, rng: random.Random, day: date) -> dict:
    first = rng.choice(FIRST_NAMES)
    last = rng.choice(LAST_NAMES)
    email = f"{first.lower()}.{last.lower()}.{customer_id[-6:]}@bikeshop.example"
    city, state = rng.choice(CITIES)
    created_at = datetime.combine(day - timedelta(days=rng.randint(30, 720)), datetime.min.time(), tzinfo=UTC)

    return {
        "customer_id": customer_id,
        "profile": {
            "first_name": first,
            "last_name": last,
            "email": email,
            "phone": f"+1-555-{rng.randint(100, 999)}-{rng.randint(1000, 9999)}",
            "lifecycle_stage": rng.choice(["new", "active", "vip"]),
        },
        "loyalty": {
            "tier": rng.choice(["bronze", "silver", "gold", "platinum"]),
            "points_balance": rng.randint(0, 120000),
            "is_newsletter_opt_in": rng.choice([True, False]),
        },
        "addresses": [
            {
                "type": "shipping",
                "line1": f"{rng.randint(100, 9999)} Trailhead Ave",
                "city": city,
                "state": state,
                "postal_code": f"{rng.randint(10000, 99999)}",
                "country": "US",
            },
            {
                "type": "billing",
                "line1": f"{rng.randint(100, 9999)} Crankset Blvd",
                "city": city,
                "state": state,
                "postal_code": f"{rng.randint(10000, 99999)}",
                "country": "US",
            },
        ],
        "bike_preferences": {
            "preferred_categories": rng.sample(
                ["road", "mountain", "gravel", "commuter", "electric"],
                k=rng.randint(1, 3),
            ),
            "frame_size_cm": rng.choice([49, 52, 54, 56, 58, 61]),
            "rider_height_cm": rng.randint(155, 198),
        },
        "created_at": created_at.isoformat(),
        "updated_at": datetime.combine(day, datetime.min.time(), tzinfo=UTC).isoformat(),
    }


def line_item_from_catalog(item: dict, rng: random.Random) -> dict:
    qty = rng.randint(1, 2 if item["category"] in {"road", "mountain", "gravel", "electric"} else 5)
    list_price = item["base_price"] * rng.uniform(0.95, 1.08)
    discount_pct = rng.choice([0, 0, 5, 10, 15])
    unit_price = list_price * ((100 - discount_pct) / 100)

    discounts = []
    if discount_pct > 0:
        discounts.append(
            {
                "type": "promo",
                "code": rng.choice(["SPRINGSPIN", "WEEKENDRIDE", "TEAMKIT"]),
                "amount": money((list_price - unit_price) * qty),
            }
        )

    return {
        "sku": item["sku"],
        "name": item["name"],
        "category": item["category"],
        "quantity": qty,
        "pricing": {
            "list_price": money(list_price),
            "unit_price": money(unit_price),
            "currency": "USD",
        },
        "discounts": discounts,
    }


def build_order(day: date, order_num: int, customer_pool: list[str], include_customer_blob: bool, rng: random.Random) -> dict:
    order_time = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(
        seconds=rng.randint(0, 86399)
    )
    customer_id = rng.choice(customer_pool)

    bike_item = line_item_from_catalog(rng.choice(BIKE_CATALOG), rng)
    accessory_count = rng.randint(1, 4)
    accessories = [
        line_item_from_catalog(rng.choice(ACCESSORY_CATALOG), rng)
        for _ in range(accessory_count)
    ]
    line_items = [bike_item, *accessories]

    subtotal = money(sum(i["pricing"]["unit_price"] * i["quantity"] for i in line_items))
    shipping = money(rng.choice([0.0, 14.99, 19.99, 24.99]))
    tax = money(subtotal * rng.uniform(0.05, 0.1))
    total = money(subtotal + shipping + tax)

    record = {
        "order_id": f"PO-{day.strftime('%Y%m%d')}-{order_num:07d}",
        "order_ts": order_time.isoformat(),
        "partition_date": day.isoformat(),
        "sales_channel": rng.choice(["online", "retail", "marketplace"]),
        "fulfillment": {
            "method": rng.choice(["ship", "pickup"]),
            "warehouse": rng.choice(["east-hub", "central-hub", "west-hub"]),
            "ship_speed": rng.choice(["ground", "2-day", "overnight"]),
        },
        "customer_ref": {
            "customer_id": customer_id,
            "segment": rng.choice(["new", "repeat", "vip"]),
        },
        "line_items": line_items,
        "totals": {
            "subtotal": subtotal,
            "shipping": shipping,
            "tax": tax,
            "grand_total": total,
            "currency": "USD",
        },
        "payment": {
            "method": rng.choice(["card", "apple_pay", "affirm", "paypal"]),
            "auth_code": uuid.uuid4().hex[:10].upper(),
            "status": rng.choice(["authorized", "captured"]),
        },
        "audit": {
            "ingested_at": datetime.now(UTC).isoformat(),
            "source_system": "bike-shop-checkout",
            "trace_id": str(uuid.uuid4()),
        },
    }

    if include_customer_blob:
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        record["customer_profile"] = {
            "name": {
                "first": first,
                "last": last,
            },
            "contact": {
                "email": f"{first.lower()}.{last.lower()}@example.net",
                "sms_opt_in": rng.choice([True, False]),
            },
            "shipping_city": rng.choice(CITIES)[0],
        }

    return record


def to_ndjson(records: list[dict]) -> str:
    return "\n".join(json.dumps(r, separators=(",", ":")) for r in records) + "\n"


def chunk_records(records: list[dict], chunks: int) -> list[list[dict]]:
    chunks = max(1, min(chunks, len(records)))
    out: list[list[dict]] = [[] for _ in range(chunks)]
    for i, rec in enumerate(records):
        out[i % chunks].append(rec)
    return out


def upload_records(
    s3_client,
    bucket: str,
    prefix: str,
    day: date,
    dataset_name: str,
    records: list[dict],
    files_per_day: int,
) -> int:
    day_prefix = f"dt={day.isoformat()}/{dataset_name}"
    chunks = chunk_records(records, files_per_day)

    uploaded = 0
    for idx, part in enumerate(chunks, start=1):
        if not part:
            continue
        key = s3_key(prefix, f"{day_prefix}/part-{idx:05d}.json")
        body = to_ndjson(part).encode("utf-8")
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/x-ndjson",
        )
        uploaded += 1
    return uploaded


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        description="Generate bike-shop nested JSON partitions for Athena demos.",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="Target S3 bucket. Defaults to environment variable aws_bucket.",
    )
    parser.add_argument(
        "--prefix",
        default="demo_athena/raw",
        help="Target S3 prefix for partitioned JSON data.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=5,
        help="How many days of partitions to generate (default: 5).",
    )
    parser.add_argument(
        "--orders-per-day",
        type=int,
        default=2500,
        help="Number of order records per day partition.",
    )
    parser.add_argument(
        "--customers-per-day",
        type=int,
        default=400,
        help="Number of customer records for days that include customers.",
    )
    parser.add_argument(
        "--order-files-per-day",
        type=int,
        default=24,
        help="How many order JSON files to create per day.",
    )
    parser.add_argument(
        "--customer-files-per-day",
        type=int,
        default=8,
        help="How many customer JSON files to create for customer days.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Start date in YYYY-MM-DD. If omitted, starts at today-(days-1).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible output.",
    )

    args = parser.parse_args()

    if args.days < 1:
        raise SystemExit("--days must be >= 1")
    if args.orders_per_day < 1:
        raise SystemExit("--orders-per-day must be >= 1")
    if args.customers_per_day < 1:
        raise SystemExit("--customers-per-day must be >= 1")

    if args.start_date:
        start = date.fromisoformat(args.start_date)
    else:
        start = date.today() - timedelta(days=args.days - 1)

    bucket = args.bucket or os.getenv("aws_bucket")
    if not bucket:
        raise SystemExit("Set aws_bucket or pass --bucket.")

    return Config(
        bucket=bucket,
        prefix=normalize_prefix(args.prefix),
        days=args.days,
        orders_per_day=args.orders_per_day,
        customers_per_day=args.customers_per_day,
        order_files_per_day=args.order_files_per_day,
        customer_files_per_day=args.customer_files_per_day,
        start_date=start,
        seed=args.seed,
    )


def main() -> None:
    config = parse_args()
    rng = random.Random(config.seed)
    s3_client = boto3.client("s3")

    total_orders = 0
    total_customers = 0
    total_files = 0

    print("Generating bike-shop demo data with these settings:")
    print(f"  bucket: {config.bucket}")
    print(f"  prefix: s3://{config.bucket}/{config.prefix}/")
    print(f"  days: {config.days}")
    print(f"  orders/day: {config.orders_per_day}")
    print(f"  customers/day (when included): {config.customers_per_day}")

    for day_offset in range(config.days):
        day = config.start_date + timedelta(days=day_offset)
        day_seed_rng = random.Random(config.seed + day_offset)
        include_customer_dataset = (day_offset % 2 == 0)
        include_customer_blob = (day_offset % 3 != 1)

        customer_ids = [
            f"CUST-{day.strftime('%Y%m%d')}-{i:06d}"
            for i in range(1, config.customers_per_day + 1)
        ]

        orders = [
            build_order(
                day=day,
                order_num=i,
                customer_pool=customer_ids,
                include_customer_blob=include_customer_blob,
                rng=day_seed_rng,
            )
            for i in range(1, config.orders_per_day + 1)
        ]

        order_files = upload_records(
            s3_client=s3_client,
            bucket=config.bucket,
            prefix=config.prefix,
            day=day,
            dataset_name="orders",
            records=orders,
            files_per_day=config.order_files_per_day,
        )
        total_orders += len(orders)
        total_files += order_files

        customer_files = 0
        customer_records_count = 0
        if include_customer_dataset:
            customers = [make_customer(cid, day_seed_rng, day) for cid in customer_ids]
            customer_files = upload_records(
                s3_client=s3_client,
                bucket=config.bucket,
                prefix=config.prefix,
                day=day,
                dataset_name="customers",
                records=customers,
                files_per_day=config.customer_files_per_day,
            )
            customer_records_count = len(customers)
            total_customers += customer_records_count
            total_files += customer_files

        print(
            f"{day.isoformat()} | orders={len(orders)} in {order_files} files | "
            f"customers={customer_records_count} in {customer_files} files | "
            f"order_has_customer_blob={include_customer_blob}"
        )

    print("Done.")
    print(f"  uploaded files: {total_files}")
    print(f"  order records: {total_orders}")
    print(f"  customer records: {total_customers}")


if __name__ == "__main__":
    main()
