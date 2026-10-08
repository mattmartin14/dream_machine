import argparse
import json
import random
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import boto3
from botocore.config import Config

BUCKET = "matt-sbx-bucket-1-us-east-1"
PREFIX = "2_dot_0_benchmark/"

STORES = ["Downtown Cycles", "Trailhead Bike Co", "Spoke & Chain", "Velo Haven", "Pedal Pushers"]
FIRST = ["Ava", "Liam", "Noah", "Emma", "Olivia", "Mason", "Sophia", "Ethan", "Mia", "Lucas"]
LAST = ["Smith", "Johnson", "Garcia", "Brown", "Davis", "Miller", "Wilson", "Moore", "Clark", "Lee"]
CITIES = [("Austin", "TX", "78701"), ("Denver", "CO", "80202"), ("Portland", "OR", "97201"),
          ("Boulder", "CO", "80302"), ("Seattle", "WA", "98101"), ("Madison", "WI", "53703")]
STREETS = ["Main St", "Oak Ave", "Cedar Ln", "Maple Dr", "Ridge Rd", "Lakeview Blvd"]
PRODUCTS = [
    ("Mountain Bike", 650, 2400), ("Road Bike", 800, 3200), ("Helmet", 25, 120),
    ("Bike Lock", 15, 70), ("Tire Tube", 5, 15), ("Cycling Gloves", 15, 45),
    ("Water Bottle", 6, 20), ("Bike Light Set", 20, 60), ("Pedals", 25, 150),
]


def make_order(order_num: int) -> dict:
    city, state, zip_code = random.choice(CITIES)
    lines = []
    for n in range(1, random.randint(1, 5) + 1):
        name, lo, hi = random.choice(PRODUCTS)
        lines.append({
            "line_number": n,
            "product": name,
            "quantity": random.randint(1, 3),
            "unit_price": round(random.uniform(lo, hi), 2),
        })
    total = round(sum(l["quantity"] * l["unit_price"] for l in lines), 2)
    return {
        "order_number": order_num,
        "order_date": (date(2025, 1, 1) + timedelta(days=random.randint(0, 650))).isoformat(),
        "total_amount": total,
        "customer_id": random.randint(1000, 99999),
        "store_name": random.choice(STORES),
        "customer": {
            "name": f"{random.choice(FIRST)} {random.choice(LAST)}",
            "address": f"{random.randint(1, 9999)} {random.choice(STREETS)}",
            "city": city,
            "state": state,
            "zip": zip_code,
        },
        "order_lines": lines,
    }


def s3_client():
    return boto3.client("s3", config=Config(max_pool_connections=64, retries={"max_attempts": 5}))


def upload_one(s3, order_num: int) -> None:
    body = json.dumps(make_order(order_num)).encode()
    key = f"{PREFIX}order_{order_num}_{uuid.uuid4().hex[:8]}.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json")


def upload(count: int, workers: int) -> None:
    s3 = s3_client()  # boto3 clients are thread-safe
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for i, _ in enumerate(pool.map(lambda n: upload_one(s3, n), range(1, count + 1)), 1):
            if i % 500 == 0 or i == count:
                print(f"uploaded {i}/{count}")


def nuke(workers: int) -> None:
    s3 = s3_client()
    batches = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            batches.append(keys)  # one page = up to 1000 keys, the delete_objects max

    def delete(keys):
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys, "Quiet": True})
        return len(keys)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        total = sum(pool.map(delete, batches))
    print(f"deleted {total} objects from s3://{BUCKET}/{PREFIX}")


def main():
    p = argparse.ArgumentParser(description="Bicycle shop dummy data -> S3")
    p.add_argument("command", choices=["sample", "upload", "nuke"])
    p.add_argument("-n", "--count", type=int, default=100, help="number of files to upload")
    p.add_argument("-w", "--workers", type=int, default=32, help="thread count")
    a = p.parse_args()

    if a.command == "sample":
        print(json.dumps(make_order(1), indent=2))
    elif a.command == "upload":
        upload(a.count, a.workers)
    else:
        nuke(a.workers)


if __name__ == "__main__":
    main()
