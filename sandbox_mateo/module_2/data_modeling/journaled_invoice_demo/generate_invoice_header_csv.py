from __future__ import annotations

import argparse
import csv
import math
from datetime import date, datetime, timedelta
from pathlib import Path
from random import Random

SENTINEL_END_TS = "9999-12-31 23:59:59"
DEFAULT_ROW_COUNT = 1000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate invoice header journal CSV with begin/end timestamps."
    )
    parser.add_argument(
        "target_row_count",
        nargs="?",
        type=int,
        default=DEFAULT_ROW_COUNT,
        help="Approximate number of rows to generate (default: 1000).",
    )
    return parser.parse_args()


def build_header_rows(target_row_count: int) -> list[dict[str, str | int | float]]:
    if target_row_count <= 0:
        raise ValueError("target_row_count must be greater than 0")

    rng = Random(42)
    base_ts = datetime(2026, 1, 1, 8, 0, 0)
    versions_per_invoice = 5
    invoice_count = max(1, math.ceil(target_row_count / versions_per_invoice))

    rows: list[dict[str, str | int | float]] = []

    for idx in range(invoice_count):
        invoice_id = 100000 + idx
        vendor_id = 2000 + (idx % 250)
        created_ts = base_ts + timedelta(minutes=7 * idx)

        draft_end = created_ts + timedelta(hours=4)
        submitted_begin = draft_end + timedelta(seconds=1)
        submitted_end = submitted_begin + timedelta(hours=6)
        approved_begin = submitted_end + timedelta(seconds=1)
        approved_end = approved_begin + timedelta(hours=5)
        posted_begin = approved_end + timedelta(seconds=1)
        posted_end = posted_begin + timedelta(days=1)
        final_begin = posted_end + timedelta(seconds=1)

        base_amount = round(150 + (idx % 17) * 23.5 + rng.uniform(1.0, 20.0), 2)
        approved_amount = round(base_amount + rng.uniform(0.5, 5.0), 2)
        posted_amount = round(approved_amount, 2)
        final_amount = round(posted_amount, 2)

        due_dt = date(2026, 1, 15) + timedelta(days=(idx % 28))

        rows.extend(
            [
                {
                    "invoice_id": invoice_id,
                    "vendor_id": vendor_id,
                    "status_cd": "DRAFT",
                    "invoice_total_amt": f"{base_amount:.2f}",
                    "due_dt": due_dt.isoformat(),
                    "updated_by": "ap_clerk",
                    "begin_ts": created_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_ts": draft_end.strftime("%Y-%m-%d %H:%M:%S"),
                },
                {
                    "invoice_id": invoice_id,
                    "vendor_id": vendor_id,
                    "status_cd": "SUBMITTED",
                    "invoice_total_amt": f"{base_amount:.2f}",
                    "due_dt": due_dt.isoformat(),
                    "updated_by": "requestor",
                    "begin_ts": submitted_begin.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_ts": submitted_end.strftime("%Y-%m-%d %H:%M:%S"),
                },
                {
                    "invoice_id": invoice_id,
                    "vendor_id": vendor_id,
                    "status_cd": "APPROVED",
                    "invoice_total_amt": f"{approved_amount:.2f}",
                    "due_dt": due_dt.isoformat(),
                    "updated_by": "manager",
                    "begin_ts": approved_begin.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_ts": approved_end.strftime("%Y-%m-%d %H:%M:%S"),
                },
                {
                    "invoice_id": invoice_id,
                    "vendor_id": vendor_id,
                    "status_cd": "POSTED",
                    "invoice_total_amt": f"{posted_amount:.2f}",
                    "due_dt": due_dt.isoformat(),
                    "updated_by": "ap_batch",
                    "begin_ts": posted_begin.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_ts": posted_end.strftime("%Y-%m-%d %H:%M:%S"),
                },
                {
                    "invoice_id": invoice_id,
                    "vendor_id": vendor_id,
                    "status_cd": "PAID" if idx % 6 != 0 else "CANCELED",
                    "invoice_total_amt": f"{final_amount:.2f}",
                    "due_dt": due_dt.isoformat(),
                    "updated_by": "treasury" if idx % 6 != 0 else "ap_manager",
                    "begin_ts": final_begin.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_ts": SENTINEL_END_TS,
                },
            ]
        )

    return rows


def write_csv(rows: list[dict[str, str | int | float]], output_path: Path) -> None:
    fieldnames = [
        "invoice_id",
        "vendor_id",
        "status_cd",
        "invoice_total_amt",
        "due_dt",
        "updated_by",
        "begin_ts",
        "end_ts",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    rows = build_header_rows(args.target_row_count)

    output_path = Path(__file__).resolve().parent / "data" / "invoice_header_jn.csv"
    write_csv(rows, output_path)

    print(f"Generated {len(rows)} header rows -> {output_path}")


if __name__ == "__main__":
    main()
