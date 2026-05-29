from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path

SENTINEL_END_TS = "9999-12-31 23:59:59"
DEFAULT_ROW_COUNT = 1000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate invoice detail journal CSV with begin/end timestamps."
    )
    parser.add_argument(
        "target_row_count",
        nargs="?",
        type=int,
        default=DEFAULT_ROW_COUNT,
        help="Approximate number of rows to generate (default: 1000).",
    )
    return parser.parse_args()


def load_invoice_starts(header_path: Path) -> list[tuple[int, str]]:
    starts_by_invoice: dict[int, datetime] = {}

    with header_path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            invoice_id = int(row["invoice_id"])
            begin_ts = datetime.strptime(row["begin_ts"], "%Y-%m-%d %H:%M:%S")
            existing = starts_by_invoice.get(invoice_id)
            if existing is None or begin_ts < existing:
                starts_by_invoice[invoice_id] = begin_ts

    return [
        (invoice_id, start_ts.strftime("%Y-%m-%d %H:%M:%S"))
        for invoice_id, start_ts in sorted(starts_by_invoice.items())
    ]


def build_detail_rows(
    invoice_starts: list[tuple[int, str]], target_row_count: int
) -> list[dict[str, str | int | float]]:
    if target_row_count <= 0:
        raise ValueError("target_row_count must be greater than 0")

    if not invoice_starts:
        raise ValueError("No invoice headers found to build detail rows.")

    rows: list[dict[str, str | int | float]] = []
    invoice_count = len(invoice_starts)
    base_lines_per_invoice = target_row_count // invoice_count
    extra_lines = target_row_count % invoice_count

    line_counter = 0
    for invoice_idx, (invoice_id, begin_ts) in enumerate(invoice_starts):
        line_count = base_lines_per_invoice + (1 if invoice_idx < extra_lines else 0)

        for line_nbr in range(1, line_count + 1):
            qty = 1 + (line_counter % 4)
            unit_price = round(10 + ((line_counter * 7) % 95) + 0.99, 2)
            line_amt = round(qty * unit_price, 2)

            rows.append(
                {
                    "invoice_id": invoice_id,
                    "line_nbr": line_nbr,
                    "item_cd": f"SKU-{(line_counter % 130):03d}",
                    "qty": qty,
                    "unit_price": f"{unit_price:.2f}",
                    "line_amt": f"{line_amt:.2f}",
                    "begin_ts": begin_ts,
                    "end_ts": SENTINEL_END_TS,
                }
            )
            line_counter += 1

    return rows


def write_csv(rows: list[dict[str, str | int | float]], output_path: Path) -> None:
    fieldnames = [
        "invoice_id",
        "line_nbr",
        "item_cd",
        "qty",
        "unit_price",
        "line_amt",
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

    demo_dir = Path(__file__).resolve().parent
    header_path = demo_dir / "data" / "invoice_header_jn.csv"
    if not header_path.exists():
        raise FileNotFoundError(
            "Header CSV not found. Run generate_invoice_header_csv.py first."
        )

    invoice_starts = load_invoice_starts(header_path)
    rows = build_detail_rows(invoice_starts, args.target_row_count)

    output_path = demo_dir / "data" / "invoice_detail_jn.csv"
    write_csv(rows, output_path)

    print(f"Generated {len(rows)} detail rows -> {output_path}")


if __name__ == "__main__":
    main()
