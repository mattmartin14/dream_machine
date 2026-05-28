from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb


def print_table(title: str, columns: list[str], rows: list[tuple]) -> None:
    print(f"\n{title}")
    print("-" * len(title))

    if not rows:
        print("(no rows)")
        return

    widths = [len(col) for col in columns]
    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(str(value)))

    header = " | ".join(col.ljust(widths[idx]) for idx, col in enumerate(columns))
    separator = "-+-".join("-" * width for width in widths)
    print(header)
    print(separator)

    for row in rows:
        print(" | ".join(str(value).ljust(widths[idx]) for idx, value in enumerate(row)))


def as_of_snapshot(conn: duckdb.DuckDBPyConnection, invoice_id: int, as_of_ts: str) -> None:
    rows = conn.execute(
        """
        select
            h.invoice_id,
            h.status_cd,
            h.invoice_total_amt,
            h.begin_ts as header_begin_ts,
            h.end_ts as header_end_ts,
            d.line_nbr,
            d.item_cd,
            d.qty,
            d.unit_price,
            d.line_amt
        from invoice_header_jn h
        left join invoice_detail_jn d
            on d.invoice_id = h.invoice_id
                     and cast(? as timestamp) between d.begin_ts and d.end_ts
        where h.invoice_id = ?
                    and cast(? as timestamp) between h.begin_ts and h.end_ts
        order by d.line_nbr
        """,
        [as_of_ts, invoice_id, as_of_ts],
    ).fetchall()

    columns = [
        "invoice_id",
        "status_cd",
        "invoice_total_amt",
        "header_begin_ts",
        "header_end_ts",
        "line_nbr",
        "item_cd",
        "qty",
        "unit_price",
        "line_amt",
    ]
    print_table(f"Invoice Snapshot at {as_of_ts}", columns, rows)


def main() -> None:
    demo_dir = Path(__file__).resolve().parent
    header_csv = demo_dir / "data" / "invoice_header_jn.csv"
    detail_csv = demo_dir / "data" / "invoice_detail_jn.csv"

    if not header_csv.exists() or not detail_csv.exists():
        raise FileNotFoundError(
            "Expected CSV files were not found. Run both generator scripts first."
        )

    conn = duckdb.connect()

    header_csv_sql = str(header_csv).replace("'", "''")
    detail_csv_sql = str(detail_csv).replace("'", "''")

    conn.execute(
        f"""
        create or replace view invoice_header_jn as
        select
            invoice_id::bigint as invoice_id,
            vendor_id::bigint as vendor_id,
            status_cd,
            invoice_total_amt::decimal(12,2) as invoice_total_amt,
            due_dt::date as due_dt,
            updated_by,
            begin_ts::timestamp as begin_ts,
            end_ts::timestamp as end_ts
        from read_csv_auto('{header_csv_sql}', header=true)
        """
    )

    conn.execute(
        f"""
        create or replace view invoice_detail_jn as
        select
            invoice_id::bigint as invoice_id,
            line_nbr::integer as line_nbr,
            item_cd,
            qty::integer as qty,
            unit_price::decimal(12,2) as unit_price,
            line_amt::decimal(12,2) as line_amt,
            begin_ts::timestamp as begin_ts,
            end_ts::timestamp as end_ts
        from read_csv_auto('{detail_csv_sql}', header=true)
        """
    )

    invoice_id = conn.execute(
        "select min(invoice_id) from invoice_header_jn"
    ).fetchone()[0]

    timeline_rows = conn.execute(
        """
        select
            invoice_id,
            status_cd,
            invoice_total_amt,
            begin_ts,
            end_ts,
            updated_by
        from invoice_header_jn
        where invoice_id = ?
        order by begin_ts
        """,
        [invoice_id],
    ).fetchall()

    timeline_cols = [
        "invoice_id",
        "status_cd",
        "invoice_total_amt",
        "begin_ts",
        "end_ts",
        "updated_by",
    ]
    print_table(f"Invoice History Timeline (invoice_id={invoice_id})", timeline_cols, timeline_rows)

    first_change_ts = timeline_rows[1][3] if len(timeline_rows) > 1 else timeline_rows[0][3]
    latest_ts = timeline_rows[-1][3]

    as_of_1 = first_change_ts.strftime("%Y-%m-%d %H:%M:%S")
    as_of_2 = latest_ts.strftime("%Y-%m-%d %H:%M:%S")

    as_of_snapshot(conn, invoice_id, as_of_1)
    as_of_snapshot(conn, invoice_id, as_of_2)

    overlap_count = conn.execute(
        """
        select count(*)
        from invoice_header_jn a
        join invoice_header_jn b
          on a.invoice_id = b.invoice_id
         and a.begin_ts <= b.end_ts
         and b.begin_ts <= a.end_ts
         and (a.begin_ts, a.end_ts) <> (b.begin_ts, b.end_ts)
        """
    ).fetchone()[0]

    print(f"\nHeader overlap check rows: {overlap_count}")
    if overlap_count != 0:
        print("Warning: overlapping header windows detected.")


if __name__ == "__main__":
    main()
