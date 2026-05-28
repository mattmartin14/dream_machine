# Journaled Invoice Tables in DuckDB

This demo shows a classic header/detail invoice model with journal windows so you can reconstruct how an invoice looked at a specific point in time.

## Design Pattern

Each journal row has:
- `begin_ts`
- `end_ts`

This example uses:
- Closed interval filtering: `begin_ts <= as_of_ts <= end_ts`
- Sentinel for current rows: `9999-12-31 23:59:59`

## Files

- `data_modeling/journaled_invoice_demo/generate_invoice_header_csv.py`
- `data_modeling/journaled_invoice_demo/generate_invoice_detail_csv.py`
- `data_modeling/journaled_invoice_demo/invoice_duckdb_asof_demo.py`
- `data_modeling/journaled_invoice_demo/data/invoice_header_jn.csv`
- `data_modeling/journaled_invoice_demo/data/invoice_detail_jn.csv`

## Script Interfaces

Simple by design:

1. Header generator
- Optional arg: `target_row_count`
- Default: `1000`

2. Detail generator
- Optional arg: `target_row_count`
- Default: `1000`

3. DuckDB query script
- No required args

## Run Order

From repo root:

```bash
python data_modeling/journaled_invoice_demo/generate_invoice_header_csv.py
python data_modeling/journaled_invoice_demo/generate_invoice_detail_csv.py
python data_modeling/journaled_invoice_demo/invoice_duckdb_asof_demo.py
```

Optional smaller sample:

```bash
python data_modeling/journaled_invoice_demo/generate_invoice_header_csv.py 100
python data_modeling/journaled_invoice_demo/generate_invoice_detail_csv.py 100
```

## Core As-Of SQL Pattern

Header row at a timestamp:

```sql
select *
from invoice_header_jn
where invoice_id = 100000
	and timestamp '2026-01-01 18:00:00' between begin_ts and end_ts;
```

Header + active detail lines at a timestamp:

```sql
select
		h.invoice_id,
		h.status_cd,
		h.invoice_total_amt,
		d.line_nbr,
		d.item_cd,
		d.qty,
		d.unit_price,
		d.line_amt
from invoice_header_jn h
left join invoice_detail_jn d
	on d.invoice_id = h.invoice_id
 and timestamp '2026-01-01 18:00:00' between d.begin_ts and d.end_ts
where h.invoice_id = 100000
	and timestamp '2026-01-01 18:00:00' between h.begin_ts and h.end_ts
order by d.line_nbr;
```

## What This Demonstrates

- Invoice lifecycle in header history (`DRAFT`, `SUBMITTED`, `APPROVED`, `POSTED`, `PAID` or `CANCELED`)
- Point-in-time reconstruction from journal windows
- A reusable temporal predicate pattern for future models

## Notes

- Keep timestamp precision and timezone strategy consistent across ingestion/querying.
- For closed intervals, avoid overlap by starting the next row after the previous `end_ts`.
