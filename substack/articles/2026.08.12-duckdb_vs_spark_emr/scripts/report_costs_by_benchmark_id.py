#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
from pathlib import Path
from typing import Any


def run_aws_json(args: list[str]) -> dict[str, Any]:
    try:
        completed = subprocess.run(
            ["aws", "--no-cli-pager", *args],
            capture_output=True,
            text=True,
            check=True,
        )
        return {"ok": True, "data": json.loads(completed.stdout)}
    except subprocess.CalledProcessError as exc:
        return {
            "ok": False,
            "error": exc.stderr.strip() or exc.stdout.strip() or str(exc),
            "args": args,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Cost Explorer costs per BenchmarkId for archived benchmark runs.")
    parser.add_argument("--root", default="build/benchmark", help="Benchmark output root directory.")
    parser.add_argument(
        "--run-ids",
        nargs="*",
        default=None,
        help="Specific benchmark IDs to report. If omitted, latest --latest IDs from registry are used.",
    )
    parser.add_argument("--latest", type=int, default=3, help="Latest N benchmark IDs from registry when --run-ids is omitted.")
    parser.add_argument("--start-date", default=None, help="Cost Explorer start date YYYY-MM-DD (inclusive).")
    parser.add_argument("--end-date", default=None, help="Cost Explorer end date YYYY-MM-DD (exclusive).")
    parser.add_argument("--output", default="build/benchmark/benchmark_cost_report.json", help="Output report path.")
    return parser.parse_args()


def load_registry_ids(registry_file: Path, latest: int) -> list[str]:
    if not registry_file.exists():
        return []

    rows = []
    for line in registry_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    ids = [x.get("benchmark_id") for x in rows if x.get("benchmark_id")]
    return ids[-latest:] if latest > 0 else ids


def default_time_period() -> tuple[str, str]:
    today = dt.date.today()
    start = today - dt.timedelta(days=3)
    end = today + dt.timedelta(days=1)
    return start.isoformat(), end.isoformat()


def aggregate_total_usd(payload: dict[str, Any]) -> float:
    amount = 0.0
    for row in payload.get("ResultsByTime", []):
        groups = row.get("Groups", [])
        for group in groups:
            metrics = group.get("Metrics", {})
            unblended = metrics.get("UnblendedCost", {})
            amount += float(unblended.get("Amount", "0") or "0")
    return round(amount, 8)


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    registry_file = root / "run_registry.jsonl"
    run_ids = args.run_ids if args.run_ids else load_registry_ids(registry_file, args.latest)
    run_ids = [x for x in run_ids if x]

    if not run_ids:
        raise SystemExit("no benchmark IDs found. provide --run-ids or create runs via scripts/run_benchmark_batch.sh")

    start_date, end_date = default_time_period()
    if args.start_date:
        start_date = args.start_date
    if args.end_date:
        end_date = args.end_date

    runs = []
    for run_id in run_ids:
        result = run_aws_json(
            [
                "ce",
                "get-cost-and-usage",
                "--time-period",
                f"Start={start_date},End={end_date}",
                "--granularity",
                "DAILY",
                "--metrics",
                "UnblendedCost",
                "--filter",
                json.dumps(
                    {
                        "Tags": {
                            "Key": "BenchmarkId",
                            "Values": [run_id],
                            "MatchOptions": ["EQUALS"],
                        }
                    }
                ),
                "--group-by",
                "Type=DIMENSION,Key=SERVICE",
            ]
        )

        run_entry: dict[str, Any] = {
            "benchmark_id": run_id,
            "query": {
                "start_date": start_date,
                "end_date": end_date,
            },
            "ce": result,
            "total_unblended_cost_usd": None,
        }

        if result.get("ok"):
            total = aggregate_total_usd(result["data"])
            run_entry["total_unblended_cost_usd"] = total

        runs.append(run_entry)

    grand_total = round(
        sum(float(x.get("total_unblended_cost_usd") or 0.0) for x in runs),
        8,
    )

    report = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "run_ids": run_ids,
        "time_period": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "grand_total_unblended_cost_usd": grand_total,
        "runs": runs,
        "notes": [
            "Cost Explorer data may lag by 24-48h or longer.",
            "Tag activation is forward-looking and may not backfill old charges.",
        ],
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
