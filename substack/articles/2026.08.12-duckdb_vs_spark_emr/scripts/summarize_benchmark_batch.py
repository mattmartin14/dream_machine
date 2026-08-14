#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import statistics
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize multiple benchmark runs from archived artifacts.")
    parser.add_argument("--root", default="build/benchmark", help="Benchmark output root directory.")
    parser.add_argument(
        "--run-ids",
        nargs="*",
        default=None,
        help="Specific benchmark IDs to include. If omitted, latest --latest run IDs from registry are used.",
    )
    parser.add_argument("--latest", type=int, default=3, help="Number of latest run IDs to use when --run-ids is omitted.")
    parser.add_argument(
        "--output",
        default="build/benchmark/benchmark_batch_summary.json",
        help="Output path for the batch summary JSON.",
    )
    return parser.parse_args()


def load_registry_ids(registry_file: Path, latest: int) -> list[str]:
    if not registry_file.exists():
        return []

    records = []
    for line in registry_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    ids = [r.get("benchmark_id") for r in records if r.get("benchmark_id")]
    if latest <= 0:
        return ids
    return ids[-latest:]


def safe_mean(values: list[float]) -> float | None:
    return round(statistics.mean(values), 6) if values else None


def safe_stdev(values: list[float]) -> float | None:
    return round(statistics.pstdev(values), 6) if values else None


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    runs_dir = root / "runs"
    registry_file = root / "run_registry.jsonl"

    run_ids = args.run_ids if args.run_ids else load_registry_ids(registry_file, args.latest)
    run_ids = [rid for rid in run_ids if rid]

    if not run_ids:
        raise SystemExit("no benchmark IDs found. provide --run-ids or create runs via scripts/run_benchmark_batch.sh")

    run_summaries: list[dict[str, Any]] = []
    missing: list[str] = []

    for run_id in run_ids:
        summary_path = runs_dir / run_id / "benchmark_summary.json"
        if not summary_path.exists():
            missing.append(run_id)
            continue
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
        run_summaries.append(payload)

    if not run_summaries:
        raise SystemExit("none of the requested run IDs has archived benchmark_summary.json")

    duckdb_times = [float(x["engines"]["duckdb"]["in_job_elapsed_seconds"]) for x in run_summaries]
    spark_times = [float(x["engines"]["spark"]["in_job_elapsed_seconds"]) for x in run_summaries]
    ratios = [float(x["comparison"]["spark_vs_duckdb_in_job_ratio"]) for x in run_summaries if x["comparison"]["spark_vs_duckdb_in_job_ratio"] is not None]

    per_run = []
    for x in run_summaries:
        per_run.append(
            {
                "benchmark_id": x["benchmark_id"],
                "run_date": x["run_date"],
                "duckdb_elapsed_seconds": x["engines"]["duckdb"]["in_job_elapsed_seconds"],
                "spark_elapsed_seconds": x["engines"]["spark"]["in_job_elapsed_seconds"],
                "spark_vs_duckdb_ratio": x["comparison"]["spark_vs_duckdb_in_job_ratio"],
                "duckdb_control_plane_seconds": x["engines"]["duckdb"]["control_plane"].get("control_plane_wall_seconds"),
                "spark_control_plane_seconds": x["engines"]["spark"]["control_plane"].get("control_plane_wall_seconds"),
                "spark_billed_vcpu_hours": ((x["engines"]["spark"].get("runtime_diagnostics") or {}).get("billed_resource_utilization") or {}).get("vCPUHour"),
                "spark_billed_memory_gb_hours": ((x["engines"]["spark"].get("runtime_diagnostics") or {}).get("billed_resource_utilization") or {}).get("memoryGBHour"),
            }
        )

    out = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "requested_run_ids": run_ids,
        "missing_run_ids": missing,
        "included_run_count": len(run_summaries),
        "aggregate": {
            "duckdb_elapsed_seconds": {
                "avg": safe_mean(duckdb_times),
                "min": round(min(duckdb_times), 6),
                "max": round(max(duckdb_times), 6),
                "stdev": safe_stdev(duckdb_times),
            },
            "spark_elapsed_seconds": {
                "avg": safe_mean(spark_times),
                "min": round(min(spark_times), 6),
                "max": round(max(spark_times), 6),
                "stdev": safe_stdev(spark_times),
            },
            "spark_vs_duckdb_ratio": {
                "avg": safe_mean(ratios),
                "min": round(min(ratios), 6) if ratios else None,
                "max": round(max(ratios), 6) if ratios else None,
                "stdev": safe_stdev(ratios),
            },
        },
        "runs": per_run,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
