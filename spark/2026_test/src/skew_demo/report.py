from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare baseline vs optimized Spark metrics")
    parser.add_argument("--baseline", default="artifacts/metrics/baseline_metrics.json")
    parser.add_argument("--optimized", default="artifacts/metrics/optimized_metrics.json")
    parser.add_argument("--out", default="artifacts/metrics/comparison_report.json")
    return parser.parse_args()


def load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Missing metrics file: {path}")
    return json.loads(p.read_text(encoding="utf-8"))


def safe_ratio(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return a / b


def main() -> None:
    args = parse_args()
    baseline = load_json(args.baseline)
    optimized = load_json(args.optimized)

    baseline_total = baseline["timing_seconds"]["total"]
    optimized_total = optimized["timing_seconds"]["total"]
    speedup = safe_ratio(baseline_total, optimized_total)

    baseline_skew = baseline["task_metrics"].get("max_over_median")
    optimized_skew = optimized["task_metrics"].get("max_over_median")
    skew_reduction = None
    if baseline_skew is not None and optimized_skew is not None:
        skew_reduction = baseline_skew - optimized_skew

    report = {
        "baseline_metrics": args.baseline,
        "optimized_metrics": args.optimized,
        "runtime_seconds": {
            "baseline_total": baseline_total,
            "optimized_total": optimized_total,
            "speedup_factor": speedup,
        },
        "task_skew": {
            "baseline_max_over_median": baseline_skew,
            "optimized_max_over_median": optimized_skew,
            "absolute_reduction": skew_reduction,
        },
        "partition_skew": {
            "baseline_largest_over_median": baseline["partition_stats"].get("largest_over_median"),
            "optimized_largest_over_median": optimized["partition_stats"].get("largest_over_median"),
        },
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))
    print(f"Wrote comparison report: {args.out}")


if __name__ == "__main__":
    main()
