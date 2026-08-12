from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path


DEFAULT_BUCKET = "matt-sbx-bucket-1-us-east-1"
DEFAULT_ROOT_PREFIX = "etl-skew-demo"
DEFAULT_DATASET_PREFIX = "raw/returns_chat"


@dataclass(frozen=True)
class DemoPaths:
    bucket: str
    root_prefix: str
    dataset_prefix: str
    run_date: str

    @property
    def raw_prefix(self) -> str:
        return f"{self.root_prefix}/{self.dataset_prefix}/run_date={self.run_date}".strip("/")

    @property
    def normalized_prefix(self) -> str:
        return f"{self.root_prefix}/normalized/returns_chat/run_date={self.run_date}".strip("/")

    @property
    def manifest_prefix(self) -> str:
        return f"{self.root_prefix}/manifests/run_date={self.run_date}".strip("/")

    def benchmark_prefix(self, benchmark_id: str) -> str:
        return f"{self.root_prefix}/results/run_date={self.run_date}/benchmark_id={benchmark_id}".strip("/")

    def engine_prefix(self, benchmark_id: str, engine: str) -> str:
        return f"{self.benchmark_prefix(benchmark_id)}/engine={engine}".strip("/")

    def input_glob(self) -> str:
        return f"s3://{self.bucket}/{self.raw_prefix}/*/chat_*.json"


def default_run_date() -> str:
    return date.today().isoformat()


def ensure_local_artifact_dirs(base_dir: str = "artifacts") -> Path:
    base = Path(base_dir)
    (base / "metrics").mkdir(parents=True, exist_ok=True)
    (base / "event_logs").mkdir(parents=True, exist_ok=True)
    return base