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


def default_run_date() -> str:
    return date.today().isoformat()


def ensure_local_artifact_dirs(base_dir: str = "artifacts") -> Path:
    base = Path(base_dir)
    (base / "metrics").mkdir(parents=True, exist_ok=True)
    (base / "event_logs").mkdir(parents=True, exist_ok=True)
    return base
