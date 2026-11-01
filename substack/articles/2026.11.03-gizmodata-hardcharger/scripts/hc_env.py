"""Locate the Hardcharger cluster URI and admin secret when env vars are not set."""

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_cluster_defaults() -> None:
    """Fill HARDCHARGER_URI from the saved cluster metadata and the secret ARN from Terraform."""
    metadata = ROOT / "hardcharger" / "cluster-config.env"
    if not os.environ.get("HARDCHARGER_URI") and metadata.is_file():
        for line in metadata.read_text().splitlines():
            key, _, value = line.partition("=")
            if key == "HARDCHARGER_URI" and value:
                os.environ["HARDCHARGER_URI"] = value.strip().strip("'\"")
    if not os.environ.get("HARDCHARGER_SECRET_ARN") and not os.environ.get("HARDCHARGER_PASSWORD"):
        result = subprocess.run(
            [
                "terraform",
                f"-chdir={ROOT / 'terraform'}",
                "output",
                "-raw",
                "hardcharger_secret_arn",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.startswith("arn:"):
            os.environ["HARDCHARGER_SECRET_ARN"] = result.stdout.strip()
