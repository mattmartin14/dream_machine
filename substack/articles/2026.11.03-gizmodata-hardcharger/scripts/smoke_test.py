#!/usr/bin/env python3
"""Check ADBC connectivity to the Hardcharger cluster."""

from __future__ import annotations

import logging

from hardcharger_demo.config import Settings
from hardcharger_demo.connection import connect

LOGGER = logging.getLogger(__name__)


def main() -> int:
    settings = Settings.from_env()
    settings.require_etl()
    with connect(settings) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT 42")
            answer = cursor.fetchone()[0]
            if answer != 42:
                raise RuntimeError("Hardcharger connectivity check returned an unexpected result")
            cursor.execute("SELECT current_database()")
            current_catalog = cursor.fetchone()[0]
        finally:
            cursor.close()
    print(f"Smoke test passed: Hardcharger query succeeded; session catalog={current_catalog!r}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
