#!/usr/bin/env python3
"""Run a .sql file against Hardcharger as the administrator, one statement at a time.

Access-control statements must be sent individually. Credentials come from the same
environment as the ETL (HARDCHARGER_SECRET_ARN, or URI/USERNAME/PASSWORD). The
${READONLY_USERNAME} and ${READONLY_PASSWORD} come from HC_READONLY_USERNAME and
HC_READONLY_PASSWORD.
"""

import os
import re
import sys
from pathlib import Path

from adbc_driver_hardcharger import dbapi
from hc_env import load_cluster_defaults

from hardcharger_demo.config import Settings


def statements(text: str) -> list[str]:
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("--")]
    return [part.strip() for part in "\n".join(lines).split(";") if part.strip()]


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} FILE.sql", file=sys.stderr)
        return 2
    load_cluster_defaults()
    text = Path(sys.argv[1]).read_text()
    quoted_password = None
    if "${READONLY_USERNAME}" in text:
        readonly_username = os.environ.get("HC_READONLY_USERNAME", "")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", readonly_username):
            print("Set HC_READONLY_USERNAME (letters, digits, underscores).", file=sys.stderr)
            return 2
        text = text.replace("${READONLY_USERNAME}", readonly_username)
    if "${READONLY_PASSWORD}" in text:
        readonly_password = os.environ.get("HC_READONLY_PASSWORD")
        if not readonly_password:
            print("Set HC_READONLY_PASSWORD first.", file=sys.stderr)
            return 2
        quoted_password = readonly_password.replace("'", "''")
        text = text.replace("${READONLY_PASSWORD}", quoted_password)

    uri, username, password = Settings.from_env().connection_credentials()
    with dbapi.connect(uri=uri, username=username, password=password) as conn:
        print(f"Connected as {conn.current_user()}")
        with conn.cursor() as cur:
            for sql in statements(text):
                shown = " ".join(sql.split())
                if quoted_password:
                    shown = shown.replace(quoted_password, "********")
                print(f"\n> {shown}")
                cur.execute(sql)
                if cur.description:
                    for row in cur.fetchall():
                        print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
