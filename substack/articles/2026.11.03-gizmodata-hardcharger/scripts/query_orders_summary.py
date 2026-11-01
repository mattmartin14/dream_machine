#!/usr/bin/env python3
"""Connect to Hardcharger and show 5 rows from order_stats.main.orders_summary.

Uses the read-only user from sql/readonly_demo/01_create_readonly_user.sql: set
HC_READONLY_USERNAME and HC_READONLY_PASSWORD. HARDCHARGER_URI is read from
hardcharger/cluster-config.env unless already set.
An optional SQL string argument replaces the default query.
"""

import os
import sys

from adbc_driver_hardcharger import dbapi
from hc_env import load_cluster_defaults

DEFAULT_SQL = "SELECT * FROM order_stats.main.orders_summary LIMIT 5"


def main() -> None:
    load_cluster_defaults()
    missing = [k for k in ("HC_READONLY_USERNAME", "HC_READONLY_PASSWORD") if k not in os.environ]
    if missing:
        sys.exit(f"Missing {', '.join(missing)}. Run 'source ~/.zshrc' or open a new terminal.")
    sql = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SQL
    with dbapi.connect(
        uri=os.environ["HARDCHARGER_URI"],
        username=os.environ["HC_READONLY_USERNAME"],
        password=os.environ["HC_READONLY_PASSWORD"],
    ) as conn:
        print(f"Connected as {conn.current_user()}")
        with conn.cursor() as cur:
            try:
                cur.execute(sql)
            except dbapi.ProgrammingError as error:
                print(f"Refused: {error}")
                return
            print([col[0] for col in cur.description])
            for row in cur.fetchall():
                print(row)


if __name__ == "__main__":
    main()
