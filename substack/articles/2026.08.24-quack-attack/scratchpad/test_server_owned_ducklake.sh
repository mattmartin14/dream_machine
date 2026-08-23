#!/usr/bin/env bash
# Tests whether Quack clients inherit a DuckLake ATTACH created in the server init session.
set -euo pipefail

port="${1:-9495}"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/quack-server-owned-ducklake.XXXXXX")"
server_pid=""

cleanup() {
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill "$server_pid"
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

cat >"$work_dir/server.sql" <<SQL
LOAD ducklake;
LOAD quack;
ATTACH 'ducklake:$work_dir/metadata.ducklake' AS dl1 (DATA_PATH '$work_dir/data');
CREATE TABLE dl1.server_seed (id INTEGER);
INSERT INTO dl1.server_seed VALUES (42);
CALL quack_serve('quack:localhost:$port', token => 'local-test-token');
SQL

echo "Starting Quack with DuckLake attached only in its init session."
sleep 86400 | duckdb -init "$work_dir/server.sql" "$work_dir/quack_catalog.duckdb" >"$work_dir/server.log" 2>&1 &
server_pid=$!

for _ in {1..30}; do
  if lsof -ti "tcp:$port" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! lsof -ti "tcp:$port" >/dev/null 2>&1; then
  echo "Quack did not start. Server log:"
  cat "$work_dir/server.log"
  exit 1
fi

echo
echo "Client attaches only Quack; it does not load DuckLake or declare DATA_PATH."
set +e
duckdb :memory: <<SQL
LOAD quack;
ATTACH 'quack:localhost:$port' AS remote (TOKEN 'local-test-token');
SELECT * FROM remote.query('INSERT INTO dl1.server_seed VALUES (99)');
SELECT * FROM remote.query('SELECT * FROM dl1.server_seed');
SQL
query_exit_code=$?

echo
echo "Testing direct remote catalog access."
duckdb :memory: <<SQL
LOAD quack;
ATTACH 'quack:localhost:$port' AS remote (TOKEN 'local-test-token');
SELECT * FROM remote.dl1.server_seed;
SQL
direct_exit_code=$?
set -e

echo
if [[ "$query_exit_code" -eq 0 ]]; then
  echo "RESULT: remote.query(...) can use the server-owned DuckLake catalog."
else
  echo "RESULT: remote.query(...) could not use the server-owned DuckLake catalog."
fi

if [[ "$direct_exit_code" -eq 0 ]]; then
  echo "RESULT: Direct remote.dl1.table access also works."
else
  echo "RESULT: Direct remote.dl1.table access does not work; use remote.query(...) instead."
fi

exit "$query_exit_code"