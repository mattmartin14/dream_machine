.shell rm -f -- "tpch.duckdb"
.shell rm -f -- "tpch.duckdb-wal"

.open tpch.duckdb
install tpch; load tpch;
call dbgen(sf=0.001);