INSTALL TPCH; LOAD TPCH;

CALL DBGEN(sf=.001);

-- The runner substitutes __BUCKET__ at execution time.
COPY orders TO 's3://__BUCKET__/tpch/orders.csv' (FORMAT CSV, HEADER true);