ATTACH 'tpch.duckdb' as tpch;

CREATE SCHEMA IF NOT EXISTS iceberg_cat.demo_ns;

DROP TABLE IF EXISTS iceberg_cat.demo_ns.orders;
CREATE TABLE iceberg_cat.demo_ns.orders AS SELECT * FROM tpch.orders ORDER BY o_orderkey LIMIT 100;
.print 'iceberg orders table created'

DROP TABLE IF EXISTS stg_orders;
CREATE TABLE stg_orders AS SELECT * FROM tpch.orders ORDER BY o_orderkey DESC LIMIT 100;

.print 'testing merge';
MERGE INTO iceberg_cat.demo_ns.orders AS target
USING stg_orders AS source
ON target.o_orderkey = source.o_orderkey
    WHEN MATCHED THEN UPDATE 
    WHEN NOT MATCHED THEN INSERT
;

.print 'testing delete';
DELETE FROM iceberg_cat.demo_ns.orders WHERE o_orderkey < 50;

.print 'testing update';
UPDATE iceberg_cat.demo_ns.orders SET o_orderstatus = 'F' WHERE o_orderkey between 50 and 75;

.print 'testing insert';
INSERT INTO iceberg_cat.demo_ns.orders SELECT * FROM stg_orders limit 10;

SELECT COUNT(*) FROM iceberg_cat.demo_ns.orders;



