


-- Aggregate the orders data to a temp duckdb table
CREATE TABLE tmp_orders_agg AS
    SELECT o_orderkey, sum(o_totalprice) as total_price, count(*) as order_count
    FROM read_csv_auto('s3://' || getenv('BUCKET') || '/tpch/orders.csv')
    GROUP BY o_orderkey;

CREATE SCHEMA IF NOT EXISTS iceberg_cat.demo_ns;

DROP TABLE IF EXISTS iceberg_cat.demo_ns.orders_agg;

CREATE TABLE iceberg_cat.demo_ns.orders_agg AS
SELECT * FROM tmp_orders_agg ORDER BY o_orderkey LIMIT 5;
.print 'iceberg orders table created'

DROP TABLE IF EXISTS stg_orders_agg;
CREATE TABLE stg_orders_agg AS SELECT * FROM tmp_orders_agg ORDER BY o_orderkey DESC LIMIT 10;

.print 'testing merge';
MERGE INTO iceberg_cat.demo_ns.orders_agg AS target
    USING stg_orders_agg AS source
        ON target.o_orderkey = source.o_orderkey
    WHEN MATCHED THEN UPDATE 
    WHEN NOT MATCHED THEN INSERT
;

.print 'testing delete';
DELETE FROM iceberg_cat.demo_ns.orders_agg WHERE o_orderkey < 50;

.print 'testing update';
UPDATE iceberg_cat.demo_ns.orders_agg SET total_price = total_price * 1.1 WHERE o_orderkey between 50 and 75;

.print 'testing insert';
INSERT INTO iceberg_cat.demo_ns.orders_agg SELECT * FROM stg_orders_agg limit 10;

SELECT COUNT(*) FROM iceberg_cat.demo_ns.orders_agg;



