INSTALL httpfs;
LOAD httpfs;

INSTALL iceberg;
LOAD iceberg;

CREATE OR REPLACE SECRET minio_secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'minio',
    SECRET 'minio12345',
    REGION 'us-east-1',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false
);

ATTACH 'demo' AS lk (
    TYPE iceberg,
    ENDPOINT 'http://localhost:8181/catalog',
    AUTHORIZATION_TYPE 'none',
    ACCESS_DELEGATION_MODE 'none'
);

ATTACH 'tpch.duckdb' as tpch;


CREATE SCHEMA IF NOT EXISTS lk.demo_ns;

DROP TABLE IF EXISTS lk.demo_ns.orders;
CREATE TABLE lk.demo_ns.orders AS SELECT * FROM tpch.orders ORDER BY o_orderkey LIMIT 100;
.print 'iceberg orders table created'

DROP TABLE IF EXISTS stg_orders;
CREATE TABLE stg_orders AS SELECT * FROM tpch.orders ORDER BY o_orderkey DESC LIMIT 100;

.print 'testing merge';
MERGE INTO lk.demo_ns.orders AS target
USING stg_orders AS source
ON target.o_orderkey = source.o_orderkey
    WHEN MATCHED THEN UPDATE 
    WHEN NOT MATCHED THEN INSERT
;

.print 'testing delete';
DELETE FROM lk.demo_ns.orders WHERE o_orderkey < 50;

.print 'testing update';
UPDATE lk.demo_ns.orders SET o_orderstatus = 'F' WHERE o_orderkey between 50 and 75;

.print 'testing insert';
INSERT INTO lk.demo_ns.orders SELECT * FROM stg_orders limit 10;

SELECT COUNT(*) FROM lk.demo_ns.orders;



