INSTALL TPCH; LOAD TPCH;

CALL DBGEN(sf=.001);

-- Use AWS credential chain for prod S3 write.
CREATE OR REPLACE SECRET (
    TYPE s3,
    PROVIDER credential_chain
);

COPY orders TO 's3://matt-sbx-bucket-1-us-east-1/tpch/orders.csv' (FORMAT CSV, HEADER true);


-- MinIO Dev
-- Switch active S3 credentials/endpoint to MinIO for local write.
CREATE OR REPLACE SECRET (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'minio',
    SECRET 'minio12345',
    REGION 'us-east-1',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false
);

COPY orders TO 's3://warehouse-rest/tpch/orders.csv' (FORMAT CSV, HEADER true);