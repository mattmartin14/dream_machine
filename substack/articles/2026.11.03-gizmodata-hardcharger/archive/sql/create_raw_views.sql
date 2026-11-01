-- Replace <RAW_BUCKET> with the deployed general-purpose S3 bucket name and run as
-- an administrator only after the Hardcharger service has documented S3 access.
-- Keep the bucket private; grant the verified service principal only s3:ListBucket
-- (tpch/*) and s3:GetObject on that prefix.
CREATE OR REPLACE VIEW main.tpch_customer AS
SELECT * FROM read_parquet('s3://<RAW_BUCKET>/tpch/customer/*.parquet');

CREATE OR REPLACE VIEW main.tpch_orders AS
SELECT * FROM read_parquet('s3://<RAW_BUCKET>/tpch/orders/*.parquet');

CREATE OR REPLACE VIEW main.tpch_lineitem AS
SELECT * FROM read_parquet('s3://<RAW_BUCKET>/tpch/lineitem/*.parquet');

CREATE OR REPLACE VIEW main.tpch_nation AS
SELECT * FROM read_parquet('s3://<RAW_BUCKET>/tpch/nation/*.parquet');

CREATE OR REPLACE VIEW main.tpch_region AS
SELECT * FROM read_parquet('s3://<RAW_BUCKET>/tpch/region/*.parquet');
