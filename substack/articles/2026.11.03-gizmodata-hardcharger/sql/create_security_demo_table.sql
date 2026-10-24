-- One-time materialization of the small synthetic TPC-H customer table so
-- documented Hardcharger masking policies can be applied to a regular table.
-- Replace <RAW_BUCKET> and ensure the managed server has documented S3 access.
CREATE TABLE IF NOT EXISTS analytics.main.customer_security_demo AS
SELECT c_custkey, c_name, c_address, c_phone
FROM read_parquet('s3://<RAW_BUCKET>/tpch/customer/*.parquet');
