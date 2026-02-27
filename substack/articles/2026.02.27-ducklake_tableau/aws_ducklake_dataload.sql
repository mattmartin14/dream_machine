CREATE OR REPLACE SECRET aws_creds (TYPE S3, PROVIDER credential_chain);

ATTACH OR REPLACE 'ducklake:aws_wh.ducklake' AS aws_wh (DATA_PATH concat('s3://', getenv('aws_bucket'), '/ducklake_tableau'));

CREATE OR REPLACE TABLE aws_wh.order_hdr AS
SELECT *
FROM read_csv('order_header.csv');

CREATE OR REPLACE TABLE aws_wh.order_dtl AS
SELECT *
FROM read_csv('order_detail.csv');
