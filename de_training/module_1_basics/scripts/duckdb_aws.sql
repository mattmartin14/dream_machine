INSTALL AWS; LOAD AWS;

CREATE OR REPLACE SECRET aws_creds (TYPE S3, PROVIDER CREDENTIAL_CHAIN);

CREATE OR REPLACE TABLE sales 
AS
SELECT 1 as id, '2024-01-01'::DATE as sale_date, 100.0 as amount
UNION ALL
SELECT 2, '2024-01-02', 150.0
UNION ALL
SELECT 3, '2024-01-03', 200.0;

.print 'creating local files'
COPY sales to 'sales.csv';
COPY sales to 'sales.parquet';

.print 'pushing files to s3'
.shell aws s3 cp sales.csv s3://$AWS_BUCKET/sales.csv
.shell aws s3 cp sales.parquet s3://$AWS_BUCKET/sales.parquet

-- duckdb cli currently does not support an expression in its copy command; you'd have to hard code the bucke name
--COPY sales TO concat('s3://', getenv('AWS_BUCKET'), '/sales.csv');

.print 'cleaning up local files'
.shell rm sales.csv
.shell rm sales.parquet

.print 'reading back from s3 as a csv file'
SELECT * FROM read_csv_auto(concat('s3://', getenv('AWS_BUCKET'), '/sales.csv'));

.print 'reading back from s3 as a parquet file'
SELECT * FROM read_parquet(concat('s3://', getenv('AWS_BUCKET'), '/sales.parquet'));

